import base64
import csv
import sqlite3
import requests
import os
import platform
import re
import threading
import random
import time
import string
import psutil
import socket
import subprocess
import json
import shutil
import docker
import logging
from requests.auth import HTTPBasicAuth
from logging.handlers import RotatingFileHandler
from statistics import mean
from threading import Lock
from tzlocal import get_localzone
from flask_login import (
    LoginManager,
    UserMixin,
    login_user,
    logout_user,
    login_required,
    current_user,
)
from flask import (
    Flask,
    abort,
    make_response,
    render_template,
    send_file,
    url_for,
    redirect,
    request,
    jsonify,
    session,
)
from src.forms import LoginForm
from src.config import Config
from src.tg_bot.audit import log_action, get_logs, get_logs_count
from flask_bcrypt import Bcrypt
from datetime import date, datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from zoneinfo._common import ZoneInfoNotFoundError
from collections import OrderedDict, defaultdict
from typing import List, Dict, Optional, Tuple
from cryptography import x509
from cryptography.hazmat.backends import default_backend
import sys

# =============================================================================
# НАСТРОЙКА ЛОГИРОВАНИЯ (БЕЗ КОНСОЛИ, РАЗДЕЛЕНИЕ ПО УРОВНЯМ)
# =============================================================================

class LevelFilter(logging.Filter):
    def __init__(self, min_level, max_level=None):
        super().__init__()
        self.min_level = min_level
        self.max_level = max_level if max_level is not None else min_level

    def filter(self, record):
        return self.min_level <= record.levelno <= self.max_level


LOG_DIR = Config.LOGS_PATH
os.makedirs(LOG_DIR, exist_ok=True)
STDOUT_LOG = os.path.join(LOG_DIR, "main.stdout.log")
STDERR_LOG = os.path.join(LOG_DIR, "main.stderr.log")
MAX_LOG_SIZE = 10 * 1024 * 1024  # 10 MB
BACKUP_COUNT = 5

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False

if logger.handlers:
    logger.handlers.clear()

formatter = logging.Formatter(
    "%(asctime)s | %(levelname)-8s | %(threadName)-12s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# DEBUG и INFO -> main.stdout.log
stdout_handler = RotatingFileHandler(
    STDOUT_LOG, maxBytes=MAX_LOG_SIZE, backupCount=BACKUP_COUNT, encoding="utf-8", delay=True
)
stdout_handler.setLevel(logging.DEBUG)
stdout_handler.addFilter(LevelFilter(logging.DEBUG, logging.INFO))
stdout_handler.setFormatter(formatter)
# logger.addHandler(stdout_handler)  # Закомментировано, если нужен только stderr для ошибок

# WARNING и выше -> main.stderr.log
stderr_handler = RotatingFileHandler(
    STDERR_LOG, maxBytes=MAX_LOG_SIZE, backupCount=BACKUP_COUNT, encoding="utf-8", delay=True
)
stderr_handler.setLevel(logging.WARNING)
stderr_handler.addFilter(LevelFilter(logging.WARNING, logging.CRITICAL))
stderr_handler.setFormatter(formatter)
logger.addHandler(stderr_handler)

# =============================================================================
# MIDDLEWARE & APP INIT
# =============================================================================

class ScriptNameMiddleware:
    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        script_name = environ.get("HTTP_X_SCRIPT_NAME", "")
        if script_name:
            script_name = script_name.rstrip("/")
            environ["SCRIPT_NAME"] = script_name
            path_info = environ.get("PATH_INFO", "")
            if path_info.startswith(script_name):
                new_path = path_info[len(script_name) :]
                environ["PATH_INFO"] = new_path if new_path else "/"
        return self.app(environ, start_response)


app = Flask(__name__)
app.config.from_object(Config)
app.wsgi_app = ScriptNameMiddleware(app.wsgi_app)

DOCKER_HUB_REPO = "devils0411/openvpn-status"
DOCKER_HUB_API = f"https://hub.docker.com/v2/repositories/{DOCKER_HUB_REPO}/tags/"
bcrypt = Bcrypt(app)
loginManager = LoginManager(app)
loginManager.login_view = "login"

LOG_FILES = Config.LOG_FILES
CLIENT_SH_PATH = Config.CLIENT_SH
OPENVPN_CONFIG_PATHS = Config.OVPN_CLIENTS_DIR

# Глобальные переменные кэширования и метрик
cached_system_info = None
last_fetch_time = 0
CACHE_DURATION = 10
cpu_history = []
ram_history = []
MAX_CPU_HISTORY = 60 * 12
DB_SAVE_INTERVAL = 300
last_db_save = 0
SAMPLE_INTERVAL = 10
MAX_HISTORY_SECONDS = 7 * 24 * 3600
LIVE_POINTS = 60
last_collect = 0

ovpn_live_stats = defaultdict(lambda: {"rx_speed": [], "tx_speed": [], "timestamps": []})
MAX_OVPN_LIVE_POINTS = 60 * 12
OVPN_DB_SAVE_INTERVAL = 300
ovpn_last_db_save = 0
ovpn_stats_lock = Lock()
ovpn_last_bytes = {}

BOT_RESTART_LOCK = Lock()
BOT_SERVICE_NAME = "telegram-bot"
ENV_PATH = Config.ENV_PATH
SETTINGS_PATH = Config.SETTINGS_PATH
LEGACY_ADMIN_INFO_PATH = Config.LEGACY_ADMIN_INFO_PATH
CLIENT_MAPPING_KEY = "CLIENT_MAPPING"


# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================

def clean_client_display_name(client_name, server_ip):
    """Возвращает безопасное отображаемое имя клиента."""
    if not client_name:
        return "Клиент"
    
    hide_ovpn_ip = read_settings().get("hide_ovpn_ip", True)
    if hide_ovpn_ip and server_ip and server_ip in client_name:
        client_name = client_name.replace(server_ip, "").strip()
    
    client_name = re.sub(r"\s+", " ", client_name).strip()
    return client_name if client_name else "Клиент"


def _host_static_info():
    os_label = ""
    try:
        rel = platform.freedesktop_os_release()
        os_label = rel.get("PRETTY_NAME") or f"{rel.get('NAME', 'Linux')} {rel.get('VERSION', '')}".strip()
    except (OSError, AttributeError):
        os_label = f"{platform.system()} {platform.release()}".strip()
    
    physical = psutil.cpu_count(logical=False)
    logical = psutil.cpu_count(logical=True)
    cpu_cores = physical if physical else (logical or 1)
    return {"os_label": os_label, "cpu_cores": cpu_cores}


HOST_STATIC_INFO = _host_static_info()


def read_env_values():
    values = {}
    try:
        with open(ENV_PATH, "r", encoding="utf-8") as env_file:
            for line in env_file:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip()
        logger.debug(f"Прочитано {len(values)} переменных из .env")
    except FileNotFoundError:
        logger.warning(f"Файл .env не найден: {ENV_PATH}")
    except Exception as e:
        logger.error(f"Ошибка чтения .env файла: {e}")
    return values


def can_start_bot(env_values=None):
    if env_values is None:
        env_values = read_env_values()
    bot_token = (env_values.get("BOT_TOKEN") or "").strip()
    admin_id = (env_values.get("ADMIN_ID") or "").strip()
    return bool(bot_token) and bool(parse_admin_ids(admin_id))


def update_env_values(updates):
    updates = {key: value for key, value in updates.items() if key}
    if not updates:
        logger.debug("Нет обновлений для .env файла")
        return

    updated_keys = set()
    lines = []
    try:
        with open(ENV_PATH, "r", encoding="utf-8") as env_file:
            lines = env_file.readlines()
    except FileNotFoundError:
        logger.warning(f"Файл .env не найден, создаю новый: {ENV_PATH}")
        lines = []
    except Exception as e:
        logger.error(f"Ошибка чтения .env файла: {e}")
        lines = []

    new_lines = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            new_lines.append(line)
            continue
        key, _ = line.split("=", 1)
        key = key.strip()
        if key in updates:
            new_lines.append(f"{key}={updates[key]}\n")
            updated_keys.add(key)
        else:
            new_lines.append(line)

    for key, value in updates.items():
        if key not in updated_keys:
            new_lines.append(f"{key}={value}\n")

    try:
        with open(ENV_PATH, "w", encoding="utf-8") as env_file:
            env_file.writelines(new_lines)
        logger.info(f"Обновлены ключи в .env: {list(updates.keys())}")
    except Exception as e:
        logger.error(f"Ошибка записи в .env файл: {e}")


DEFAULT_SETTINGS = {
    "app_name": "OpenVPN-Status",
    "telegram_admins": {},
    "bot_enabled": False,
    "show_ovpn_menu": True,
    "show_wg_menu": True,
    "hide_ovpn_ip": True,
    "hide_wg_ip": True,
    "stats_retention_days": 365,
    "history_max_records": 1000,
}

MONTH_OPTIONS_RU = [
    (1, "Январь"), (2, "Февраль"), (3, "Март"), (4, "Апрель"),
    (5, "Май"), (6, "Июнь"), (7, "Июль"), (8, "Август"),
    (9, "Сентябрь"), (10, "Октябрь"), (11, "Ноябрь"), (12, "Декабрь"),
]


def write_settings_data(settings_data):
    try:
        with open(SETTINGS_PATH, "w", encoding="utf-8") as settings_file:
            json.dump(settings_data, settings_file, ensure_ascii=False, indent=4)
            settings_file.write("\n")
        logger.debug(f"Настройки сохранены: {SETTINGS_PATH}")
    except Exception as e:
        logger.error(f"Ошибка сохранения настроек: {e}")


def read_settings():
    try:
        with open(SETTINGS_PATH, "r", encoding="utf-8") as settings_file:
            data = json.load(settings_file)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.warning(f"Файл настроек не найден или ошибка парсинга: {e}")
        data = {}
    except Exception as e:
        logger.error(f"Неожиданная ошибка загрузки настроек: {e}")
        data = {}

    if not isinstance(data, dict):
        logger.warning("Данные настроек не являются словарём, инициализирую пустой dict")
        data = {}

    merged = DEFAULT_SETTINGS.copy()
    merged.update(data)

    if not os.path.exists(SETTINGS_PATH):
        write_settings_data(merged)
        logger.info(f"Файл настроек создан: {SETTINGS_PATH}")

    telegram_admins = merged.get("telegram_admins")
    if not isinstance(telegram_admins, dict):
        telegram_admins = {}
        merged["telegram_admins"] = telegram_admins

    if not telegram_admins and os.path.exists(LEGACY_ADMIN_INFO_PATH):
        try:
            with open(LEGACY_ADMIN_INFO_PATH, "r", encoding="utf-8") as legacy_file:
                legacy_data = json.load(legacy_file)
            if isinstance(legacy_data, dict):
                merged["telegram_admins"] = legacy_data
                write_settings_data(merged)
        except (FileNotFoundError, json.JSONDecodeError):
            pass

    return merged


def write_settings(updated_settings):
    current_settings = read_settings()
    current_settings.update(updated_settings)
    write_settings_data(current_settings)


def parse_stats_retention_days(raw_value):
    try:
        days = int(str(raw_value).strip())
    except (TypeError, ValueError):
        return 365
    return max(30, min(days, 3650))


def get_stats_retention_days():
    return parse_stats_retention_days(read_settings().get("stats_retention_days", 365))


def parse_history_max_records(raw_value):
    try:
        value = int(str(raw_value).strip())
    except (TypeError, ValueError):
        return 1000
    return max(100, min(value, 100000))


def get_available_stat_years(db_path, table_name, date_column):
    years = []
    try:
        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT DISTINCT substr({date_column}, 1, 4) AS y
                FROM {table_name}
                WHERE substr({date_column}, 1, 4) GLOB '[0-9][0-9][0-9][0-9]'
                ORDER BY y DESC
                """
            ).fetchall()
            years = [int(row[0]) for row in rows if row and row[0].isdigit()]
    except sqlite3.Error:
        years = []
    
    current_year = datetime.now().year
    if current_year not in years:
        years.append(current_year)
    years = sorted(set(years), reverse=True)
    return years


def parse_date_yyyy_mm_dd(raw_value):
    value = (raw_value or "").strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return None


def resolve_client_timezone():
    tz_name = (request.args.get("tz") or "").strip()
    if tz_name:
        try:
            return ZoneInfo(tz_name), tz_name
        except ZoneInfoNotFoundError:
            pass
    server_tz = get_localzone()
    server_tz_name = getattr(server_tz, "key", None) or str(server_tz)
    return server_tz, server_tz_name


def _floor_to_hour(dt_value):
    return dt_value.replace(minute=0, second=0, microsecond=0)


def _ceil_to_hour(dt_value):
    floored = _floor_to_hour(dt_value)
    if dt_value == floored:
        return floored
    return floored + timedelta(hours=1)


def get_server_hour_window_for_client_day(day_ymd, client_tz):
    server_tz = get_localzone()
    day_dt = datetime.strptime(day_ymd, "%Y-%m-%d")
    start_client = day_dt.replace(tzinfo=client_tz)
    end_client = start_client + timedelta(days=1)
    start_server = _floor_to_hour(start_client.astimezone(server_tz))
    end_server = _ceil_to_hour(end_client.astimezone(server_tz))
    return (
        start_server.strftime("%Y-%m-%d %H:00"),
        end_server.strftime("%Y-%m-%d %H:00"),
    )


def read_admin_info():
    data = read_settings().get("telegram_admins", {})
    if not isinstance(data, dict):
        return {}
    return data


def parse_admin_ids(admin_id_value):
    admin_ids = []
    for item in admin_id_value.split(","):
        item = item.strip()
        if not item:
            continue
        admin_ids.append(item)
    return admin_ids


def format_admin_ids(admin_ids):
    return ",".join(admin_ids)


def format_admin_display(admin_id, admin_info):
    info = admin_info.get(admin_id, {})
    display_name = (info.get("display_name") or "").strip()
    username = (info.get("username") or "").strip()
    if display_name and username:
        return f"{display_name} (@{username})"
    if display_name:
        return display_name
    if username:
        return f"@{username}"
    return f"ID: {admin_id}"


def build_admin_display_list(admin_id_value, admin_info):
    admin_ids = parse_admin_ids(admin_id_value)
    return [{"id": admin_id, "display": format_admin_display(admin_id, admin_info)} for admin_id in admin_ids]


def parse_client_mapping(env_values):
    raw_value = (env_values.get(CLIENT_MAPPING_KEY) or "").strip()
    if not raw_value:
        return {}
    mapping = {}
    for item in raw_value.split(","):
        item = item.strip()
        if not item or ":" not in item:
            continue
        telegram_id, client_name = item.split(":", 1)
        telegram_id = telegram_id.strip()
        client_name = client_name.strip()
        if not telegram_id or not client_name:
            continue
        mapping[telegram_id] = client_name
    return mapping


def build_client_mapping_list(env_values, admin_info):
    mapping = parse_client_mapping(env_values)
    mapping_list = []
    for telegram_id, client_name in mapping.items():
        display = format_admin_display(telegram_id, admin_info)
        mapping_list.append({"telegram_id": telegram_id, "display": display, "client_name": client_name})
    mapping_list.sort(key=lambda item: item["client_name"].lower())
    return mapping_list


def build_available_admin_candidates(admin_info, admin_ids):
    available = []
    admin_id_set = set(admin_ids)
    for admin_id in admin_info.keys():
        if admin_id in admin_id_set:
            continue
        available.append({"id": admin_id, "display": format_admin_display(admin_id, admin_info)})
    available.sort(key=lambda item: item["display"].lower())
    return available


# =============================================================================
# TELEGRAM BOT MANAGEMENT
# =============================================================================

def restart_telegram_bot_async():
    with BOT_RESTART_LOCK:
        try:
            result = subprocess.run(
                ["supervisorctl", "restart", BOT_SERVICE_NAME],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if result.returncode == 0:
                logger.info("Бот telegram-bot успешно перезапущен")
                return True, None
            else:
                error_msg = result.stderr.strip() or result.stdout.strip() or "неизвестная ошибка"
                logger.error(f"Ошибка перезапуска бота: {error_msg}")
                return False, error_msg
        except Exception as exc:
            logger.error(f"Исключение при перезапуске бота: {exc}")
            return False, str(exc) or "неизвестная ошибка"


def restart_telegram_bot():
    thread = threading.Thread(target=restart_telegram_bot_async)
    thread.daemon = True
    thread.start()
    logger.info("Запущен асинхронный перезапуск бота")
    return True, None


def stop_telegram_bot():
    with BOT_RESTART_LOCK:
        try:
            result = subprocess.run(
                ["supervisorctl", "stop", BOT_SERVICE_NAME],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if result.returncode == 0:
                logger.info("Бот telegram-bot успешно остановлен")
                return True, None
            else:
                error_msg = result.stderr.strip() or result.stdout.strip() or "неизвестная ошибка"
                logger.error(f"Ошибка остановки бота: {error_msg}")
                return False, error_msg
        except Exception as exc:
            logger.error(f"Исключение при остановке бота: {exc}")
            return False, str(exc) or "неизвестная ошибка"


def get_telegram_bot_status():
    try:
        result = subprocess.run(
            ["supervisorctl", "status", BOT_SERVICE_NAME],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        status = result.stdout.strip().upper()
        if "RUNNING" in status or "STARTING" in status:
            logger.debug("Бот telegram-bot активен")
            return True
        logger.debug("Бот telegram-bot не активен")
        return False
    except Exception as e:
        logger.error(f"Ошибка проверки статуса бота: {e}")
        return False


# =============================================================================
# DATABASE & USERS
# =============================================================================

def get_db_connection():
    conn = sqlite3.connect(app.config["DATABASE_PATH"])
    conn.row_factory = sqlite3.Row
    return conn


def create_users_table():
    conn = get_db_connection()
    conn.execute(
        """CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            role TEXT NOT NULL,
            password TEXT NOT NULL
        )"""
    )
    conn.commit()
    conn.close()


create_users_table()


@loginManager.user_loader
def load_user(user_id):
    conn = get_db_connection()
    user = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    conn.close()
    if user:
        return User(
            user_id=user["id"],
            username=user["username"],
            role=user["role"],
            password=user["password"],
        )
    return None


class User(UserMixin):
    def __init__(self, user_id, username, role, password):
        self.id = user_id
        self.username = username
        self.role = role
        self.password = password


def add_user(username, role, password):
    conn = get_db_connection()
    existing_user = conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
    if existing_user:
        logger.info(f"Пользователь {username} уже существует.")
        conn.close()
        return
    hashed_password = bcrypt.generate_password_hash(password).decode("utf-8")
    conn.execute(
        "INSERT INTO users (username, role, password) VALUES (?, ?, ?)",
        (username, role, hashed_password),
    )
    conn.commit()
    conn.close()
    logger.info(f"Пользователь {username} успешно добавлен")


def get_random_pass(length=10):
    characters = string.ascii_letters + string.digits
    return "".join(random.choice(characters) for _ in range(length))


def add_admin():
    conn = get_db_connection()
    passw = get_random_pass()
    count = conn.execute("SELECT COUNT(*) FROM users WHERE role = 'admin'").fetchone()[0]
    if count < 1:
        add_user("admin", "admin", passw)
        logger.info("Создан администратор. Пароль сгенерирован.")
        print(f"{passw}")
    else:
        logger.debug("Администратор уже существует")
    conn.close()
    return passw


def change_admin_password():
    conn = get_db_connection()
    admin_user = conn.execute("SELECT * FROM users WHERE role = 'admin'").fetchone()
    if not admin_user:
        logger.warning("Администратор не найден.")
        conn.close()
        return
    passw = get_random_pass()
    hashed_password = bcrypt.generate_password_hash(passw).decode("utf-8")
    conn.execute(
        "UPDATE users SET password = ? WHERE username = ? AND role = 'admin'",
        (hashed_password, "admin"),
    )
    conn.commit()
    conn.close()
    print(f"{passw}")
    logger.info("Пароль администратора изменён")


def change_admin_password_2(new_password):
    if not new_password:
        logger.warning("Новый пароль не может быть пустым.")
        return
    conn = get_db_connection()
    admin_user = conn.execute("SELECT * FROM users WHERE role = 'admin'").fetchone()
    if not admin_user:
        logger.warning("Администратор не найден.")
        conn.close()
        return
    hashed_password = bcrypt.generate_password_hash(new_password).decode("utf-8")
    conn.execute(
        "UPDATE users SET password = ? WHERE username = ? AND role = 'admin'",
        (hashed_password, "admin"),
    )
    conn.commit()
    conn.close()
    print(f"Пароль администратора успешно изменён: {new_password}")
    logger.info("Пароль администратора успешно изменён")


# =============================================================================
# AMNEZIA WG API & HEALTH CHECK
# =============================================================================

_API_HEALTH = {
    "available": True,
    "last_check": 0,
    "fail_count": 0,
    "lock": threading.Lock(),
    "COOLDOWN": 30,
    "MAX_FAILS": 3,
    "RETRY_AFTER": 120,
}


class AmneziaHealthChecker:
    @staticmethod
    def is_api_available() -> bool:
        with _API_HEALTH["lock"]:
            now = time.time()
            if not _API_HEALTH["available"]:
                if now - _API_HEALTH["last_check"] < _API_HEALTH["RETRY_AFTER"]:
                    return False
                _API_HEALTH["fail_count"] = 0
            if now - _API_HEALTH["last_check"] < _API_HEALTH["COOLDOWN"]:
                return _API_HEALTH["available"]
            return True

    @staticmethod
    def record_success():
        with _API_HEALTH["lock"]:
            if not _API_HEALTH["available"]:
                logger.info("API Amnezia восстановил доступность")
            _API_HEALTH["available"] = True
            _API_HEALTH["fail_count"] = 0
            _API_HEALTH["last_check"] = time.time()

    @staticmethod
    def record_failure():
        with _API_HEALTH["lock"]:
            _API_HEALTH["fail_count"] += 1
            _API_HEALTH["last_check"] = time.time()
            if _API_HEALTH["fail_count"] >= _API_HEALTH["MAX_FAILS"] and _API_HEALTH["available"]:
                _API_HEALTH["available"] = False
                logger.warning(f"API Amnezia помечен как недоступный (ошибок: {_API_HEALTH['fail_count']})")


class AmneziaDiscoverer:
    @staticmethod
    def get_connection_info():
        try:
            client = docker.from_env()
            for container in client.containers.list():
                if "amnezia" in container.name.lower():
                    nets = container.attrs.get("NetworkSettings", {}).get("Networks", {})
                    ip = next((net.get("IPAddress") for net in nets.values() if net.get("IPAddress")), None)
                    if not ip:
                        continue
                    password = port = None
                    for env in container.attrs.get("Config", {}).get("Env", []):
                        if env.startswith("WIREGUARD_PASSWORD="):
                            password = env.split("=", 1)[1].strip()
                        elif env.startswith("PORT="):
                            port = env.split("=", 1)[1].strip()
                    if ip and password:
                        return ip, password, port or "8080"
        except Exception as e:
            logger.error(f"Ошибка обнаружения контейнера Amnezia: {e}")
        return None


class AmneziaApiSyncClient:
    def __init__(self, base_url: str, password: str, username: str = "admin"):
        self.base_url = base_url.rstrip("/")
        self.password = password
        self.username = username
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(username, password)

    def login(self) -> None:
        resp = self.session.get(f"{self.base_url}/api/client")
        resp.raise_for_status()
        logger.debug("✅ Успешная аутентификация в AmneziaWG Easy")
    
    def get_clients(self) -> List[Dict]:
        resp = self.session.get(
            f"{self.base_url}/api/client",
        )
        resp.raise_for_status()
        clients = resp.json()
        return clients if clients is not None else [] 

def _fetch_wg_api_data():
    if not AmneziaHealthChecker.is_api_available():
        return None
    conn_info = AmneziaDiscoverer.get_connection_info()
    if not conn_info:
        AmneziaHealthChecker.record_failure()
        return None

    ip, password, port = conn_info
    try:
        api = AmneziaApiSyncClient(f"http://{ip}:{port}", password)
        api.login()
        clients = api.get_clients()
        if not clients:
            logger.warning("⚠️ API вернул пустой список клиентов или None")
            return None
        daily_map = get_daily_stats_map()
        peers = []

        for c in clients:
            peer_key = c.get("publicKey") or c.get("id", "")
            name = c.get("name", "N/A")
            address = c.get("address", "N/A")
            enabled = c.get("enabled", True)
            rx = int(c.get("transferRx") or c.get("transfer_rx", 0))
            tx = int(c.get("transferTx") or c.get("transfer_tx", 0))
            last_hs = c.get("latestHandshakeAt")
            raw_endpoint = c.get("endpoint") or ""
            
            endpoint_ip = raw_endpoint.rsplit(":", 1)[0] if ":" in raw_endpoint else raw_endpoint.strip()
            if not endpoint_ip:
                endpoint_ip = "N/A"
            
            online = False
            if last_hs:
                try:
                    ts = last_hs.replace("Z", "+00:00")
                    hs_dt = datetime.fromisoformat(ts)
                    if (datetime.now(timezone.utc) - hs_dt) < timedelta(minutes=3):
                        online = True
                except Exception:
                    pass

            daily = daily_map.get((peer_key, "wg0"))
            daily_rx = daily["received"] if daily else 0
            daily_tx = daily["sent"] if daily else 0
            total = rx + tx
            daily_total = daily_rx + daily_tx

            peers.append(
                {
                    "peer": peer_key,
                    "masked_peer": peer_key[:4] + "..." + peer_key[-4:] if len(peer_key) > 8 else peer_key,
                    "client": name,
                    "assigned_address": address,
                    "endpoint": endpoint_ip,
                    "allowed_ips": [address] if address != "N/A" else [],
                    "visible_ips": [address] if address != "N/A" else [],
                    "hidden_ips": [],
                    "latest_handshake": "Now" if online else "Никогда",
                    "online": online,
                    "received": humanize_bytes(rx),
                    "sent": humanize_bytes(tx),
                    "received_bytes": rx,
                    "sent_bytes": tx,
                    "daily_received": humanize_bytes(daily_rx),
                    "daily_sent": humanize_bytes(daily_tx),
                    "daily_traffic_percentage": round((daily_total / total * 100)) if total > 0 else 0,
                    "received_percentage": round((rx / total * 100), 2) if total > 0 else 0,
                    "sent_percentage": round((tx / total * 100), 2) if total > 0 else 0,
                    "enabled": enabled,
                    "blocked": not enabled,
                }
            )

        AmneziaHealthChecker.record_success()
        return [{"interface": "wg0", "public_key": "N/A", "listening_port": "N/A", "peers": peers}]

    except requests.exceptions.ConnectionError as e:
        logger.debug(f"API Amnezia недоступен (сеть): {e}")
        AmneziaHealthChecker.record_failure()
    except requests.exceptions.Timeout as e:
        logger.debug(f"API Amnezia таймаут: {e}")
        AmneziaHealthChecker.record_failure()
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP ошибка API Amnezia: {e}")
        AmneziaHealthChecker.record_failure()
    except Exception as e:
        logger.error(f"Ошибка API Amnezia WG: {e}")
        AmneziaHealthChecker.record_failure()
    return None


def get_wireguard_stats():
    if not AmneziaHealthChecker.is_api_available():
        return None
    return _fetch_wg_api_data()


def get_disabled_wg_peers():
    """Получает отключённых пиров через API Amnezia WireGuard."""
    result = {}
    try:
        if not AmneziaHealthChecker.is_api_available():
            return result
        
        conn_info = AmneziaDiscoverer.get_connection_info()
        if not conn_info:
            return result
            
        ip, password, port = conn_info
        api = AmneziaApiSyncClient(f"http://{ip}:{port}", password)
        api.login()
        clients = api.get_clients()

        disabled_peers = []
        for c in clients:
            if not c.get("enabled", True):
                peer_key = c.get("publicKey") or c.get("id", "")
                name = c.get("name", "N/A")
                address = c.get("address", "N/A")
                masked = peer_key[:4] + "..." + peer_key[-4:] if len(peer_key) > 8 else peer_key
                allowed_ips = [address] if address and address != "N/A" else []

                disabled_peers.append(
                    {
                        "peer": peer_key,
                        "masked_peer": masked,
                        "client": name,
                        "enabled": False,
                        "online": False,
                        "blocked": True,
                        "endpoint": "N/A",
                        "visible_ips": allowed_ips[:1],
                        "hidden_ips": allowed_ips[1:],
                        "latest_handshake": "Никогда",
                        "daily_received": "0 B",
                        "daily_sent": "0 B",
                        "received": "0 B",
                        "sent": "0 B",
                        "received_bytes": 0,
                        "sent_bytes": 0,
                        "daily_traffic_percentage": 0,
                        "received_percentage": 0,
                        "sent_percentage": 0,
                        "allowed_ips": allowed_ips,
                        "assigned_address": address,
                    }
                )

        if disabled_peers:
            result["wg0"] = disabled_peers
    except Exception as e:
        logger.warning(f"Ошибка получения отключённых пиров через API: {e}")
    return result


def format_handshake_time(handshake_string):
    time_units = re.findall(r"(\d+)\s+(\w+)", handshake_string)
    abbreviations = {
        "year": "г.", "years": "г.", "month": "мес.", "months": "мес.",
        "week": "нед.", "weeks": "нед.", "day": "дн.", "days": "дн.",
        "hour": "ч.", "hours": "ч.", "minute": "мин.", "minutes": "мин.",
        "second": "сек.", "seconds": "сек.",
    }
    return " ".join(f"{value} {abbreviations[unit]}" for value, unit in time_units)


def is_peer_online(last_handshake):
    if not last_handshake:
        return False
    return datetime.now() - last_handshake < timedelta(minutes=3)


def parse_relative_time(relative_time):
    now = datetime.now()
    time_deltas = {"days": 0, "hours": 0, "minutes": 0, "seconds": 0}
    parts = relative_time.split()
    i = 0
    while i < len(parts):
        try:
            value = int(parts[i])
            unit = parts[i + 1]
            if "д" in unit or "day" in unit:
                time_deltas["days"] += value
            elif "ч" in unit or "hour" in unit:
                time_deltas["hours"] += value
            elif "мин" in unit or "minute" in unit:
                time_deltas["minutes"] += value
            elif "сек" in unit or "second" in unit:
                time_deltas["seconds"] += value
            i += 2
        except (ValueError, IndexError):
            break
    delta = timedelta(**time_deltas)
    return now - delta


def read_wg_config(file_path):
    client_mapping = {}
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)
            data = {k.strip(): v for k, v in data.items()}
            clients = data.get("clients", {})
            for client_id, client_info in clients.items():
                clean_info = {k.strip(): v.strip() if isinstance(v, str) else v for k, v in client_info.items()}
                public_key = clean_info.get("publicKey", "").strip()
                name = clean_info.get("name", "N/A").strip()
                address = clean_info.get("address", "N/A").strip()
                if public_key:
                    client_mapping[public_key] = {"name": name, "address": address}
    except FileNotFoundError:
        logger.warning(f"Конфигурационный файл {file_path} не найден.")
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка парсинга JSON {file_path}: {e}")
    except Exception as e:
        logger.error(f"Ошибка чтения конфига WireGuard {file_path}: {e}")
    return client_mapping


def get_daily_stats_map():
    today = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(app.config["WG_STATS_PATH"])
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM wg_daily_stats WHERE date = ?", (today,))
    rows = cursor.fetchall()
    conn.close()
    return {(row["peer"], row["interface"]): row for row in rows}


def humanize_bytes(num, suffix="B"):
    for unit in ["", "K", "M", "G", "T"]:
        if abs(num) < 1024.0:
            return f"{num:.1f} {unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f} P{suffix}"


def get_daily_stats():
    conn = sqlite3.connect(app.config["WG_STATS_PATH"])
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    date_today = date.today().isoformat()
    cursor.execute("SELECT interface, client, received, sent FROM wg_daily_stats WHERE date = ?", (date_today,))
    rows = cursor.fetchall()
    conn.close()
    stats = {}
    for row in rows:
        iface = row["interface"]
        client = row["client"]
        if iface not in stats:
            stats[iface] = {}
        stats[iface][client] = {"received": row["received"], "sent": row["sent"]}
    return stats


def format_bytes(size):
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"


def parse_bytes(value):
    if not value or not isinstance(value, str):
        return 0
    try:
        parts = value.split()
        if len(parts) != 2:
            return 0
        size = float(parts[0])
        unit = parts[1].lower()
        if unit in ("kb", "kib"):
            return size * 1024
        elif unit in ("mb", "mib"):
            return size * 1024**2
        elif unit in ("gb", "gib"):
            return size * 1024**3
        elif unit in ("tb", "tib"):
            return size * 1024**4
        elif unit in ("pb", "pib"):
            return size * 1024**5
        return size
    except (ValueError, IndexError, TypeError) as e:
        logger.warning(f"Не удалось распарсить размер '{value}': {e}")
        return 0


def pluralize_clients(count):
    if 11 <= count % 100 <= 19:
        return f"{count} клиентов"
    elif count % 10 == 1:
        return f"{count} клиент"
    elif 2 <= count % 10 <= 4:
        return f"{count} клиента"
    else:
        return f"{count} клиентов"


def _ovpn_session_row_key(name, protocol):
    raw = f"{name}\x1f{protocol}".encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def get_external_ip():
    try:
        response = requests.get("https://api.ipify.org", timeout=10)
        if response.status_code == 200:
            return response.text
        logger.warning(f"Не удалось получить внешний IP. Статус: {response.status_code}")
        return "IP не найден"
    except requests.Timeout:
        logger.error("Запрос превысил время ожидания при получении IP.")
        return "Ошибка: запрос превысил время ожидания."
    except requests.ConnectionError:
        logger.error("Нет подключения к интернету при получении IP.")
        return "Ошибка: нет подключения к интернету."
    except requests.RequestException as e:
        logger.error(f"Ошибка при запросе внешнего IP: {e}")
        return f"Ошибка при запросе: {e}"


def format_date(date_string):
    date_obj = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
    server_timezone = get_localzone()
    localized_date = date_obj.replace(tzinfo=server_timezone)
    utc_date = localized_date.astimezone(timezone.utc)
    return utc_date.isoformat()


def extract_protocol_from_address(real_address, config_protocol):
    if not real_address:
        return config_protocol or "unknown"
    if ":" in real_address:
        parts = real_address.split(":", 1)
        if len(parts) == 2 and parts[0].lower() in ["udp4", "tcp4", "udp6", "tcp6"]:
            protocol = parts[0].lower()
            if protocol in ["udp4", "udp6"]:
                return "UDP"
            elif protocol in ["tcp4", "tcp6"]:
                return "TCP"
    return config_protocol or "unknown"


def mask_ip(ip_address, hide=True):
    if not ip_address:
        return "0.0.0.0"
    if ":" in ip_address:
        parts = ip_address.split(":", 1)
        if len(parts) == 2 and parts[0].lower() in ["udp4", "tcp4", "udp6", "tcp6"]:
            ip_address = parts[1]
    ip = ip_address.split(":")[0] if ":" in ip_address else ip_address
    port = ":" + ip_address.split(":")[1] if ":" in ip_address else ""
    parts = ip.split(".")
    if len(parts) == 4:
        try:
            parts = [str(int(part)) for part in parts]
            if hide:
                return f"{parts[0]}.***.***.{parts[3]}{port}"
            return f"{ip}{port}"
        except ValueError:
            return ip_address
    return ip_address


def format_duration(start_time):
    now = datetime.now()
    delta = now - start_time
    days = delta.days
    seconds = delta.seconds
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if days >= 30:
        months = days // 30
        days %= 30
        return f"{months} мес. {days} дн. {hours} ч. {minutes} мин."
    elif days > 0:
        return f"{days} дн. {hours} ч. {minutes} мин."
    elif hours > 0:
        return f"{hours} ч. {minutes} мин."
    elif minutes > 0:
        return f"{minutes} мин."
    else:
        return f"{seconds} сек."


client_cache = defaultdict(lambda: {"received": 0, "sent": 0, "timestamp": None})


def normalize_real_address(addr):
    if addr.startswith(("udp4:", "tcp4:", "tcp4-server:", "udp6:", "tcp6:")):
        addr = addr.split(":", 1)[1]
    if ":" in addr:
        addr = addr.rsplit(":", 1)[0]
    return addr


def read_csv(file_path, config_protocol):
    data = []
    total_received, total_sent = 0, 0
    current_time = datetime.now()
    if not os.path.exists(file_path):
        logger.warning(f"Файл логов не найден: {file_path}")
        return [], 0, 0, None

    with open(file_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        for row in reader:
            if row[0] == "CLIENT_LIST":
                client_name = row[1]
                real_address = normalize_real_address(row[2])
                received = int(row[5])
                sent = int(row[6])
                total_received += received
                total_sent += sent

                start_date = datetime.strptime(row[7], "%Y-%m-%d %H:%M:%S")
                duration = format_duration(start_date)
                protocol = extract_protocol_from_address(real_address, config_protocol)

                previous_data = client_cache.get(client_name, {"received": 0, "sent": 0, "timestamp": current_time})
                previous_received = previous_data["received"]
                previous_sent = previous_data["sent"]
                previous_time = previous_data["timestamp"]

                time_diff = (current_time - previous_time).total_seconds()
                if time_diff >= 30:
                    download_speed = (received - previous_received) / time_diff if received >= previous_received else 0
                    upload_speed = (sent - previous_sent) / time_diff if sent >= previous_sent else 0
                else:
                    download_speed = 0
                    upload_speed = 0

                client_cache[client_name] = {"received": received, "sent": sent, "timestamp": current_time}
                data.append(
                    [
                        client_name,
                        real_address,
                        row[3],
                        format_bytes(received),
                        format_bytes(sent),
                        f"{format_bytes(max(download_speed, 0))}/s",
                        f"{format_bytes(max(upload_speed, 0))}/s",
                        format_date(row[7]),
                        duration,
                        protocol,
                        max(download_speed, 0),
                        max(upload_speed, 0),
                    ]
                )

    logger.debug(f"Прочитано {len(data)} клиентов из {file_path}")
    return data, total_received, total_sent, None


# =============================================================================
# DATABASE MAINTENANCE & METRICS
# =============================================================================

def ensure_db():
    conn = sqlite3.connect(app.config["SYSTEM_STATS_PATH"])
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS system_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,
            cpu_percent REAL,
            ram_percent REAL
        )
        """
    )
    conn.commit()
    conn.close()


STATS_DB_CLEAR_OVPN_PHRASE = "OpenVPN"
STATS_DB_CLEAR_WG_PHRASE = "WireGuard"


def get_ovpn_wg_database_sizes():
    specs = [
        ("ovpn", "OpenVPN", app.config["LOGS_DATABASE_PATH"]),
        ("wg", "WireGuard", app.config["WG_STATS_PATH"]),
    ]
    items = []
    total = 0
    for key, label, path in specs:
        try:
            sz = os.path.getsize(path)
        except OSError:
            sz = 0
        total += sz
        items.append({"key": key, "label": label, "bytes": sz, "size_fmt": format_bytes(sz)})
    return items, total


def _delete_tables_and_vacuum(db_path, tables):
    with sqlite3.connect(db_path) as conn:
        for t in tables:
            try:
                conn.execute(f"DELETE FROM {t}")
            except sqlite3.OperationalError:
                pass
    with sqlite3.connect(db_path) as conn:
        conn.execute("VACUUM")


def clear_openvpn_stats_database():
    try:
        _delete_tables_and_vacuum(
            app.config["LOGS_DATABASE_PATH"],
            ("daily_stats", "monthly_stats", "years_stats", "connection_logs", "last_client_stats"),
        )
        return True, None
    except Exception as e:
        return False, str(e)


def clear_wireguard_stats_database():
    try:
        _delete_tables_and_vacuum(
            app.config["WG_STATS_PATH"],
            ("wg_hourly_stats", "wg_daily_stats", "wg_monthly_stats", "wg_intermediate", "wg_total_stats"),
        )
        return True, None
    except Exception as e:
        return False, str(e)


def save_minute_average_to_db():
    now = datetime.now()
    cutoff = now - timedelta(seconds=DB_SAVE_INTERVAL)
    to_avg = [p for p in cpu_history if p["timestamp"] >= cutoff]
    if not to_avg:
        return

    cpu_avg = mean([p["cpu"] for p in to_avg])
    ram_avg = mean([p["ram"] for p in to_avg])

    try:
        conn = sqlite3.connect(app.config["SYSTEM_STATS_PATH"])
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO system_stats (timestamp, cpu_percent, ram_percent) VALUES (?, ?, ?)",
            (now.strftime("%Y-%m-%d %H:%M:%S"), round(cpu_avg, 3), round(ram_avg, 3)),
        )
        cutoff_db = now - timedelta(days=7)
        cur.execute("DELETE FROM system_stats WHERE timestamp < ?", (cutoff_db.strftime("%Y-%m-%d %H:%M:%S"),))
        conn.commit()
        conn.close()
        logger.debug(f"Сохранены метрики в БД: CPU={cpu_avg:.2f}%, RAM={ram_avg:.2f}%")
    except Exception as e:
        logger.error(f"[DB ERROR] save_minute_average_to_db: {e}")


def group_rows(rows, interval="minute"):
    grouped = {}
    for r in rows:
        ts = r["timestamp"]
        if interval == "minute":
            key = ts.replace(second=0, microsecond=0)
        elif interval == "hour":
            key = ts.replace(minute=0, second=0, microsecond=0)
        elif interval == "day":
            key = ts.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            key = ts

        if key not in grouped:
            grouped[key] = {"cpu": [], "ram": []}
        grouped[key]["cpu"].append(r["cpu"])
        grouped[key]["ram"].append(r["ram"])

    result = []
    for key, values in grouped.items():
        result.append(
            {
                "timestamp": key,
                "cpu": sum(values["cpu"]) / len(values["cpu"]),
                "ram": sum(values["ram"]) / len(values["ram"]),
            }
        )
    return sorted(result, key=lambda x: x["timestamp"])


def resample_to_n(data, n):
    if not data:
        return []
    if len(data) <= n:
        return data
    step = len(data) / n
    out = []
    for i in range(n):
        idx = int(i * step)
        if idx >= len(data):
            idx = len(data) - 1
        out.append(data[idx])
    return out


def get_default_interface():
    try:
        result = subprocess.run(["/usr/bin/ip", "route"], capture_output=True, text=True, check=True)
        for line in result.stdout.splitlines():
            if "default" in line:
                return line.split()[4]
    except Exception as e:
        logger.error(f"Ошибка получения интерфейса: {e}")
    return None


def get_network_stats(interface):
    try:
        with open(f"/sys/class/net/{interface}/statistics/rx_bytes", "r", encoding="utf-8") as f:
            rx_bytes = int(f.read().strip())
        with open(f"/sys/class/net/{interface}/statistics/tx_bytes", "r", encoding="utf-8") as f:
            tx_bytes = int(f.read().strip())
        return {"interface": interface, "rx": rx_bytes, "tx": tx_bytes}
    except FileNotFoundError:
        logger.warning(f"Интерфейс {interface} не найден в /sys/class/net/. Удаляю из базы vnstat...")
        try:
            result = subprocess.run(
                ["/usr/bin/vnstat", "--remove", "-i", interface, "--force"],
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode == 0:
                logger.info(f"Интерфейс {interface} успешно удалён из базы vnstat")
        except Exception as e:
            logger.error(f"Ошибка при вызове vnstat для удаления {interface}: {e}")
        return None


def get_network_load():
    net_io_start = psutil.net_io_counters(pernic=True)
    time.sleep(1)
    net_io_end = psutil.net_io_counters(pernic=True)
    network_data = {}
    vnstat_interfaces = get_vnstat_interfaces()
    alias_map = {iface["name"]: iface["alias"] for iface in vnstat_interfaces}

    for interface in net_io_start:
        if interface not in net_io_end:
            logger.debug(f"Интерфейс {interface} исчез между снимками — пропускаем")
            continue
        alias = alias_map.get(interface, interface)
        is_exception = "amnezia" in alias.lower() or "openvpn-1" in alias.lower()
        if not is_exception and interface.startswith(("lo", "docker", "veth", "br-")):
            continue

        sent_start, recv_start = net_io_start[interface].bytes_sent, net_io_start[interface].bytes_recv
        sent_end, recv_end = net_io_end[interface].bytes_sent, net_io_end[interface].bytes_recv

        sent_speed = (sent_end - sent_start) * 8 / 1e6
        recv_speed = (recv_end - recv_start) * 8 / 1e6

        if sent_speed > 0 or recv_speed > 0:
            network_data[interface] = {"sent_speed": round(sent_speed, 2), "recv_speed": round(recv_speed, 2)}
    return network_data


def get_uptime():
    try:
        return subprocess.check_output("/usr/bin/uptime -p", shell=True).decode().strip()
    except subprocess.CalledProcessError:
        logger.warning("Не удалось получить uptime системы")
        return "Не удалось получить время работы"


def format_uptime(uptime_string):
    pattern = r"(?:(\d+)\syears?|(\d+)\smonths?|(\d+)\sweeks?|(\d+)\sdays?|(\d+)\shours?|(\d+)\sminutes?)"
    years = months = weeks = days = hours = minutes = 0
    for match in re.findall(pattern, uptime_string):
        if match[0]:
            years = int(match[0])
        elif match[1]:
            months = int(match[1])
        elif match[2]:
            weeks = int(match[2])
        elif match[3]:
            days = int(match[3])
        elif match[4]:
            hours = int(match[4])
        elif match[5]:
            minutes = int(match[5])

    result = []
    if years > 0:
        result.append(f"{years} г.")
    if months > 0:
        result.append(f"{months} мес.")
    if weeks > 0:
        result.append(f"{weeks} нед.")
    if days > 0:
        result.append(f"{days} дн.")
    if hours > 0:
        result.append(f"{hours} ч.")
    if minutes > 0:
        result.append(f"{minutes} мин.")
    return " ".join(result)


def count_online_clients(file_paths):
    total_openvpn = 0
    results = {}
    try:
        wg_data = _fetch_wg_api_data()
        results["WireGuard"] = sum(1 for p in wg_data[0]["peers"] if p.get("online")) if wg_data else 0
    except Exception as e:
        logger.debug(f"Ошибка подсчёта клиентов WireGuard: {e}")
        results["WireGuard"] = 0

    for path, _ in file_paths:
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("CLIENT_LIST"):
                        total_openvpn += 1
        except Exception as e:
            logger.warning(f"Ошибка чтения файла логов {path}: {e}")
    results["OpenVPN"] = total_openvpn
    logger.debug(f"Онлайн клиенты: WG={results['WireGuard']}, OVPN={results['OpenVPN']}")
    return results


def ensure_ovpn_stats_db():
    conn = sqlite3.connect(app.config["LOGS_DATABASE_PATH"])
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ovpn_speed_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,
            client_name TEXT,
            rx_speed REAL,
            tx_speed REAL
        )
        """
    )
    conn.commit()
    conn.close()


def save_ovpn_stats_to_db():
    now = datetime.now().astimezone()
    cutoff = now - timedelta(seconds=OVPN_DB_SAVE_INTERVAL)
    with ovpn_stats_lock:
        for client, stats in ovpn_live_stats.items():
            if client == "UNDEF":
                continue
            to_avg = []
            for i, ts_str in enumerate(stats["timestamps"]):
                try:
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    ts_local = ts.astimezone()
                    if ts_local >= cutoff:
                        to_avg.append(
                            {
                                "timestamp": ts_local,
                                "rx": stats["rx_speed"][i] if i < len(stats["rx_speed"]) else 0,
                                "tx": stats["tx_speed"][i] if i < len(stats["tx_speed"]) else 0,
                            }
                        )
                except Exception as e:
                    logger.error(f"Ошибка парсинга времени {ts_str}: {e}")

            if not to_avg:
                continue

            avg_rx = mean([p["rx"] for p in to_avg])
            avg_tx = mean([p["tx"] for p in to_avg])

            try:
                conn = sqlite3.connect(app.config["LOGS_DATABASE_PATH"])
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO ovpn_speed_stats (timestamp, client_name, rx_speed, tx_speed) VALUES (?, ?, ?, ?)",
                    (now.strftime("%Y-%m-%d %H:%M:%S"), client, round(avg_rx, 2), round(avg_tx, 2)),
                )
                cutoff_db = now - timedelta(days=7)
                cur.execute(
                    "DELETE FROM ovpn_speed_stats WHERE timestamp < ? AND client_name = ?",
                    (cutoff_db.strftime("%Y-%m-%d %H:%M:%S"), client),
                )
                conn.commit()
                conn.close()
                logger.debug(f"Сохранены OVPN stats для {client}: RX={avg_rx:.2f} бит/с, TX={avg_tx:.2f} бит/с")
            except Exception as e:
                logger.error(f"[DB ERROR] save_ovpn_stats_to_db для {client}: {e}")


def ovpn_db_save_loop():
    logger.info("Поток ovpn_db_save_loop запущен")
    global ovpn_last_db_save
    ensure_ovpn_stats_db()
    while True:
        now = time.time()
        if now - ovpn_last_db_save >= OVPN_DB_SAVE_INTERVAL:
            logger.info(f"Попытка сохранения OVPN stats (клиентов в памяти: {len(ovpn_live_stats)})")
            save_ovpn_stats_to_db()
            ovpn_last_db_save = now
        time.sleep(5)


def get_system_info():
    return cached_system_info


def update_system_info_loop():
    global last_db_save, last_collect
    ensure_db()
    while True:
        now = time.time()
        if now - last_collect >= SAMPLE_INTERVAL:
            cpu = psutil.cpu_percent(interval=None)
            ram = psutil.virtual_memory().percent
            ts = datetime.now()
            cpu_history.append({"timestamp": ts, "cpu": cpu, "ram": ram})
            cutoff = datetime.now() - timedelta(seconds=MAX_HISTORY_SECONDS)
            while cpu_history and cpu_history[0]["timestamp"] < cutoff:
                cpu_history.pop(0)
            last_collect = now

        if now - last_db_save >= DB_SAVE_INTERVAL:
            save_minute_average_to_db()
            last_db_save = now
        time.sleep(1)


def update_system_info():
    global cached_system_info, last_fetch_time, cpu_history, last_db_save
    while True:
        current_time = time.time()
        if not cached_system_info or (current_time - last_fetch_time >= CACHE_DURATION):
            cpu_percent = psutil.cpu_percent(interval=1)
            ram_percent = psutil.virtual_memory().percent
            timestamp = datetime.now()
            cpu_history.append({"timestamp": timestamp, "cpu": cpu_percent, "ram": ram_percent})
            if len(cpu_history) > MAX_CPU_HISTORY:
                cpu_history.pop(0)

            vnstat_ifaces = get_vnstat_interfaces()
            network_stats_dict = {}
            for iface_info in vnstat_ifaces:
                iface_name = iface_info.get("name")
                if iface_name:
                    stats = get_network_stats(iface_name)
                    if stats:
                        network_stats_dict[iface_name] = {
                            "rx": format_bytes(stats["rx"]),
                            "tx": format_bytes(stats["tx"]),
                            "rx_bytes": stats["rx"],
                            "tx_bytes": stats["tx"],
                            "alias": iface_info.get("alias", iface_name),
                        }

            vpn_clients = count_online_clients(LOG_FILES)
            _mem = psutil.virtual_memory()
            _disk = psutil.disk_usage("/")

            cached_system_info = {
                **HOST_STATIC_INFO,
                "cpu_load": round(cpu_percent, 1),
                "memory_used": _mem.used // (1024**2),
                "memory_total": _mem.total // (1024**2),
                "memory_percent": round(_mem.percent, 1),
                "disk_used": round(_disk.used / (1024**3), 1),
                "disk_total": round(_disk.total / (1024**3), 1),
                "network_load": get_network_load(),
                "uptime": format_uptime(get_uptime()),
                "network_interfaces": network_stats_dict,
                "vpn_clients": vpn_clients,
            }
            last_fetch_time = current_time
            logger.debug(f"Системная информация обновлена: CPU={cpu_percent}%, RAM={ram_percent}%")
        time.sleep(CACHE_DURATION)


def update_ovpn_live_stats():
    global ovpn_last_bytes
    while True:
        try:
            current_time = datetime.now()
            clients_found = 0
            for file_path, _ in LOG_FILES:
                if os.path.exists(file_path):
                    with open(file_path, "r", encoding="utf-8") as f:
                        for line in f:
                            if line.startswith("CLIENT_LIST"):
                                clients_found += 1
                                parts = line.strip().split(",")
                                if len(parts) > 6:
                                    client = parts[1].strip()
                                    if client == "UNDEF":
                                        continue
                                    rx_bytes = int(parts[5])
                                    tx_bytes = int(parts[6])
                                    prev = ovpn_last_bytes.get(client, {"rx": 0, "tx": 0, "time": current_time})
                                    time_diff = (current_time - prev["time"]).total_seconds()

                                    if rx_bytes < prev["rx"]:
                                        rx_bytes = 0
                                    if tx_bytes < prev["tx"]:
                                        tx_bytes = 0

                                    rx_delta = max(0, rx_bytes - prev["rx"])
                                    tx_delta = max(0, tx_bytes - prev["tx"])

                                    rx_speed = (rx_delta * 8) / time_diff if time_diff > 0 else 0
                                    tx_speed = (tx_delta * 8) / time_diff if time_diff > 0 else 0

                                    with ovpn_stats_lock:
                                        stats = ovpn_live_stats[client]
                                        stats["timestamps"].append(current_time.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
                                        stats["rx_speed"].append(round(rx_speed, 2))
                                        stats["tx_speed"].append(round(tx_speed, 2))
                                        if len(stats["rx_speed"]) > MAX_OVPN_LIVE_POINTS:
                                            stats["timestamps"].pop(0)
                                            stats["rx_speed"].pop(0)
                                            stats["tx_speed"].pop(0)

                                    ovpn_last_bytes[client] = {"rx": rx_bytes, "tx": tx_bytes, "time": current_time}
            time.sleep(5)
        except Exception as e:
            logger.error(f"Ошибка сбора Live stats: {e}")
            time.sleep(5)


# Запуск фоновых задач
threading.Thread(target=update_system_info, daemon=True).start()
threading.Thread(target=update_system_info_loop, daemon=True).start()
threading.Thread(target=update_ovpn_live_stats, daemon=True).start()
threading.Thread(target=ovpn_db_save_loop, daemon=True).start()
logger.info("Фоновые задачи мониторинга запущены")


def get_vnstat_interfaces():
    try:
        result = subprocess.run(["/usr/bin/vnstat", "--json"], capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)
        interfaces = []
        for iface in data.get("interfaces", []):
            name = iface.get("name")
            alias = iface.get("alias") or iface.get("description") or name
            traffic = iface.get("traffic", {}).get("total", {})
            rx = traffic.get("rx", 0)
            tx = traffic.get("tx", 0)
            if (rx + tx) > 0:
                interfaces.append({"name": name, "alias": alias})
                logger.debug(f"Добавлен интерфейс vnstat: {name} (alias: {alias})")
        logger.info(f"Найдено интерфейсов vnstat: {len(interfaces)}")
        return interfaces
    except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
        logger.error(f"Ошибка при получении интерфейсов vnstat: {e}")
        return []


# =============================================================================
# FLASK ROUTES
# =============================================================================

@app.route("/logout", methods=["GET", "POST"])
@login_required
def logout():
    username = current_user.username
    logout_user()
    session.pop("last_activity", None)
    logger.info(f"Пользователь {username} вышел из системы")
    return redirect(url_for("login"))


@app.before_request
def track_last_activity():
    if request.path.startswith("/api/"):
        return
    session.permanent = True
    session["last_activity"] = time.time()


@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("home"))
    form = LoginForm()
    error_message = None
    if form.validate_on_submit():
        conn = get_db_connection()
        user = conn.execute("SELECT * FROM users WHERE username = ?", (form.username.data,)).fetchone()
        conn.close()
        if user and bcrypt.check_password_hash(user["password"], form.password.data):
            user_obj = User(
                user_id=user["id"],
                username=user["username"],
                role=user["role"],
                password=user["password"],
            )
            login_user(user_obj, remember=form.remember_me.data)
            logger.info(f"Пользователь {form.username.data} успешно вошёл в систему")
            session.permanent = form.remember_me.data
            client_ip = request.headers.get("X-Forwarded-For", request.remote_addr)
            if client_ip and "," in client_ip:
                client_ip = client_ip.split(",")[0].strip()
            log_action("web", user["username"], user["username"], "web_login", "", client_ip or "")
            next_page = request.args.get("next")
            return redirect(next_page or url_for("home"))
        else:
            error_message = "Неправильный логин или пароль!"
            logger.warning(f"Неудачная попытка входа для пользователя: {form.username.data}")
    resp = make_response(render_template("login.html", form=form, error_message=error_message))
    resp.headers["X-Robots-Tag"] = "noindex, nofollow"
    return resp


def get_git_version():
    try:
        version = subprocess.check_output(["/usr/bin/git", "describe", "--tags", "--abbrev=0"], stderr=subprocess.DEVNULL).strip().decode()
        logger.debug(f"Git версия: {version}")
        return version
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        logger.warning(f"Не удалось получить Git версию: {e}")
        return "unknown"


def get_docker_hub_version():
    try:
        response = requests.get(DOCKER_HUB_API, params={"page_size": 100}, timeout=10)
        response.raise_for_status()
        data = response.json()
        tags = [t["name"] for t in data.get("results", []) if t.get("name")]
        semver_pattern = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)$")
        version_tags = []
        for tag in tags:
            match = semver_pattern.match(tag)
            if match:
                version_tags.append({"tag": tag, "major": int(match.group(1)), "minor": int(match.group(2)), "patch": int(match.group(3))})
        if version_tags:
            version_tags.sort(key=lambda v: (v["major"], v["minor"], v["patch"]), reverse=True)
            logger.debug(f"Docker Hub версия: {version_tags[0]['tag']}")
            return version_tags[0]["tag"]
        version = tags[0] if tags else "unknown"
        logger.debug(f"Docker Hub версия (fallback): {version}")
        return version
    except requests.RequestException as e:
        logger.warning(f"Failed to fetch version from Docker Hub: {e}")
        return "unknown"
    except Exception as e:
        logger.error(f"Unexpected error fetching Docker Hub version: {e}")
        return "unknown"


@app.context_processor
def inject_info():
    settings_data = read_settings()
    app_name = settings_data.get("app_name", "OpenVPN-Status")
    show_ovpn_menu = bool(settings_data.get("show_ovpn_menu", True))
    show_wg_menu = bool(settings_data.get("show_wg_menu", True))
    return {
        "hostname": socket.gethostname(),
        "server_ip": get_external_ip(),
        "version": get_docker_hub_version(),
        "base_path": request.script_root or "",
        "app_name": app_name,
        "show_ovpn_menu": show_ovpn_menu,
        "show_wg_menu": show_wg_menu,
        "host_os_label": HOST_STATIC_INFO["os_label"],
    }


@app.route("/")
@login_required
def home():
    server_ip = get_external_ip()
    system_info = get_system_info()
    hostname = socket.gethostname()
    logger.debug(f"Запрошена главная страница пользователем {current_user.username}")
    return render_template("index.html", server_ip=server_ip, system_info=system_info, hostname=hostname, active_page="home")


@app.route("/settings", methods=["GET", "POST"])
@login_required
def settings():
    settings_message = settings_error = stats_db_message = stats_db_error = None
    if request.method == "POST":
        form_type = request.form.get("form_type")
        if form_type == "settings_all":
            app_name = request.form.get("app_name", "").strip()
            show_ovpn_menu = request.form.get("show_ovpn_menu") == "on"
            show_wg_menu = request.form.get("show_wg_menu") == "on"
            hide_ovpn_ip = request.form.get("hide_ovpn_ip") == "on"
            hide_wg_ip = request.form.get("hide_wg_ip") == "on"
            retention_days = parse_stats_retention_days(request.form.get("stats_retention_days", "365"))
            history_max_records = parse_history_max_records(request.form.get("history_max_records", "1000"))
            write_settings({"app_name": app_name, "show_ovpn_menu": show_ovpn_menu, "show_wg_menu": show_wg_menu, "hide_ovpn_ip": hide_ovpn_ip, "hide_wg_ip": hide_wg_ip, "stats_retention_days": retention_days, "history_max_records": history_max_records})
            settings_message = "Настройки сохранены."
        elif form_type == "stats_db_clear_ovpn":
            phrase = (request.form.get("confirm_phrase") or "").strip()
            if phrase != STATS_DB_CLEAR_OVPN_PHRASE:
                stats_db_error = "Неверная фраза. Введите: OpenVPN"
            else:
                ok, err = clear_openvpn_stats_database()
                if ok:
                    stats_db_message = "База статистики OpenVPN очищена."
                    client_ip = request.headers.get("X-Forwarded-For", request.remote_addr)
                    if client_ip and "," in client_ip:
                        client_ip = client_ip.split(",")[0].strip()
                    log_action("web", current_user.username, current_user.username, "stats_db_clear_ovpn", "", client_ip or "")
                else:
                    stats_db_error = f"Ошибка очистки OpenVPN: {err}"
        elif form_type == "stats_db_clear_wg":
            phrase = (request.form.get("confirm_phrase") or "").strip()
            if phrase != STATS_DB_CLEAR_WG_PHRASE:
                stats_db_error = "Неверная фраза. Введите: WireGuard"
            else:
                ok, err = clear_wireguard_stats_database()
                if ok:
                    stats_db_message = "База статистики WireGuard очищена."
                    client_ip = request.headers.get("X-Forwarded-For", request.remote_addr)
                    if client_ip and "," in client_ip:
                        client_ip = client_ip.split(",")[0].strip()
                    log_action("web", current_user.username, current_user.username, "stats_db_clear_wg", "", client_ip or "")
                else:
                    stats_db_error = f"Ошибка очистки WireGuard: {err}"

    settings_data = read_settings()
    current_app_name = settings_data.get("app_name", "OpenVPN-Status")
    show_ovpn_menu = bool(settings_data.get("show_ovpn_menu", True))
    show_wg_menu = bool(settings_data.get("show_wg_menu", True))
    hide_ovpn_ip = settings_data.get("hide_ovpn_ip", True)
    hide_wg_ip = settings_data.get("hide_wg_ip", True)
    stats_retention_days = parse_stats_retention_days(settings_data.get("stats_retention_days", 365))
    history_max_records = parse_history_max_records(settings_data.get("history_max_records", 1000))

    stats_db_items, stats_db_total_bytes = get_ovpn_wg_database_sizes()
    return render_template(
        "settings/settings.html",
        app_name=settings_data.get("app_name", "OpenVPN-Status"),
        show_ovpn_menu=show_ovpn_menu,
        show_wg_menu=show_wg_menu,
        hide_ovpn_ip=settings_data.get("hide_ovpn_ip", True),
        hide_wg_ip=settings_data.get("hide_wg_ip", True),
        settings_message=settings_message,
        settings_error=settings_error,
        stats_retention_days=parse_stats_retention_days(settings_data.get("stats_retention_days", 365)),
        history_max_records=history_max_records,
        stats_db_items=stats_db_items,
        stats_db_total_fmt=format_bytes(stats_db_total_bytes),
        stats_db_message=stats_db_message,
        stats_db_error=stats_db_error,
        stats_clear_ovpn_phrase=STATS_DB_CLEAR_OVPN_PHRASE,
        stats_clear_wg_phrase=STATS_DB_CLEAR_WG_PHRASE,
        active_page="settings",
    )


@app.route("/settings/telegram", methods=["GET", "POST"])
@login_required
def settings_telegram():
    bot_message = bot_error = None
    if request.method == "POST":
        form_type = request.form.get("form_type")
        if form_type == "bot":
            old_env = read_env_values()
            old_token = old_env.get("BOT_TOKEN", "")
            old_admin_id = old_env.get("ADMIN_ID", "")
            old_settings = read_settings()
            old_bot_enabled = bool(old_settings.get("bot_enabled", False)) or get_telegram_bot_status()
            bot_token = request.form.get("bot_token", "").strip()
            admin_id = request.form.get("admin_id", old_admin_id).strip()
            bot_enabled = request.form.get("bot_enabled") == "on"
            update_env_values({"BOT_TOKEN": bot_token, "ADMIN_ID": admin_id})
            write_settings({"bot_enabled": bot_enabled})
            client_ip = request.headers.get("X-Forwarded-For", request.remote_addr)
            if client_ip and "," in client_ip:
                client_ip = client_ip.split(",")[0].strip()
            if bot_token != old_token:
                log_action("web", current_user.username, current_user.username, "bot_token_change", "изменён" if bot_token else "удалён", client_ip or "")
                logger.info("Токен бота изменён")
            if admin_id != old_admin_id:
                log_action("web", current_user.username, current_user.username, "bot_admins_change", f"{old_admin_id} → {admin_id}", client_ip or "")
                logger.info("Админы бота изменены: %s → %s", old_admin_id, admin_id)
            should_start = bool(bot_enabled and bot_token)
            if should_start:
                restart_ok, restart_err = restart_telegram_bot()
                if restart_ok:
                    bot_message = "Настройки бота сохранены. Бот перезапущен."
                    if not old_bot_enabled:
                        log_action("web", current_user.username, current_user.username, "bot_toggle", "включён", client_ip or "")
                else:
                    bot_error = f"Настройки бота сохранены, но перезапуск не удался: {restart_err}"
                    logger.error("Ошибка перезапуска бота: %s", restart_err)
            else:
                restart_ok, restart_error = stop_telegram_bot()
                if restart_ok:
                    if not bot_token:
                        bot_message = "Настройки бота сохранены. API токен бота пустой, бот остановлен."
                    else:
                        bot_message = "Настройки бота сохранены. Бот остановлен."
                    if old_bot_enabled:
                        log_action("web", current_user.username, current_user.username, "bot_toggle", "отключён", client_ip or "")
                else:
                    bot_error = f"Настройки бота сохранены, но остановка не удалась: {restart_error}"
                    logger.error("Ошибка остановки бота: %s", restart_error)

    env_values = read_env_values()
    bot_token_value = env_values.get("BOT_TOKEN", "")
    admin_id_value = env_values.get("ADMIN_ID", "")
    settings_data = read_settings()
    admin_info = settings_data.get("telegram_admins", {})
    admin_display_list = build_admin_display_list(admin_id_value, admin_info)
    available_admins = build_available_admin_candidates(admin_info, parse_admin_ids(admin_id_value))
    client_mapping_list = build_client_mapping_list(env_values, admin_info)
    bot_service_active = get_telegram_bot_status()
    bot_enabled = bool(settings_data.get("bot_enabled", False)) or bot_service_active
    logger.debug(f"Запрошена страница настроек Telegram пользователем {current_user.username}")
    return render_template(
        "settings/telegram.html",
        bot_token=bot_token_value,
        admin_id=admin_id_value,
        admin_display_list=admin_display_list,
        available_admins=available_admins,
        client_mapping_list=client_mapping_list,
        bot_service_active=bot_service_active,
        bot_enabled=bot_enabled,
        bot_message=bot_message,
        bot_error=bot_error,
        active_page="settings_telegram",
    )


@app.route("/settings/audit")
@login_required
def settings_audit():
    page = request.args.get("page", 1, type=int)
    action_filter = request.args.get("action", None)
    per_page = 20
    if action_filter == "all":
        action_filter = None
    total = get_logs_count(action_filter)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    offset = (page - 1) * per_page
    logs = get_logs(limit=per_page, offset=offset, action_filter=action_filter)
    action_labels = {
        "client_create": "Создание клиента",
        "client_delete": "Удаление клиента",
        "files_recreate": "Пересоздание файлов",
        "server_reboot": "Перезагрузка сервера",
        "web_login": "Вход в панель",
        "peer_toggle": "Переключение WG пира",
        "bot_token_change": "Изменение токена бота",
        "bot_admins_change": "Изменение админов бота",
        "bot_toggle": "Вкл/выкл бота",
        "request_approve": "Привязка клиента",
        "request_reject": "Отклонение запроса",
    }
    logger.debug(f"Запрошена страница аудита пользователем {current_user.username}")
    return render_template(
        "settings/audit.html",
        logs=logs,
        page=page,
        total_pages=total_pages,
        action_filter=action_filter or "all",
        action_labels=action_labels,
        active_page="settings_audit",
    )


@app.route("/api/admins/add", methods=["POST"])
@login_required
def api_admins_add():
    payload = request.get_json(silent=True) or {}
    telegram_id = str(payload.get("telegram_id", "")).strip()
    if not telegram_id:
        logger.warning("Попытка добавить админа без ID")
        return jsonify({"success": False, "message": "ID не указан."}), 400
    admin_info = read_admin_info()
    env_values = read_env_values()
    admin_id_value = env_values.get("ADMIN_ID", "")
    admin_ids = parse_admin_ids(admin_id_value)
    if telegram_id in admin_ids:
        logger.info(f"Администратор {telegram_id} уже в списке")
        return jsonify({"success": False, "message": "Администратор уже в списке.", "admins": build_admin_display_list(admin_id_value, admin_info), "available_admins": build_available_admin_candidates(admin_info, admin_ids), "admin_id_value": admin_id_value, "bot_service_active": get_telegram_bot_status()}), 400
    admin_ids.append(telegram_id)
    updated_admin_id_value = format_admin_ids(admin_ids)
    update_env_values({"ADMIN_ID": updated_admin_id_value})
    logger.info(f"Администратор {telegram_id} добавлен в список")
    return jsonify({"success": True, "message": "Администратор добавлен. Нажмите «Сохранить», чтобы применить изменения.", "admins": build_admin_display_list(updated_admin_id_value, admin_info), "available_admins": build_available_admin_candidates(admin_info, admin_ids), "admin_id_value": updated_admin_id_value, "bot_service_active": get_telegram_bot_status()}), 200


@app.route("/api/admins/remove", methods=["POST"])
@login_required
def api_admins_remove():
    payload = request.get_json(silent=True) or {}
    telegram_id = str(payload.get("telegram_id", "")).strip()
    if not telegram_id:
        logger.warning("Попытка удалить админа без ID")
        return jsonify({"success": False, "message": "ID не указан."}), 400
    admin_info = read_admin_info()
    env_values = read_env_values()
    admin_id_value = env_values.get("ADMIN_ID", "")
    admin_ids = parse_admin_ids(admin_id_value)
    if telegram_id not in admin_ids:
        logger.warning(f"Администратор {telegram_id} не найден в списке")
        return jsonify({"success": False, "message": "Администратор не найден в списке.", "admins": build_admin_display_list(admin_id_value, admin_info), "available_admins": build_available_admin_candidates(admin_info, admin_ids), "admin_id_value": admin_id_value, "bot_service_active": get_telegram_bot_status()}), 400
    if len(admin_ids) <= 1:
        logger.warning("Попытка удалить последнего администратора")
        return jsonify({"success": False, "message": "Нельзя удалить последнего администратора.", "admins": build_admin_display_list(admin_id_value, admin_info), "available_admins": build_available_admin_candidates(admin_info, admin_ids), "admin_id_value": admin_id_value, "bot_service_active": get_telegram_bot_status()}), 400
    admin_ids = [admin_id for admin_id in admin_ids if admin_id != telegram_id]
    updated_admin_id_value = format_admin_ids(admin_ids)
    update_env_values({"ADMIN_ID": updated_admin_id_value})
    logger.info(f"Администратор {telegram_id} удалён из списка")
    return jsonify({"success": True, "message": "Администратор удалён. Нажмите «Сохранить», чтобы применить изменения.", "admins": build_admin_display_list(updated_admin_id_value, admin_info), "available_admins": build_available_admin_candidates(admin_info, admin_ids), "admin_id_value": updated_admin_id_value, "bot_service_active": get_telegram_bot_status()}), 200


@app.route("/api/system_info")
@login_required
def api_system_info():
    return jsonify(get_system_info())


@app.route("/wg")
@login_required
def wg():
    settings_data = read_settings()
    hide_wg_ip = settings_data.get("hide_wg_ip", True)
    stats = get_wireguard_stats()
    if stats is None:
        stats = [{"interface": "wg0", "public_key": "N/A", "listening_port": "N/A", "peers": []}]
    disabled_peers = get_disabled_wg_peers()
    for interface_data in stats:
        for peer in interface_data.get("peers", []):
            peer["enabled"] = True
        iface = interface_data.get("interface")
        if iface in disabled_peers:
            interface_data.setdefault("peers", []).extend(disabled_peers[iface])
    logger.debug(f"Запрошена страница WireGuard пользователем {current_user.username}")
    return render_template("wg/wg.html", stats=stats, active_section="wg", active_page="wg_clients")


@app.route("/wg/client-status")
@login_required
def wg_client_status():
    return redirect(url_for("wg"))


@app.route("/api/wg/stats")
@login_required
def api_wg_stats():
    try:
        stats = get_wireguard_stats()
        if stats is None:
            stats = [{"interface": "wg0", "public_key": "N/A", "listening_port": "N/A", "peers": []}]
        disabled_peers = get_disabled_wg_peers()
        for interface_data in stats:
            for peer in interface_data.get("peers", []):
                peer["enabled"] = True
            iface = interface_data.get("interface")
            if iface in disabled_peers:
                interface_data.setdefault("peers", []).extend(disabled_peers[iface])
        return jsonify(stats)
    except Exception as e:
        logger.error(f"Ошибка получения статистики WireGuard: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/wg/peer/toggle", methods=["POST"])
@login_required
def toggle_wg_peer():
    data = request.get_json()
    peer = data.get("peer")
    interface = data.get("interface")
    enable = data.get("enable")
    if not peer or not interface or enable is None:
        return jsonify({"error": "Отсутствуют обязательные параметры"}), 400
    config_path = f"/etc/wireguard/{interface}.conf"
    if not os.path.exists(config_path):
        return jsonify({"error": "Конфигурация не найдена"}), 404
    try:
        success = toggle_peer_config(config_path, peer, enable)
        if not success:
            return jsonify({"error": "Пир не найден в конфигурации"}), 404
        wg_quick = shutil.which("wg-quick") or "/usr/bin/wg-quick"
        wg_bin = shutil.which("wg") or "/usr/bin/wg"
        if not os.path.isfile(wg_quick):
            return jsonify({"error": "wg-quick не найден. Установите wireguard-tools."}), 500
        if not os.path.isfile(wg_bin):
            return jsonify({"error": "wg не найден. Установите wireguard-tools."}), 500
        subprocess.run([f"{wg_bin} syncconf {interface} <({wg_quick} strip {interface})"], shell=True, check=True, env={**os.environ, "PATH": "/usr/bin:/bin"})
        client_name = data.get("client_name", peer[:8] + "...")
        action_str = "включён" if enable else "отключён"
        client_ip = request.headers.get("X-Forwarded-For", request.remote_addr)
        if client_ip and "," in client_ip:
            client_ip = client_ip.split(",")[0].strip()
        log_action("web", current_user.username, current_user.username, "peer_toggle", f"{client_name} ({action_str})", client_ip or "")
        logger.info(f"WireGuard пир {client_name} {action_str}")
        return jsonify({"success": True, "enabled": enable})
    except Exception as e:
        logger.error(f"Ошибка переключения пира: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/wg/stats")
@login_required
def wg_stats():
    try:
        sort_by = request.args.get("sort", "client")
        order = request.args.get("order", "asc").lower()
        period = request.args.get("period", "day")
        client_tz, selected_tz = resolve_client_timezone()
        now = datetime.now(client_tz)
        today = now.date()
        selected_date_from = (request.args.get("date_from") or "").strip()
        selected_date_to = (request.args.get("date_to") or "").strip()

        def format_period_date(dt_value):
            label = dt_value.strftime("%d.%m.%Y")
            if dt_value.date() == today:
                return f"{label} (сегодня)"
            return label

        allowed_sorts = {"client": "client", "total_sent": "SUM(sent)", "total_received": "SUM(received)"}
        sort_column = allowed_sorts.get(sort_by, "client")
        order_sql = "DESC" if order == "desc" else "ASC"

        if period == "day":
            date_from = now.strftime("%Y-%m-%d")
            date_to = None
            selected_date_from = date_from
            selected_date_to = date_from
            interval_label = f"за {format_period_date(now)}"
        elif period == "week":
            week_start = now - timedelta(days=now.weekday())
            week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
            date_from = week_start.strftime("%Y-%m-%d")
            date_to = None
            selected_date_from = date_from
            selected_date_to = now.strftime("%Y-%m-%d")
            interval_label = f"с {format_period_date(week_start)} по {format_period_date(now)}"
        elif period == "month":
            month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            date_from = month_start.strftime("%Y-%m-%d")
            date_to = None
            selected_date_from = date_from
            selected_date_to = now.strftime("%Y-%m-%d")
            month_name = dict(MONTH_OPTIONS_RU).get(month_start.month, month_start.strftime("%m"))
            interval_label = f"{month_name} {month_start.year}"
        elif period == "year":
            year_start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
            date_from = year_start.strftime("%Y-%m-%d")
            date_to = None
            selected_date_from = date_from
            selected_date_to = now.strftime("%Y-%m-%d")
            interval_label = str(year_start.year)
        elif period == "custom":
            date_from_dt = parse_date_yyyy_mm_dd(selected_date_from)
            date_to_dt = parse_date_yyyy_mm_dd(selected_date_to)
            if date_from_dt and date_to_dt:
                if date_from_dt > date_to_dt:
                    date_from_dt, date_to_dt = date_to_dt, date_from_dt
                selected_date_from = date_from_dt.strftime("%Y-%m-%d")
                selected_date_to = date_to_dt.strftime("%Y-%m-%d")
                date_from = selected_date_from
                date_to = (date_to_dt + timedelta(days=1)).strftime("%Y-%m-%d")
                interval_label = f"за {format_period_date(date_from_dt)}" if date_from_dt.date() == date_to_dt.date() else f"с {format_period_date(date_from_dt)} по {format_period_date(date_to_dt)}"
            else:
                period = "day"
                date_from = now.strftime("%Y-%m-%d")
                date_to = None
                selected_date_from = date_from
                selected_date_to = date_from
                interval_label = f"за {format_period_date(now)}"
        else:
            period = "day"
            date_from = now.strftime("%Y-%m-%d")
            date_to = None
            selected_date_from = date_from
            selected_date_to = date_from
            interval_label = f"за {format_period_date(now)}"

        is_single_day = period == "day" or (period == "custom" and selected_date_from and selected_date_from == selected_date_to)

        stats_list = []
        total_received, total_sent = 0, 0

        with sqlite3.connect(app.config["WG_STATS_PATH"]) as conn:
            if is_single_day:
                target_date = date_from
                start_hour, end_hour = get_server_hour_window_for_client_day(target_date, client_tz)
                query = f"""
                    SELECT client,
                           SUM(received) as total_received,
                           SUM(sent) as total_sent
                    FROM wg_hourly_stats
                    WHERE hour >= ? AND hour < ?
                      AND interface != 'warp'
                    GROUP BY client
                    HAVING SUM(received) > 0 OR SUM(sent) > 0
                    ORDER BY {sort_column} {order_sql}
                """
                rows = conn.execute(query, (start_hour, end_hour)).fetchall()
            elif period == "year":
                year_month_from = year_start.strftime("%Y-%m")
                query = f"""
                    SELECT client,
                           SUM(received) as total_received,
                           SUM(sent) as total_sent
                    FROM wg_monthly_stats
                    WHERE month >= ?
                      AND interface != 'warp'
                    GROUP BY client
                    HAVING SUM(received) > 0 OR SUM(sent) > 0
                    ORDER BY {sort_column} {order_sql}
                """
                rows = conn.execute(query, (year_month_from,)).fetchall()
            elif date_to:
                query = f"""
                    SELECT client,
                           SUM(received) as total_received,
                           SUM(sent) as total_sent
                    FROM wg_daily_stats
                    WHERE date >= ? AND date < ?
                      AND interface != 'warp'
                    GROUP BY client
                    HAVING SUM(received) > 0 OR SUM(sent) > 0
                    ORDER BY {sort_column} {order_sql}
                """
                rows = conn.execute(query, (date_from, date_to)).fetchall()
            else:
                query = f"""
                    SELECT client,
                           SUM(received) as total_received,
                           SUM(sent) as total_sent
                    FROM wg_daily_stats
                    WHERE date >= ?
                      AND interface != 'warp'
                    GROUP BY client
                    HAVING SUM(received) > 0 OR SUM(sent) > 0
                    ORDER BY {sort_column} {order_sql}
                """
                rows = conn.execute(query, (date_from,)).fetchall()

            for row in rows:
                client, received, sent = row
                received = received or 0
                sent = sent or 0
                total_received += received
                total_sent += sent
                stats_list.append(
                    {
                        "client": client,
                        "total_received": format_bytes(received),
                        "total_sent": format_bytes(sent),
                        "total_received_raw": received,
                        "total_sent_raw": sent,
                    }
                )

        return render_template(
            "wg/wg_stats.html",
            total_received=format_bytes(total_received),
            total_sent=format_bytes(total_sent),
            active_section="wg",
            active_page="wg_stats",
            stats=stats_list,
            period=period,
            sort_by=sort_by,
            order=order_sql.lower(),
            selected_date_from=selected_date_from,
            selected_date_to=selected_date_to,
            selected_tz=selected_tz,
            interval_label=interval_label,
        )
    except Exception as e:
        error_message = f"Произошла непредвиденная ошибка: {e}"
        return render_template("wg/wg_stats.html", error_message=error_message, active_section="wg", active_page="wg_stats"), 500


def _collect_openvpn_clients_unsorted():
    file_paths = LOG_FILES
    online_clients_raw = []
    total_received = total_sent = 0
    total_download_speed_raw = total_upload_speed_raw = 0.0
    errors = []
    online_client_names = set()

    for file_path, protocol in file_paths:
        file_data, received, sent, error = read_csv(file_path, protocol)
        if error:
            errors.append(f"Ошибка в файле {file_path}: {error}")
        else:
            online_clients_raw.extend(file_data)
            total_received += received
            total_sent += sent
            for client_row in file_data:
                if client_row[0] != "UNDEF":
                    online_client_names.add(client_row[0])
                    total_download_speed_raw += client_row[10]
                    total_upload_speed_raw += client_row[11]

    all_clients = get_all_openvpn_clients()
    server_ip = get_external_ip()
    all_clients_list = []

    for client_row in online_clients_raw:
        client_name = client_row[0]
        if client_name == "UNDEF":
            continue
        all_clients_list.append(
            {
                "name": client_name,
                "display_name": clean_client_display_name(client_name, server_ip),
                "online": True,
                "real_ip": client_row[1],
                "local_ip": client_row[2],
                "received": client_row[3],
                "sent": client_row[4],
                "download_speed": client_row[5],
                "upload_speed": client_row[6],
                "connected_since": client_row[7],
                "duration": client_row[8],
                "protocol": client_row[9],
            }
        )

    for client_name in sorted(all_clients):
        if client_name not in online_client_names:
            all_clients_list.append(
                {
                    "name": client_name,
                    "display_name": clean_client_display_name(client_name, server_ip),
                    "online": False,
                    "real_ip": "-",
                    "local_ip": "-",
                    "received": "-",
                    "sent": "-",
                    "download_speed": "-",
                    "upload_speed": "-",
                    "connected_since": "-",
                    "duration": "-",
                    "protocol": "-",
                }
            )

    return all_clients_list, total_received, total_sent, errors, total_download_speed_raw, total_upload_speed_raw


def get_all_openvpn_clients():
    if not os.path.exists(CLIENT_SH_PATH):
        clients = set()
        for base_dir in OPENVPN_CONFIG_PATHS:
            if not os.path.exists(base_dir):
                continue
            for root, _, files in os.walk(base_dir):
                for filename in files:
                    if not filename.endswith(".ovpn"):
                        continue
                    client_name = _extract_client_name_from_ovpn(filename)
                    if client_name:
                        clients.add(client_name)
        return clients
    try:
        env = os.environ.copy()
        env["PATH"] = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
        proc = subprocess.run([CLIENT_SH_PATH, "3"], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
    except Exception:
        return set()
    if proc.returncode != 0:
        return set()
    clients = set()
    for raw in (proc.stdout or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("OpenVPN client names:") or line.startswith("OpenVPN - List clients"):
            continue
        clients.add(line)
    return clients


def _dedupe_openvpn_client_status_rows(rows):
    groups = OrderedDict()
    for row in rows:
        name = row["name"]
        if name not in groups:
            groups[name] = []
        groups[name].append(row)
    out = []
    for grp in groups.values():
        if len(grp) == 1:
            out.append(grp[0])
            continue
        merged = dict(grp[0])
        merged["online"] = any(r["online"] for r in grp)
        merged["blocked"] = any(r["blocked"] for r in grp)
        online_protocols = [r["protocol"] for r in grp if r["online"] and r.get("protocol") not in (None, "-", " ")]
        if len(online_protocols) > 1:
            merged["protocol"] = " "
        elif len(online_protocols) == 1:
            merged["protocol"] = online_protocols[0]
        else:
            merged["protocol"] = grp[0]["protocol"]
        out.append(merged)
    return out


def _build_openvpn_clients_sorted(sort_by, order):
    all_clients_list, total_received, total_sent, errors, total_dl_speed, total_ul_speed = _collect_openvpn_clients_unsorted()
    reverse_order = order == "desc"

    def sort_key(x):
        online_priority = 0 if x["online"] else 1
        if sort_by == "client":
            return (online_priority, x["name"].lower())
        elif sort_by == "realIp":
            return (online_priority, x["real_ip"])
        elif sort_by == "localIp":
            return (online_priority, x["local_ip"])
        elif sort_by == "sent":
            return (online_priority, parse_bytes(x["sent"]) if x["sent"] != "-" else -1)
        elif sort_by == "received":
            return (online_priority, parse_bytes(x["received"]) if x["received"] != "-" else -1)
        elif sort_by == "connection-time" or sort_by == "duration":
            return (online_priority, x["connected_since"] if x["connected_since"] != "-" else " ")
        elif sort_by == "protocol":
            return (online_priority, x["protocol"])
        elif sort_by == "status":
            return (0 if x["online"] else 1, 0 if not x["blocked"] else 1)
        return (online_priority, x["name"].lower())

    all_clients_list.sort(key=sort_key, reverse=reverse_order)
    total_online = len([c for c in all_clients_list if c["online"]])
    for c in all_clients_list:
        c["row_key"] = _ovpn_session_row_key(c["name"], c["protocol"])
    return all_clients_list, total_received, total_sent, errors, total_online, total_dl_speed, total_ul_speed


@app.route("/api/ovpn/clients")
@login_required
def api_ovpn_clients():
    sort_by = request.args.get("sort", "client")
    order = request.args.get("order", "asc")
    try:
        all_clients_list, total_received, total_sent, errors, total_online, total_dl_speed, total_ul_speed = _build_openvpn_clients_sorted(sort_by, order)
        online = [c for c in all_clients_list if c["online"]]
        return jsonify(
            {
                "ok": True,
                "online": online,
                "total_received": format_bytes(total_received),
                "total_sent": format_bytes(total_sent),
                "total_clients_str": pluralize_clients(total_online),
                "total_online": total_online,
                "total_download_speed": f"{format_bytes(total_dl_speed)}/s",
                "total_upload_speed": f"{format_bytes(total_ul_speed)}/s",
                "errors": errors,
            }
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/ovpn")
@login_required
def ovpn():
    try:
        sort_by = request.args.get("sort", "client")
        order = request.args.get("order", "asc")
        all_clients_list, total_received, total_sent, errors, total_online, total_dl_speed, total_ul_speed = _build_openvpn_clients_sorted(sort_by, order)
        return render_template(
            "ovpn/ovpn.html",
            clients=all_clients_list,
            total_clients_str=pluralize_clients(total_online),
            total_received=format_bytes(total_received),
            total_sent=format_bytes(total_sent),
            total_download_speed=f"{format_bytes(total_dl_speed)}/s",
            total_upload_speed=f"{format_bytes(total_ul_speed)}/s",
            active_section="ovpn",
            active_page="clients",
            errors=errors,
            sort_by=sort_by,
            order=order,
        )
    except ZoneInfoNotFoundError:
        return render_template(
            "ovpn/ovpn.html",
            error_message="Обнаружены конфликтующие настройки часового пояса в файлах /etc/timezone и /etc/localtime. Попробуйте установить правильный часовой пояс с помощью команды: sudo dpkg-reconfigure tzdata",
            active_section="ovpn",
            active_page="clients",
        ), 500
    except Exception as e:
        logger.error(f"Ошибка на странице OpenVPN: {e}")
        return render_template("ovpn/ovpn.html", error_message=f"Произошла непредвиденная ошибка: {str(e)}", active_section="ovpn", active_page="clients"), 500


@app.route("/ovpn/history")
@login_required
def ovpn_history():
    q = (request.args.get("q") or "").strip()
    conn_logs = None
    try:
        page = request.args.get("page", 1, type=int)
        per_page = 20
        conn_logs = sqlite3.connect(app.config["LOGS_DATABASE_PATH"])
        filter_clause = "client_name != 'UNDEF'"
        filter_params = []
        if q:
            like_value = f"%{q.lower()}%"
            filter_clause += (
                " AND (lower(client_name) LIKE ? OR lower(real_ip) LIKE ? "
                "OR lower(local_ip) LIKE ? OR lower(protocol) LIKE ?)"
            )
            filter_params.extend([like_value, like_value, like_value, like_value])
        total_count = conn_logs.execute(
            f"SELECT COUNT(*) FROM connection_logs WHERE {filter_clause}",
            filter_params
        ).fetchone()[0]
        total_pages = max(1, (total_count + per_page - 1) // per_page)
        page = max(1, min(page, total_pages))
        offset = (page - 1) * per_page
        logs_reader = conn_logs.execute(
            f"SELECT * FROM connection_logs WHERE {filter_clause} ORDER BY connected_since DESC LIMIT ? OFFSET ?",
            (*filter_params, per_page, offset)
        ).fetchall()
        hide_ovpn_ip = read_settings().get("hide_ovpn_ip", True)
        def format_ip(ip):
            real_ip = normalize_real_address(ip) if "normalize_real_address" in globals() else ip
            return mask_ip(real_ip, hide=hide_ovpn_ip)
        logs = [{"client_name": row[1], "real_ip": format_ip(row[3]), "local_ip": row[2], "connection_since": row[4], "protocol": row[7]} for row in logs_reader]
        logger.debug(f"Запрошена история OpenVPN ({len(logs)} записей, страница {page})")
        return render_template("ovpn/ovpn_history.html", active_section="ovpn", active_page="history", logs=logs, page=page, total_pages=total_pages, q=q)
    except Exception as e:
        logger.error(f"Ошибка на странице истории OpenVPN: {e}", exc_info=True)
        return render_template("ovpn/ovpn_history.html", error_message="Произошла ошибка при загрузке данных.", q=q), 500
    finally:
        if conn_logs:
            conn_logs.close()


@app.route("/ovpn/stats")
@login_required
def ovpn_stats():
    try:
        sort_by = request.args.get("sort", "client_name")
        sort_by = {"total_bytes_sent": "client_bytes_sent", "total_bytes_received": "client_bytes_received",}.get(sort_by, sort_by)
        order = request.args.get("order", "asc").lower()
        period = request.args.get("period", "day")
        client_tz, selected_tz = resolve_client_timezone()
        now = datetime.now(client_tz)
        today = now.date()
        selected_date_from = (request.args.get("date_from") or "").strip()
        selected_date_to = (request.args.get("date_to") or "").strip()

        def format_period_date(dt_value):
            label = dt_value.strftime("%d.%m.%Y")
            return f"{label} (сегодня)" if dt_value.date() == today else label

        allowed_sorts = {"client_name": "client_name", "client_bytes_sent": "SUM(total_bytes_received)", "client_bytes_received": "SUM(total_bytes_sent)", "last_connected": "MAX(last_connected)"}
        sort_column = allowed_sorts.get(sort_by, "client_name")
        order_sql = "DESC" if order == "desc" else "ASC"

        if period == "day":
            date_from = now.strftime("%Y-%m-%d")
            date_to = None
            selected_date_from = selected_date_to = date_from
            interval_label = f"за {format_period_date(now)}"
        elif period == "week":
            week_start = now - timedelta(days=now.weekday())
            week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
            date_from = week_start.strftime("%Y-%m-%d")
            date_to = None
            selected_date_from = date_from
            selected_date_to = now.strftime("%Y-%m-%d")
            interval_label = f"с {format_period_date(week_start)} по {format_period_date(now)}"
        elif period == "month":
            month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            date_from = month_start.strftime("%Y-%m-%d")
            date_to = None
            selected_date_from = date_from
            selected_date_to = now.strftime("%Y-%m-%d")
            month_name = dict(MONTH_OPTIONS_RU).get(month_start.month, month_start.strftime("%m"))
            interval_label = f"{month_name} {month_start.year}"
        elif period == "year":
            year_start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
            date_from = year_start.strftime("%Y-%m-%d")
            date_to = None
            selected_date_from = date_from
            selected_date_to = now.strftime("%Y-%m-%d")
            interval_label = str(year_start.year)
        elif period == "custom":
            date_from_dt = parse_date_yyyy_mm_dd(selected_date_from)
            date_to_dt = parse_date_yyyy_mm_dd(selected_date_to)
            if date_from_dt and date_to_dt:
                if date_from_dt > date_to_dt:
                    date_from_dt, date_to_dt = date_to_dt, date_from_dt
                selected_date_from = date_from_dt.strftime("%Y-%m-%d")
                selected_date_to = date_to_dt.strftime("%Y-%m-%d")
                date_from = selected_date_from
                date_to = (date_to_dt + timedelta(days=1)).strftime("%Y-%m-%d")
                interval_label = f"за {format_period_date(date_from_dt)}" if date_from_dt.date() == date_to_dt.date() else f"с {format_period_date(date_from_dt)} по {format_period_date(date_to_dt)}"
            else:
                period = "day"
                date_from = now.strftime("%Y-%m-%d")
                selected_date_from = selected_date_to = date_from
                interval_label = f"за {format_period_date(now)}"
        else:
            period = "day"
            date_from = now.strftime("%Y-%m-%d")
            selected_date_from = selected_date_to = date_from
            interval_label = f"за {format_period_date(now)}"

        is_single_day = period == "day" or (period == "custom" and selected_date_from == selected_date_to)

        stats_list = []
        total_received = total_sent = 0

        with sqlite3.connect(app.config["LOGS_DATABASE_PATH"]) as conn:
            if is_single_day:
                target_date = date_from
                start_hour, end_hour = get_server_hour_window_for_client_day(target_date, client_tz)
                query = f"""
                    SELECT client_name,
                           SUM(total_bytes_received),
                           SUM(total_bytes_sent),
                           MAX(last_connected)
                    FROM daily_stats
                    WHERE hour >= ? AND hour < ?
                    GROUP BY client_name
                    ORDER BY {sort_column} {order_sql}
                """
                rows = conn.execute(query, (start_hour, end_hour)).fetchall()
            elif period == "year":
                year_month_from = year_start.strftime("%Y-%m")
                query = f"""
                    SELECT client_name,
                           SUM(total_bytes_received),
                           SUM(total_bytes_sent),
                           MAX(last_connected)
                    FROM years_stats
                    WHERE month >= ?
                    GROUP BY client_name
                    ORDER BY {sort_column} {order_sql}
                """
                rows = conn.execute(query, (year_month_from,)).fetchall()
            elif date_to:
                query = f"""
                    SELECT client_name,
                           SUM(total_bytes_received),
                           SUM(total_bytes_sent),
                           MAX(last_connected)
                    FROM monthly_stats
                    WHERE month >= ? AND month < ?
                    GROUP BY client_name
                    ORDER BY {sort_column} {order_sql}
                """
                rows = conn.execute(query, (date_from, date_to)).fetchall()
            else:
                query = f"""
                    SELECT client_name,
                           SUM(total_bytes_received),
                           SUM(total_bytes_sent),
                           MAX(last_connected)
                    FROM monthly_stats
                    WHERE month >= ?
                    GROUP BY client_name
                    ORDER BY {sort_column} {order_sql}
                """
                rows = conn.execute(query, (date_from,)).fetchall()

            for client_name, received, sent, last_connected in rows:
                total_received += received or 0
                total_sent += sent or 0
                stats_list.append(
                    {
                        "client_name": client_name,
                        "client_bytes_sent": format_bytes(received),
                        "client_bytes_received": format_bytes(sent),
                        "total_bytes_sent": format_bytes(received),
                        "total_bytes_received": format_bytes(sent),
                        "client_bytes_sent_raw": received or 0,
                        "client_bytes_received_raw": sent or 0,
                        "total_bytes_sent_raw": received or 0,
                        "total_bytes_received_raw": sent or 0,
                        "last_connected": last_connected,
                    }
                )

        return render_template(
            "ovpn/ovpn_stats.html",
            total_client_received=format_bytes(total_sent),
            total_client_sent=format_bytes(total_received),
            total_received=format_bytes(total_received),
            total_sent=format_bytes(total_sent),
            active_section="ovpn",
            active_page="stats",
            stats=stats_list,
            period=period,
            sort_by=sort_by,
            order=order_sql.lower(),
            selected_date_from=selected_date_from,
            selected_date_to=selected_date_to,
            selected_tz=selected_tz,
            interval_label=interval_label,
        )
    except Exception as e:
        logger.error(f"Ошибка на странице статистики OVPN: {e}")
        return render_template("ovpn/ovpn_stats.html", error_message=f"Произошла непредвиденная ошибка: {e}", active_section="ovpn", active_page="stats"), 500


@app.route("/api/ovpn/client_chart")
@login_required
def api_ovpn_client_chart():
    client_name = request.args.get("client")
    period = request.args.get("period", "day")
    client_tz, _ = resolve_client_timezone()
    now = datetime.now(client_tz)
    selected_date_from = (request.args.get("date_from") or "").strip()
    selected_date_to = (request.args.get("date_to") or "").strip()
    if not client_name:
        return jsonify({"error": "client parameter required"}), 400

    is_single_day = False
    if period == "day":
        target_date = now.strftime("%Y-%m-%d")
        selected_date_from = selected_date_to = target_date
        is_single_day = True
    elif period == "week":
        week_start = now - timedelta(days=now.weekday())
        week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
        date_from = week_start.strftime("%Y-%m-%d")
        date_to = None
    elif period == "month":
        date_from = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d")
        date_to = None
    elif period == "year":
        year_month_from = now.replace(month=1, day=1).strftime("%Y-%m")
    elif period == "custom":
        date_from_dt = parse_date_yyyy_mm_dd(selected_date_from)
        date_to_dt = parse_date_yyyy_mm_dd(selected_date_to)
        if date_from_dt and date_to_dt:
            if date_from_dt > date_to_dt:
                date_from_dt, date_to_dt = date_to_dt, date_from_dt
            if date_from_dt == date_to_dt:
                target_date = date_from_dt.strftime("%Y-%m-%d")
                selected_date_from = selected_date_to = target_date
                is_single_day = True
            else:
                date_from = date_from_dt.strftime("%Y-%m-%d")
                date_to = (date_to_dt + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            target_date = now.strftime("%Y-%m-%d")
            selected_date_from = selected_date_to = target_date
            is_single_day = True
    else:
        target_date = now.strftime("%Y-%m-%d")
        selected_date_from = selected_date_to = target_date
        is_single_day = True

    try:
        with sqlite3.connect(app.config["LOGS_DATABASE_PATH"]) as conn:
            if is_single_day:
                start_hour, end_hour = get_server_hour_window_for_client_day(target_date, client_tz)
                rows = conn.execute(
                    "SELECT hour, SUM(total_bytes_received) as rx, SUM(total_bytes_sent) as tx FROM daily_stats WHERE client_name = ? AND hour >= ? AND hour < ? GROUP BY hour ORDER BY hour ASC",
                    (client_name, start_hour, end_hour),
                ).fetchall()
                hour_data = {h: (rx or 0, tx or 0) for h, rx, tx in rows}
                day_start_client = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=client_tz)
                day_end_client = day_start_client + timedelta(days=1)
                now_hour_client = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
                display_end_client = min(day_end_client, now_hour_client)
                labels, rx_data, tx_data = [], [], []
                point_dt_client = day_start_client
                while point_dt_client < display_end_client:
                    point_dt_server = point_dt_client.astimezone(get_localzone())
                    server_hour_key = point_dt_server.strftime("%Y-%m-%d %H:00")
                    labels.append(point_dt_client.strftime("%Y-%m-%d %H:00"))
                    rx, tx = hour_data.get(server_hour_key, (0, 0))
                    rx_data.append(rx or 0)
                    tx_data.append(tx or 0)
                    point_dt_client += timedelta(hours=1)
            elif period == "year":
                rows = conn.execute("SELECT month, SUM(total_bytes_received) as rx, SUM(total_bytes_sent) as tx FROM years_stats WHERE client_name = ? AND month >= ? GROUP BY month ORDER BY month ASC", (client_name, year_month_from)).fetchall()
                labels, rx_data, tx_data = [r[0] for r in rows], [r[1] or 0 for r in rows], [r[2] or 0 for r in rows]
            elif date_to:
                rows = conn.execute("SELECT month, SUM(total_bytes_received) as rx, SUM(total_bytes_sent) as tx FROM monthly_stats WHERE client_name = ? AND month >= ? AND month < ? GROUP BY month ORDER BY month ASC", (client_name, date_from, date_to)).fetchall()
                labels, rx_data, tx_data = [r[0] for r in rows], [r[1] or 0 for r in rows], [r[2] or 0 for r in rows]
            else:
                rows = conn.execute("SELECT month, SUM(total_bytes_received) as rx, SUM(total_bytes_sent) as tx FROM monthly_stats WHERE client_name = ? AND month >= ? GROUP BY month ORDER BY month ASC", (client_name, date_from)).fetchall()
                labels, rx_data, tx_data = [r[0] for r in rows], [r[1] or 0 for r in rows], [r[2] or 0 for r in rows]
        return jsonify({"client": client_name, "labels": labels, "rx_bytes": rx_data, "tx_bytes": tx_data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/wg/client_chart")
@login_required
def api_wg_client_chart():
    client_name = request.args.get("client")
    period = request.args.get("period", "day")
    client_tz, _ = resolve_client_timezone()
    now = datetime.now(client_tz)
    selected_date_from = (request.args.get("date_from") or "").strip()
    selected_date_to = (request.args.get("date_to") or "").strip()
    if not client_name:
        return jsonify({"error": "client parameter required"}), 400

    is_single_day = False
    if period == "day":
        target_date = now.strftime("%Y-%m-%d")
        selected_date_from = selected_date_to = target_date
        is_single_day = True
    elif period == "week":
        week_start = now - timedelta(days=now.weekday())
        week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
        date_from = week_start.strftime("%Y-%m-%d")
        date_to = None
    elif period == "month":
        date_from = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d")
        date_to = None
    elif period == "year":
        year_month_from = now.replace(month=1, day=1).strftime("%Y-%m")
    elif period == "custom":
        date_from_dt = parse_date_yyyy_mm_dd(selected_date_from)
        date_to_dt = parse_date_yyyy_mm_dd(selected_date_to)
        if date_from_dt and date_to_dt:
            if date_from_dt > date_to_dt:
                date_from_dt, date_to_dt = date_to_dt, date_from_dt
            if date_from_dt == date_to_dt:
                target_date = date_from_dt.strftime("%Y-%m-%d")
                selected_date_from = selected_date_to = target_date
                is_single_day = True
            else:
                date_from = date_from_dt.strftime("%Y-%m-%d")
                date_to = (date_to_dt + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            target_date = now.strftime("%Y-%m-%d")
            selected_date_from = selected_date_to = target_date
            is_single_day = True
    else:
        target_date = now.strftime("%Y-%m-%d")
        selected_date_from = selected_date_to = target_date
        is_single_day = True

    try:
        with sqlite3.connect(app.config["WG_STATS_PATH"]) as conn:
            if is_single_day:
                start_hour, end_hour = get_server_hour_window_for_client_day(target_date, client_tz)
                rows = conn.execute("SELECT hour, SUM(received) as rx, SUM(sent) as tx FROM wg_hourly_stats WHERE client = ? AND hour >= ? AND hour < ? AND interface != 'warp' GROUP BY hour ORDER BY hour ASC", (client_name, start_hour, end_hour)).fetchall()
                hour_data = {h: (rx or 0, tx or 0) for h, rx, tx in rows}
                day_start_client = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=client_tz)
                day_end_client = day_start_client + timedelta(days=1)
                now_hour_client = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
                display_end_client = min(day_end_client, now_hour_client)
                labels, rx_data, tx_data = [], [], []
                point_dt_client = day_start_client
                while point_dt_client < display_end_client:
                    point_dt_server = point_dt_client.astimezone(get_localzone())
                    server_hour_key = point_dt_server.strftime("%Y-%m-%d %H:00")
                    labels.append(point_dt_client.strftime("%Y-%m-%d %H:00"))
                    rx, tx = hour_data.get(server_hour_key, (0, 0))
                    rx_data.append(rx or 0)
                    tx_data.append(tx or 0)
                    point_dt_client += timedelta(hours=1)
            elif period == "year":
                rows = conn.execute("SELECT month, SUM(received) as rx, SUM(sent) as tx FROM wg_monthly_stats WHERE client = ? AND month >= ? AND interface != 'warp' GROUP BY month ORDER BY month ASC", (client_name, year_month_from)).fetchall()
                labels, rx_data, tx_data = [r[0] for r in rows], [r[1] or 0 for r in rows], [r[2] or 0 for r in rows]
            elif date_to:
                rows = conn.execute("SELECT date, SUM(received) as rx, SUM(sent) as tx FROM wg_daily_stats WHERE client = ? AND date >= ? AND date < ? AND interface != 'warp' GROUP BY date ORDER BY date ASC", (client_name, date_from, date_to)).fetchall()
                labels, rx_data, tx_data = [r[0] for r in rows], [r[1] or 0 for r in rows], [r[2] or 0 for r in rows]
            else:
                rows = conn.execute("SELECT date, SUM(received) as rx, SUM(sent) as tx FROM wg_daily_stats WHERE client = ? AND date >= ? AND interface != 'warp' GROUP BY date ORDER BY date ASC", (client_name, date_from)).fetchall()
                labels, rx_data, tx_data = [r[0] for r in rows], [r[1] or 0 for r in rows], [r[2] or 0 for r in rows]
        logger.debug(f"График WireGuard клиента {client_name} сформирован")
        return jsonify({"client": client_name, "labels": labels, "rx_bytes": rx_data, "tx_bytes": tx_data})
    except Exception as e:
        logger.error(f"Ошибка графика WireGuard: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/bw")
@login_required
def api_bw():
    q_iface = request.args.get("iface")
    period = request.args.get("period", "day")
    vnstat_bin = os.environ.get("VNSTAT_BIN", "/usr/bin/vnstat")
    try:
        proc = subprocess.run([vnstat_bin, "--json"], check=True, capture_output=True, text=True)
        data = json.loads(proc.stdout)
        interfaces = [iface["name"] for iface in data.get("interfaces", [])]
    except subprocess.CalledProcessError:
        logger.warning("vnstat вернул ошибку при получении интерфейсов")
        interfaces = []
    except json.JSONDecodeError:
        logger.error("Ошибка парсинга JSON от vnstat")
        interfaces = []
    if not interfaces:
        logger.error("Нет интерфейсов vnstat")
        return jsonify({"error": "Нет интерфейсов vnstat", "iface": None}), 500

    iface = q_iface if q_iface in interfaces else interfaces[0]
    if period == "hour":
        vnstat_option, points, interval_seconds = "f", 12, 300
    elif period == "day":
        vnstat_option, points, interval_seconds = "h", 24, 3600
    elif period in ("week", "month"):
        vnstat_option, points, interval_seconds = "d", 30, 86400
    else:
        vnstat_option, points, interval_seconds = "h", 24, 3600

    try:
        proc = subprocess.run([vnstat_bin, "--json", vnstat_option, "-i", iface], check=True, capture_output=True, text=True)
        data = json.loads(proc.stdout)
    except subprocess.CalledProcessError as e:
        logger.error(f"vnstat вернул код ошибки: {e.returncode}")
        return jsonify({"error": f"vnstat вернул код ошибки: {e.returncode}", "iface": iface}), 500
    except Exception as e:
        logger.error(f"Ошибка получения данных vnstat: {e}")
        return jsonify({"error": str(e), "iface": iface}), 500

    traffic_data = []
    for it in data.get("interfaces", []):
        if it.get("name") == iface:
            traffic = it.get("traffic") or {}
            if vnstat_option == "f":
                traffic_data = traffic.get("fiveminute") or []
            elif vnstat_option == "h":
                traffic_data = traffic.get("hour") or []
            elif vnstat_option == "d":
                traffic_data = traffic.get("day") or []
            break

    def sort_key(h):
        d, t = h.get("date") or {}, h.get("time") or {}
        return (d.get("year", 0), d.get("month", 0), d.get("day", 0), t.get("hour", 0), t.get("minute", 0))

    sorted_data = sorted(traffic_data, key=sort_key)
    if points:
        sorted_data = sorted_data[-points:]
    labels, utc_labels, rx_mbps, tx_mbps = [], [], [], []
    server_tz = datetime.now().astimezone().tzinfo

    for m in sorted_data:
        d, t = m.get("date") or {}, m.get("time") or {}
        year, month, day = int(d.get("year", 0)), int(d.get("month", 0)), int(d.get("day", 0))
        hour, minute = int(t.get("hour", 0)), int(t.get("minute", 0))
        if vnstat_option == "f":
            labels.append(f"{hour:02d}:{minute:02d}")
        elif vnstat_option == "h":
            labels.append(f"{hour:02d}:00")
        else:
            labels.append(f"{day:02d}.{month:02d}")
        try:
            local_dt = datetime(year, month, day, hour, minute, tzinfo=server_tz)
        except Exception:
            local_dt = datetime.now().astimezone(server_tz)
        utc_labels.append(local_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
        rx, tx = int(m.get("rx", 0)), int(m.get("tx", 0))
        rx_mbps.append(round((rx * 8) / (interval_seconds * 1_000_000), 3))
        tx_mbps.append(round((tx * 8) / (interval_seconds * 1_000_000), 3))

    return jsonify({"iface": iface, "labels": labels, "utc_labels": utc_labels, "rx_mbps": rx_mbps, "tx_mbps": tx_mbps, "server_time": datetime.now(timezone.utc).isoformat()})


@app.route("/api/bw/monthly_traffic")
@login_required
def api_monthly_traffic():
    iface = request.args.get("iface")
    vnstat_bin = os.environ.get("VNSTAT_BIN", "/usr/bin/vnstat")
    try:
        proc = subprocess.run([vnstat_bin, "--json"], check=True, capture_output=True, text=True)
        data = json.loads(proc.stdout)
        vnstat_ifaces = get_vnstat_interfaces()
        all_interfaces = [inf["name"] for inf in vnstat_ifaces]
        if not all_interfaces:
            return jsonify({"error": "Нет интерфейсов vnstat"}), 500
    except Exception as e:
        logger.error(f"Ошибка получения интерфейсов: {e}")
        return jsonify({"error": str(e)}), 500

    if iface:
        if iface not in all_interfaces:
            return jsonify({"error": f"Интерфейс {iface} не найден"}), 404
        monthly_stats = {}
        try:
            proc = subprocess.run([vnstat_bin, "--json", "m", "-i", iface], check=True, capture_output=True, text=True)
            data = json.loads(proc.stdout)
            for it in data.get("interfaces", []):
                if it.get("name") == iface:
                    traffic = it.get("traffic") or {}
                    monthly_data = traffic.get("month") or []
                    now = datetime.now()
                    current_year, current_month = now.year, now.month
                    current_month_data = next((m for m in monthly_data if m.get("date", {}).get("year") == current_year and m.get("date", {}).get("month") == current_month), None)
                    rx_bytes, tx_bytes = (int(current_month_data.get("rx", 0)) if current_month_data else 0), (int(current_month_data.get("tx", 0)) if current_month_data else 0)
                    alias = next((inf["alias"] for inf in vnstat_ifaces if inf["name"] == iface), iface)
                    monthly_stats[iface] = {"alias": alias, "rx_bytes": rx_bytes, "tx_bytes": tx_bytes, "rx_human": format_bytes(rx_bytes), "tx_human": format_bytes(tx_bytes)}
                    break
        except Exception as e:
            logger.error(f"Ошибка получения месячных данных для {iface}: {e}")
            return jsonify({"error": str(e)}), 500
        return jsonify(monthly_stats)

    monthly_stats = {}
    for iface_name in all_interfaces:
        try:
            proc = subprocess.run([vnstat_bin, "--json", "m", "-i", iface_name], check=True, capture_output=True, text=True)
            data = json.loads(proc.stdout)
            for it in data.get("interfaces", []):
                if it.get("name") == iface_name:
                    traffic = it.get("traffic") or {}
                    monthly_data = traffic.get("month") or []
                    now = datetime.now()
                    current_year, current_month = now.year, now.month
                    current_month_data = next((m for m in monthly_data if m.get("date", {}).get("year") == current_year and m.get("date", {}).get("month") == current_month), None)
                    rx_bytes, tx_bytes = (int(current_month_data.get("rx", 0)) if current_month_data else 0), (int(current_month_data.get("tx", 0)) if current_month_data else 0)
                    alias = next((inf["alias"] for inf in vnstat_ifaces if inf["name"] == iface_name), iface_name)
                    monthly_stats[iface_name] = {"alias": alias, "rx_bytes": rx_bytes, "tx_bytes": tx_bytes, "rx_human": format_bytes(rx_bytes), "tx_human": format_bytes(tx_bytes)}
                    break
        except Exception as e:
            logger.error(f"Ошибка получения месячных данных для {iface_name}: {e}")
            monthly_stats[iface_name] = {"alias": iface_name, "rx_bytes": 0, "tx_bytes": 0, "rx_human": "0 B", "tx_human": "0 B"}
    return jsonify(monthly_stats)


@app.route("/api/interfaces")
def api_interfaces():
    return jsonify({"interfaces": get_vnstat_interfaces()})


@app.route("/api/cpu")
def api_cpu():
    period = request.args.get("period", "live")
    now = datetime.now()
    targets = {"live": LIVE_POINTS, "hour": 60, "day": 24, "month": now.day}
    max_points = targets.get(period, LIVE_POINTS)
    mem_rows = list(cpu_history)

    if period == "live":
        last = mem_rows[-LIVE_POINTS:] if len(mem_rows) > LIVE_POINTS else mem_rows
        data = [{"timestamp": r["timestamp"], "cpu": r["cpu"], "ram": r["ram"]} for r in last]
    else:
        if period == "hour":
            bucket, cutoff = "minute", now - timedelta(hours=1)
        elif period == "day":
            bucket, cutoff = "hour", now - timedelta(days=1)
        elif period == "month":
            bucket, cutoff = "day", now.replace(day=1)
        else:
            bucket, cutoff = "minute", now - timedelta(hours=1)

        mem_candidates = [r for r in mem_rows if r["timestamp"] >= cutoff]
        need_db = True
        if need_db:
            try:
                conn = sqlite3.connect(app.config["SYSTEM_STATS_PATH"])
                cur = conn.cursor()
                cur.execute("SELECT timestamp, cpu_percent, ram_percent FROM system_stats WHERE timestamp >= ? ORDER BY timestamp ASC", (cutoff.strftime("%Y-%m-%d %H:%M:%S"),))
                rows = cur.fetchall()
                conn.close()
                source_rows = [{"timestamp": datetime.strptime(ts, "%Y-%m-%d %H:%M:%S"), "cpu": cpu, "ram": ram} for ts, cpu, ram in rows]
            except Exception as e:
                logger.error(f"[DB ERROR] api_cpu: {e}")
                source_rows = mem_candidates
        else:
            source_rows = mem_candidates

        grouped = group_rows(source_rows, interval=bucket)
        data = resample_to_n(grouped, max_points)

    utc_labels = [d["timestamp"].astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") for d in data]
    return jsonify({"utc_labels": utc_labels, "cpu_percent": [round(d["cpu"], 2) for d in data], "ram_percent": [round(d["ram"], 2) for d in data], "period": period})


@app.route("/api/ovpn/live_chart")
@login_required
def api_ovpn_live_chart():
    client_name = request.args.get("client")
    if not client_name:
        return jsonify({"client": None, "labels": [], "rx_speed": [], "tx_speed": [], "unit": "бит/с"})
    with ovpn_stats_lock:
        if client_name not in ovpn_live_stats:
            return jsonify({"client": client_name, "labels": [], "rx_speed": [], "tx_speed": [], "unit": "бит/с"})
        data = ovpn_live_stats[client_name]
        return jsonify({"client": client_name, "labels": data["timestamps"][-MAX_OVPN_LIVE_POINTS:], "rx_speed": data["rx_speed"][-MAX_OVPN_LIVE_POINTS:], "tx_speed": data["tx_speed"][-MAX_OVPN_LIVE_POINTS:], "unit": "бит/с"})


@app.route("/api/ovpn/speed_stats")
@login_required
def api_ovpn_speed_stats():
    client_name = request.args.get("client")
    period = request.args.get("period", "live")
    if not client_name:
        return jsonify({"error": "client parameter required"}), 400

    now = datetime.now()
    if period == "live":
        with ovpn_stats_lock:
            if client_name not in ovpn_live_stats:
                return jsonify({"labels": [], "rx_speed": [], "tx_speed": []})
            data = ovpn_live_stats[client_name]
            return jsonify({"labels": data["timestamps"][-60:], "rx_speed": data["rx_speed"][-60:], "tx_speed": data["tx_speed"][-60:]})
    else:
        if period == "hour":
            cutoff, bucket = now - timedelta(hours=1), "minute"
        elif period == "day":
            cutoff, bucket = now - timedelta(days=1), "hour"
        elif period == "week":
            cutoff, bucket = now - timedelta(weeks=1), "day"
        else:
            cutoff, bucket = now - timedelta(days=1), "hour"

        try:
            conn = sqlite3.connect(app.config["LOGS_DATABASE_PATH"])
            cur = conn.cursor()
            cur.execute("SELECT timestamp, rx_speed, tx_speed FROM ovpn_speed_stats WHERE client_name = ? AND timestamp >= ? ORDER BY timestamp ASC", (client_name, cutoff.strftime("%Y-%m-%d %H:%M:%S")))
            rows = cur.fetchall()
            conn.close()
            grouped = {}
            for ts, rx, tx in rows:
                dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                if bucket == "minute":
                    key = dt.replace(second=0, microsecond=0)
                elif bucket == "hour":
                    key = dt.replace(minute=0, second=0, microsecond=0)
                else:
                    key = dt.replace(hour=0, minute=0, second=0, microsecond=0)
                if key not in grouped:
                    grouped[key] = {"rx": [], "tx": []}
                grouped[key]["rx"].append(rx)
                grouped[key]["tx"].append(tx)

            labels, rx_speed, tx_speed = [], [], []
            for key in sorted(grouped.keys()):
                labels.append(key.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
                rx_speed.append(round(mean(grouped[key]["rx"]), 2))
                tx_speed.append(round(mean(grouped[key]["tx"]), 2))
            return jsonify({"labels": labels, "rx_speed": rx_speed, "tx_speed": tx_speed})
        except Exception as e:
            logger.error(f"Ошибка OVPN speed stats: {e}")
            return jsonify({"labels": [], "rx_speed": [], "tx_speed": []}), 500


@app.route("/api/ovpn/chart/clients")
@login_required
def api_ovpn_chart_clients():
    try:
        clients = set()
        with ovpn_stats_lock:
            clients = list(ovpn_live_stats.keys())
        if not clients:
            with sqlite3.connect(app.config["LOGS_DATABASE_PATH"]) as conn:
                rows = conn.execute("SELECT DISTINCT client_name FROM daily_stats WHERE hour >= datetime('now', '-1 hour') ORDER BY client_name").fetchall()
                clients = [row[0] for row in rows]
        return jsonify(clients)
    except Exception as e:
        logger.error(f"Ошибка получения списка клиентов: {e}")
        return jsonify([]), 500


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("ЗАПУСК MAIN.PY (Flask приложение)")
    logger.info("=" * 60)
    logger.info("Версия Python: %s", sys.version)
    logger.info("Запуск на порту: 1234")
    logger.info("Путь к логам: %s", LOG_DIR)
    admin_pass = add_admin()
    if admin_pass:
        logger.info("Администратор создан/обновлён")
    logger.info("Запуск Flask сервера...")
    app.run(debug=False, host="0.0.0.0", port=1234)