import csv
import sqlite3
import requests
import os
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

import logging
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
    make_response,
    render_template,
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
from zoneinfo._common import ZoneInfoNotFoundError
from collections import defaultdict

# ============================================================================
# НАСТРОЙКА ЛОГИРОВАНИЯ С РАЗДЕЛЕНИЕМ ПО УРОВНЯМ И РОТАЦИЕЙ ФАЙЛОВ
# ============================================================================
import sys

LOG_DIR = Config.LOGS_PATH
os.makedirs(LOG_DIR, exist_ok=True)
STDOUT_LOG = os.path.join(LOG_DIR, 'main.stdout.log')
STDERR_LOG = os.path.join(LOG_DIR, 'main.stderr.log')
MAX_LOG_SIZE = 10 * 1024 * 1024
BACKUP_COUNT = 5

class LevelFilter(logging.Filter):
    def __init__(self, min_level, max_level=None):
        super().__init__()
        self.min_level = min_level
        self.max_level = max_level or min_level
    
    def filter(self, record):
        return self.min_level <= record.levelno <= self.max_level

# Получаем логгер ПЕРВЫМ делом
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False

# ⚠️ КРИТИЧНО: Очищаем handlers ТОЛЬКО если они уже есть
if logger.handlers:
    logger.handlers.clear()

# Очищаем корневой логгер
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# Создаём обработчики
stderr_handler = RotatingFileHandler(STDERR_LOG, maxBytes=MAX_LOG_SIZE, 
                                      backupCount=BACKUP_COUNT, encoding='utf-8', delay=True)
stderr_handler.setLevel(logging.WARNING)
stderr_handler.addFilter(LevelFilter(logging.WARNING, logging.CRITICAL))
stderr_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                                               datefmt='%d-%m-%Y %H:%M:%S'))

stdout_handler = RotatingFileHandler(STDOUT_LOG, maxBytes=MAX_LOG_SIZE, 
                                      backupCount=BACKUP_COUNT, encoding='utf-8', delay=True)
stdout_handler.setLevel(logging.DEBUG)
stdout_handler.addFilter(LevelFilter(logging.DEBUG, logging.INFO))
stdout_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                                               datefmt='%d-%m-%Y %H:%M:%S'))

logger.addHandler(stderr_handler)
#logger.addHandler(stdout_handler)


class ScriptNameMiddleware:
    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        # Получаем префикс из заголовка X-Script-Name
        script_name = environ.get('HTTP_X_SCRIPT_NAME', '')
        
        # Если заголовок присутствует, устанавливаем SCRIPT_NAME
        if script_name:
            # Убираем завершающий слэш, если есть
            script_name = script_name.rstrip('/')
            environ['SCRIPT_NAME'] = script_name
            
            # Корректируем PATH_INFO, убирая префикс (если Nginx его не удалил)
            # Это нужно на случай, если proxy_pass настроен без завершающего слэша
            path_info = environ.get('PATH_INFO', '')
            if path_info.startswith(script_name):
                new_path = path_info[len(script_name):]
                environ['PATH_INFO'] = new_path if new_path else '/'
        
        return self.app(environ, start_response)


app = Flask(__name__)
app.config.from_object(Config)

# Применяем middleware для обработки префикса пути
app.wsgi_app = ScriptNameMiddleware(app.wsgi_app)

DOCKER_HUB_REPO = "devils0411/openvpn-status"  # Укажите ваш namespace/repo
DOCKER_HUB_API = f"https://hub.docker.com/v2/repositories/{DOCKER_HUB_REPO}/tags/"
bcrypt = Bcrypt(app)
loginManager = LoginManager(app)
loginManager.login_view = "login"

# Получаем LOG_FILES из конфигурации
LOG_FILES = Config.LOG_FILES

# Переменная для хранения кэшированных данных
cached_system_info = None
last_fetch_time = 0
CACHE_DURATION = 10  # обновление кэша каждые 10 секунд

cpu_history = []
ram_history = []
MAX_CPU_HISTORY = 60 * 12  # хранить 12 часов с шагом 1 минута
DB_SAVE_INTERVAL = 300  # запись в БД каждые 5 минут
last_db_save = 0
SAMPLE_INTERVAL = 10  # текущая частота сбора
MAX_HISTORY_SECONDS = 7 * 24 * 3600  # сколько секунд хранить в памяти
LIVE_POINTS = 60
last_collect = 0

#Переменные для OpenVPN
ovpn_live_stats = defaultdict(lambda: {"rx_speed": [], "tx_speed": [], "timestamps": []})
MAX_OVPN_LIVE_POINTS = 60 * 12  # хранить 12 часов с шагом 1 минута
ovpn_stats_lock = Lock()
ovpn_last_bytes = {}  # Для расчёта дельты
ovpn_last_db_save = 0
OVPN_DB_SAVE_INTERVAL = 30  # запись в БД каждые 30 секунд

BOT_RESTART_LOCK = Lock()
BOT_SERVICE_NAME = "telegram-bot"
ENV_PATH = Config.ENV_PATH
SETTINGS_PATH = Config.SETTINGS_PATH
LEGACY_ADMIN_INFO_PATH = Config.LEGACY_ADMIN_INFO_PATH
CLIENT_MAPPING_KEY = "CLIENT_MAPPING"


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
        logger.debug(f"✅ Прочитано {len(values)} переменных из .env")
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
        logger.info(f"✅ Обновлены ключи в .env: {list(updates.keys())}")
    except Exception as e:
        logger.error(f"❌ Ошибка записи в .env файл: {e}")


DEFAULT_SETTINGS = {
    "app_name": "OpenVPN-Status",
    "telegram_admins": {},
    "bot_enabled": False,
    "hide_ovpn_ip": True,
    "hide_wg_ip": True,
}


def write_settings_data(settings_data):
    try:
        with open(SETTINGS_PATH, "w", encoding="utf-8") as settings_file:
            json.dump(settings_data, settings_file, ensure_ascii=False, indent=4)
            settings_file.write("\n")
        logger.debug(f"✅ Настройки сохранены: {SETTINGS_PATH}")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения настроек: {e}")


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
        logger.info(f"✅ Файл настроек создан: {SETTINGS_PATH}")

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


def read_admin_info():
    data = read_settings().get("telegram_admins", {})
    if not isinstance(data, dict):
        return {}
    return data


def parse_admin_ids(admin_id_value):
    placeholder = ""
    admin_ids = []
    for item in admin_id_value.split(","):
        item = item.strip()
        if not item:
            continue
        if item == placeholder:
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
    return [
        {"id": admin_id, "display": format_admin_display(admin_id, admin_info)}
        for admin_id in admin_ids
    ]


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
        mapping_list.append(
            {
                "telegram_id": telegram_id,
                "display": display,
                "client_name": client_name,
            }
        )
    mapping_list.sort(key=lambda item: item["client_name"].lower())
    return mapping_list


def build_available_admin_candidates(admin_info, admin_ids):
    available = []
    admin_id_set = set(admin_ids)
    for admin_id in admin_info.keys():
        if admin_id in admin_id_set:
            continue
        available.append(
            {"id": admin_id, "display": format_admin_display(admin_id, admin_info)}
        )
    available.sort(key=lambda item: item["display"].lower())
    return available


def restart_telegram_bot_async():
    """
    Перезапускает службу telegram-bot через supervisorctl.
    Возвращает кортеж (успех: bool, ошибка: str или None).
    """
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
                logger.info("✅ Бот telegram-bot успешно перезапущен")
                return True, None
            else:
                error_msg = result.stderr.strip() or result.stdout.strip() or "неизвестная ошибка"
                logger.error(f"❌ Ошибка перезапуска бота: {error_msg}")
                return False, error_msg
        except Exception as exc:
            logger.error(f"❌ Исключение при перезапуске бота: {exc}")
            return False, str(exc) or "неизвестная ошибка"


def restart_telegram_bot():
    """Запускает перезапуск в отдельном потоке"""
    thread = threading.Thread(target=restart_telegram_bot_async)
    thread.daemon = True
    thread.start()
    logger.info("🔄 Запущен асинхронный перезапуск бота")
    return True, None  # Возвращаем сразу успех


def stop_telegram_bot():
    """
    Останавливает службу telegram-bot через supervisorctl.
    Возвращает кортеж (успех: bool, ошибка: str или None).
    """
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
                logger.info("✅ Бот telegram-bot успешно остановлен")
                return True, None
            else:
                error_msg = result.stderr.strip() or result.stdout.strip() or "неизвестная ошибка"
                logger.error(f"❌ Ошибка остановки бота: {error_msg}")
                return False, error_msg
        except Exception as exc:
            logger.error(f"❌ Исключение при остановке бота: {exc}")
            return False, str(exc) or "неизвестная ошибка"


def get_telegram_bot_status():
    """
    Проверяет статус службы telegram-bot через supervisorctl.
    Возвращает True, если служба активна (RUNNING), False во всех остальных случаях.
    """
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
            logger.debug("🟢 Бот telegram-bot активен")
            return True
        logger.debug("🔴 Бот telegram-bot не активен")
        return False
    except Exception as e:
        logger.error(f"❌ Ошибка проверки статуса бота: {e}")
        return False


# Функция для подключения к базе данных SQLite
def get_db_connection():
    conn = sqlite3.connect(app.config["DATABASE_PATH"])
    conn.row_factory = sqlite3.Row  # Для получения результатов в виде словаря
    return conn


# Создаем таблицу для пользователей (один раз при старте)
def create_users_table():
    conn = get_db_connection()
    conn.execute(
        """CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL UNIQUE,
        role  TEXT NOT NULL,
        password TEXT NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()


# Вызываем функцию для создания таблицы при запуске приложения
create_users_table()


# Flask-Login: Загрузка пользователей по его ID
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


# Класс пользователя для Flask-Login
class User(UserMixin):
    def __init__(self, user_id, username, role, password):
        self.id = user_id
        self.username = username
        self.role = role
        self.password = password


# Функция для добавления нового пользователя с зашифрованным паролем
def add_user(username, role, password):
    conn = get_db_connection()
    # Проверяем, существует ли пользователь с таким именем
    existing_user = conn.execute(
        "SELECT * FROM users WHERE username = ?", (username,)
    ).fetchone()
    if existing_user:
        print(f"Пользователь уже существует.")
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
    logger.info(f"✅ Пользователь {username} успешно добавлен")
    return


# Функция для генерации случайного пароля
def get_random_pass(length=10):
    characters = string.ascii_letters + string.digits  # Буквы и цифры
    random_pass = "".join(random.choice(characters) for _ in range(length))
    return random_pass


# Добавление администратора при первом запуске
def add_admin():
    conn = get_db_connection()
    passw = get_random_pass()
    count = conn.execute("SELECT COUNT(*) FROM users WHERE role = 'admin'").fetchone()[0]
    if count < 1:
        add_user("admin", "admin", passw)
        logger.info(f"🔑 Создан администратор. Пароль: {passw}")
    else:
        logger.debug("ℹ️ Администратор уже существует")
    conn.close()
    return passw


# Функция для изменения пароля администратора
def change_admin_password():
    conn = get_db_connection()
    admin_user = conn.execute("SELECT * FROM users WHERE role = 'admin'").fetchone()

    if not admin_user:
        logger.warning("⚠️ Администратор не найден.")
        conn.close()
        return

    passw = get_random_pass()  # Генерация нового пароля
    hashed_password = bcrypt.generate_password_hash(passw).decode("utf-8")

    conn.execute(
        "UPDATE users SET password = ? WHERE username = ? AND role = 'admin'",
        (hashed_password, "admin"),
    )
    conn.commit()
    conn.close()

    print(f"{passw}")
    logger.info(f"🔑 Пароль администратора изменён: {passw}")


def change_admin_password_2(new_password):
    """
    Функция для изменения пароля администратора через переданное значение.
    :param new_password: Новый пароль администратора (строка).
    """
    if not new_password:
        logger.warning("⚠️ Новый пароль не может быть пустым.")
        return
    # Подключаемся к базе данных
    conn = get_db_connection()
    admin_user = conn.execute("SELECT * FROM users WHERE role = 'admin'").fetchone()

    if not admin_user:
        logger.warning("⚠️ Администратор не найден.")
        conn.close()
        return

    # Хешируем новый пароль
    hashed_password = bcrypt.generate_password_hash(new_password).decode("utf-8")

    # Обновляем пароль администратора
    conn.execute(
        "UPDATE users SET password = ? WHERE username = ? AND role = 'admin'",
        (hashed_password, "admin"),
    )
    conn.commit()
    conn.close()
    print(f"Пароль администратора успешно изменён: {new_password}")
    logger.info(f"✅ Пароль администратора успешно изменён")


# ---------WireGuard----------
# Функция для получения данных WireGuard
def get_wireguard_stats():
    try:
        # 1. Получаем ID контейнера
        id_result = subprocess.run(
            ['docker', 'ps', '--filter', 'name=amnezia', '--format', '{{.ID}}'],
            capture_output=True, text=True, check=True
        )
        container_id = id_result.stdout.strip().splitlines()[0] if id_result.stdout.strip() else None
        
        if not container_id:
            return "Ошибка: Контейнер amnezia не найден"

        # 2. Выполняем wg show внутри контейнера
        result = subprocess.run(
            ['docker', 'exec', container_id, '/usr/bin/wg', 'show'],
            capture_output=True, text=True, check=True
        )
        logger.debug("✅ Команда wg show выполнена успешно через Docker")
        return result.stdout
        
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Ошибка Docker/wg: {e.stderr}")
        return f"Ошибка выполнения команды: {e.stderr}"
    except Exception as e:
        logger.error(f"❌ Исключение: {e}")
        return f"Ошибка: {str(e)}"


def format_handshake_time(handshake_string):
    time_units = re.findall(r"(\d+)\s+(\w+)", handshake_string)
    # Словарь для перевода единиц времени в сокращения
    abbreviations = {
        "year": "г.",
        "years": "г.",
        "month": "мес.",
        "months": "мес.",
        "week": "нед.",
        "weeks": "нед.",
        "day": "дн.",
        "days": "дн.",
        "hour": "ч.",
        "hours": "ч.",
        "minute": "мин.",
        "minutes": "мин.",
        "second": "сек.",
        "seconds": "сек.",
    }

    # Формируем сокращенную строку
    formatted_time = " ".join(
        f"{value} {abbreviations[unit]}" for value, unit in time_units
    )

    return formatted_time


def is_peer_online(last_handshake):
    if not last_handshake:
        return False
    return datetime.now() - last_handshake < timedelta(minutes=3)


def parse_relative_time(relative_time):
    """Преобразует строку с днями, часами, минутами и секундами в абсолютное время."""
    now = datetime.now()
    time_deltas = {"days": 0, "hours": 0, "minutes": 0, "seconds": 0}
    # Разбиваем строку на части
    parts = relative_time.split()
    i = 0
    while i < len(parts):
        try:
            value = int(parts[i])  # Извлекаем число
            unit = parts[i + 1]  # Следующее слово — это единица времени
            if "д" in unit or "day" in unit:
                time_deltas["days"] += value
            elif "ч" in unit or "hour" in unit:
                time_deltas["hours"] += value
            elif "мин" in unit or "minute" in unit:
                time_deltas["minutes"] += value
            elif "сек" in unit or "second" in unit:
                time_deltas["seconds"] += value
            i += 2  # Пропускаем число и единицу времени
        except (ValueError, IndexError):
            break  # Если данные некорректны, прерываем

    # Вычисляем итоговую разницу времени
    delta = timedelta(
        days=time_deltas["days"],
        hours=time_deltas["hours"],
        minutes=time_deltas["minutes"],
        seconds=time_deltas["seconds"],
    )

    return now - delta


def read_wg_config(file_path):
    """Считывает клиентские данные из конфигурационного файла WireGuard."""
    client_mapping = {}
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            current_client_name = None

            for line in file:
                line = line.strip()

                # Если строка начинается с # Client =, то сохраняем имя клиента
                if line.startswith("# Client ="):
                    current_client_name = line.split("=", 1)[1].strip()

                # Если строка начинается с [Peer], сбрасываем имя клиента
                elif line.startswith("[Peer]"):
                    # Проверяем, есть ли имя клиента, если нет, то оставляем 'N/A'
                    current_client_name = current_client_name or "N/A"

                # Если строка начинается с PublicKey =, сохраняем публичный ключ с именем клиента
                elif line.startswith("PublicKey =") and current_client_name:
                    public_key = line.split("=", 1)[1].strip()
                    client_mapping[public_key] = current_client_name

    except FileNotFoundError:
        logger.warning(f"⚠️ Конфигурационный файл {file_path} не найден.")
    except Exception as e:
        logger.error(f"❌ Ошибка чтения конфига WireGuard {file_path}: {e}")

    return client_mapping

# ========= WireGuard Toggle Functions =========
def get_disabled_wg_peers():
    """Получает отключённых пиров из конфигурационных файлов WireGuard."""
    configs = {
        "vpn": "/etc/wireguard/vpn.conf",
        "antizapret": "/etc/wireguard/antizapret.conf",
    }
    result = {}
    for interface, config_path in configs.items():
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
        except FileNotFoundError:
            continue

        disabled = []
        i = 0
        while i < len(lines):
            s = lines[i].strip()
            if s.startswith("# Client ="):
                client_name = s.split("=", 1)[1].strip()
                for j in range(i + 1, min(i + 3, len(lines))):
                    if lines[j].strip().startswith("#~ [Peer]"):
                        public_key = None
                        allowed_ips = []
                        for k in range(j + 1, min(j + 10, len(lines))):
                            ks = lines[k].strip()
                            if ks.startswith("#~ PublicKey ="):
                                public_key = ks.split("=", 1)[1].strip()
                            elif ks.startswith("#~ AllowedIPs ="):
                                allowed_ips = [ip.strip() for ip in ks.split("=", 1)[1].strip().split(",")]
                            elif not ks.startswith("#~") and ks != "":
                                break
                        if public_key:
                            masked = public_key[:4] + "..." + public_key[-4:]
                            disabled.append({
                                "peer": public_key,
                                "masked_peer": masked,
                                "client": client_name,
                                "enabled": False,
                                "online": False,
                                "endpoint": "N/A",
                                "visible_ips": allowed_ips[:1],
                                "hidden_ips": allowed_ips[1:],
                                "latest_handshake": None,
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
                            })
                        break
            i += 1
        if disabled:
            result[interface] = disabled
    return result

def toggle_peer_config(config_path, public_key, enable):
    """Включает или отключает пир в конфигурационном файле WireGuard."""
    with open(config_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    
    key_line_idx = None
    for i, line in enumerate(lines):
        s = line.strip()
        clean = s.replace("#~ ", "", 1) if s.startswith("#~ ") else s
        if clean.startswith("PublicKey =") and public_key in clean:
            key_line_idx = i
            break
    
    if key_line_idx is None:
        return False

    block_start = key_line_idx
    for i in range(key_line_idx - 1, -1, -1):
        s = lines[i].strip()
        if s.startswith("# Client ="):
            block_start = i
            break
        elif s.startswith("[Peer]") or s.startswith("#~ [Peer]"):
            block_start = i
            break

    block_end = key_line_idx + 1
    for i in range(key_line_idx + 1, len(lines)):
        s = lines[i].strip()
        if s.startswith("# Client =") or s.startswith("[Interface]") or s.startswith("[Peer]"):
            block_end = i
            break
        block_end = i + 1

    new_lines = lines[:block_start]
    for i in range(block_start, block_end):
        line = lines[i]
        s = line.strip()
        if enable:
            if s.startswith("#~ "):
                new_lines.append(line.replace("#~ ", "", 1))
            else:
                new_lines.append(line)
        else:
            if s == "" or s.startswith("#"):
                new_lines.append(line)
            else:
                new_lines.append("#~ " + line.lstrip())
    new_lines.extend(lines[block_end:])

    with open(config_path, "w", encoding="utf-8") as f:
        f.writelines(new_lines)
    return True

def get_daily_stats_map():
    """Получение ежедневной статистики WG"""
    today = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(app.config["WG_STATS_PATH"])
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM wg_daily_stats WHERE date = ?", (today,))
    rows = cursor.fetchall()
    conn.close()
    return {(row["peer"], row["interface"]): row for row in rows}


def humanize_bytes(num, suffix="B"):
    """Функция для преобразования байт в удобный формат"""
    for unit in ["", "K", "M", "G", "T"]:
        if abs(num) < 1024.0:
            return f"{num:.1f} {unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f} P{suffix}"


def parse_wireguard_output(output, hide_ip=True):
    """Парсинг вывода команды wg show с опцией скрытия IP."""
    stats = []
    lines = output.strip().splitlines()
    interface_data = {}
    vpn_mapping = read_wg_config("/etc/wireguard/vpn.conf")
    antizapret_mapping = read_wg_config("/etc/wireguard/antizapret.conf")
    client_mapping = {**vpn_mapping, **antizapret_mapping}
    daily_stats_map = get_daily_stats_map()
    
    for line in lines:
        line = line.strip()
        if line.startswith("interface:"):
            if interface_data:
                stats.append(interface_data)
                interface_data = {}
            interface_data["interface"] = line.split(":  ")[1]
        elif line.startswith("public key:"):
            public_key = line.split(":  ")[1]
            interface_data["public_key"] = public_key
        elif line.startswith("listening port:"):
            interface_data["listening_port"] = line.split(":  ")[1]
        elif line.startswith("peer:"):
            if "peers" not in interface_data:
                interface_data["peers"] = []
            peer_data = {"peer": line.split(":  ")[1].strip()}
            masked_peer = peer_data["peer"][:4] + "..." + peer_data["peer"][-4:]
            peer_data["masked_peer"] = masked_peer
            peer_data["client"] = client_mapping.get(peer_data["peer"], "N/A")

            daily_row = daily_stats_map.get(
                (peer_data["peer"], interface_data["interface"])
            )
            if daily_row:
                peer_data["daily_received"] = humanize_bytes(daily_row["received"])
                peer_data["daily_sent"] = humanize_bytes(daily_row["sent"])
                try:
                    total = parse_bytes(peer_data["received"]) + parse_bytes(
                        peer_data["sent"]
                    )
                    daily_total = daily_row["received"] + daily_row["sent"]
                    round_res = round((daily_total / total * 100) if total > 0 else 0)
                    peer_data["daily_traffic_percentage"] = round_res
                except Exception:
                    peer_data["daily_traffic_percentage"] = 0
            else:
                peer_data["daily_received"] = "0 B"
                peer_data["daily_sent"] = "0 B"
                peer_data["daily_traffic_percentage"] = 0
            interface_data["peers"].append(peer_data)
        elif line.startswith("endpoint:"):
            peer_data["endpoint"] = mask_ip(line.split(":")[1].strip(), hide=hide_ip)
        elif line.startswith("allowed ips:"):
            allowed_ips = line.split(":  ")[1].split(",  ")
            peer_data["allowed_ips"] = allowed_ips
            peer_data["visible_ips"] = allowed_ips[:1]
            peer_data["hidden_ips"] = allowed_ips[1:]
        elif line.startswith("latest handshake:"):
            handshake_time = line.split(":  ")[1].strip()

            if handshake_time.lower() == "now":
                formatted_handshake_time = datetime.now()
                peer_data["latest_handshake"] = "Now"
                peer_data["online"] = True

            elif any(
                unit in handshake_time
                for unit in ["мин", "час", "сек", "minute", "hour", "second", "day", "week"]
            ):
                formatted_handshake_time = parse_relative_time(handshake_time)
                peer_data["latest_handshake"] = format_handshake_time(handshake_time)
                peer_data["online"] = is_peer_online(formatted_handshake_time)

            else:
                formatted_handshake_time = datetime.strptime(
                    handshake_time, "%Y-%m-%d %H:%M:%S"
                )
                peer_data["latest_handshake"] = format_handshake_time(handshake_time)
                peer_data["online"] = is_peer_online(formatted_handshake_time)
    
        elif line.startswith("transfer:"):
            transfer_data = line.split(": ")[1].strip().split(",  ")
            received = transfer_data[0].replace(" received", " ").strip()
            sent = transfer_data[1].replace(" sent", " ").strip()

            received_str = transfer_data[0].replace(" received", " ").strip()
            sent_str = transfer_data[1].replace(" sent", " ").strip()

            # Конвертируем строки в байты
            peer_data["received_bytes"] = (
                parse_bytes(received_str) if received_str else 0
            )
            peer_data["sent_bytes"] = parse_bytes(sent_str) if sent_str else 0

            peer_data["received"] = received if received else "0 B"
            peer_data["sent"] = sent if sent else "0 B"

            total_bytes = peer_data["received_bytes"] + peer_data["sent_bytes"]
            peer_data["received_percentage"] = (
                round((peer_data["received_bytes"] / total_bytes * 100), 2)
                if total_bytes > 0
                else 0
            )
            peer_data["sent_percentage"] = (
                round((peer_data["sent_bytes"] / total_bytes * 100), 2)
                if total_bytes > 0
                else 0
            )

    if interface_data:
        stats.append(interface_data)

    logger.debug(f"✅ Распарсено {len(stats)} интерфейсов WireGuard")
    return stats


def get_daily_stats():
    """Получение ежедневной статистики"""
    conn = sqlite3.connect(app.config["WG_STATS_PATH"])
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    date_today = date.today().isoformat()
    cursor.execute(
        "SELECT interface, client, received, sent FROM wg_daily_stats WHERE date = ?",
        (date_today,),
    )
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


# ---------OpenVPN----------
# Функция для преобразования байт в удобный формат
def format_bytes(size):
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"


def parse_bytes(value):
    """Преобразует строку с размером данных в байты."""
    size, unit = value.split("  ")
    size = float(size)
    unit = unit.lower()
    if unit == "kb":
        return size * 1024
    elif unit == "mb":
        return size * 1024 ** 2
    elif unit == "gb":
        return size * 1024 ** 3
    elif unit == "tb":
        return size * 1024 ** 4
    return size


# Функция для склонения слова "клиент"
def pluralize_clients(count):
    if 11 <= count % 100 <= 19:
        return f"{count} клиентов"
    elif count % 10 == 1:
        return f"{count} клиент"
    elif 2 <= count % 10 <= 4:
        return f"{count} клиента"
    else:
        return f"{count} клиентов"


# Функция для получения внешнего IP-адреса
def get_external_ip():
    try:
        response = requests.get("https://api.ipify.org", timeout=10)
        if response.status_code == 200:
            return response.text
        logger.warning(f"⚠️ Не удалось получить внешний IP. Статус: {response.status_code}")
        return "IP не найден"
    except requests.Timeout:
        logger.error("❌ Ошибка: запрос превысил время ожидания при получении IP.")
        return "Ошибка: запрос превысил время ожидания."
    except requests.ConnectionError:
        logger.error("❌ Ошибка: нет подключения к интернету при получении IP.")
        return "Ошибка: нет подключения к интернету."
    except requests.RequestException as e:
        logger.error(f"❌ Ошибка при запросе внешнего IP: {e}")
        return f"Ошибка при запросе: {e}"


# Преобразование даты
def format_date(date_string):
    date_obj = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
    server_timezone = get_localzone()
    localized_date = date_obj.replace(tzinfo=server_timezone)
    utc_date = localized_date.astimezone(timezone.utc)
    return utc_date.isoformat()

def extract_protocol_from_address(real_address, config_protocol):
    """
    Извлекает протокол из Real Address.
    Если префикс не найден, возвращает протокол из config.py.
    """
    if not real_address:
        return config_protocol or "unknown"
    
    # Проверяем наличие префикса протокола (udp4:, tcp4:, udp6:, tcp6:)
    if ":" in real_address:
        parts = real_address.split(":", 1)
        if len(parts) == 2 and parts[0].lower() in ["udp4", "tcp4", "udp6", "tcp6"]:
            protocol = parts[0].lower()
            # Нормализуем названия протоколов
            if protocol in ["udp4", "udp6"]:
                return "UDP"
            elif protocol in ["tcp4", "tcp6"]:
                return "TCP"
    
    # Если префикс не найден, используем протокол из config.py
    return config_protocol or "unknown"


def mask_ip(ip_address, hide=True):
    """Маскирует IP адрес с опцией скрытия."""
    if not ip_address:
        return "0.0.0.0"
    
    # Убираем префикс протокола (udp4:, tcp4:, udp6:, tcp6:)
    if ":" in ip_address:
        parts = ip_address.split(":", 1)
        if len(parts) == 2 and parts[0].lower() in ["udp4", "tcp4", "udp6", "tcp6"]:
            ip_address = parts[1]

    # Извлекаем IP из IP:PORT
    ip = ip_address.split(":")[0] if ":" in ip_address else ip_address
    port = ":" + ip_address.split(":")[1] if ":" in ip_address else ""

    parts = ip.split(".")
    if len(parts) == 4:
        try:
            parts = [str(int(part)) for part in parts]
            if hide:
                return f"{parts[0]}.***.***.{parts[3]}{port}"
            return f"{parts[0]}.{parts[1]}.{parts[2]}.{parts[3]}{port}"
        except ValueError:
            return ip_address

    return ip_address


# Отсчет времени
def format_duration(start_time):
    now = datetime.now()  # Текущее время
    delta = now - start_time  # Разница во времени
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


# Чтение данных из CSV и обработка
def read_csv(file_path, config_protocol):
    data = []
    total_received, total_sent = 0, 0
    current_time = datetime.now()
    if not os.path.exists(file_path):
        logger.warning(f"⚠️ Файл логов не найден: {file_path}")
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

                # 🔹 Извлекаем протокол из Real Address или берём из config
                protocol = extract_protocol_from_address(real_address, config_protocol)

                # Получение предыдущих данных из кэша
                previous_data = client_cache.get(
                    client_name, {"received": 0, "sent": 0, "timestamp": current_time}
                )
                previous_received = previous_data["received"]
                previous_sent = previous_data["sent"]
                previous_time = previous_data["timestamp"]

                # Рассчитываем скорость только при валидной разнице времени
                time_diff = (current_time - previous_time).total_seconds()
                if time_diff >= 30:  # Учитываем фиксированный интервал обновления логов
                    download_speed = (
                        (received - previous_received) / time_diff
                        if received >= previous_received
                        else 0
                    )
                    upload_speed = (
                        (sent - previous_sent) / time_diff
                        if sent >= previous_sent
                        else 0
                    )
                else:
                    download_speed = 0
                    upload_speed = 0

                # Обновляем кэш
                client_cache[client_name] = {
                    "received": received,
                    "sent": sent,
                    "timestamp": current_time,
                }

                # Добавляем данные клиента
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
                    ]
                )

    logger.debug(f"✅ Прочитано {len(data)} клиентов из {file_path}")
    return data, total_received, total_sent, None


# ---------Метрики----------
def ensure_db():
    """Создает таблицу system_stats, если она не существует."""
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


def save_minute_average_to_db():
    """Сохраняет средние значения CPU и RAM за последний интервал в БД."""
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
        # записываем timestamp = now (local)
        cur.execute(
            "INSERT INTO system_stats (timestamp, cpu_percent, ram_percent) VALUES (?, ?, ?)",
            (now.strftime("%Y-%m-%d %H:%M:%S"), round(cpu_avg, 3), round(ram_avg, 3)),
        )

        # Очищаем старые записи старше 7 дней
        cutoff_db = now - timedelta(days=7)
        cur.execute(
            "DELETE FROM system_stats WHERE timestamp < ?",
            (cutoff_db.strftime("%Y-%m-%d %H:%M:%S"),),
        )

        conn.commit()
        conn.close()
        logger.debug(f"✅ Сохранены метрики в БД: CPU={cpu_avg:.2f}%, RAM={ram_avg:.2f}%")
    except Exception as e:
        logger.error(f"[DB ERROR] save_minute_average_to_db: {e}")


def group_rows(rows, interval="minute"):
    """Группирует ряды по интервалу и усредняет значения CPU и RAM."""
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

    # Усреднение
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
    """Возвращает ровно n точек (если меньше — возвращает всё). Берёт равномерно распределённые индексы."""
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
        result = subprocess.run(
            ["/usr/bin/ip", "route"], capture_output=True, text=True, check=True
        )
        for line in result.stdout.splitlines():
            if "default" in line:
                return line.split()[4]
    except Exception as e:
        logger.error(f"❌ Ошибка получения интерфейса: {e}")
        return None


def get_network_stats(interface):
    try:
        with open(
            f"/sys/class/net/{interface}/statistics/rx_bytes", "r", encoding="utf-8"
        ) as f:
            rx_bytes = int(f.read().strip())
        with open(
            f"/sys/class/net/{interface}/statistics/tx_bytes", "r", encoding="utf-8"
        ) as f:
            tx_bytes = int(f.read().strip())
        return {"interface": interface, "rx": rx_bytes, "tx": tx_bytes}
    except FileNotFoundError:
        logger.warning(f"⚠️ Интерфейс {interface} не найден")
        return None  # Если интерфейс не найден


def get_network_load():
    net_io_start = psutil.net_io_counters(pernic=True)
    time.sleep(1)
    net_io_end = psutil.net_io_counters(pernic=True)
    network_data = {}
    for interface in net_io_start:
        if interface.startswith(("lo", "docker", "veth", "br-")):
            continue

        sent_start, recv_start = (
            net_io_start[interface].bytes_sent,
            net_io_start[interface].bytes_recv,
        )
        sent_end, recv_end = (
            net_io_end[interface].bytes_sent,
            net_io_end[interface].bytes_recv,
        )

        sent_speed = (sent_end - sent_start) * 8 / 1e6
        recv_speed = (recv_end - recv_start) * 8 / 1e6

        if sent_speed > 0 or recv_speed > 0:
            network_data[interface] = {
                "sent_speed": round(sent_speed, 2),
                "recv_speed": round(recv_speed, 2),
            }

    return network_data


def get_uptime():
    try:
        uptime = (
            subprocess.check_output("/usr/bin/uptime -p", shell=True).decode().strip()
        )
    except subprocess.CalledProcessError:
        uptime = "Не удалось получить время работы"
        logger.warning("⚠️ Не удалось получить uptime системы")
    return uptime


def format_uptime(uptime_string):
    # Регулярное выражение с учетом лет, месяцев, недель, дней, часов и минут
    pattern = r"(?:(\d+)\syears?|(\d+)\smonths?|(\d+)\sweeks?|(\d+)\sdays?|(\d+)\shours?|(\d+)\sminutes?)"
    years = 0
    months = 0
    weeks = 0
    days = 0
    hours = 0
    minutes = 0

    matches = re.findall(pattern, uptime_string)

    for match in matches:
        if match[0]:  # Годы
            years = int(match[0])
        elif match[1]:  # Месяцы
            months = int(match[1])
        elif match[2]:  # Недели
            weeks = int(match[2])
        elif match[3]:  # Дни
            days = int(match[3])
        elif match[4]:  # Часы
            hours = int(match[4])
        elif match[5]:  # Минуты
            minutes = int(match[5])

    # Итоговая строка
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
    # Подсчёт WireGuard
#    try:
#        wg_output = subprocess.check_output(["/usr/bin/wg", "show"], text=True)
#        wg_latest_handshakes = re.findall(r"latest handshake: (.+)", wg_output)
#        online_wg = 0
#        for handshake in wg_latest_handshakes:
#            handshake_str = handshake.strip()
#            if handshake_str == "0 seconds ago":
#                online_wg += 1
#            else:
#                try:
                    # Используем parse_relative_time и is_peer_online для определения онлайн-статуса
#                    handshake_time = parse_relative_time(handshake_str)
#                    if is_peer_online(handshake_time):
#                        online_wg += 1
#                except Exception:
#                    continue
#        results["WireGuard"] = online_wg
#    except Exception as e:
#        logger.error(f"❌ Ошибка подсчёта клиентов WireGuard: {e}")
#        results["WireGuard"] = 0
    
    # Подсчёт OpenVPN
    for path, _ in file_paths:
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("CLIENT_LIST"):
                        total_openvpn += 1
        except Exception as e:
            logger.warning(f"⚠️ Ошибка чтения файла логов {path}: {e}")
            continue

    results["OpenVPN"] = total_openvpn
    logger.debug(f"📊 Онлайн клиенты: OVPN={results['OpenVPN']}")
#    logger.debug(f"📊 Онлайн клиенты: WG={results['WireGuard']}, OVPN={results['OpenVPN']}")
    return results

# Метрики для статистики скорости для активных пользователей OpenVPN
def ensure_ovpn_stats_db():
    """Создает таблицу ovpn_speed_stats, если она не существует."""
    conn = sqlite3.connect(app.config["LOGS_DATABASE_PATH"])
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ovpn_speed_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME,
        client_name TEXT,
        rx_speed REAL,
        tx_speed REAL
    )
    """)
    conn.commit()
    conn.close()


def save_ovpn_stats_to_db():
    """Сохраняет средние значения скорости OpenVPN за последний интервал в БД."""
    now = datetime.now().astimezone()
    cutoff = now - timedelta(seconds=OVPN_DB_SAVE_INTERVAL)
    
    with ovpn_stats_lock:
        for client, stats in ovpn_live_stats.items():
            if client == "UNDEF":
                continue
            to_avg = []
            for i, ts_str in enumerate(stats["timestamps"]):
                try:
                    ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                    ts_local = ts.astimezone() 
                    logger.debug(f"🕐 Клиент {client}: ts_str={ts_str}, ts_local={ts_local}, cutoff={cutoff}")
                    if ts_local >= cutoff:
                        to_avg.append({
                            "timestamp": ts_local,
                            "rx": stats["rx_speed"][i] if i < len(stats["rx_speed"]) else 0,
                            "tx": stats["tx_speed"][i] if i < len(stats["tx_speed"]) else 0
                        })
                except Exception as e:
                    logger.error(f"❌ Ошибка парсинга времени {ts_str}: {e}")
                    continue
            
            if not to_avg:
                logger.debug(f"⚠️ Нет данных для клиента {client} за интервал (точек: {len(to_avg)})")
                continue
            
            # Рассчитываем среднее
            avg_rx = mean([p["rx"] for p in to_avg]) if to_avg else 0
            avg_tx = mean([p["tx"] for p in to_avg]) if to_avg else 0
            
            try:
                conn = sqlite3.connect(app.config["LOGS_DATABASE_PATH"])
                cur = conn.cursor()
                # записываем timestamp = now (local)
                cur.execute(
                    "INSERT INTO ovpn_speed_stats (timestamp, client_name, rx_speed, tx_speed) VALUES (?, ?, ?, ?)",
                    (now.strftime("%Y-%m-%d %H:%M:%S"), client, round(avg_rx, 2), round(avg_tx, 2))
                )
                
                # Очищаем старые записи старше 7 дней
                cutoff_db = now - timedelta(days=7)
                cur.execute(
                    "DELETE FROM ovpn_speed_stats WHERE timestamp < ? AND client_name = ?",
                    (cutoff_db.strftime("%Y-%m-%d %H:%M:%S"), client)
                )
                
                conn.commit()
                conn.close()
                logger.debug(f"✅ Сохранены OVPN stats для {client}: RX={avg_rx:.2f} бит/с, TX={avg_tx:.2f} бит/с")
            except Exception as e:
                logger.error(f"[DB ERROR] save_ovpn_stats_to_db для {client}: {e}")


def ovpn_db_save_loop():
    """Фоновый цикл сохранения статистики OpenVPN в БД"""
    logger.info("✅ Поток ovpn_db_save_loop ЗАПУЩЕН")
    global ovpn_last_db_save
    ensure_ovpn_stats_db()
    
    while True:
        now = time.time()
        
        # сохранить среднее в БД каждые OVPN_DB_SAVE_INTERVAL
        if now - ovpn_last_db_save >= OVPN_DB_SAVE_INTERVAL:
            logger.info(f"🔄 Попытка сохранения (клиентов в памяти: {len(ovpn_live_stats)})")
            save_ovpn_stats_to_db()
            ovpn_last_db_save = now

        time.sleep(1)

# -----------------------------------------------


def get_system_info():
    global cached_system_info
    return cached_system_info


def update_system_info_loop():
    global last_db_save, last_collect
    ensure_db()
    while True:
        now = time.time()
        if now - last_collect >= SAMPLE_INTERVAL:
            # psutil: non-blocking
            cpu = psutil.cpu_percent(interval=None)
            ram = psutil.virtual_memory().percent
            ts = datetime.now()
            cpu_history.append({"timestamp": ts, "cpu": cpu, "ram": ram})
            # trim by time (keep at most MAX_HISTORY_SECONDS seconds)
            cutoff = datetime.now() - timedelta(seconds=MAX_HISTORY_SECONDS)
            # remove from start while older than cutoff
            while cpu_history and cpu_history[0]["timestamp"] < cutoff:
                cpu_history.pop(0)
            last_collect = now

        # сохранить среднее в БД каждые DB_SAVE_INTERVAL
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

            # Обновление live истории в памяти
            cpu_history.append(
                {"timestamp": timestamp, "cpu": cpu_percent, "ram": ram_percent}
            )
            if len(cpu_history) > MAX_CPU_HISTORY:
                cpu_history.pop(0)  # удаляем старые записи

            interface = get_default_interface()
            # Проверяем, чтобы интерфейс не начинался с lo, docker, veth, br-
            if interface and not interface.startswith(("lo", "docker", "veth", "br-")):
                network_stats = get_network_stats(interface) if interface else None
            else:
                network_stats = None
            vpn_clients = count_online_clients(LOG_FILES)

            cached_system_info = {
                "cpu_load": cpu_percent,
                "memory_used": psutil.virtual_memory().used // (1024 ** 2),
                "memory_total": psutil.virtual_memory().total // (1024 ** 2),
                "disk_used": psutil.disk_usage("/").used // (1024 ** 3),
                "disk_total": psutil.disk_usage("/").total // (1024 ** 3),
                "network_load": get_network_load(),
                "uptime": format_uptime(get_uptime()),
                "network_interface": interface or "Не найдено",
                "rx_bytes": format_bytes(network_stats["rx"]) if network_stats else 0,
                "tx_bytes": format_bytes(network_stats["tx"]) if network_stats else 0,
                "vpn_clients": vpn_clients,
            }

            last_fetch_time = current_time
            logger.debug(f"📊 Системная информация обновлена: CPU={cpu_percent}%, RAM={ram_percent}%")

        time.sleep(CACHE_DURATION)


def update_ovpn_live_stats():
    """Собирает трафик OpenVPN каждую минуту в память (скорость в бит/с)"""
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
                                    
                                    # Получаем предыдущие значения
                                    prev = ovpn_last_bytes.get(client, {
                                        "rx": 0, "tx": 0, "time": current_time
                                    })
                                    
                                    # Рассчитываем дельту
                                    time_diff = (current_time - prev["time"]).total_seconds()
                                    
                                    # Защита от сброса счётчика
                                    if rx_bytes < prev["rx"]:
                                        rx_bytes = 0
                                    if tx_bytes < prev["tx"]:
                                        tx_bytes = 0
                                    
                                    rx_delta = max(0, rx_bytes - prev["rx"])
                                    tx_delta = max(0, tx_bytes - prev["tx"])
                                    
                                    # Конвертируем в бит/с
                                    if time_diff > 0:
                                        rx_speed = (rx_delta * 8) / time_diff
                                        tx_speed = (tx_delta * 8) / time_diff
                                    else:
                                        rx_speed = 0
                                        tx_speed = 0
                                    
                                    # Сохраняем скорость (не байты!)
                                    with ovpn_stats_lock:
                                        stats = ovpn_live_stats[client]
                                        stats["timestamps"].append(current_time.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
                                        stats["rx_speed"].append(round(rx_speed, 2))  # бит/с
                                        stats["tx_speed"].append(round(tx_speed, 2))  # бит/с
                                        
                                        if len(stats["rx_speed"]) > MAX_OVPN_LIVE_POINTS:
                                            stats["timestamps"].pop(0)
                                            stats["rx_speed"].pop(0)
                                            stats["tx_speed"].pop(0)
                                    
                                    # Сохраняем для следующего цикла
                                    ovpn_last_bytes[client] = {
                                        "rx": rx_bytes,
                                        "tx": tx_bytes,
                                        "time": current_time
                                    }
            
            time.sleep(5)  # Опрос раз в 5 секунд
            
        except Exception as e:
            logger.error(f"Ошибка сбора Live stats: {e}")
            time.sleep(5)


# Запуск фоновой задачи
threading.Thread(target=update_system_info, daemon=True).start()
threading.Thread(target=update_system_info_loop, daemon=True).start()
threading.Thread(target=update_ovpn_live_stats, daemon=True).start()
threading.Thread(target=ovpn_db_save_loop, daemon=True).start() 
logger.info("✅ Фоновые задачи мониторинга запущены")


def get_vnstat_interfaces():
    try:
        result = subprocess.run(
            ["/usr/bin/vnstat", "--json"], capture_output=True, text=True, check=True
        )
        data = json.loads(result.stdout)
        interfaces = []
        for iface in data.get("interfaces", []):
            name = iface.get("name")
            traffic = iface.get("traffic", {}).get("total", {})
            rx = traffic.get("rx", 0)
            tx = traffic.get("tx", 0)

            # Добавляем только если есть трафик
            if (rx + tx) > 0:
                interfaces.append(name)

        return interfaces

    except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
        logger.error(f"❌ Ошибка при получении интерфейсов vnstat: {e}")
        return []


# Маршрут для выхода из системы
@app.route("/logout", methods=["GET", "POST"])
@login_required
def logout():
    username = current_user.username
    logout_user()
    session.pop("last_activity", None)
    logger.info(f"👤 Пользователь {username} вышел из системы")
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
        user = conn.execute(
            "SELECT * FROM users WHERE username = ?", (form.username.data,)
        ).fetchone()
        conn.close()
        
        if user and bcrypt.check_password_hash(user["password"], form.password.data):
            user_obj = User(
                user_id=user["id"],
                username=user["username"],
                role=user["role"],
                password=user["password"],
            )
            login_user(user_obj, remember=form.remember_me.data)
            logger.info(f"✅ Пользователь {form.username.data} успешно вошёл в систему")
            
            session.permanent = form.remember_me.data
            
            # ✅ Аудит входа
            client_ip = request.headers.get("X-Forwarded-For", request.remote_addr)
            if client_ip and ", " in client_ip:
                client_ip = client_ip.split(", ")[0].strip()
            log_action("web", user["username"], user["username"], "web_login", "", client_ip or "")
            
            next_page = request.args.get("next")
            return redirect(next_page or url_for("home"))
        else:
            error_message = "Неправильный логин или пароль!"
            logger.warning(f"⚠️ Неудачная попытка входа для пользователя: {form.username.data}")
    
    resp = make_response(
        render_template("login.html", form=form, error_message=error_message)
    )
    resp.headers["X-Robots-Tag"] = "noindex, nofollow"
    return resp


def get_git_version():
    try:
        version = (
            subprocess.check_output(
                ["/usr/bin/git", "describe", "--tags", "--abbrev=0"],
                stderr=subprocess.DEVNULL,
            )
            .strip()
            .decode()
        )
        logger.debug(f"📦 Git версия: {version}")
        return version
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        logger.warning(f"⚠️ Не удалось получить Git версию: {e}")
        return "unknown"


def get_docker_hub_version():
    """
    Получает последний семантический тег из Docker Hub.
    Возвращает актуальную версию, отсортированную по семантическому версионированию.
    """
    try:
        # Запрашиваем теги с Docker Hub API
        response = requests.get(
            DOCKER_HUB_API,
            params={"page_size": 100},
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        tags = [t["name"] for t in data.get("results", []) if t.get("name")]
        
        # Фильтруем теги: оставляем только семантические версии (vX.Y.Z или X.Y.Z)
        import re
        semver_pattern = re.compile(r'^v?(\d+)\.(\d+)\.(\d+)$')
        version_tags = []
        
        for tag in tags:
            match = semver_pattern.match(tag)
            if match:
                # Сохраняем тег и его числовые компоненты для сортировки
                version_tags.append({
                    'tag': tag,
                    'major': int(match.group(1)),
                    'minor': int(match.group(2)),
                    'patch': int(match.group(3))
                })
        
        if version_tags:
            # Сортируем версии по major.minor.patch (от новой к старой)
            version_tags.sort(
                key=lambda v: (v['major'], v['minor'], v['patch']),
                reverse=True
            )
            # Возвращаем самую новую версию
            version = version_tags[0]['tag']
            logger.debug(f"📦 Docker Hub версия: {version}")
            return version
        
        # fallback: если нет semver-тегов, возвращаем первый непустой тег
        version = tags[0] if tags else "unknown"
        logger.debug(f"📦 Docker Hub версия (fallback): {version}")
        return version
        
    except requests.RequestException as e:
        logger.warning(f"⚠️ Failed to fetch version from Docker Hub: {e}")
        return "unknown"
    except Exception as e:
        logger.error(f"❌ Unexpected error fetching Docker Hub version: {e}")
        return "unknown"


@app.context_processor
def inject_info():
    app_name = read_settings().get("app_name", "OpenVPN-Status")
    return {
        "hostname": socket.gethostname(),
        "server_ip": get_external_ip(),
#        "version": get_git_version(),
        "version": get_docker_hub_version(),
        "base_path": request.script_root or "",
        "app_name": app_name,
    }


@app.route("/")
@login_required
def home():
    server_ip = get_external_ip()
    system_info = get_system_info()
    hostname = socket.gethostname()
    logger.debug(f"📄 Запрошена главная страница пользователем {current_user.username}")
    return render_template(
        "index.html",
        server_ip=server_ip,
        system_info=system_info,
        hostname=hostname,
        active_page="home",
    )


# ========= Обновлённый /settings (только общие настройки) =========
@app.route("/settings", methods=["GET", "POST"])
@login_required
def settings():
    app_message = None
    app_error = None
    ip_message = None
    
    if request.method == "POST":
        form_type = request.form.get("form_type")
        
        if form_type == "app_name":
            app_name = request.form.get("app_name", "").strip()
            write_settings({"app_name": app_name})
            if app_name:
                app_message = "Название приложения обновлено."
                logger.info(f"✅ Название приложения обновлено: {app_name}")
            else:
                app_message = "Название приложения убрано."
                logger.info("ℹ️ Название приложения сброшено")
        
        elif form_type == "ip_settings":
            hide_ovpn_ip = request.form.get("hide_ovpn_ip") == "on"
            hide_wg_ip = request.form.get("hide_wg_ip") == "on"
            write_settings({"hide_ovpn_ip": hide_ovpn_ip, "hide_wg_ip": hide_wg_ip})
            ip_message = "Настройки отображения IP сохранены."
            logger.info(f"✅ Настройки IP обновлены: OVPN={hide_ovpn_ip}, WG={hide_wg_ip}")
    
    settings_data = read_settings()
    current_app_name = settings_data.get("app_name", "OpenVPN-Status")
    hide_ovpn_ip = settings_data.get("hide_ovpn_ip", True)
    hide_wg_ip = settings_data.get("hide_wg_ip", True)
    
    logger.debug(f"📄 Запрошена страница настроек пользователем {current_user.username}")
    return render_template(
        "settings/settings.html",
        app_name=current_app_name,
        hide_ovpn_ip=hide_ovpn_ip,
        hide_wg_ip=hide_wg_ip,
        app_message=app_message,
        app_error=app_error,
        ip_message=ip_message,
        active_page="settings",
    )

# ========= Новый маршрут /settings/telegram =========
@app.route("/settings/telegram", methods=["GET", "POST"])
@login_required
def settings_telegram():
    bot_message = None
    bot_error = None
    
    if request.method == "POST":
        form_type = request.form.get("form_type")
        
        if form_type == "bot":
            old_env = read_env_values()
            old_token = old_env.get("BOT_TOKEN", "")
            old_admin_id = old_env.get("ADMIN_ID", "")
            old_settings = read_settings()
            old_bot_enabled = bool(old_settings.get("bot_enabled", False)) or get_telegram_bot_status()
            
            bot_token = request.form.get("bot_token", "").strip()
            admin_id = request.form.get("admin_id")
            if admin_id is None:
                admin_id = old_admin_id
            admin_id = admin_id.strip()
            bot_enabled = request.form.get("bot_enabled") == "on"
            update_env_values({"BOT_TOKEN": bot_token, "ADMIN_ID": admin_id})
            write_settings({"bot_enabled": bot_enabled})
            
            client_ip = request.headers.get("X-Forwarded-For", request.remote_addr)
            if client_ip and ", " in client_ip:
                client_ip = client_ip.split(", ")[0].strip()
            
            # Аудит изменений
            if bot_token != old_token:
                token_changed = "изменён" if bot_token else "удалён"
                log_action("web", current_user.username, current_user.username, "bot_token_change", token_changed, client_ip or "")
                logger.info(f"🔑 Токен бота {token_changed}")
            
            if admin_id != old_admin_id:
                log_action("web", current_user.username, current_user.username, "bot_admins_change", f"{old_admin_id} → {admin_id}", client_ip or "")
                logger.info(f"👥 Админы бота изменены: {old_admin_id} → {admin_id}")
            
            should_start = bool(bot_enabled and bot_token)
            
            if should_start:
                restart_ok, restart_err = restart_telegram_bot()
                if restart_ok:
                    bot_message = "Настройки бота сохранены. Бот перезапущен."
                    if not old_bot_enabled:
                        log_action("web", current_user.username, current_user.username, "bot_toggle", "включён", client_ip or "")
                        logger.info("✅ Бот включён")
                else:
                    bot_error = f"Настройки бота сохранены, но перезапуск не удался: {restart_err}"
                    logger.error(f"❌ Ошибка перезапуска бота: {restart_err}")
            else:
                restart_ok, restart_error = stop_telegram_bot()
                if restart_ok:
                    if not bot_token:
                        bot_message = "Настройки бота сохранены. API токен бота пустой, бот остановлен."
                    else:
                        bot_message = "Настройки бота сохранены. Бот остановлен."
                    if old_bot_enabled:
                        log_action("web", current_user.username, current_user.username, "bot_toggle", "отключён", client_ip or "")
                        logger.info("🛑 Бот отключён")
                else:
                    bot_error = f"Настройки бота сохранены, но остановка не удалась: {restart_error}"
                    logger.error(f"❌ Ошибка остановки бота: {restart_error}")
    
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
    
    logger.debug(f"📄 Запрошена страница настроек Telegram пользователем {current_user.username}")
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

# ========= Новый маршрут /settings/audit =========
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
    
    logger.debug(f"📄 Запрошена страница аудита пользователем {current_user.username}")
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
        logger.warning(f"⚠️ Попытка добавить админа без ID")
        return jsonify({"success": False, "message": "ID не указан."}), 400
    admin_info = read_admin_info()

    env_values = read_env_values()
    admin_id_value = env_values.get("ADMIN_ID", "")
    admin_ids = parse_admin_ids(admin_id_value)
    if telegram_id in admin_ids:
        logger.info(f"ℹ️ Администратор {telegram_id} уже в списке")
        return (
            jsonify(
                {
                    "success": False,
                    "message": "Администратор уже в списке.",
                    "admins": build_admin_display_list(admin_id_value, admin_info),
                    "available_admins": build_available_admin_candidates(
                        admin_info, admin_ids
                    ),
                    "admin_id_value": admin_id_value,
                    "bot_service_active": get_telegram_bot_status(),
                }
            ),
            400,
        )

    admin_ids.append(telegram_id)
    updated_admin_id_value = format_admin_ids(admin_ids)
    update_env_values({"ADMIN_ID": updated_admin_id_value})

    admin_display_list = build_admin_display_list(updated_admin_id_value, admin_info)
    available_admins = build_available_admin_candidates(admin_info, admin_ids)
    response = {
        "success": True,
        "message": "Администратор добавлен. Нажмите «Сохранить», чтобы применить изменения.",
        "admins": admin_display_list,
        "available_admins": available_admins,
        "admin_id_value": updated_admin_id_value,
        "bot_service_active": get_telegram_bot_status(),
    }
    logger.info(f"✅ Администратор {telegram_id} добавлен в список")
    return jsonify(response), 200


@app.route("/api/admins/remove", methods=["POST"])
@login_required
def api_admins_remove():
    payload = request.get_json(silent=True) or {}
    telegram_id = str(payload.get("telegram_id", "")).strip()
    if not telegram_id:
        logger.warning(f"⚠️ Попытка удалить админа без ID")
        return jsonify({"success": False, "message": "ID не указан."}), 400
    admin_info = read_admin_info()
    env_values = read_env_values()
    admin_id_value = env_values.get("ADMIN_ID", "")
    admin_ids = parse_admin_ids(admin_id_value)
    if telegram_id not in admin_ids:
        logger.warning(f"⚠️ Администратор {telegram_id} не найден в списке")
        return (
            jsonify(
                {
                    "success": False,
                    "message": "Администратор не найден в списке.",
                    "admins": build_admin_display_list(admin_id_value, admin_info),
                    "available_admins": build_available_admin_candidates(
                        admin_info, admin_ids
                    ),
                    "admin_id_value": admin_id_value,
                    "bot_service_active": get_telegram_bot_status(),
                }
            ),
            400,
        )

    if len(admin_ids) <= 1:
        logger.warning(f"⚠️ Попытка удалить последнего администратора")
        return (
            jsonify(
                {
                    "success": False,
                    "message": "Нельзя удалить последнего администратора.",
                    "admins": build_admin_display_list(admin_id_value, admin_info),
                    "available_admins": build_available_admin_candidates(
                        admin_info, admin_ids
                    ),
                    "admin_id_value": admin_id_value,
                    "bot_service_active": get_telegram_bot_status(),
                }
            ),
            400,
        )

    admin_ids = [admin_id for admin_id in admin_ids if admin_id != telegram_id]
    updated_admin_id_value = format_admin_ids(admin_ids)
    update_env_values({"ADMIN_ID": updated_admin_id_value})

    admin_display_list = build_admin_display_list(updated_admin_id_value, admin_info)
    available_admins = build_available_admin_candidates(admin_info, admin_ids)
    response = {
        "success": True,
        "message": "Администратор удалён. Нажмите «Сохранить», чтобы применить изменения.",
        "admins": admin_display_list,
        "available_admins": available_admins,
        "admin_id_value": updated_admin_id_value,
        "bot_service_active": get_telegram_bot_status(),
    }
    logger.info(f"✅ Администратор {telegram_id} удалён из списка")
    return jsonify(response), 200


@app.route("/api/system_info")
@login_required
def api_system_info():
    system_info = get_system_info()
    return jsonify(system_info)


@app.route("/wg")
@login_required
def wg():
    hide_wg_ip = read_settings().get("hide_wg_ip", True)
    stats = parse_wireguard_output(get_wireguard_stats(), hide_ip=hide_wg_ip)
    disabled_peers = get_disabled_wg_peers()
    
    for interface_data in stats:
        for peer in interface_data.get("peers", []):
            peer["enabled"] = True
        iface = interface_data.get("interface")
        if iface in disabled_peers:
            interface_data.setdefault("peers", []).extend(disabled_peers[iface])
    
    logger.debug(f"📄 Запрошена страница WireGuard пользователем {current_user.username}")
    return render_template("wg/wg.html", stats=stats, active_section="wg", active_page="wg_clients")

@app.route("/api/wg/stats")
@login_required
def api_wg_stats():
    try:
        hide_wg_ip = read_settings().get("hide_wg_ip", True)
        stats = parse_wireguard_output(get_wireguard_stats(), hide_ip=hide_wg_ip)
        disabled_peers = get_disabled_wg_peers()
        
        for interface_data in stats:
            for peer in interface_data.get("peers", []):
                peer["enabled"] = True
            iface = interface_data.get("interface")
            if iface in disabled_peers:
                interface_data.setdefault("peers", []).extend(disabled_peers[iface])
        
        return jsonify(stats)
    except Exception as e:
        logger.error(f"❌ Ошибка получения статистики WireGuard: {e}")
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
        
        subprocess.run(
            [
                "/bin/bash",
                "-c",
                f"{wg_bin} syncconf {interface} <({wg_quick} strip {interface})",
            ],
            check=True,
            env={**os.environ, "PATH": "/usr/bin:/bin"},
        )
        
        client_name = data.get("client_name", peer[:8] + "...")
        action_str = "включён" if enable else "отключён"
        
        # ✅ Аудит действия
        client_ip = request.headers.get("X-Forwarded-For", request.remote_addr)
        if client_ip and ", " in client_ip:
            client_ip = client_ip.split(", ")[0].strip()
        log_action("web", current_user.username, current_user.username, "peer_toggle", f"{client_name} ({action_str})", client_ip or "")
        
        logger.info(f"🔌 WireGuard пир {client_name} {action_str}")
        return jsonify({"success": True, "enabled": enable})
    
    except Exception as e:
        logger.error(f"❌ Ошибка переключения пира: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/ovpn")
@login_required
def ovpn():
    try:
        # Пути к файлам и протоколы
        clients = []
        total_received, total_sent = 0, 0
        errors = []
        for file_path, protocol in LOG_FILES:
            file_data, received, sent, error = read_csv(file_path, protocol)
            if error:
                errors.append(f"Ошибка в файле {file_path}: {error}")
                logger.warning(f"⚠️ {errors[-1]}")
            else:
                clients.extend(file_data)
                total_received += received
                total_sent += sent

        # Сортировка данных
        sort_by = request.args.get("sort", "client")
        order = request.args.get("order", "asc")
        reverse_order = order == "desc"

        if sort_by == "client":
            clients.sort(key=lambda x: x[0], reverse=reverse_order)
        elif sort_by == "realIp":
            clients.sort(key=lambda x: x[1], reverse=reverse_order)
        elif sort_by == "localIp":
            clients.sort(key=lambda x: x[2], reverse=reverse_order)
        elif sort_by == "sent":
            clients.sort(key=lambda x: parse_bytes(x[3]), reverse=reverse_order)
        elif sort_by == "received":
            clients.sort(key=lambda x: parse_bytes(x[4]), reverse=reverse_order)
        elif sort_by == "connection-time":
            clients.sort(key=lambda x: x[7], reverse=reverse_order)
        elif sort_by == "duration":
            clients.sort(key=lambda x: x[7], reverse=reverse_order)
        elif sort_by == "protocol":
            clients.sort(key=lambda x: x[9], reverse=reverse_order)

        total_clients = len(clients)
        hide_ovpn_ip = read_settings().get("hide_ovpn_ip", True)
        logger.debug(f"📄 Запрошена страница OpenVPN ({total_clients} клиентов)")
        return render_template(
            "ovpn/ovpn.html",
            clients=clients,
            total_clients_str=pluralize_clients(total_clients),
            total_received=format_bytes(total_received),
            total_sent=format_bytes(total_sent),
            active_section="ovpn",
            active_page="clients",
            errors=errors,
            sort_by=sort_by,
            order=order,
            hide_ip=hide_ovpn_ip,
        )

    except ZoneInfoNotFoundError:
        error_message = (
            "Обнаружены конфликтующие настройки часового пояса "
            "в файлах /etc/timezone и /etc/localtime. "
            "Попробуйте установить правильный часовой пояс "
            "с помощью команды: sudo dpkg-reconfigure tzdata"
        )
        logger.error(f"❌ Ошибка часового пояса: {error_message}")
        return render_template("ovpn/ovpn.html", error_message=error_message), 500

    except Exception as e:
        error_message = f"Произошла непредвиденная ошибка: {str(e)}"
        logger.error(f"❌ Ошибка на странице OpenVPN: {e}")
        return render_template("ovpn/ovpn.html", error_message=error_message), 500


@app.route("/ovpn/history")
@login_required
def ovpn_history():
    try:
        page = request.args.get("page", 1, type=int)
        per_page = 20
        
        conn_logs = sqlite3.connect(app.config["LOGS_DATABASE_PATH"])
        
        total_count = conn_logs.execute(
            "SELECT COUNT(*) FROM connection_logs WHERE client_name != 'UNDEF'"
        ).fetchone()[0]
        
        total_pages = max(1, (total_count + per_page - 1) // per_page)
        page = max(1, min(page, total_pages))
        offset = (page - 1) * per_page
        
        logs_reader = conn_logs.execute(
            """SELECT * FROM connection_logs 
               WHERE client_name != 'UNDEF'
               ORDER BY connected_since DESC 
               LIMIT ? OFFSET ?""",
            (per_page, offset),
        ).fetchall()
        conn_logs.close()
        
        hide_ovpn_ip = read_settings().get("hide_ovpn_ip", True)
        
        def format_ip(ip):
            real_ip = normalize_real_address(ip) if 'normalize_real_address' in globals() else ip
            return mask_ip(real_ip, hide=hide_ovpn_ip)
        
        logs = [
            {
                "client_name": row[1],
                "real_ip": format_ip(row[3]),
                "local_ip": row[2],
                "connection_since": row[4],
                "protocol": row[7],
            }
            for row in logs_reader
        ]
        
        logger.debug(f"📄 Запрошена история OpenVPN ({len(logs)} записей, страница {page})")
        return render_template(
            "ovpn/ovpn_history.html",
            active_section="ovpn",
            active_page="history",
            logs=logs,
            page=page,
            total_pages=total_pages,
        )
    
    except Exception as e:
        error_message = f"Произошла непредвиденная ошибка: {str(e)}"
        logger.error(f"❌ Ошибка на странице истории OpenVPN: {e}")
        return render_template("ovpn/ovpn_history.html", error_message=error_message), 500

@app.route("/ovpn/stats")
@login_required
def ovpn_stats():
    try:
        sort_by = request.args.get("sort", "client_name")
        order = request.args.get("order", "asc").lower()
        period = request.args.get("period", "month")
        target_date = request.args.get("date")
        
        # ✅ ОТЛАДКА: Логируем параметры
        logger.debug(f"📊 Параметры запроса: period={period}, target_date={target_date}")
        
        # Разрешённые поля сортировки (ключ -> SQL)
        allowed_sorts = {
            "client_name": "client_name",
            "total_bytes_sent": "SUM(total_bytes_sent)",
            "total_bytes_received": "SUM(total_bytes_received)",
            "last_connected": "MAX(last_connected)",
        }
        sort_column = allowed_sorts.get(sort_by, "client_name")
        order_sql = "DESC" if order == "desc" else "ASC"

        now = datetime.now()
        date_from = None
        date_to = None

        # ✅ ЛОГИКА ФИЛЬТРАЦИИ ДАТ
        if period == "day":
            if target_date:
                try:
                    date_obj = datetime.strptime(target_date, "%Y-%m-%d")
                    date_from = date_obj.strftime("%Y-%m-%d")
                    date_to = (date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
                except ValueError:
                    date_from = now.strftime("%Y-%m-%d")
                    date_to = (now + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                date_from = now.strftime("%Y-%m-%d")
                date_to = (now + timedelta(days=1)).strftime("%Y-%m-%d")
                
        elif period == "month":
            # ✅ Для месяца: от 1 числа до 1 числа следующего месяца
            if target_date:
                try:
                    # Пробуем распарсить как YYYY-MM
                    date_obj = datetime.strptime(target_date, "%Y-%m")
                    date_from = date_obj.strftime("%Y-%m-01")
                    # Первое число следующего месяца
                    if date_obj.month == 12:
                        date_to = f"{date_obj.year + 1}-01-01"
                    else:
                        date_to = f"{date_obj.year}-{date_obj.month + 1:02d}-01"
                except ValueError:
                    # Если не получилось, используем текущий месяц
                    date_from = now.replace(day=1).strftime("%Y-%m-%d")
                    if now.month == 12:
                        date_to = f"{now.year + 1}-01-01"
                    else:
                        date_to = f"{now.year}-{now.month + 1:02d}-01"
            else:
                date_from = now.replace(day=1).strftime("%Y-%m-%d")
                if now.month == 12:
                    date_to = f"{now.year + 1}-01-01"
                else:
                    date_to = f"{now.year}-{now.month + 1:02d}-01"
                    
        elif period == "year":
            if target_date:
                try:
                    year = int(target_date)
                    date_from = f"{year}-01-01"
                    date_to = f"{year + 1}-01-01"
                except ValueError:
                    date_from = now.replace(month=1, day=1).strftime("%Y-%m-%d")
                    date_to = (now.replace(year=now.year + 1, month=1, day=1)).strftime("%Y-%m-%d")
            else:
                date_from = now.replace(month=1, day=1).strftime("%Y-%m-%d")
                date_to = (now.replace(year=now.year + 1, month=1, day=1)).strftime("%Y-%m-%d")
        else:
            period = "month"
            date_from = now.replace(day=1).strftime("%Y-%m-%d")
            if now.month == 12:
                date_to = f"{now.year + 1}-01-01"
            else:
                date_to = f"{now.year}-{now.month + 1:02d}-01"

        # ✅ ОТЛАДКА: Логируем даты
        logger.debug(f"📊 Диапазон дат: date_from={date_from}, date_to={date_to}")

        stats_list = []
        total_received, total_sent = 0, 0

        with sqlite3.connect(app.config["LOGS_DATABASE_PATH"]) as conn:
            # ✅ Для периода "day" используем daily_stats, для остальных monthly_stats
            if period == "day":
                query = f"""
                    SELECT client_name,
                           SUM(total_bytes_sent) as sent,
                           SUM(total_bytes_received) as received,
                           MAX(last_connected)
                    FROM daily_stats
                    WHERE hour >= ? AND hour < ?
                    GROUP BY client_name
                    HAVING (SUM(total_bytes_sent) > 0 OR SUM(total_bytes_received) > 0)
                    ORDER BY {sort_column} {order_sql}
                """
                rows = conn.execute(query, (date_from, date_to)).fetchall()
            else:
                # ✅ ИСПРАВЛЕНО: Используем strftime для сравнения по месяцу
                query = f"""
                    SELECT client_name,
                           SUM(total_bytes_sent) as sent,
                           SUM(total_bytes_received) as received,
                           MAX(last_connected)
                    FROM monthly_stats
                    WHERE month >= ? AND month < ?
                    GROUP BY client_name
                    HAVING (SUM(total_bytes_sent) > 0 OR SUM(total_bytes_received) > 0)
                    ORDER BY {sort_column} {order_sql}
                """
                rows = conn.execute(query, (date_from, date_to)).fetchall()
            
            # ✅ ОТЛАДКА: Логируем количество записей
            logger.debug(f"📊 Найдено записей: {len(rows)}")

            for client_name, sent, received, last_connected in rows:
                total_received += received or 0
                total_sent += sent or 0
                stats_list.append(
                    {
                        "client_name": client_name,
                        "total_bytes_sent": format_bytes(sent),
                        "total_bytes_received": format_bytes(received),
                        "last_connected": last_connected,
                    }
                )

        logger.debug(f"📄 Запрошена статистика OpenVPN за {date_from} - {date_to} (активных клиентов: {len(stats_list)})")
        return render_template(
            "ovpn/ovpn_stats.html",
            total_received=format_bytes(total_received),
            total_sent=format_bytes(total_sent),
            active_section="ovpn",
            active_page="stats",
            stats=stats_list,
            period=period,
            sort_by=sort_by,
            order=order_sql.lower(),
            target_date=target_date,
        )

    except Exception as e:
        error_message = f"Произошла непредвиденная ошибка: {e}"
        logger.error(f"❌ Ошибка на странице статистики OpenVPN: {e}")
        return render_template("ovpn/ovpn_stats.html", error_message=error_message), 500


@app.route("/api/ovpn/client_chart")
@login_required
def api_ovpn_client_chart():
    client_name = request.args.get("client")
    period = request.args.get("period", "month")
    target_date = request.args.get("date")
    
    if not client_name:
        return jsonify({"error": "client parameter required"}), 400
    
    now = datetime.now()
    date_from = None
    date_to = None
    
    # Логика дат для графика
    if period == "day":
        if target_date:
            try:
                date_obj = datetime.strptime(target_date, "%Y-%m-%d")
                date_from = date_obj.strftime("%Y-%m-%d")
                date_to = (date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
            except ValueError:
                date_from = now.strftime("%Y-%m-%d")
                date_to = (now + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            date_from = now.strftime("%Y-%m-%d")
            date_to = (now + timedelta(days=1)).strftime("%Y-%m-%d")
        
        try:
            with sqlite3.connect(app.config["LOGS_DATABASE_PATH"]) as conn:
                rows = conn.execute(
                    """SELECT hour,
                          SUM(total_bytes_received) as rx,
                          SUM(total_bytes_sent) as tx,
                          SUM(total_connections) as connections,
                          MAX(last_connected) as last_conn
                   FROM daily_stats
                   WHERE client_name = ? AND hour >= ? AND hour < ?
                   GROUP BY hour
                   ORDER BY hour ASC""",
                    (client_name, date_from, date_to),
                ).fetchall()
                
                labels = []
                rx_data = []
                tx_data = []
                
                for hour_val, rx, tx, conn_count, last_conn in rows:
                    if hour_val and 'T' in hour_val and not hour_val.endswith('Z'):
                        labels.append(hour_val + 'Z')
                    else:
                        labels.append(hour_val)
                    rx_data.append(rx or 0)
                    tx_data.append(tx or 0)
                
                logger.debug(f"📊 График трафика OpenVPN клиента {client_name} (день: {date_from})")
                return jsonify({
                    "client": client_name,
                    "labels": labels,
                    "rx_bytes": rx_data,
                    "tx_bytes": tx_data
                })
        except Exception as e:
            logger.error(f"❌ Ошибка графика OpenVPN (day): {e}")
            return jsonify({
                "client": client_name,
                "labels": [],
                "rx_bytes": [],
                "tx_bytes": []
            })
    
    elif period == "year":
        if target_date:
            try:
                year = int(target_date)
                target_year_str = f"{year}"
            except ValueError:
                target_year_str = str(now.year)
        else:
            target_year_str = str(now.year)
        
        try:
            with sqlite3.connect(app.config["LOGS_DATABASE_PATH"]) as conn:
                rows = conn.execute(
                    """SELECT month,
                          SUM(total_bytes_received) as rx,
                          SUM(total_bytes_sent) as tx
                   FROM years_stats
                   WHERE client_name = ? AND SUBSTR(month, 1, 4) = ?
                   GROUP BY month
                   ORDER BY month ASC""",
                    (client_name, target_year_str),
                ).fetchall()
            
            labels = []
            rx_data = []
            tx_data = []
            
            for month_val, rx, tx in rows:
                try:
                    dt = datetime.strptime(month_val, "%Y-%m")
                    labels.append(dt.strftime("%b %Y"))
                except:
                    labels.append(month_val)
                rx_data.append(rx or 0)
                tx_data.append(tx or 0)
            
            return jsonify({
                "client": client_name,
                "labels": labels,
                "rx_bytes": rx_data,
                "tx_bytes": tx_data
            })
        except Exception as e:
            logger.error(f"❌ Ошибка графика OpenVPN (year): {e}")
            return jsonify({
                "client": client_name,
                "labels": [],
                "rx_bytes": [],
                "tx_bytes": []
            })
    
    elif period == "month":
        if target_date:
            try:
                date_obj = datetime.strptime(target_date, "%Y-%m")
                date_from = date_obj.strftime("%Y-%m-01")
                if date_obj.month == 12:
                    date_to = f"{date_obj.year + 1}-01-01"
                else:
                    date_to = f"{date_obj.year}-{date_obj.month + 1:02d}-01"
            except ValueError:
                date_from = now.replace(day=1).strftime("%Y-%m-%d")
                date_to = (now.replace(day=1) + timedelta(days=32)).replace(day=1).strftime("%Y-%m-%d")
        else:
            date_from = now.replace(day=1).strftime("%Y-%m-%d")
            date_to = (now.replace(day=1) + timedelta(days=32)).replace(day=1).strftime("%Y-%m-%d")
        
        try:
            with sqlite3.connect(app.config["LOGS_DATABASE_PATH"]) as conn:
                rows = conn.execute(
                    """SELECT strftime('%Y-%m-%d', hour) as day_label,
                          SUM(total_bytes_received) as rx,
                          SUM(total_bytes_sent) as tx
                   FROM daily_stats
                   WHERE client_name = ? AND hour >= ? AND hour < ?
                   GROUP BY day_label
                   ORDER BY day_label ASC""",
                    (client_name, date_from, date_to),
                ).fetchall()
            
            labels = []
            rx_data = []
            tx_data = []
            
            for day_label, rx, tx in rows:
                try:
                    day_obj = datetime.strptime(day_label, "%Y-%m-%d")
                    labels.append(day_obj.strftime("%d.%m"))
                except:
                    labels.append(day_label)
                rx_data.append(rx or 0)
                tx_data.append(tx or 0)
            
            return jsonify({
                "client": client_name,
                "labels": labels,
                "rx_bytes": rx_data,
                "tx_bytes": tx_data
            })
        except Exception as e:
            logger.error(f"❌ Ошибка графика OpenVPN (month): {e}")
            return jsonify({
                "client": client_name,
                "labels": [],
                "rx_bytes": [],
                "tx_bytes": []
            })
    
    else:
        date_from = now.replace(day=1).strftime("%Y-%m-%d")
        date_to = (now.replace(day=1) + timedelta(days=32)).replace(day=1).strftime("%Y-%m-%d")
        
        try:
            with sqlite3.connect(app.config["LOGS_DATABASE_PATH"]) as conn:
                rows = conn.execute(
                    """SELECT strftime('%Y-%m-%d', hour) as day_label,
                          SUM(total_bytes_received) as rx,
                          SUM(total_bytes_sent) as tx
                   FROM daily_stats
                   WHERE client_name = ? AND hour >= ? AND hour < ?
                   GROUP BY day_label
                   ORDER BY day_label ASC""",
                    (client_name, date_from, date_to),
                ).fetchall()
            
            labels = []
            rx_data = []
            tx_data = []
            
            for day_label, rx, tx in rows:
                try:
                    day_obj = datetime.strptime(day_label, "%Y-%m-%d")
                    labels.append(day_obj.strftime("%d.%m"))
                except:
                    labels.append(day_label)
                rx_data.append(rx or 0)
                tx_data.append(tx or 0)
            
            return jsonify({
                "client": client_name,
                "labels": labels,
                "rx_bytes": rx_data,
                "tx_bytes": tx_data
            })
        except Exception as e:
            logger.error(f"❌ Ошибка графика OpenVPN: {e}")
            return jsonify({"error": str(e)}), 500


@app.route("/api/wg/client_chart")
@login_required
def api_wg_client_chart():
    client_name = request.args.get("client")
    period = request.args.get("period", "month")
    
    if not client_name:
        return jsonify({"error": "client parameter required"}), 400
    
    now = datetime.now()
    if period == "day":
        date_from = now.strftime("%Y-%m-%d")
    elif period == "month":
        date_from = now.replace(day=1).strftime("%Y-%m-%d")
    elif period == "year":
        date_from = now.replace(month=1, day=1).strftime("%Y-%m-%d")
    else:
        date_from = now.replace(day=1).strftime("%Y-%m-%d")
    
    try:
        with sqlite3.connect(app.config["WG_STATS_PATH"]) as conn:
            rows = conn.execute(
                """SELECT date,
                          SUM(received) as rx,
                          SUM(sent) as tx
                   FROM wg_daily_stats
                   WHERE client = ? AND date >= ?
                   GROUP BY date
                   ORDER BY date ASC""",
                (client_name, date_from),
            ).fetchall()
        
        labels = []
        rx_data = []
        tx_data = []
        for date_val, rx, tx in rows:
            labels.append(date_val)
            rx_data.append(rx or 0)
            tx_data.append(tx or 0)
        
        logger.debug(f"📊 График WireGuard клиента {client_name}")
        return jsonify({
            "client": client_name,
            "labels": labels,
            "rx_bytes": rx_data,
            "tx_bytes": tx_data,
        })
    except Exception as e:
        logger.error(f"❌ Ошибка графика WireGuard: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/bw")
@login_required
def api_bw():
    q_iface = request.args.get("iface")
    period = request.args.get("period", "day")
    vnstat_bin = os.environ.get("VNSTAT_BIN", "/usr/bin/vnstat")
    # Получаем список интерфейсов
    try:
        proc = subprocess.run(
            [vnstat_bin, "--json"], check=True, capture_output=True, text=True
        )
        data = json.loads(proc.stdout)
        interfaces = [iface["name"] for iface in data.get("interfaces", [])]
    except subprocess.CalledProcessError:
        interfaces = []
        logger.warning("⚠️ vnstat вернул ошибку при получении интерфейсов")
    except json.JSONDecodeError:
        interfaces = []
        logger.error("❌ Ошибка парсинга JSON от vnstat")

    if not interfaces:
        logger.error("❌ Нет интерфейсов vnstat")
        return jsonify({"error": "Нет интерфейсов vnstat", "iface": None}), 500

    iface = q_iface if q_iface in interfaces else interfaces[0]

    # Настройка периодов
    if period == "hour":
        vnstat_option = "f"  # каждые 5 минут
        points = 12
        interval_seconds = 300
    elif period == "day":
        vnstat_option = "h"
        points = 24
        interval_seconds = 3600
    elif period == "month":
        vnstat_option = "d"
        points = now.day
        interval_seconds = 86400
    else:
        vnstat_option = "h"
        points = 24
        interval_seconds = 3600

    # Получаем JSON от vnstat
    try:
        proc = subprocess.run(
            [vnstat_bin, "--json", vnstat_option, "-i", iface],
            check=True,
            capture_output=True,
            text=True,
        )
        data = json.loads(proc.stdout)
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ vnstat вернул код ошибки: {e.returncode}")
        return (
            jsonify({"error": f"vnstat вернул код ошибки: {e.returncode}", "iface": iface}),
            500,
        )
    except Exception as e:
        logger.error(f"❌ Ошибка получения данных vnstat: {e}")
        return jsonify({"error": str(e), "iface": iface}), 500

    # Извлекаем массив данных
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

    # Сортировка по дате
    def sort_key(h):
        d = h.get("date") or {}
        t = h.get("time") or {}
        return (
            d.get("year", 0),
            d.get("month", 0),
            d.get("day", 0),
            t.get("hour", 0),
            t.get("minute", 0),
        )

    sorted_data = sorted(traffic_data, key=sort_key)
    if points:
        sorted_data = sorted_data[-points:]

    labels, utc_labels, rx_mbps, tx_mbps = [], [], [], []

    server_tz = datetime.now().astimezone().tzinfo  # серверный локальный timezone

    for m in sorted_data:
        d = m.get("date") or {}
        t = m.get("time") or {}

        year = int(d.get("year", 0))
        month = int(d.get("month", 0))
        day = int(d.get("day", 0))
        hour = int(t.get("hour", 0))
        minute = int(t.get("minute", 0))

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

        utc_iso = local_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        utc_labels.append(utc_iso)

        rx = int(m.get("rx", 0))
        tx = int(m.get("tx", 0))
        rx_mbps.append(round((rx * 8) / (interval_seconds * 1_000_000), 3))
        tx_mbps.append(round((tx * 8) / (interval_seconds * 1_000_000), 3))

    server_time_utc = datetime.now(timezone.utc).isoformat()

    return jsonify(
        {
            "iface": iface,
            "labels": labels,
            "utc_labels": utc_labels,
            "rx_mbps": rx_mbps,
            "tx_mbps": tx_mbps,
            "server_time": server_time_utc,
        }
    )


@app.route("/api/interfaces")
def api_interfaces():
    interfaces = get_vnstat_interfaces()
    return jsonify({"interfaces": interfaces})


@app.route("/api/cpu")
def api_cpu():
    period = request.args.get("period", "live")
    now = datetime.now()

    # Количество точек для каждого фильтра
    targets = {
        "live": LIVE_POINTS,
        "hour": 60,
        "day": 24,
        "month": now.day,
    }
    max_points = targets.get(period, LIVE_POINTS)

    mem_rows = list(cpu_history)

    # ----------------- LIVE -----------------
    if period == "live":
        # просто последние N точек без группировки
        last = mem_rows[-LIVE_POINTS:] if len(mem_rows) > LIVE_POINTS else mem_rows

        data = [
            {"timestamp": r["timestamp"], "cpu": r["cpu"], "ram": r["ram"]}
            for r in last
        ]

    # ----------------- Остальные периоды -----------------
    else:
        # Настройка интервала и среза
        if period == "hour":
            bucket = "minute"
            cutoff = now - timedelta(hours=1)
        elif period == "day":
            bucket = "hour"
            cutoff = now - timedelta(days=1)
        elif period == "month":
            bucket = "day"
            cutoff = now.replace(day=1)
        else:
            bucket = "minute"
            cutoff = now - timedelta(hours=1)

        mem_candidates = [
            r for r in mem_rows if r["timestamp"] >= cutoff
        ]  # Данные из памяти за период
        need_db = True  # Если данных в памяти недостаточно, берём из БД
        if need_db:
            try:
                conn = sqlite3.connect(app.config["SYSTEM_STATS_PATH"])
                cur = conn.cursor()

                cur.execute(
                    """
                    SELECT timestamp, cpu_percent, ram_percent
                    FROM system_stats
                    WHERE timestamp >= ?
                    ORDER BY timestamp ASC
                 """,
                    (cutoff.strftime("%Y-%m-%d %H:%M:%S"),),
                )

                rows = cur.fetchall()
                conn.close()

                source_rows = [
                    {
                        "timestamp": datetime.strptime(ts, "%Y-%m-%d %H:%M:%S"),
                        "cpu": cpu,
                        "ram": ram,
                    }
                    for ts, cpu, ram in rows
                ]

            except Exception as e:
                logger.error(f"[DB ERROR] api_cpu: {e}")
                source_rows = mem_candidates
        else:
            source_rows = mem_candidates

        # Группировка по bucket (minute/hour/day)
        grouped = group_rows(source_rows, interval=bucket)
        data = resample_to_n(grouped, max_points)

    utc_labels = [
        d["timestamp"].astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        for d in data
    ]

    return jsonify(
        {
            "utc_labels": utc_labels,
            "cpu_percent": [round(d["cpu"], 2) for d in data],
            "ram_percent": [round(d["ram"], 2) for d in data],
            "period": period,
        }
    )


@app.route("/api/ovpn/live_chart")
@login_required
def api_ovpn_live_chart():
    """Возвращает данные для Live-графика скорости OpenVPN (бит/с)"""
    client_name = request.args.get("client")
    
    if not client_name:
        return jsonify({
            "client": None,
            "labels": [],
            "rx_speed": [],
            "tx_speed": [],
            "unit": "бит/с"
        })
    
    with ovpn_stats_lock:
        if client_name not in ovpn_live_stats:
            return jsonify({
                "client": client_name,
                "labels": [],
                "rx_speed": [],
                "tx_speed": [],
                "unit": "бит/с"
            })
        
        data = ovpn_live_stats[client_name]
        
        return jsonify({
            "client": client_name,
            "labels": data["timestamps"][-MAX_OVPN_LIVE_POINTS:],
            "rx_speed": data["rx_speed"][-MAX_OVPN_LIVE_POINTS:],
            "tx_speed": data["tx_speed"][-MAX_OVPN_LIVE_POINTS:],
            "unit": "бит/с"
        })


@app.route("/api/ovpn/speed_stats")
@login_required
def api_ovpn_speed_stats():
    """Возвращает статистику скорости для графика (по аналогии с /api/cpu)"""
    client_name = request.args.get("client")
    period = request.args.get("period", "live")
    
    if not client_name:
        return jsonify({"error": "client parameter required"}), 400
    
    now = datetime.now()
    
    # Определение периода
    if period == "live":
        # Возвращаем из памяти (текущий live_chart)
        with ovpn_stats_lock:
            if client_name not in ovpn_live_stats:
                return jsonify({"labels": [], "rx_speed": [], "tx_speed": []})
            
            data = ovpn_live_stats[client_name]
            return jsonify({
                "labels": data["timestamps"][-60:],
                "rx_speed": data["rx_speed"][-60:],
                "tx_speed": data["tx_speed"][-60:],
            })
    
    else:
        # Исторические данные из БД
        if period == "hour":
            cutoff = now - timedelta(hours=1)
            bucket = "minute"
        elif period == "day":
            cutoff = now - timedelta(days=1)
            bucket = "hour"
        elif period == "week":
            cutoff = now - timedelta(weeks=1)
            bucket = "day"
        else:
            cutoff = now - timedelta(days=1)
            bucket = "hour"
        
        try:
            conn = sqlite3.connect(app.config["LOGS_DATABASE_PATH"])
            cur = conn.cursor()
            
            cur.execute("""
                SELECT timestamp, rx_speed, tx_speed
                FROM ovpn_speed_stats
                WHERE client_name = ? AND timestamp >= ?
                ORDER BY timestamp ASC
            """, (client_name, cutoff.strftime("%Y-%m-%d %H:%M:%S")))
            
            rows = cur.fetchall()
            conn.close()
            
            # Группировка по интервалам
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
            
            # Усреднение
            labels = []
            rx_speed = []
            tx_speed = []
            
            for key in sorted(grouped.keys()):
                labels.append(key.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
                rx_speed.append(round(mean(grouped[key]["rx"]), 2))
                tx_speed.append(round(mean(grouped[key]["tx"]), 2))
            
            return jsonify({
                "labels": labels,
                "rx_speed": rx_speed,
                "tx_speed": tx_speed,
            })
            
        except Exception as e:
            logger.error(f"Ошибка OVPN speed stats: {e}")
            return jsonify({"labels": [], "rx_speed": [], "tx_speed": []}), 500


@app.route("/api/ovpn/clients")
@login_required
def api_ovpn_clients():
    """Возвращает список активных клиентов OpenVPN"""
    try:
        clients = set()
        with ovpn_stats_lock:
            clients = list(ovpn_live_stats.keys())
        
        if not clients:
            with sqlite3.connect(app.config["LOGS_DATABASE_PATH"]) as conn:
                rows = conn.execute("""
                    SELECT DISTINCT client_name 
                    FROM daily_stats 
                    WHERE hour >= datetime('now', '-1 hour')
                    ORDER BY client_name
                """).fetchall()
                clients = [row[0] for row in rows]
        
        return jsonify(clients)
    except Exception as e:
        logger.error(f"❌ Ошибка получения списка клиентов: {e}")
        return jsonify([]), 500


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("🚀 ЗАПУСК MAIN.PY (Flask приложение)")
    logger.info("=" * 60)
    logger.info(f"📍 Версия Python: {sys.version}")
    logger.info(f"🌐 Запуск на порту: 1234")
    logger.info(f"📁 Путь к логам: {LOG_DIR}")
    
    admin_pass = add_admin()
    if admin_pass:
        logger.info(f"✅ Администратор создан/обновлён")
    
    logger.info("📡 Запуск Flask сервера...")
    app.run(debug=False, host="0.0.0.0", port=1234)