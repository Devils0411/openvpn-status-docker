#!/usr/bin/env python3
"""Оптимизированный скрипт сбора и сохранения статистики WireGuard (через синхронное Amnezia API)"""
import os
import sys
import time
import json
import sqlite3
import logging
import docker
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
import schedule
from typing import Optional, List, Dict, Tuple
from config import Config

# =============================================================================
# НАСТРОЙКА ЛОГИРОВАНИЯ
# =============================================================================
LOG_DIR = Config.LOGS_PATH
os.makedirs(LOG_DIR, exist_ok=True)
INFO_LOG = os.path.join(LOG_DIR, 'wg_stats.info.log')
ERROR_LOG = os.path.join(LOG_DIR, 'wg_stats.stderr.log')
MAX_LOG_SIZE = 10 * 1024 * 1024  # 10 MB
BACKUP_COUNT = 5
LOG_LEVEL = getattr(Config, 'LOG_LEVEL', logging.INFO)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False
logger.handlers.clear()
formatter = logging.Formatter(
    '%(asctime)s [%(levelname)-8s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class LevelRangeFilter(logging.Filter):
    def __init__(self, min_level: int, max_level: int):
        super().__init__()
        self.min_level = min_level
        self.max_level = max_level
    def filter(self, record) -> bool:
        return self.min_level <= record.levelno <= self.max_level

info_handler = RotatingFileHandler(INFO_LOG, maxBytes=MAX_LOG_SIZE, backupCount=BACKUP_COUNT, encoding='utf-8', delay=True)
info_handler.setLevel(logging.DEBUG)
info_handler.addFilter(LevelRangeFilter(logging.DEBUG, logging.INFO))
info_handler.setFormatter(formatter)

error_handler = RotatingFileHandler(ERROR_LOG, maxBytes=MAX_LOG_SIZE, backupCount=BACKUP_COUNT, encoding='utf-8', delay=True)
error_handler.setLevel(logging.WARNING)
error_handler.setFormatter(formatter)

logger.addHandler(error_handler)
# logger.addHandler(info_handler)  # Оставлено закомментированным как в оригинале

# =============================================================================
# СИНХРОННЫЙ API КЛИЕНТ И ОБНАРУЖЕНИЕ КОНТЕЙНЕРА
# =============================================================================
class AmneziaDiscoverer:
    """Синхронное обнаружение контейнера AmneziaWG и извлечение учетных данных."""
    @staticmethod
    def get_connection_info() -> Optional[Tuple[str, str, str]]:
        try:
            client = docker.from_env()
            for container in client.containers.list():
                if "amnezia" in container.name.lower():
                    nets = container.attrs.get("NetworkSettings", {}).get("Networks", {})
                    ip = next((net.get("IPAddress") for net in nets.values() if net.get("IPAddress")), None)
                    if not ip:
                        continue

                    password = None
                    port = "8080"  # Fallback по умолчанию
                    for env in container.attrs.get("Config", {}).get("Env", []):
                        if env.startswith("WIREGUARD_PASSWORD="):
                            password = env.split("=", 1)[1].strip()
                        elif env.startswith("PORT="):
                            port = env.split("=", 1)[1].strip()

                    if ip and password:
                        logger.debug(f"🔍 Найдено: IP={ip}, PORT={port}")
                        return ip, password, port
        except Exception as e:
            logger.error(f"❌ Ошибка обнаружения контейнера: {e}")
        return None

class AmneziaApiSyncClient:
    """Синхронный клиент для взаимодействия с API AmneziaWG Easy."""
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

    def get_clients_stats(self) -> List[Dict]:
        resp = self.session.get(
            f"{self.base_url}/api/client",
        )
        resp.raise_for_status()
        clients = resp.json()
        stats = []
        for c in clients:
            rx = c.get("transferRx") or c.get("transfer_rx", 0)
            tx = c.get("transferTx") or c.get("transfer_tx", 0)
            peer_key = c.get("publicKey") or c.get("id", "")
            client_name = c.get("name", "Unknown")

            stats.append({
                "peer": peer_key,
                "client": client_name,
                "interface": "wg0",
                "received": str(rx),
                "sent": str(tx)
            })
        return stats

# =============================================================================
# КОНФИГУРАЦИЯ И КЭШИ
# =============================================================================
DB_PATH = Config.WG_STATS_PATH
SETTINGS_PATH = Config.SETTINGS_PATH
EVERY_TIME = 30
SYNC_TIME = 5
_config_cache = {"mapping": {}, "mtime": 0}

def get_stats_retention_days(default_days: int = 365) -> int:
    try:
        with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
            settings_data = json.load(f)
            days = int(settings_data.get("stats_retention_days", default_days))
            return max(30, min(days, 3650))
    except (FileNotFoundError, json.JSONDecodeError, ValueError, TypeError):
        return default_days

def get_retention_windows(total_days: int) -> tuple[int, int, int]:
    hourly_days = max(1, round(total_days * 30 / 365))
    daily_days = max(hourly_days, round(total_days * 90 / 365))
    monthly_days = max(daily_days, total_days)
    return hourly_days, daily_days, monthly_days

# =============================================================================
# ФУНКЦИИ WIREGUARD (ОБНОВЛЕНЫ ПОД API)
# =============================================================================
def get_wireguard_stats() -> Optional[List[Dict]]:
    """Получение статистики через синхронное API Amnezia WG Easy."""
    conn_info = AmneziaDiscoverer.get_connection_info()
    if not conn_info:
        logger.debug("⚠️ Контейнер Amnezia не найден через Docker")
        return None
    ip, password, port = conn_info
    base_url = f"http://{ip}:{port}"
    try:
        # Добавляем username (по умолчанию 'admin' или читаем из env)
        username = os.environ.get("WG_USERNAME", "admin")
        client = AmneziaApiSyncClient(base_url, password, username)
        client.login()
        return client.get_clients_stats()
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка запроса к API Amnezia ({base_url}): {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Исключение при получении статистики WG: {e}", exc_info=True)
        return None

def parse_wireguard_stats(output: str | List[Dict]) -> List[Dict]:
    """Адаптер: возвращает список как есть, если данные уже получены через API."""
    if isinstance(output, list):
        return output
    return []

def read_wg_config(file_path: str) -> dict[str, str]:
    """Кэшированное чтение конфигурации клиентов (оставлено для fallback/отладки)."""
    try:
        mtime = os.path.getmtime(file_path) if os.path.exists(file_path) else 0
    except OSError:
        mtime = 0
    if file_path.endswith(".conf"):
        json_path = file_path.replace(".conf", ".json")
        if os.path.exists(json_path):
            file_path = json_path

    if _config_cache["mapping"] and mtime <= _config_cache["mtime"]:
        return _config_cache["mapping"]

    client_mapping = {}
    try:
        if file_path.endswith(".json"):
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                for client_id, client_info in data.get("clients", {}).items():
                    clean = {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in client_info.items()}
                    pub_key = clean.get("publicKey", "").strip()
                    name = clean.get("name", "N/A").strip()
                    if pub_key:
                        client_mapping[pub_key] = name
        else:
            current_name = None
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("# Client:"):
                        current_name = line.split(":", 1)[1].strip().split("(")[0].strip()
                    elif line.startswith("[Peer]"):
                        current_name = current_name or "N/A"
                    elif line.startswith("PublicKey =") and current_name:
                        pub_key = line.split("=", 1)[1].strip()
                        client_mapping[pub_key] = current_name
        logger.debug(f"🔄 Кэш конфигурации обновлён: {len(client_mapping)} клиентов")
        _config_cache["mapping"] = client_mapping
        _config_cache["mtime"] = mtime
    except FileNotFoundError:
        logger.warning(f"⚠️ Конфигурационный файл {file_path} не найден")
    except Exception as e:
        logger.error(f"❌ Ошибка чтения конфига {file_path}: {e}", exc_info=True)
    return client_mapping

def convert_to_bytes(value: str | int | float) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    if not isinstance(value, str):
        return 0
    value = value.strip()
    if not value or value in ("0 B", "0B"):
        return 0
    units = {
        "B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "TiB": 1024**4,
        "KB": 1000, "MB": 1000**2, "GB": 1000**3, "TB": 1000**4,
    }
    parts = value.split()
    if len(parts) == 1 and parts[0].replace('.', '', 1).isdigit():
        return int(float(parts[0]))
    if len(parts) == 2:
        num, unit = parts
        return int(float(num) * units.get(unit, 1))
    return 0

# =============================================================================
# ФУНКЦИИ БАЗЫ ДАННЫХ
# =============================================================================
def init_db() -> None:
    tables = [
        ("wg_daily_stats", "(date TEXT NOT NULL, peer TEXT NOT NULL, client TEXT NOT NULL, received INTEGER NOT NULL, sent INTEGER NOT NULL, interface TEXT NOT NULL, PRIMARY KEY (date, peer, interface))"),
        ("wg_intermediate", "(peer TEXT NOT NULL, interface TEXT NOT NULL, last_received INTEGER NOT NULL, last_sent INTEGER NOT NULL, date TEXT NOT NULL, PRIMARY KEY (peer, interface))"),
        ("wg_total_stats", "(peer TEXT NOT NULL, client TEXT NOT NULL, total_received INTEGER NOT NULL, total_sent INTEGER NOT NULL, interface TEXT NOT NULL, PRIMARY KEY (peer, interface))"),
        ("wg_hourly_stats", "(hour TEXT NOT NULL, peer TEXT NOT NULL, client TEXT NOT NULL, received INTEGER NOT NULL, sent INTEGER NOT NULL, interface TEXT NOT NULL, PRIMARY KEY (hour, peer, interface))"),
        ("wg_monthly_stats", "(month TEXT NOT NULL, peer TEXT NOT NULL, client TEXT NOT NULL, received INTEGER NOT NULL, sent INTEGER NOT NULL, interface TEXT NOT NULL, PRIMARY KEY (month, peer, interface))"),
    ]
    with sqlite3.connect(DB_PATH) as conn:
        for name, schema in tables:
            conn.execute(f"CREATE TABLE IF NOT EXISTS {name} {schema}")
    logger.info("✅ База данных WireGuard инициализирована")

def clear_wg_total_stats() -> bool:
    try:
        stats = get_wireguard_stats()
        if not stats:
            return False
        current_peers = {(d["peer"], d["interface"]) for d in stats}
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT peer, interface FROM wg_total_stats")
            db_peers = set(cursor.fetchall())
            to_remove = db_peers - current_peers
            if to_remove:
                conn.executemany("DELETE FROM wg_total_stats WHERE peer = ? AND interface = ?", to_remove)
                logger.info(f"🧹 Удалено {len(to_remove)} устаревших пиров из wg_total_stats")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка очистки wg_total_stats: {e}", exc_info=True)
        return False

def sync_new_peers() -> None:
    if not clear_wg_total_stats():
        logger.warning("⚠️ Очистка wg_total_stats пропущена, синхронизация отменена")
        return
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                INSERT INTO wg_intermediate (peer, interface, last_received, last_sent, date)
                SELECT t.peer, t.interface, 0, 0, ?
                FROM wg_total_stats t
                LEFT JOIN wg_intermediate i ON t.peer = i.peer AND t.interface = i.interface
                WHERE i.peer IS NULL
                GROUP BY t.peer, t.interface
            """, (datetime.now().strftime("%Y-%m-%d"),))
            if conn.total_changes > 0:
                logger.info(f"🔄 Синхронизировано {conn.total_changes} новых пиров")
    except Exception as e:
        logger.error(f"❌ Ошибка синхронизации новых пиров: {e}", exc_info=True)

# =============================================================================
# СОХРАНЕНИЕ И ОЧИСТКА
# =============================================================================
_last_cleanup_date = None
def clean_old_daily_stats(days: int = 365) -> None:
    global _last_cleanup_date
    today = datetime.now().date()
    if _last_cleanup_date == today:
        return
    try:
        hourly_days, daily_days, monthly_days = get_retention_windows(days)
        now = datetime.now()
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                INSERT INTO wg_monthly_stats (month, peer, client, received, sent, interface)
                SELECT substr(date, 1, 7), peer, MAX(client), SUM(received), SUM(sent), interface
                FROM wg_daily_stats
                GROUP BY peer, interface, substr(date, 1, 7)
                ON CONFLICT(month, peer, interface) DO UPDATE SET
                received = excluded.received, sent = excluded.sent, client = excluded.client
            """)
            conn.execute("DELETE FROM wg_hourly_stats WHERE hour < ?", ((now - timedelta(days=hourly_days)).strftime("%Y-%m-%d"),))
            conn.execute("DELETE FROM wg_daily_stats WHERE date < ?", ((now - timedelta(days=daily_days)).strftime("%Y-%m-%d"),))
            conn.execute("DELETE FROM wg_monthly_stats WHERE month < ?", ((now - timedelta(days=monthly_days)).strftime("%Y-%m"),))
        _last_cleanup_date = today
        logger.debug("🧹 Очистка старых записей выполнена")
    except Exception as e:
        logger.error(f"❌ Ошибка очистки старых записей: {e}", exc_info=True)

def save_wg_stats() -> None:
    now_dt = datetime.now()
    if now_dt.hour == 0 and now_dt.minute == 0 and now_dt.second <= 5:
        stats = get_wireguard_stats()
        if not stats:
            return
        date = now_dt.strftime("%Y-%m-%d")
        with sqlite3.connect(DB_PATH) as conn:
            conn.executemany(
                "INSERT OR REPLACE INTO wg_intermediate (peer, interface, last_received, last_sent, date) VALUES (?, ?, ?, ?, ?)",
                [(d["peer"], d["interface"], convert_to_bytes(d["received"]), convert_to_bytes(d["sent"]), date) for d in stats]
            )
            conn.executemany(
                "INSERT OR REPLACE INTO wg_total_stats (peer, client, total_received, total_sent, interface) VALUES (?, ?, ?, ?, ?)",
                [(d["peer"], d["client"], convert_to_bytes(d["received"]), convert_to_bytes(d["sent"]), d["interface"]) for d in stats]
            )
        logger.info("✅ Статистика за полночь сохранена в wg_total_stats")
    clean_old_daily_stats(get_stats_retention_days())

def save_daily_stats(dailysave: bool = False) -> bool:
    try:
        stats = get_wireguard_stats()
        if not stats:
            return False

        date = datetime.now().strftime("%Y-%m-%d")
        now_str = datetime.now().strftime("%H:%M:%S")
        if dailysave:
            logger.info(f"📊 Фиксирование дневной статистики: {now_str}")
            with sqlite3.connect(DB_PATH) as conn:
                conn.executemany(
                    "INSERT OR REPLACE INTO wg_intermediate (peer, interface, last_received, last_sent, date) VALUES (?, ?, ?, ?, ?)",
                    [(d["peer"], d["interface"], convert_to_bytes(d["received"]), convert_to_bytes(d["sent"]), date) for d in stats]
                )
            logger.info("✅ Дневная статистика зафиксирована")
            return True

        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT peer, client, total_received, total_sent, interface FROM wg_total_stats")
            total_stats = { (r[0], r[4]): r for r in cursor.fetchall() }
            cursor.execute("SELECT peer, interface, last_received, last_sent FROM wg_intermediate")
            intermediate = { (r[0], r[1]): r for r in cursor.fetchall() }
            hour = datetime.now().strftime("%Y-%m-%d %H:00")
            updated = inserted = 0
            for key, total in total_stats.items():
                if key not in intermediate:
                    continue
                peer, interface = key
                client = total[1]
                curr_rx, curr_tx = int(total[2]), int(total[3])
                last_rx, last_tx = int(intermediate[key][2]), int(intermediate[key][3])
                rx_diff = curr_rx - last_rx if curr_rx >= last_rx else curr_rx
                tx_diff = curr_tx - last_tx if curr_tx >= last_tx else curr_tx
                if rx_diff < 0 or tx_diff < 0:
                    logger.warning(f"⚠️ Сброс счётчиков {peer} ({interface}), используем текущие")
                    rx_diff, tx_diff = curr_rx, curr_tx
                b_rx, b_tx = convert_to_bytes(rx_diff), convert_to_bytes(tx_diff)
                cursor.execute("SELECT 1 FROM wg_daily_stats WHERE date=? AND peer=? AND interface=?", (date, peer, interface))
                if cursor.fetchone():
                    cursor.execute("UPDATE wg_daily_stats SET received=?, sent=?, client=? WHERE date=? AND peer=? AND interface=?",
                                   (b_rx, b_tx, client, date, peer, interface))
                    updated += 1
                else:
                    cursor.execute("INSERT INTO wg_daily_stats (date, peer, client, received, sent, interface) VALUES (?,?,?,?,?,?)",
                                   (date, peer, client, b_rx, b_tx, interface))
                cursor.execute("SELECT COALESCE(SUM(received),0), COALESCE(SUM(sent),0) FROM wg_hourly_stats WHERE peer=? AND interface=? AND substr(hour,1,10)=? AND hour!=?",
                               (peer, interface, date, hour))
                prev_h_rx, prev_h_tx = cursor.fetchone()
                cursor.execute("INSERT OR REPLACE INTO wg_hourly_stats (hour, peer, client, received, sent, interface) VALUES (?,?,?,?,?,?)",
                               (hour, peer, client, max(0, b_rx - prev_h_rx), max(0, b_tx - prev_h_tx), interface))
                inserted += 1
            logger.debug(f"✅ Ежедневная статистика: upd={updated}, ins={inserted}")
            return True
    except Exception as e:
        logger.error(f"❌ Ошибка ежедневного сохранения: {e}", exc_info=True)
        return False

# =============================================================================
# ТАЙМЕРЫ И ЗАПУСК
# =============================================================================
_jobs = []
def _start_timers():
    global _jobs
    _jobs = [
        schedule.every(EVERY_TIME).seconds.do(save_daily_stats),
        schedule.every(EVERY_TIME).seconds.do(save_wg_stats),
        schedule.every(SYNC_TIME).minutes.do(sync_new_peers)
    ]
    logger.info(f"✅ Таймеры запущены: каждые {EVERY_TIME} сек (статистика), каждые {SYNC_TIME} мин (синхронизация)")

def _stop_and_reset_daily():
    global _jobs
    for job in _jobs:
        schedule.cancel_job(job)
    _jobs = []
    logger.info("⏸️ Таймеры остановлены для финального сохранения за сутки")
    time.sleep(2)
    save_daily_stats(True)
    logger.info("▶️ Таймеры перезапущены после фиксации дневной статистики")
    _start_timers()

def main():
    logger.info("=" * 60)
    logger.info("🚀 ЗАПУСК WG_STATS.PY (Сбор статистики WireGuard через API)")
    logger.info("=" * 60)
    logger.info(f"📍 Python: {__import__('sys').version.split()[0]}")
    logger.info(f"📁 БД: {DB_PATH} | Логи: {LOG_DIR}")
    logger.info(f"📅 Хранение: {get_stats_retention_days()} дней")

    if not AmneziaDiscoverer.get_connection_info():
        logger.info("❌ КОНТЕЙНЕР AMNEZIA НЕ НАЙДЕН. Скрипт завершает работу.")
        sys.exit(1)
    try:
        init_db()
        inter_date = datetime.now().strftime("%Y-%m-%d")
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.execute("SELECT date FROM wg_intermediate LIMIT 1")
            row = cursor.fetchone()
            if row:
                inter_date = row[0]
            if inter_date != datetime.now().strftime("%Y-%m-%d"):
                logger.info(f"📊 Обнаружена смена даты. Фиксируем статистику...")
                save_daily_stats(True)
                time.sleep(2)
                clean_old_daily_stats(get_stats_retention_days())
                time.sleep(1)
        _start_timers()
        schedule.every().day.at("23:59:55").do(_stop_and_reset_daily)
        logger.info("🔄 Основной цикл запущен...")
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("🛑 Получен сигнал остановки")
    except Exception as e:
        logger.critical("❌ Фатальная ошибка", exc_info=True)
    finally:
        logger.info("=" * 60)
        logger.info("🏁 WG_STATS.PY завершён")
        logger.info("=" * 60)

if __name__ == "__main__":
    main()