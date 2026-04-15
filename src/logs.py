import os
import sqlite3
import csv
import json
import time
import schedule
import logging
import sys
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone, timedelta
from tzlocal import get_localzone
from config import Config

# =============================================================================
# НАСТРОЙКА ЛОГИРОВАНИЯ
# =============================================================================
LOG_DIR = Config.LOGS_PATH
os.makedirs(LOG_DIR, exist_ok=True)
STDOUT_LOG = os.path.join(LOG_DIR, 'logs.stdout.log')
STDERR_LOG = os.path.join(LOG_DIR, 'logs.stderr.log')
MAX_LOG_SIZE = 10 * 1024 * 1024  # 10 MB
BACKUP_COUNT = 5
LOG_LEVEL = getattr(Config, 'LOG_LEVEL', logging.INFO)

class LevelFilter(logging.Filter):
    def __init__(self, min_level, max_level=None):
        super().__init__()
        self.min_level = min_level
        self.max_level = max_level or min_level

    def filter(self, record):
        return self.min_level <= record.levelno <= self.max_level

# Очистка существующих обработчиков корня
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)
logger.propagate = False
logger.handlers.clear()

stderr_handler = RotatingFileHandler(
    STDERR_LOG, maxBytes=MAX_LOG_SIZE, backupCount=BACKUP_COUNT,
    encoding='utf-8', delay=True
)
stderr_handler.setLevel(logging.WARNING)
stderr_handler.addFilter(LevelFilter(logging.WARNING, logging.CRITICAL))
stderr_handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S'
))

stdout_handler = RotatingFileHandler(
    STDOUT_LOG, maxBytes=MAX_LOG_SIZE, backupCount=BACKUP_COUNT,
    encoding='utf-8', delay=True
)
stdout_handler.setLevel(logging.DEBUG)
stdout_handler.addFilter(LevelFilter(logging.DEBUG, logging.INFO))
stdout_handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S'
))

logger.addHandler(stderr_handler)
# logger.addHandler(stdout_handler)  # Раскомментируйте для полноты отладки

# =============================================================================
# КОНФИГУРАЦИЯ
# =============================================================================
DB_PATH = Config.LOGS_DATABASE_PATH
SETTINGS_PATH = Config.SETTINGS_PATH
LOG_FILES = Config.LOG_FILES
LOCAL_TZ = get_localzone()  # Кэшируем часовой пояс один раз

def get_stats_retention_days(default_days=365):
    try:
        with open(SETTINGS_PATH, "r", encoding="utf-8") as settings_file:
            settings_data = json.load(settings_file)
            days = int(settings_data.get("stats_retention_days", default_days))
            return max(30, min(days, 3650))
    except (FileNotFoundError, json.JSONDecodeError, ValueError, TypeError):
        return default_days

def get_retention_windows(total_days):
    hourly_days = max(1, round(total_days * 30 / 365))
    daily_days = max(hourly_days, round(total_days * 90 / 365))
    monthly_days = max(daily_days, total_days)
    return hourly_days, daily_days, monthly_days

# =============================================================================
# ФУНКЦИИ БД
# =============================================================================
def initialize_database():
    """Создаёт таблицы, если они не существуют."""
    logger.info("🔍 Проверка и инициализация структуры БД...")
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_name TEXT,
                    hour TEXT,
                    total_bytes_received INTEGER,
                    total_bytes_sent INTEGER,
                    total_connections INTEGER,
                    last_connected TEXT,
                    UNIQUE (client_name, hour)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS monthly_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_name TEXT,
                    month TEXT, 
                    total_bytes_received INTEGER,
                    total_bytes_sent INTEGER,
                    total_connections INTEGER,
                    last_connected TEXT,
                    UNIQUE(client_name, month)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS yearly_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_name TEXT,
                    month TEXT, 
                    total_bytes_received INTEGER,
                    total_bytes_sent INTEGER,
                    total_connections INTEGER,
                    last_connected TEXT,
                    UNIQUE(client_name, month)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS last_client_stats (
                    client_name TEXT PRIMARY KEY,
                    connected_since TEXT,
                    bytes_received INTEGER,
                    bytes_sent INTEGER
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS connection_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_name TEXT,
                    local_ip TEXT,
                    real_ip TEXT,
                    connected_since TEXT,
                    bytes_received INTEGER,
                    bytes_sent INTEGER,
                    protocol TEXT
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_client_since ON connection_logs(client_name, connected_since)")
            conn.commit()
            logger.info("✅ База данных успешно инициализирована.")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации БД: {e}", exc_info=True)
        raise

def ensure_column_exists():
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(monthly_stats)")
            columns = [row[1] for row in cursor.fetchall()]
            if "last_connected" not in columns:
                cursor.execute("ALTER TABLE monthly_stats ADD COLUMN last_connected TEXT")
                conn.commit()
                logger.info("🔧 Добавлен отсутствующий столбец 'last_connected' в monthly_stats.")
    except Exception as e:
        logger.error(f"❌ Ошибка проверки/обновления колонок: {e}", exc_info=True)

# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================
def format_date(date_string):
    try:
        date_obj = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
        localized_date = date_obj.replace(tzinfo=LOCAL_TZ)
        return localized_date.astimezone(timezone.utc).isoformat()
    except Exception as e:
        logger.warning(f"⚠️ Ошибка форматирования даты '{date_string}': {e}. Использую текущее время.")
        return datetime.now(timezone.utc).isoformat()

def format_duration(start_time):
    try:
        now = datetime.now()
        delta = now - start_time
        days = delta.days
        seconds = delta.seconds
        hours, remainder = divmod(seconds, 3600)
        minutes, _ = divmod(remainder, 60)

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
        return f"{seconds} сек."
    except Exception as e:
        logger.debug(f"Ошибка расчета длительности: {e}")
        return "0 сек."

def mask_ip(ip_address):
    if not ip_address:
        return "0.0.0.0"
    for prefix in ("UDP4", "UDP6", "TCP4", "TCP6"):
        if ip_address.upper().startswith(prefix):
            ip_address = ip_address[len(prefix):].lstrip(": ")
            break
    if ":" in ip_address:
        return ip_address.rsplit(":", 1)[0]
    return ip_address

def extract_protocol_from_address(real_address, config_protocol):
    if not real_address:
        return config_protocol or "unknown"
    for prefix in ("UDP4", "UDP6", "TCP4", "TCP6"):
        if real_address.upper().startswith(prefix):
            return prefix.upper()[:3]
    return config_protocol or "unknown"

def parse_log_file(log_file, config_protocol):
    logs = []
    if not os.path.exists(log_file):
        logger.warning(f"📁 Файл лога не найден, пропускаю: {log_file}")
        return logs
    logger.debug(f"📖 Начало парсинга: {log_file}")
    try:
        with open(log_file, newline="", encoding="utf-8") as file:
            reader = csv.reader(file)
            next(reader, None)  # Пропуск заголовка
            for row in reader:
                if len(row) < 8 or row[0] != "CLIENT_LIST":
                    continue
                logs.append({
                    "client_name": row[1],
                    "real_ip": mask_ip(row[2]),
                    "local_ip": row[3],
                    "bytes_received": int(row[5]) if row[5].isdigit() else 0,
                    "bytes_sent": int(row[6]) if row[6].isdigit() else 0,
                    "connected_since": format_date(row[7]),
                    "duration": format_duration(datetime.strptime(row[7], "%Y-%m-%d %H:%M:%S")),
                    "protocol": extract_protocol_from_address(row[2], config_protocol),
                })
        logger.info(f"📄 Обработан {os.path.basename(log_file)}: найдено {len(logs)} активных сессий.")
    except Exception as e:
        logger.error(f"❌ Ошибка парсинга {log_file}: {e}", exc_info=True)
    return logs

# =============================================================================
# СТАТИСТИКА И АГРЕГАЦИЯ
# =============================================================================
def save_daily_stats(logs):
    logger.info("💾 Сохранение почасовой статистики (daily_stats)...")
    current_hour = datetime.today().strftime("%Y-%m-%d %H:00")

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        # Загружаем все последние состояния за один запрос
        cursor.execute("SELECT client_name, connected_since, bytes_received, bytes_sent FROM last_client_stats")
        last_states = {row[0]: row[1:] for row in cursor.fetchall()}

        aggregated_data = {}
        last_stats_updates = []

        for log in logs:
            try:
                connected_since = datetime.fromisoformat(log["connected_since"])
            except (ValueError, TypeError):
                continue

            client_name = log["client_name"]
            new_bytes_received = log.get("bytes_received", 0)
            new_bytes_sent = log.get("bytes_sent", 0)

            last_state = last_states.get(client_name)
            if last_state:
                last_conn, last_recv, last_sent = last_state
                if last_conn != log["connected_since"]:
                    diff_recv, diff_sent = new_bytes_received, new_bytes_sent
                else:
                    diff_recv = max(0, new_bytes_received - last_recv)
                    diff_sent = max(0, new_bytes_sent - last_sent)
            else:
                diff_recv, diff_sent = new_bytes_received, new_bytes_sent

            last_stats_updates.append((client_name, log["connected_since"], new_bytes_received, new_bytes_sent))

            key = (client_name, current_hour)
            if key not in aggregated_data:
                aggregated_data[key] = {
                    "total_bytes_received": 0,
                    "total_bytes_sent": 0,
                    "total_connections": 0,
                    "last_connected": connected_since
                }

            aggregated_data[key]["total_bytes_received"] += diff_recv
            aggregated_data[key]["total_bytes_sent"] += diff_sent
            aggregated_data[key]["total_connections"] += 1
            if connected_since > aggregated_data[key]["last_connected"]:
                aggregated_data[key]["last_connected"] = connected_since

        # Массовое обновление last_client_stats
        cursor.executemany("""
            INSERT INTO last_client_stats (client_name, connected_since, bytes_received, bytes_sent)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(client_name) DO UPDATE SET
            connected_since = excluded.connected_since,
            bytes_received = excluded.bytes_received,
            bytes_sent = excluded.bytes_sent
        """, last_stats_updates)

        # Массовое обновление daily_stats
        daily_stats_updates = [
            (k[0], k[1], v["total_bytes_received"], v["total_bytes_sent"],
             v["total_connections"], v["last_connected"].isoformat())
            for k, v in aggregated_data.items()
        ]
        cursor.executemany("""
            INSERT INTO daily_stats (client_name, hour, total_bytes_received, total_bytes_sent, total_connections, last_connected)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(client_name, hour) DO UPDATE SET
            total_bytes_received = total_bytes_received + excluded.total_bytes_received,
            total_bytes_sent = total_bytes_sent + excluded.total_bytes_sent,
            total_connections = total_connections + excluded.total_connections,
            last_connected = MAX(COALESCE(last_connected, ''), excluded.last_connected)
        """, daily_stats_updates)

        conn.commit()
        logger.info(f"✅ daily_stats обновлен: {len(aggregated_data)} записей, {len(set(log['client_name'] for log in logs))} уникальных клиентов.")

def aggregate_daily_to_monthly():
    logger.info("🔄 Агрегация daily_stats -> monthly_stats...")
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                INSERT INTO monthly_stats (client_name, month, total_bytes_received, total_bytes_sent, total_connections, last_connected)
                SELECT ds.client_name, strftime('%Y-%m-%d', ds.hour), SUM(ds.total_bytes_received), SUM(ds.total_bytes_sent), SUM(ds.total_connections), MAX(ds.last_connected)
                FROM daily_stats ds
                GROUP BY ds.client_name, strftime('%Y-%m-%d', ds.hour)
                ON CONFLICT(client_name, month) DO UPDATE SET
                total_bytes_received = excluded.total_bytes_received,
                total_bytes_sent = excluded.total_bytes_sent,
                total_connections = excluded.total_connections,
                last_connected = excluded.last_connected
            """)
            conn.commit()
        logger.info("✅ Агрегация daily -> monthly завершена.")
    except Exception as e:
        logger.error(f"❌ Ошибка агрегации daily->monthly: {e}", exc_info=True)
        raise

def aggregate_monthly_to_yearly():
    logger.info("🔄 Агрегация monthly_stats -> yearly_stats...")
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                INSERT INTO yearly_stats (client_name, month, total_bytes_received, total_bytes_sent, total_connections, last_connected)
                SELECT ms.client_name, strftime('%Y-%m', ms.month), SUM(ms.total_bytes_received), SUM(ms.total_bytes_sent), SUM(ms.total_connections), MAX(ms.last_connected)
                FROM monthly_stats ms
                GROUP BY ms.client_name, strftime('%Y-%m', ms.month)
                ON CONFLICT(client_name, month) DO UPDATE SET
                total_bytes_received = excluded.total_bytes_received,
                total_bytes_sent = excluded.total_bytes_sent,
                total_connections = excluded.total_connections,
                last_connected = excluded.last_connected
            """)
            conn.commit()
        logger.info("✅ Агрегация monthly -> yearly завершена.")
    except Exception as e:
        logger.error(f"❌ Ошибка агрегации monthly->yearly: {e}", exc_info=True)
        raise

def cleanup_old_stats(total_days=None):
    retention_days = total_days or get_stats_retention_days(default_days=365)
    hourly_days, daily_days, monthly_days = get_retention_windows(retention_days)
    logger.info(f"🗑 Очистка старых данных. Retention: {retention_days}д. (H:{hourly_days}d, D:{daily_days}d, M:{monthly_days}d)")

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        now = datetime.today()
        cutoffs = [
            ("daily_stats", "hour", now - timedelta(days=hourly_days), "%Y-%m-%d %H:00"),
            ("monthly_stats", "month", now - timedelta(days=daily_days), "%Y-%m-%d"),
            ("yearly_stats", "month", now - timedelta(days=monthly_days), "%Y-%m")
        ]
        for table, col, cutoff_dt, fmt in cutoffs:
            cutoff_str = cutoff_dt.strftime(fmt)
            if table == "monthly_stats":
                cursor.execute(f"DELETE FROM {table} WHERE {col} < ? OR length({col}) != 10", (cutoff_str,))
            else:
                cursor.execute(f"DELETE FROM {table} WHERE {col} < ?", (cutoff_str,))
            logger.info(f"🧹 Удалено {cursor.rowcount} записей из {table} (старше {cutoff_str}).")
        conn.commit()

def save_connection_logs(logs):
    logger.info("💾 Сохранение истории подключений (connection_logs)...")
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, client_name, connected_since, bytes_received, bytes_sent FROM connection_logs")
        existing = {(row[1], row[2]): (row[0], row[3], row[4]) for row in cursor.fetchall()}

        new_records, update_records = [], []
        for log in logs:
            key = (log["client_name"], log["connected_since"])
            if key not in existing:
                new_records.append((
                    log["client_name"], log["local_ip"], log["real_ip"],
                    log["connected_since"], log["bytes_received"],
                    log["bytes_sent"], log["protocol"]
                ))
            else:
                eid, e_recv, e_sent = existing[key]
                diff_recv = max(0, log["bytes_received"] - e_recv)
                diff_sent = max(0, log["bytes_sent"] - e_sent)
                if diff_recv > 0 or diff_sent > 0:
                    update_records.append((diff_recv, diff_sent, eid))

        if new_records:
            cursor.executemany("""
                INSERT INTO connection_logs (client_name, local_ip, real_ip, connected_since, bytes_received, bytes_sent, protocol)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, new_records)

        if update_records:
            cursor.executemany("""
                UPDATE connection_logs SET bytes_received = bytes_received + ?, bytes_sent = bytes_sent + ? WHERE id = ?
            """, update_records)

        conn.commit()
        logger.info(f"✅ connection_logs обновлен: +{len(new_records)} новых, ~{len(update_records)} обновленных сессий.")

# =============================================================================
# ОСНОВНОЙ ЦИКЛ
# =============================================================================
def process_logs():
    start_time = time.time()
    logger.info("=" * 50)
    logger.info("🚀 ЗАПУСК ОБРАБОТКИ ЛОГОВ")
    try:
        initialize_database()
        ensure_column_exists()
        cleanup_old_stats()
        
        all_logs = []
        for log_file, protocol in LOG_FILES:
            all_logs.extend(parse_log_file(log_file, protocol))
        
        if all_logs:
            save_daily_stats(all_logs)
            aggregate_daily_to_monthly()
            aggregate_monthly_to_yearly()
            save_connection_logs(all_logs)
            logger.info("✅ Обработка завершена успешно.")
        else:
            logger.warning("⚠️ Нет активных сессий для обработки.")
    except Exception as e:
        logger.critical(f"❌ Критическая ошибка в процессе обработки: {e}", exc_info=True)
    finally:
        elapsed = time.time() - start_time
        logger.info(f"⏱ Выполнение цикла заняло: {elapsed:.2f} сек.")
        logger.info("=" * 50)

if __name__ == "__main__":
    logger.info("📡 logs.py запущен. Режим: демон. Интервал: 30 сек.")
    schedule.every(30).seconds.do(process_logs)
    process_logs()

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("🛑 Получен сигнал остановки (Ctrl+C). Завершение работы...")
        logger.info("👋 До свидания.")
    except Exception as e:
        logger.critical(f"❌ Фатальная ошибка в основном цикле: {e}", exc_info=True)
        sys.exit(1)