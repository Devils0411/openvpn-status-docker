import os
import sqlite3
import csv
import time
import logging
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
MAX_LOG_SIZE = 10 * 1024 * 1024
BACKUP_COUNT = 5

LOG_LEVEL = getattr(Config, 'LOG_LEVEL', logging.INFO)

class LevelFilter(logging.Filter):
    def __init__(self, min_level, max_level=None):
        super().__init__()
        self.min_level = min_level
        self.max_level = max_level or min_level

    def filter(self, record):
        return self.min_level <= record.levelno <= self.max_level

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
#logger.addHandler(stdout_handler)

# =============================================================================
# КОНФИГУРАЦИЯ
# =============================================================================
DB_PATH = Config.LOGS_DATABASE_PATH
LOG_FILES = Config.LOG_FILES
RETENTION_MONTHS = 180
RETENTION_YEARS = 365
HOURLY_RETENTION_DAYS = 30

def initialize_database():
    """Создаёт таблицы, если они не существуют."""
    try:
        # ✅ Открываем соединение с БД в контексте `with`
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()  # ✅ Создаём курсор ВНУТРИ контекста

            # Таблица hourly_stats (для графика по дням)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS hourly_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_name TEXT,
                    hour TEXT,
                    total_bytes_received INTEGER,
                    total_bytes_sent INTEGER,
                    total_connections INTEGER,
                    last_connected TEXT,
                    UNIQUE(client_name, hour)
                )
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_hourly_client_hour
                ON hourly_stats(client_name, hour)
            """)

            # Таблица monthly_stats (детализированная по дням/IP)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS monthly_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_name TEXT,
                    ip_address TEXT,
                    month TEXT, -- формат YYYY-MM-DD (хранит день, но данные агрегированы за месяц)
                    total_bytes_received INTEGER,
                    total_bytes_sent INTEGER,
                    total_connections INTEGER,
                    last_connected TEXT,
                    UNIQUE(client_name, month, ip_address)
                )
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_monthly_client_month
                ON monthly_stats(client_name, month)
            """)

            # ✅ НОВАЯ ТАБЛИЦА: monthly_aggregated_stats (агрегация по месяцу без IP)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS monthly_aggregated_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_name TEXT,
                    month TEXT, -- формат YYYY-MM (месяц в виде строки)
                    total_bytes_received INTEGER DEFAULT 0,
                    total_bytes_sent INTEGER DEFAULT 0,
                    total_connections INTEGER DEFAULT 0,
                    last_connected TEXT,
                    UNIQUE(client_name, month) -- уникальность по клиенту и месяцу
                )
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_agg_client_month
                ON monthly_aggregated_stats(client_name, month)
            """)

            conn.commit()
            logging.info("🔧 База данных инициализирована (таблицы проверены/созданы).")

    except Exception as e:
        logging.error(f"❌ Ошибка инициализации БД: {e}")
        raise  # Пробрасываем исключение, чтобы основной процесс его перехватил


def ensure_column_exists():
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(monthly_stats)")
            columns = [row[1] for row in cursor.fetchall()]
            if "last_connected" not in columns:
                cursor.execute("ALTER TABLE monthly_stats ADD COLUMN last_connected TEXT")
                conn.commit()
    except Exception as e:
        logger.error(f"❌ Ошибка проверки колонки: {e}")

def cleanup_old_data():
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()

            cutoff_hourly = (datetime.now() - timedelta(days=HOURLY_RETENTION_DAYS)).strftime("%Y-%m-%d")
            cursor.execute("DELETE FROM hourly_stats WHERE hour < ?", (cutoff_hourly,))
            
            cutoff_months = (datetime.now() - timedelta(days=RETENTION_MONTHS)).strftime("%Y-%m-%d")
            cursor.execute("DELETE FROM monthly_stats WHERE month < ?", (cutoff_months,))            

            cutoff_years = (datetime.now() - timedelta(days=RETENTION_YEARS)).strftime("%Y-%m")
            cursor.execute("DELETE FROM monthly_aggregated_stats WHERE month < ?", (cutoff_years,))
            
            conn.commit()
    except Exception as e:
        logger.error(f"❌ Ошибка очистки данных: {e}")

def mask_ip(ip_address):
    if not ip_address:
        return "0.0.0.0"
    if ":" in ip_address:
        parts = ip_address.split(":", 1)
        if len(parts) == 2 and parts[0].lower() in ["udp4", "tcp4", "udp6", "tcp6"]:
            ip_address = parts[1]
    ip = ip_address.split(":")[0] if ":" in ip_address else ip_address
    return ip

def format_date(date_string):
    try:
        date_obj = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
        server_timezone = get_localzone()
        localized_date = date_obj.replace(tzinfo=server_timezone)
        utc_date = localized_date.astimezone(timezone.utc)
        return utc_date.isoformat()
    except Exception as e:
        logger.error(f"Ошибка форматирования даты: {e}")
        return datetime.now(timezone.utc).isoformat()

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

def parse_log_file(log_file, config_protocol):
    logs = []
    if not os.path.exists(log_file):
        logger.error(f"❌ Файл не найден: {log_file}")
        return []

    try:
        with open(log_file, newline="", encoding="utf-8") as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                # ✅ ИСПРАВЛЕНО: Убран лишний пробел в строке
                if row[0] == "CLIENT_LIST":
                    client_name = row[1]
                    real_address = row[2]
                    received = int(row[5])
                    sent = int(row[6])
                    start_date = datetime.strptime(row[7], "%Y-%m-%d %H:%M:%S")
                    duration = format_duration(start_date)
                    protocol = extract_protocol_from_address(real_address, config_protocol)
                    
                    logs.append({
                        "client_name": client_name,
                        "real_ip": mask_ip(real_address),
                        "local_ip": row[3],
                        "bytes_received": received,
                        "connected_since": format_date(row[7]),
                        "bytes_sent": sent,
                        "duration": duration,
                        "protocol": protocol,
                    })
        logger.info(f"📄 Обработан {os.path.basename(log_file)}: {len(logs)} клиентов")
    except Exception as e:
        logger.error(f"❌ Ошибка парсинга: {e}")
    return logs


def save_monthly_stats(logs):
    """Сохраняет статистику в monthly_stats и hourly_stats."""
    current_date = datetime.today().strftime("%Y-%m-%d")
    current_hour = datetime.now().strftime("%Y-%m-%dT%H:00:00")
    
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            aggregated_data = {}
            hourly_aggregated = {}

            for log in logs:
                try:
                    connected_since = datetime.fromisoformat(log["connected_since"])
                except (ValueError, TypeError) as e: 
                    logger.warning(f"Ошибка parsing даты: {e}")
                    continue

                client_name = log["client_name"]
                ip_address = log["local_ip"]
                new_bytes_received = log.get("bytes_received", 0)
                new_bytes_sent = log.get("bytes_sent", 0)

                cursor.execute("""
                    SELECT connected_since, bytes_received, bytes_sent 
                    FROM last_client_stats 
                    WHERE client_name = ? AND ip_address = ?
                """, (client_name, ip_address))
                last_state = cursor.fetchone()

                if last_state:
                    last_connected_since, last_bytes_received, last_bytes_sent = last_state
                    if last_connected_since != log["connected_since"]:
                        diff_received = new_bytes_received
                        diff_sent = new_bytes_sent
                    else:
                        diff_received = max(0, new_bytes_received - last_bytes_received)
                        diff_sent = max(0, new_bytes_sent - last_bytes_sent)
                else:
                    diff_received = new_bytes_received
                    diff_sent = new_bytes_sent

                # Агрегация monthly
                key = (client_name, ip_address, current_date)
                if key not in aggregated_data:
                    aggregated_data[key] = {
                        "total_bytes_received": 0,
                        "total_bytes_sent": 0,
                        "total_connections": 0,
                        "last_connected": connected_since,
                    }

                aggregated_data[key]["total_bytes_received"] += diff_received
                aggregated_data[key]["total_bytes_sent"] += diff_sent
                aggregated_data[key]["total_connections"] += 1
                if connected_since > aggregated_data[key]["last_connected"]:
                    aggregated_data[key]["last_connected"] = connected_since

                # Агрегация hourly
                hour_key = (client_name, current_hour)
                if hour_key not in hourly_aggregated:
                    hourly_aggregated[hour_key] = {
                        "total_bytes_received": 0,
                        "total_bytes_sent": 0,
                        "total_connections": 0,
                        "last_connected": connected_since,
                    }
                hourly_aggregated[hour_key]["total_bytes_received"] += diff_received
                hourly_aggregated[hour_key]["total_bytes_sent"] += diff_sent
                hourly_aggregated[hour_key]["total_connections"] += 1
                if connected_since > hourly_aggregated[hour_key]["last_connected"]:
                    hourly_aggregated[hour_key]["last_connected"] = connected_since

                cursor.execute("""
                    INSERT INTO last_client_stats (client_name, ip_address, connected_since, bytes_received, bytes_sent)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(client_name, ip_address) DO UPDATE SET
                    connected_since = excluded.connected_since,
                    bytes_received = excluded.bytes_received,
                    bytes_sent = excluded.bytes_sent
                """, (client_name, ip_address, log["connected_since"], new_bytes_received, new_bytes_sent))

            # Сохранение monthly
            for (client_name, ip_address, date), data in aggregated_data.items(): 
                cursor.execute("""
                    INSERT INTO monthly_stats 
                    (client_name, ip_address, month, total_bytes_received, total_bytes_sent, total_connections, last_connected)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(client_name, month, ip_address) DO UPDATE SET
                    total_bytes_received = total_bytes_received + excluded.total_bytes_received,
                    total_bytes_sent = total_bytes_sent + excluded.total_bytes_sent,
                    total_connections = total_connections + excluded.total_connections,
                    last_connected = MAX(COALESCE(last_connected, ''), excluded.last_connected)
                """, (
                    client_name, ip_address, date,
                    data["total_bytes_received"], data["total_bytes_sent"],
                    data["total_connections"], data["last_connected"].isoformat(),
                ))
            
            # Сохранение hourly
            for (client_name, hour), data in hourly_aggregated.items():
                cursor.execute("""
                    INSERT INTO hourly_stats 
                    (client_name, hour, total_bytes_received, total_bytes_sent, total_connections, last_connected)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(client_name, hour) DO UPDATE SET
                    total_bytes_received = total_bytes_received + excluded.total_bytes_received,
                    total_bytes_sent = total_bytes_sent + excluded.total_bytes_sent,
                    total_connections = total_connections + excluded.total_connections,
                    last_connected = MAX(COALESCE(last_connected, ''), excluded.last_connected)
                """, (
                    client_name, hour,
                    data["total_bytes_received"], data["total_bytes_sent"],
                    data["total_connections"], data["last_connected"].isoformat(),
                ))
            
            conn.commit()
            logger.info(f"💾 Статистика сохранена")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения статистики: {e}")
        raise


def aggregate_monthly_stats():
    """
    Агрегирует данные из таблицы monthly_stats в monthly_aggregated_stats
    по клиенту и месяцу (YYYY-MM), суммируя байты и соединения, 
    и находя последнее подключение.
    """
    logging.info("🔄 Начата агрегация данных в monthly_aggregated_stats...")
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            
            # Шаг 1: Обновляем существующие записи в aggregated_stats
            # Суммируем данные из monthly_stats за тот же месяц
            cursor.execute("""
                INSERT INTO monthly_aggregated_stats (client_name, month, total_bytes_received, total_bytes_sent, total_connections, last_connected)
                SELECT 
                    ms.client_name,
                    strftime('%Y-%m', ms.month) as agg_month, -- извлекаем месяц из поля month (YYYY-MM-DD)
                    SUM(ms.total_bytes_received) as sum_rx,
                    SUM(ms.total_bytes_sent) as sum_tx,
                    SUM(ms.total_connections) as sum_conns,
                    MAX(ms.last_connected) as latest_conn
                FROM monthly_stats ms
                -- Выбираем только те записи, которые не учтены в aggregated_stats или есть новые данные в monthly_stats
                -- (например, если monthly_stats обновилась)
                -- В простом случае просто группируем и вставляем, игнорируя дубликаты
                GROUP BY ms.client_name, agg_month
                ON CONFLICT(client_name, month) DO UPDATE SET
                    total_bytes_received = excluded.total_bytes_received,
                    total_bytes_sent = excluded.total_bytes_sent,
                    total_connections = excluded.total_connections,
                    last_connected = excluded.last_connected
            """)
            
            # Шаг 2: Удаляем старые записи в aggregated_stats, если в monthly_stats ничего нет за этот месяц
            # (опционально, если нужно поддерживать точное соответствие)
            # В данном случае, ON CONFLICT UPDATE делает большую часть работы
            
            conn.commit()
            logging.info("✅ Агрегация данных в monthly_aggregated_stats завершена.")
    except Exception as e:
        logging.error(f"❌ Ошибка при агрегации данных: {e}")
        raise


def save_connection_logs(logs):
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            for log in logs:
                cursor.execute("""
                    SELECT id, bytes_received, bytes_sent FROM connection_logs 
                    WHERE client_name = ? AND connected_since = ? LIMIT 1
                """, (log["client_name"], log["connected_since"]))
                existing_log = cursor.fetchone()

                if existing_log is None:
                    cursor.execute("""
                        INSERT INTO connection_logs (client_name, local_ip, real_ip, connected_since, bytes_received, bytes_sent, protocol)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        log["client_name"], log["local_ip"], log["real_ip"],
                        log["connected_since"], log["bytes_received"],
                        log["bytes_sent"], log["protocol"],
                    ))
                else:
                    existing_id, existing_bytes_received, existing_bytes_sent = existing_log
                    diff_received = log["bytes_received"] - existing_bytes_received
                    diff_sent = log["bytes_sent"] - existing_bytes_sent
                    if diff_received > 0 or diff_sent > 0:
                        cursor.execute("""
                            UPDATE connection_logs
                            SET bytes_received = bytes_received + ?, bytes_sent = bytes_sent + ?
                            WHERE id = ?
                        """, (diff_received, diff_sent, existing_id))
            conn.commit()
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения журналов: {e}")
        raise

def process_logs():
    logger.info("=" * 40)
    logger.info("🚀 ЗАПУСК ОБРАБОТКИ ЛОГОВ")
    logger.info("=" * 40)
    try:
        initialize_database()
        ensure_column_exists()
        cleanup_old_data()
        
        all_logs = []
        for log_file, protocol in LOG_FILES:
            logs = parse_log_file(log_file, protocol)
            all_logs.extend(logs)
        
        if all_logs:
            save_monthly_stats(all_logs)
            aggregate_monthly_stats()
            save_connection_logs(all_logs)
            logger.info("✅ Обработка завершена успешно")
        else:
            logger.warning("⚠️ Нет данных для обработки")
    except Exception as e:
        logger.critical(f"❌ Критическая ошибка: {e}", exc_info=True)
        raise
    finally:
        logger.info("=" * 40)

if __name__ == "__main__":
    try:
        process_logs()
    except Exception as e:
        logger.critical(f"❌ Фатальная ошибка: {e}", exc_info=True)
        raise