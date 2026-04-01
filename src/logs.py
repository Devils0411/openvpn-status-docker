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
HOURLY_RETENTION_DAYS = 60
CONNECTION_RETENTION_DAYS = 3

def initialize_database():
    """Создаёт таблицы, если они не существуют."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            
            # 1. Таблица daily_stats (почасовая статистика)
            # Уникальность: клиент + час
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
                CREATE INDEX IF NOT EXISTS idx_daily_client_hour
                ON daily_stats(client_name, hour)
            """)

            # 2. Таблица monthly_stats (дневная статистика)
            # Уникальность: клиент + день (YYYY-MM-DD)
            # IP адрес удален
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
                CREATE INDEX IF NOT EXISTS idx_monthly_client_month
                ON monthly_stats(client_name, month)
            """)

            # 3. Таблица years_stats (месячная статистика)
            # Уникальность: клиент + месяц (YYYY-MM)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS years_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_name TEXT,
                    month TEXT,
                    total_bytes_received INTEGER DEFAULT 0,
                    total_bytes_sent INTEGER DEFAULT 0,
                    total_connections INTEGER DEFAULT 0,
                    last_connected TEXT,
                    UNIQUE(client_name, month)
                )
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_agg_client_month
                ON years_stats(client_name, month)
            """)

            # 4. Таблица last_client_stats (для расчета дельты трафика)
            # Ключ только по client_name
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS last_client_stats (
                    client_name TEXT PRIMARY KEY,
                    connected_since TEXT,
                    bytes_received INTEGER,
                    bytes_sent INTEGER
                )
            """)
            # 5. Таблица connection_logs (ИСТОРИЯ ПОДКЛЮЧЕНИЙ - для /ovpn/history)
            # Здесь СОХРАНЯЕМ ip_address и protocol для отображения в истории
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
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_logs_client_since
                ON connection_logs(client_name, connected_since)
            """)

            conn.commit()
            logging.info("🔧 База данных инициализирована.")

    except Exception as e:
        logging.error(f"❌ Ошибка инициализации БД: {e}")
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
    except Exception as e:
        logger.error(f"❌ Ошибка проверки колонки: {e}")


def cleanup_old_data():
    """Очистка старых данных."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()

            # Очистка connection_logs
            cutoff_con_log = (datetime.now() - timedelta(days=CONNECTION_RETENTION_DAYS)).isoformat()
            cursor.execute("DELETE FROM connection_logs WHERE connected_since < ?", (cutoff_con_log,))
            
            # Очистка daily_stats
            cutoff_hourly = (datetime.now() - timedelta(days=HOURLY_RETENTION_DAYS)).strftime("%Y-%m-%d")
            cursor.execute("DELETE FROM daily_stats WHERE hour < ?", (cutoff_hourly,))
            
            # Очистка monthly_stats
            cutoff_months = (datetime.now() - timedelta(days=RETENTION_MONTHS)).strftime("%Y-%m-%d")
            cursor.execute("DELETE FROM monthly_stats WHERE month < ?", (cutoff_months,))            

            # Очистка years_stats
            cutoff_years = (datetime.now() - timedelta(days=RETENTION_YEARS)).strftime("%Y-%m")
            cursor.execute("DELETE FROM years_stats WHERE month < ?", (cutoff_years,))
            
            conn.commit()
            logger.info("🧹 Старые данные очищены.")
    except Exception as e:
        logger.error(f"❌ Ошибка очистки данных: {e}")


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


def mask_ip(ip_address):
    if not ip_address:
        return "0.0.0.0"
    if ":" in ip_address:
        parts = ip_address.split(":", 1)
        if len(parts) == 2 and parts[0].lower() in ["udp4", "tcp4", "udp6", "tcp6"]:
            ip_address = parts[1]
    ip = ip_address.split(":")[0] if ":" in ip_address else ip_address
    return ip


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


def save_daily_stats(logs):
    """
    Сохраняет данные в таблицу daily_stats.
    Рассчитывает дельту трафика на основе last_client_stats (только по client_name).
    """
    current_hour = datetime.now().strftime("%Y-%m-%dT%H:00:00")
    
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            aggregated_data = {}

            for log in logs:
                try:
                    connected_since = datetime.fromisoformat(log["connected_since"])
                except (ValueError, TypeError) as e: 
                    logger.warning(f"Ошибка parsing даты: {e}")
                    continue

                client_name = log["client_name"]
                new_bytes_received = log.get("bytes_received", 0)
                new_bytes_sent = log.get("bytes_sent", 0)

                # Получаем предыдущее состояние ТОЛЬКО по client_name
                cursor.execute("""
                    SELECT connected_since, bytes_received, bytes_sent 
                    FROM last_client_stats 
                    WHERE client_name = ?
                """, (client_name,))
                last_state = cursor.fetchone()

                if last_state:
                    last_connected_since, last_bytes_received, last_bytes_sent = last_state
                    # Если время подключения изменилось (новая сессия), считаем весь трафик как новый
                    if last_connected_since != log["connected_since"]:
                        diff_received = new_bytes_received
                        diff_sent = new_bytes_sent
                    else:
                        diff_received = max(0, new_bytes_received - last_bytes_received)
                        diff_sent = max(0, new_bytes_sent - last_bytes_sent)
                else:
                    diff_received = new_bytes_received
                    diff_sent = new_bytes_sent

                # Агрегация для daily_stats (группируем по клиенту и текущему часу)
                key = (client_name, current_hour)
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

                # Обновляем last_client_stats для следующего запуска
                cursor.execute("""
                    INSERT INTO last_client_stats (client_name, connected_since, bytes_received, bytes_sent)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(client_name) DO UPDATE SET
                    connected_since = excluded.connected_since,
                    bytes_received = excluded.bytes_received,
                    bytes_sent = excluded.bytes_sent
                """, (client_name, log["connected_since"], new_bytes_received, new_bytes_sent))

            # Сохранение в daily_stats
            for (client_name, hour), data in aggregated_data.items(): 
                cursor.execute("""
                    INSERT INTO daily_stats 
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
            logger.info("💾 Данные сохранены в daily_stats")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения статистики: {e}")
        raise


def aggregate_daily_to_monthly():
    """
    Агрегирует данные из daily_stats в monthly_stats.
    Группировка: Клиент + День (YYYY-MM-DD).
    """
    logging.info("🔄 Агрегация daily_stats -> monthly_stats...")
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO monthly_stats (client_name, month, total_bytes_received, total_bytes_sent, total_connections, last_connected)
                SELECT 
                    ds.client_name,
                    strftime('%Y-%m-%d', ds.hour) as day_month,
                    SUM(ds.total_bytes_received),
                    SUM(ds.total_bytes_sent),
                    SUM(ds.total_connections),
                    MAX(ds.last_connected)
                FROM daily_stats ds
                GROUP BY ds.client_name, day_month
                ON CONFLICT(client_name, month) DO UPDATE SET
                    total_bytes_received = excluded.total_bytes_received,
                    total_bytes_sent = excluded.total_bytes_sent,
                    total_connections = excluded.total_connections,
                    last_connected = excluded.last_connected
            """)
            
            conn.commit()
            logging.info("✅ Агрегация daily_stats -> monthly_stats завершена.")
    except Exception as e:
        logging.error(f"❌ Ошибка при агрегации daily->monthly: {e}")
        raise

def aggregate_monthly_to_yearly():
    """
    Агрегирует данные из monthly_stats в years_stats.
    Группировка: Клиент + Месяц (YYYY-MM).
    """
    logging.info("🔄 Агрегация monthly_stats -> years_stats...")
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO years_stats (client_name, month, total_bytes_received, total_bytes_sent, total_connections, last_connected)
                SELECT 
                    ms.client_name,
                    strftime('%Y-%m', ms.month) as agg_month,
                    SUM(ms.total_bytes_received),
                    SUM(ms.total_bytes_sent),
                    SUM(ms.total_connections),
                    MAX(ms.last_connected)
                FROM monthly_stats ms
                GROUP BY ms.client_name, agg_month
                ON CONFLICT(client_name, month) DO UPDATE SET
                    total_bytes_received = excluded.total_bytes_received,
                    total_bytes_sent = excluded.total_bytes_sent,
                    total_connections = excluded.total_connections,
                    last_connected = excluded.last_connected
            """)
            
            conn.commit()
            logging.info("✅ Агрегация monthly_stats -> years_stats завершена.")
    except Exception as e:
        logging.error(f"❌ Ошибка при агрегации monthly->yearly: {e}")
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
                    # Новая сессия - вставляем запись
                    cursor.execute("""
                        INSERT INTO connection_logs (client_name, local_ip, real_ip, connected_since, bytes_received, bytes_sent, protocol)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        log["client_name"], log["local_ip"], log["real_ip"],
                        log["connected_since"], log["bytes_received"],
                        log["bytes_sent"], log["protocol"],
                    ))
                else:
                   # Сессия существует - обновляем трафик (дельта)
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
            logger.info("💾 Логи подключений сохранены в connection_logs")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения журналов: {e}")
        raise


def fix_monthly_stats_uniqueness():
    """
    Удаляет дубликаты в monthly_stats и создаёт уникальный индекс.
    """
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            
            # 1. Находим дубликаты и создаём временную таблицу с агрегированными данными
            cursor.execute("""
                CREATE TEMP TABLE IF NOT EXISTS monthly_stats_dedup AS
                SELECT 
                    client_name,
                    month,
                    SUM(total_bytes_received) as total_bytes_received,
                    SUM(total_bytes_sent) as total_bytes_sent,
                    SUM(total_connections) as total_connections,
                    MAX(last_connected) as last_connected
                FROM monthly_stats
                GROUP BY client_name, month
            """)
            
            # 2. Удаляем все старые записи
            cursor.execute("DELETE FROM monthly_stats")
            
            # 3. Вставляем очищенные данные обратно
            cursor.execute("""
                INSERT INTO monthly_stats 
                (client_name, month, total_bytes_received, total_bytes_sent, 
                 total_connections, last_connected)
                SELECT * FROM monthly_stats_dedup
            """)
            
            # 4. Удаляем временную таблицу
            cursor.execute("DROP TABLE IF EXISTS monthly_stats_dedup")
            
            # 5. Создаём уникальный индекс
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_monthly_unique 
                ON monthly_stats(client_name, month)
            """)
            
            conn.commit()
            logger.info("✅ monthly_stats: дубликаты удалены, индекс создан")
            
    except Exception as e:
        logger.error(f"❌ Ошибка очистки monthly_stats: {e}")
        raise

def fix_years_stats_uniqueness():
    """
    Удаляет дубликаты в years_stats и создаёт уникальный индекс.
    """
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            
            # Агрегируем дубликаты
            cursor.execute("""
                CREATE TEMP TABLE IF NOT EXISTS years_stats_dedup AS
                SELECT 
                    client_name,
                    month,
                    SUM(total_bytes_received) as total_bytes_received,
                    SUM(total_bytes_sent) as total_bytes_sent,
                    SUM(total_connections) as total_connections,
                    MAX(last_connected) as last_connected
                FROM years_stats
                GROUP BY client_name, month
            """)
            
            cursor.execute("DELETE FROM years_stats")
            
            cursor.execute("""
                INSERT INTO years_stats 
                (client_name, month, total_bytes_received, total_bytes_sent, 
                 total_connections, last_connected)
                SELECT * FROM years_stats_dedup
            """)
            
            cursor.execute("DROP TABLE IF EXISTS years_stats_dedup")
            
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_years_unique 
                ON years_stats(client_name, month)
            """)
            
            conn.commit()
            logger.info("✅ years_stats: дубликаты удалены, индекс создан")
            
    except Exception as e:
        logger.error(f"❌ Ошибка очистки years_stats: {e}")
        raise

def add_unique_indexes():
    """
    Создаёт уникальные индексы для всех таблиц агрегации.
    Предварительно очищает дубликаты.
    """
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            
            # daily_stats: client_name + hour
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_unique 
                ON daily_stats(client_name, hour)
            """)
            
            # monthly_stats: client_name + month
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_monthly_unique 
                ON monthly_stats(client_name, month)
            """)
            
            # years_stats: client_name + month
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_years_unique 
                ON years_stats(client_name, month)
            """)
            
            conn.commit()
            logger.info("✅ Уникальные индексы добавлены")
    except sqlite3.IntegrityError as e:
        # Если есть дубликаты — запускаем очистку
        logger.warning(f"⚠️ Найдены дубликаты, запускаю очистку: {e}")
        fix_monthly_stats_uniqueness()
        fix_years_stats_uniqueness()
        # Повторная попытка создать индексы
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_monthly_unique ON monthly_stats(client_name, month)")
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_years_unique ON years_stats(client_name, month)")
            conn.commit()
        logger.info("✅ Индексы созданы после очистки дубликатов")
    except Exception as e:
        logger.error(f"❌ Ошибка создания индексов: {e}")
        raise


def process_logs():
    logger.info("=" * 40)
    logger.info("🚀 ЗАПУСК ОБРАБОТКИ ЛОГОВ")
    logger.info("=" * 40)
    try:
        initialize_database()
        ensure_column_exists()
        add_unique_indexes()
        cleanup_old_data()
        
        all_logs = []
        for log_file, protocol in LOG_FILES:
            logs = parse_log_file(log_file, protocol)
            all_logs.extend(logs)
        
        if all_logs:
            # 1. Сбор данных в daily_stats
            save_daily_stats(all_logs)
            # 2. Агрегация daily -> monthly
            aggregate_daily_to_monthly()
            # 3. Агрегация monthly -> yearly
            aggregate_monthly_to_yearly()
            # 4. Сохранение истории подключений (connection_logs)
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