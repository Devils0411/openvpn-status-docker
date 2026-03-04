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
# НАСТРОЙКА ЛОГИРОВАНИЯ С РАЗДЕЛЕНИЕМ ПО УРОВНЯМ И РОТАЦИЕЙ ФАЙЛОВ
# =============================================================================

# Берем путь к папке Logs из конфиг файла
LOG_DIR = Config.LOGS_PATH

# Полные пути к файлам логов
STDOUT_LOG = os.path.join(LOG_DIR, 'logs.stdout.log')
STDERR_LOG = os.path.join(LOG_DIR, 'logs.stderr.log')

# Параметры ротации
MAX_LOG_SIZE = 10 * 1024 * 1024  # 10 МБ
BACKUP_COUNT = 5                 # Хранить 5 последних файлов

# Очищаем ВСЕ обработчики корневого логгера (важно!)
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)


class LevelFilter(logging.Filter):
    """Фильтр для разделения логов по уровням."""
    def __init__(self, min_level, max_level=None):
        super().__init__()
        self.min_level = min_level
        self.max_level = max_level or min_level

    def filter(self, record):
        return self.min_level <= record.levelno <= self.max_level


# Создаём logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Принимаем все уровни
logger.propagate = False  # ❗ Важно: не передавать логи в root-логгер

# Очищаем существующие обработчики
logger.handlers.clear()


# =============================================================================
# ФАЙЛ ДЛЯ WARNING, ERROR, CRITICAL (stderr)
# =============================================================================
stderr_handler = RotatingFileHandler(
    STDERR_LOG,
    maxBytes=MAX_LOG_SIZE,
    backupCount=BACKUP_COUNT,
    encoding='utf-8',
    delay=True
)
stderr_handler.setLevel(logging.WARNING)  # Только WARNING и выше
stderr_handler.addFilter(LevelFilter(logging.WARNING, logging.CRITICAL))
stderr_formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S'
)
stderr_handler.setFormatter(stderr_formatter)


# =============================================================================
# ФАЙЛ ДЛЯ INFO, DEBUG (stdout)
# =============================================================================
stdout_handler = RotatingFileHandler(
    STDOUT_LOG,
    maxBytes=MAX_LOG_SIZE,
    backupCount=BACKUP_COUNT,
    encoding='utf-8',
    delay=True
)
stdout_handler.setLevel(logging.DEBUG)  # DEBUG и INFO
stdout_handler.addFilter(LevelFilter(logging.DEBUG, logging.INFO))
stdout_formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S'
)
stdout_handler.setFormatter(stdout_formatter)


# =============================================================================
# ДОБАВЛЯЕМ ОБРАБОТЧИКИ К LOGGER
# =============================================================================
logger.addHandler(stderr_handler)
# logger.addHandler(stdout_handler) #Раскомментировать, если необходимо отследить какие-либо действия скрипта

# =============================================================================
# КОНФИГУРАЦИЯ
# =============================================================================

# Путь к базе данных
DB_PATH = Config.LOGS_DATABASE_PATH
# Получаем LOG_FILES из конфигурации
LOG_FILES = Config.LOG_FILES
# 📅 Хранить данные за последние 3 месяца
RETENTION_MONTHS = 3


def initialize_database():
    """Создаёт таблицы базы данных, если их нет."""
    logger.info("Инициализация базы данных...")
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Таблица для ежемесячной статистики
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS monthly_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_name TEXT,
                ip_address TEXT,
                month TEXT,
                total_bytes_received INTEGER,
                total_bytes_sent INTEGER,
                total_connections INTEGER,
                last_connected TEXT,
                UNIQUE(client_name, month, ip_address)
            )
            """
        )
        logger.debug("Таблица monthly_stats создана/проверена")

        # Таблица для журналов подключений
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS connection_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_name TEXT,
                local_ip TEXT,
                real_ip TEXT,
                connected_since DATETIME,
                bytes_received INTEGER,
                bytes_sent INTEGER,
                protocol TEXT
            )
            """
        )
        logger.debug("Таблица connection_logs создана/проверена")
        
        # Хранит последнее состояние клиентов
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS last_client_stats (
                client_name TEXT,
                ip_address TEXT,
                connected_since TEXT,
                bytes_received INTEGER,
                bytes_sent INTEGER,
                PRIMARY KEY (client_name, ip_address)
            )
            """
        )
        logger.debug("Таблица last_client_stats создана/проверена")

        conn.commit()
        conn.close()
        logger.info("✅ База данных инициализирована успешно")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации базы данных: {e}")
        raise


def ensure_column_exists():
    """Проверяет и добавляет отсутствующие колонки."""
    logger.debug("Проверка наличия колонки last_connected...")
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(monthly_stats)")
            columns = [row[1] for row in cursor.fetchall()]
            
            if "last_connected" not in columns:
                cursor.execute("ALTER TABLE monthly_stats ADD COLUMN last_connected TEXT")
                conn.commit()
                logger.info("Добавлена колонка last_connected в monthly_stats")
            else:
                logger.debug("Колонка last_connected уже существует")
    except Exception as e:
        logger.error(f"❌ Ошибка проверки колонки: {e}")


def cleanup_old_data():
    """
    🔹 Удаляет данные старше 6 месяцев из monthly_stats.
    Вызывается перед сохранением новой статистики.
    """
    logger.info(f"🧹 Очистка данных старше {RETENTION_MONTHS} месяцев...")
    
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            
            # Вычисляем дату 6 месяцев назад
            cutoff_date = datetime.now() - timedelta(days=RETENTION_MONTHS * 30)
            cutoff_month_str = cutoff_date.strftime("%b. %Y")
            
            # Получаем список всех месяцев в базе
            cursor.execute("SELECT DISTINCT month FROM monthly_stats ORDER BY month")
            all_months = [row[0] for row in cursor.fetchall()]
            
            # Определяем месяцы для удаления
            months_to_delete = []
            for month_str in all_months:
                try:
                    # Парсим формат "Jan. 2024"
                    month_obj = datetime.strptime(month_str, "%b. %Y")
                    if month_obj < cutoff_date:
                        months_to_delete.append(month_str)
                except ValueError:
                    logger.warning(f"Некорректный формат месяца: {month_str}")
                    continue
            
            if months_to_delete:
                # Удаляем старые записи
                placeholders = ','.join('?' * len(months_to_delete))
                cursor.execute(
                    f"DELETE FROM monthly_stats WHERE month IN ({placeholders})",
                    months_to_delete
                )
                deleted_count = cursor.rowcount
                conn.commit()
                logger.info(f"✅ Удалено записей за {len(months_to_delete)} месяцев: {deleted_count} строк")
                logger.debug(f"🗑️ Удалённые месяцы: {', '.join(months_to_delete)}")
            else:
                logger.debug("📁 Нет данных для очистки (все записи в пределах 6 месяцев)")
            
            # Очищаем last_client_stats для клиентов, которых нет в monthly_stats
            cursor.execute("""
                DELETE FROM last_client_stats 
                WHERE (client_name, ip_address) NOT IN (
                    SELECT client_name, ip_address FROM monthly_stats
                )
            """)
            orphaned = cursor.rowcount
            if orphaned > 0:
                conn.commit()
                logger.debug(f"🗑️ Удалено {orphaned} записей last_client_stats без привязки")
                
    except Exception as e:
        logger.error(f"❌ Ошибка очистки старых данных: {e}")


def mask_ip(ip_address):
    """Маскирует IP адрес."""
    if not ip_address:
        return "0.0.0.0"  # значение по умолчанию
    
    ip = ip_address.split(":")[0]
    parts = ip.split(".")

    if len(parts) == 4:
        try:
            parts = [str(int(part)) for part in parts]
            return f"{parts[0]}.{parts[1]}.{parts[2]}.{parts[3]}"
        except ValueError:
            logger.warning(f"Некорректный IP адрес: {ip_address}")
            return ip

    return ip_address


def format_date(date_string):
    """Форматирует дату в UTC."""
    try:
        date_obj = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
        server_timezone = get_localzone()
        localized_date = date_obj.replace(tzinfo=server_timezone)
        utc_date = localized_date.astimezone(timezone.utc)
        return utc_date.isoformat()
    except Exception as e:
        logger.error(f"Ошибка форматирования даты {date_string}: {e}")
        return datetime.now(timezone.utc).isoformat()


def format_duration(start_time):
    """Форматирует длительность подключения."""
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


def parse_log_file(log_file, protocol):
    """Читает и парсит файл лога."""
    logger.info(f"📄 Парсинг файла лога: {log_file} ({protocol})")
    logs = []
    total_received = 0
    total_sent = 0
    parse_count = 0
    skipped_count = 0
    
    if not os.path.exists(log_file):
        logger.error(f"❌ Файл не найден: {log_file}")
        return []
    
    try:
        with open(log_file, newline="", encoding="utf-8") as file:
            reader = csv.reader(file)
            next(reader)  # Пропускаем заголовок

            for row in reader:
                if row[0] == "CLIENT_LIST":
                    parse_count += 1
                    client_name = row[1]
                    received = int(row[5])
                    sent = int(row[6])
                    total_received += received
                    total_sent += sent
                    start_date = datetime.strptime(row[7], "%Y-%m-%d %H:%M:%S")
                    duration = format_duration(start_date)
                    
                    logs.append(
                        {
                            "client_name": client_name,
                            "real_ip": mask_ip(row[2]),
                            "local_ip": row[3],
                            "bytes_received": received,
                            "connected_since": format_date(row[7]),
                            "bytes_sent": sent,
                            "duration": duration,
                            "protocol": protocol,
                        }
                    )
                    logger.debug(f"Обработано: {client_name} | RX: {received} | TX: {sent}")
                else:
                    skipped_count += 1

        logger.info(f"✅ Файл {log_file} обработан: {parse_count} записей, {skipped_count} пропущено")
        logger.info(f"📊 Трафик: RX={total_received} байт, TX={total_sent} байт")
    except Exception as e:
        logger.error(f"❌ Ошибка парсинга файла {log_file}: {e}")
    
    return logs


def save_monthly_stats(logs):
    """Сохраняет суммарные данные в таблицу monthly_stats."""
    logger.info(f"💾 Сохранение ежемесячной статистики ({len(logs)} записей)...")
    current_month = datetime.today().strftime("%b. %Y")

    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()

            aggregated_data = {}

            cursor.execute(
                """
                INSERT INTO monthly_stats (client_name, ip_address, month, total_bytes_received, total_bytes_sent, total_connections)
                SELECT client_name, ip_address, ?, 0, 0, 0 FROM monthly_stats
                WHERE month != ? AND (client_name, ip_address) NOT IN 
                (SELECT client_name, ip_address FROM monthly_stats WHERE month = ?)
                """,
                (current_month, current_month, current_month),
            )

            for log in logs:
                try:
                    connected_since = datetime.fromisoformat(log["connected_since"])
                    month = connected_since.strftime("%b. %Y")
                except (ValueError, TypeError) as e:
                    logger.warning(f"Ошибка parsing даты подключения: {e}")
                    continue

                client_name = log["client_name"]
                ip_address = log["local_ip"]
                new_bytes_received = log.get("bytes_received", 0)
                new_bytes_sent = log.get("bytes_sent", 0)

                cursor.execute(
                    """
                    SELECT connected_since, bytes_received, bytes_sent 
                    FROM last_client_stats 
                    WHERE client_name = ? AND ip_address = ?
                    """,
                    (client_name, ip_address),
                )
                last_state = cursor.fetchone()

                if last_state:
                    last_connected_since, last_bytes_received, last_bytes_sent = last_state
                    last_connected_month = datetime.fromisoformat(
                        last_connected_since
                    ).strftime("%b. %Y")

                    if last_connected_month != current_month:
                        diff_received = new_bytes_received
                        diff_sent = new_bytes_sent
                    elif last_connected_since != log["connected_since"]:
                        diff_received = new_bytes_received
                        diff_sent = new_bytes_sent
                    else:
                        diff_received = max(0, new_bytes_received - last_bytes_received)
                        diff_sent = max(0, new_bytes_sent - last_bytes_sent)
                else:
                    diff_received = new_bytes_received
                    diff_sent = new_bytes_sent

                key = (client_name, ip_address, month)
                if key not in aggregated_data:
                    aggregated_data[key] = {
                        "total_bytes_received": 0,
                        "total_bytes_sent": 0,
                        "total_connections": 0,
                    }

                aggregated_data[key]["total_bytes_received"] += diff_received
                aggregated_data[key]["total_bytes_sent"] += diff_sent
                aggregated_data[key]["total_connections"] += 1

                # Обновляем время последнего подключения
                aggregated_data[key]["last_connected"] = max(
                    aggregated_data[key].get("last_connected", connected_since),
                    connected_since,
                )

                cursor.execute(
                    """
                    INSERT INTO last_client_stats (client_name, ip_address, connected_since, bytes_received, bytes_sent)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(client_name, ip_address) DO UPDATE SET
                    connected_since = excluded.connected_since,
                    bytes_received = excluded.bytes_received,
                    bytes_sent = excluded.bytes_sent
                    """,
                    (
                        client_name,
                        ip_address,
                        log["connected_since"],
                        new_bytes_received,
                        new_bytes_sent,
                    ),
                )

            for (client_name, ip_address, month), data in aggregated_data.items():
                cursor.execute(
                    """
                    SELECT total_bytes_received, total_bytes_sent, total_connections, last_connected 
                    FROM monthly_stats WHERE client_name = ? AND ip_address = ? AND month = ?
                    """,
                    (client_name, ip_address, month),
                )
                existing_log = cursor.fetchone()

                if existing_log:
                    existing_bytes_received, existing_bytes_sent, existing_connections, existing_last_connected = existing_log
                    last_connected = max(existing_last_connected or "", data["last_connected"].isoformat())

                    cursor.execute(
                        """
                        UPDATE monthly_stats
                        SET total_bytes_received = total_bytes_received + ?, 
                            total_bytes_sent = total_bytes_sent + ?, 
                            total_connections = total_connections + ?,
                            last_connected = ?
                        WHERE client_name = ? AND ip_address = ? AND month = ?
                        """,
                        (
                            data["total_bytes_received"],
                            data["total_bytes_sent"],
                            data["total_connections"],
                            last_connected,
                            client_name,
                            ip_address,
                            month,
                        ),
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO monthly_stats (client_name, ip_address, month, total_bytes_received, total_bytes_sent, total_connections, last_connected)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            client_name,
                            ip_address,
                            month,
                            data["total_bytes_received"],
                            data["total_bytes_sent"],
                            data["total_connections"],
                            data["last_connected"].isoformat(),
                        ),
                    )
            
            conn.commit()
            logger.info(f"✅ Ежемесячная статистика сохранена: {len(aggregated_data)} записей")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения ежемесячной статистики: {e}")
        raise


def save_connection_logs(logs):
    """Сохраняет данные подключений в таблицу connection_logs."""
    logger.info(f"💾 Сохранение журналов подключений ({len(logs)} записей)...")
    
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            
            new_count = 0
            update_count = 0

            for log in logs:
                # Проверяем, существует ли уже запись с такими же client_name и connected_since
                cursor.execute(
                    """
                    SELECT id, bytes_received, bytes_sent FROM connection_logs 
                    WHERE client_name = ? AND connected_since = ?
                    LIMIT 1
                    """,
                    (log["client_name"], log["connected_since"]),
                )
                existing_log = cursor.fetchone()

                if existing_log is None:
                    # Если записи нет, добавляем новую
                    cursor.execute(
                        """
                        INSERT INTO connection_logs (client_name, local_ip, real_ip, connected_since, bytes_received, bytes_sent, protocol)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            log["client_name"],
                            log["local_ip"],
                            log["real_ip"],
                            log["connected_since"],
                            log["bytes_received"],
                            log["bytes_sent"],
                            log["protocol"],
                        ),
                    )
                    new_count += 1
                else:
                    # Если запись существует, вычисляем разницу в трафике
                    existing_id, existing_bytes_received, existing_bytes_sent = existing_log

                    # Вычисляем разницу
                    diff_received = log["bytes_received"] - existing_bytes_received
                    diff_sent = log["bytes_sent"] - existing_bytes_sent

                    # Если разница больше нуля, обновляем данные
                    if diff_received > 0 or diff_sent > 0:
                        cursor.execute(
                            """
                            UPDATE connection_logs
                            SET bytes_received = bytes_received + ?, bytes_sent = bytes_sent + ?
                            WHERE id = ?
                            """,
                            (diff_received, diff_sent, existing_id),
                        )
                        update_count += 1

            # Удаляем старые записи, если их больше 100
            cursor.execute(
                """
                DELETE FROM connection_logs
                WHERE id NOT IN (
                    SELECT id FROM connection_logs ORDER BY id DESC LIMIT 100
                )
                """
            )

            conn.commit()
            logger.info(f"✅ Журналы подключений сохранены: {new_count} новых, {update_count} обновлено")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения журналов подключений: {e}")
        raise


def process_logs():
    """Основная функция для обработки логов."""
    logger.info("=" * 60)
    logger.info("🚀 ЗАПУСК ОБРАБОТКИ ЛОГОВ")
    logger.info("=" * 60)
    
    try:
        initialize_database()
        ensure_column_exists()

       # 🧹 Очищаем старые данные ПЕРЕД обработкой новых
        cleanup_old_data()
        
        all_logs = []
        for log_file, protocol in LOG_FILES:
            logs = parse_log_file(log_file, protocol)
            all_logs.extend(logs)
        
        logger.info(f"📊 Всего обработано записей: {len(all_logs)}")
        
        if all_logs:
            save_monthly_stats(all_logs)
            save_connection_logs(all_logs)
            logger.info("✅ Обработка логов завершена успешно")
        else:
            logger.warning("⚠️ Нет данных для обработки")
        
    except Exception as e:
        logger.critical(f"❌ Критическая ошибка при обработке логов: {e}", exc_info=True)
        raise
    finally:
        logger.info("=" * 60)


if __name__ == "__main__":
    try:
        process_logs()
    except Exception as e:
        logger.critical(f"❌ Фатальная ошибка при запуске: {e}", exc_info=True)
        raise