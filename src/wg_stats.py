#!/root/web/venv/bin/python
"""Скрипт сбора и сохранения статистики WireGuard"""
from datetime import datetime, timedelta
import os
import time
import sqlite3
import subprocess
import json
import schedule
import logging
from logging.handlers import RotatingFileHandler
from config import Config

# =============================================================================
# НАСТРОЙКА ЛОГИРОВАНИЯ
# =============================================================================
LOG_DIR = Config.LOGS_PATH
os.makedirs(LOG_DIR, exist_ok=True)
STDOUT_LOG = os.path.join(LOG_DIR, 'wg_stats.stdout.log')
STDERR_LOG = os.path.join(LOG_DIR, 'wg_stats.stderr.log')
MAX_LOG_SIZE = 10 * 1024 * 1024  # 10 MB
BACKUP_COUNT = 5
LOG_LEVEL = getattr(Config, 'LOG_LEVEL', logging.INFO)

class LevelFilter(logging.Filter):
    """Фильтр для разделения логов по уровням"""
    def __init__(self, min_level, max_level=None):
        super().__init__()
        self.min_level = min_level
        self.max_level = max_level or min_level
    
    def filter(self, record):
        return self.min_level <= record.levelno <= self.max_level

# Очищаем корневой логгер
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# Настраиваем логгер
logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)
logger.propagate = False
logger.handlers.clear()

# Обработчик для WARNING и выше (stderr)
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

# Обработчик для DEBUG и INFO (stdout)
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
DB_PATH = Config.WG_STATS_PATH
SETTINGS_PATH = Config.SETTINGS_PATH
SAVE_TIME = "23:59"  # Время для фиксирования дневного трафика
START_TIME = "00:00"  # Время для начала записи нового дня
EVERY_TIME = 30  # Интервал сохранения дневного и общего трафика в секундах
SYNS_TIME = 5  # Интервал синхронизации клиентов в минутах


def get_stats_retention_days(default_days=365):
    try:
        with open(SETTINGS_PATH, "r", encoding="utf-8") as settings_file:
            settings_data = json.load(settings_file)
        days = int(settings_data.get("stats_retention_days", default_days))
        return max(30, min(days, 3650))
    except (FileNotFoundError, json.JSONDecodeError, ValueError, TypeError):
        return default_days



# =============================================================================
# ФУНКЦИИ БАЗЫ ДАННЫХ
# =============================================================================
def init_db():
    """Инициализация базы данных"""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS wg_daily_stats (
            date TEXT NOT NULL,
            peer TEXT NOT NULL,
            client TEXT NOT NULL,
            received INTEGER NOT NULL,
            sent INTEGER NOT NULL,
            interface TEXT NOT NULL,
            PRIMARY KEY (date, peer, interface)
            )
            """)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS wg_intermediate (
            peer TEXT NOT NULL,
            interface TEXT NOT NULL,
            last_received INTEGER NOT NULL,
            last_sent INTEGER NOT NULL,
            date TEXT NOT NULL,
            PRIMARY KEY (peer, interface)
            )
            """)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS wg_total_stats (
            peer TEXT NOT NULL,
            client TEXT NOT NULL,
            total_received INTEGER NOT NULL,
            total_sent INTEGER NOT NULL,
            interface TEXT NOT NULL,
            PRIMARY KEY (peer, interface)
            )
            """)
            conn.commit()
        logger.info("✅ База данных WireGuard инициализирована")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации БД: {e}")
        raise

def get_wg_intermediate(data="all"):
    """Получение данных с таблицы wg_intermediate"""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            if data == "all":
                cursor.execute("""SELECT * from wg_intermediate""")
                intermediate = cursor.fetchall()
                logger.debug(f"📊 Получено {len(intermediate)} записей из wg_intermediate")
                return intermediate
            if data == "date":
                cursor.execute("""SELECT date from wg_intermediate""")
                result = [row[0] for row in cursor.fetchall()]
                return result[0] if result else None
    except Exception as e:
        logger.error(f"❌ Ошибка получения данных из wg_intermediate: {e}")
        return None

def get_wg_daily_stats():
    """Получение данных с таблицы wg_daily_stats"""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM wg_daily_stats")
            rows = cursor.fetchall()
            logger.debug(f"📊 Получено {len(rows)} записей из wg_daily_stats")
            return rows
    except Exception as e:
        logger.error(f"❌ Ошибка получения данных из wg_daily_stats: {e}")
        return []

def get_wg_total_stats():
    """Получение данных с таблицы wg_total_stats"""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("""SELECT * from wg_total_stats""")
            rows = cursor.fetchall()
            logger.debug(f"📊 Получено {len(rows)} записей из wg_total_stats")
            return rows
    except Exception as e:
        logger.error(f"❌ Ошибка получения данных из wg_total_stats: {e}")
        return []

# =============================================================================
# ФУНКЦИИ WIREGUARD
# =============================================================================
def get_wireguard_stats():
    """Получение данных из wg show через Docker"""
    try:
        # 1. Получаем ID контейнера
        id_result = subprocess.run(
            ['docker', 'ps', '--filter', 'name=amnezia', '--format', '{{.ID}}'],
            capture_output=True, text=True, check=True
        )
        container_id = id_result.stdout.strip().splitlines()[0] if id_result.stdout.strip() else None
        
        if not container_id:
            logger.error("❌ Контейнер amnezia не найден")
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
    except FileNotFoundError as e:
        logger.error(f"❌ Команда docker не найдена: {e}")
        return f"Ошибка: Команда docker не найдена"
    except Exception as e:
        logger.error(f"❌ Исключение при получении статистики WG: {e}")
        return f"Ошибка: {str(e)}"


def read_wg_config(file_path):
    """Считывает клиентские данные из JSON конфигурационного файла WireGuard."""
    client_mapping = {}
    
    # Проверяем, какой файл передан. Если просят .conf, но есть .json — используем JSON.
    if file_path.endswith(".conf"):
        json_path = file_path.replace(".conf", ".json")
        if os.path.exists(json_path):
            file_path = json_path
        else:
            # Если JSON нет, оставляем путь к .conf для старого парсера ниже
            pass

    try:
        if file_path.endswith(".json"):
            with open(file_path, "r", encoding="utf-8") as file:
                data = json.load(file)
            
            # Убираем пробелы из ключей верхнего уровня (на случай "clients ")
            data = {k.strip(): v for k, v in data.items()}
            clients = data.get("clients", {})
            
            for client_id, client_info in clients.items():
                # Очищаем ключи и строковые значения от пробелов
                clean_info = {k.strip(): v.strip() if isinstance(v, str) else v for k, v in client_info.items()}
                
                public_key = clean_info.get("publicKey", "").strip()
                name = clean_info.get("name", "N/A").strip()
                
                if public_key:
                    client_mapping[public_key] = name
            
            logger.debug(f"✅ Прочитано {len(client_mapping)} клиентов из {file_path} (JSON)")
        else:
            # Старый парсер .conf (оставлен как запасной вариант)
            current_client_name = None
            with open(file_path, "r", encoding="utf-8") as file:
                for line in file:
                    line = line.strip()
                    if line.startswith("# Client:"):
                        current_client_name = line.split(":", 1)[1].strip().split("(")[0].strip()
                    elif line.startswith("[Peer]"):
                        current_client_name = current_client_name or "N/A"
                    elif line.startswith("PublicKey =") and current_client_name:
                        public_key = line.split("=", 1)[1].strip()
                        client_mapping[public_key] = current_client_name
            logger.debug(f"✅ Прочитано {len(client_mapping)} клиентов из {file_path} (CONF)")
            
    except FileNotFoundError:
        logger.warning(f"⚠️ Конфигурационный файл {file_path} не найден")
    except json.JSONDecodeError as e:
        logger.error(f"❌ Ошибка парсинга JSON {file_path}: {e}")
    except Exception as e:
        logger.error(f"❌ Ошибка чтения конфига {file_path}: {e}")
    
    return client_mapping


def convert_to_bytes(value):
    """Преобразует значение в байты."""
    units = {
        "B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "TiB": 1024**4,
        "KB": 1000, "MB": 1000**2, "GB": 1000**3, "TB": 1000**4,
    }
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        value = value.strip()
        if not value or value in ("0 B", "0B"):
            return 0
        parts = value.split()
        if len(parts) == 1:
            return int(parts[0]) if parts[0].isdigit() else 0
        elif len(parts) == 2:
            num, unit = parts
            return int(float(num) * units.get(unit, 1)) if unit in units else 0
    return 0

def parse_wireguard_stats(output):
    """Парсинг вывода wg show, извлекаем только peer, client, received, sent, interface."""
    stats = []
    lines = output.strip().splitlines()
    interface_name = None
    client_mapping = read_wg_config("/root/web/awg/wg0.json")
    
    for line in lines:
        line = line.strip()
        if line.startswith("interface:"):
            interface_name = line.split(": ")[1]
        elif line.startswith("peer:"):
            peer = line.split(": ")[1].strip()
            client_name = client_mapping.get(peer, "Unknown")
            stats.append({
                "peer": peer,
                "client": client_name,
                "received": "0 B",
                "sent": "0 B",
                "interface": interface_name if interface_name else "Unknown",
            })
        elif line.startswith("transfer:") and stats:
            transfer_data = line.split(":")[1].strip().split(", ")
            stats[-1]["received"] = transfer_data[0].replace(" received", "").strip()
            stats[-1]["sent"] = transfer_data[1].replace(" sent", "").strip()
    
    logger.debug(f"✅ Распарсено {len(stats)} пиров WireGuard")
    return stats

def clear_wg_total_stats():
    """Очистка таблицы wg_total_stats от лишних записей"""
    try:
        output = get_wireguard_stats()
        stats = parse_wireguard_stats(output)
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            current_peers = {(data["peer"], data["interface"]) for data in stats}
            cursor.execute("SELECT peer, interface FROM wg_total_stats")
            db_peers = set(cursor.fetchall())
            peers_to_remove = db_peers - current_peers
            
            for peer, interface in peers_to_remove:
                cursor.execute(
                    "DELETE FROM wg_total_stats WHERE peer = ? AND interface = ?",
                    (peer, interface),
                )
            conn.commit()
            
            if peers_to_remove:
                logger.info(f"🧹 Удалено {len(peers_to_remove)} устаревших пиров из wg_total_stats")
            return True
    except sqlite3.Error as e:
        logger.error(f"❌ Ошибка SQLite при очистке таблицы: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Ошибка при очистке wg_total_stats: {e}")
        return False

# =============================================================================
# ФУНКЦИИ СОХРАНЕНИЯ СТАТИСТИКИ
# =============================================================================
def save_wg_stats():
    """Функция сохранения статистики"""
    try:
        output = get_wireguard_stats()
        if not output or output.startswith("Ошибка"):
            logger.warning(f"⚠️ Нет данных для сохранения: {output}")
            return
        
        stats = parse_wireguard_stats(output)
        now = datetime.now().strftime("%H:%M:%S")
        logger.debug(f"🕐 Сохранение статистики: {now} (пиров: {len(stats)})")
        
        clean_old_daily_stats(get_stats_retention_days(default_days=365))
        
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            now_dt = datetime.now()
            
            for data in stats:
                peer = data["peer"]
                date = now_dt.strftime("%Y-%m-%d")
                client = data["client"]
                received_now = convert_to_bytes(data["received"])
                sent_now = convert_to_bytes(data["sent"])
                interface = data["interface"]
                
                if now_dt.hour == 0 and now_dt.minute == 0 and now_dt.second <= 5:
                    cursor.execute(
                        """INSERT OR REPLACE INTO wg_intermediate
                        (peer, interface, last_received, last_sent, date)
                        VALUES (?, ?, ?, ?, ?)""",
                        (peer, interface, received_now, sent_now, date),
                    )
                
                cursor.execute(
                    """INSERT OR REPLACE INTO wg_total_stats
                    (peer, client, total_received, total_sent, interface)
                    VALUES (?, ?, ?, ?, ?)""",
                    (peer, client, received_now, sent_now, interface),
                )
            
            conn.commit()
        logger.debug("✅ Статистика WireGuard сохранена в wg_total_stats")
    
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения статистики WG: {e}")

def save_daily_stats(dailysave=False):
    """Функция сохранения статистики за день"""
    try:
        output = get_wireguard_stats()
        if not output or output.startswith("Ошибка"):
            logger.warning(f"⚠️ Нет данных для ежедневного сохранения: {output}")
            return False
        
        stats = parse_wireguard_stats(output)
        date = datetime.now().strftime("%Y-%m-%d")
        now = datetime.now().strftime("%H:%M:%S")
        
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            
            if dailysave:
                # Фиксирование дневной статистики в wg_intermediate
                logger.info(f"📊 Фиксирование дневной статистики: {now}")
                for data in stats:
                    try:
                        cursor.execute(
                            """INSERT OR REPLACE INTO wg_intermediate
                            (peer, interface, last_received, last_sent, date)
                            VALUES (?, ?, ?, ?, ?)""",
                            (
                                data["peer"],
                                data["interface"],
                                convert_to_bytes(data["received"]),
                                convert_to_bytes(data["sent"]),
                                date,
                            ),
                        )
                    except sqlite3.Error as e:
                        logger.error(f"❌ Ошибка при сохранении {data['peer']}: {e}")
                conn.commit()
                logger.info("✅ Дневная статистика зафиксирована")
                return True
            else:
                # Ежедневное сохранение статистики в wg_daily_stats
                current_stats = get_wg_total_stats()
                intermediate_stats = get_wg_intermediate()
                
                intermediate_dict = {(row[0], row[1]): row for row in intermediate_stats}
                updated_count = 0
                inserted_count = 0
                
                for stats_row in current_stats:
                    peer, client = stats_row[0], stats_row[1]
                    interface = stats_row[4]
                    key = (peer, interface)
                    
                    if key in intermediate_dict:
                        inter_row = intermediate_dict[key]
                        current_received = int(stats_row[2])
                        current_sent = int(stats_row[3])
                        last_received = int(inter_row[2])
                        last_sent = int(inter_row[3])
                        
                        if current_received >= last_received and current_sent >= last_sent:
                            received_diff = current_received - last_received
                            sent_diff = current_sent - last_sent
                        else:
                            logger.warning(
                                f"⚠️ Обнаружен сброс счетчиков для {peer} на {interface}. "
                                f"Сохраняем текущие значения."
                            )
                            received_diff = current_received
                            sent_diff = current_sent
                        
                        cursor.execute(
                            """SELECT 1 FROM wg_daily_stats
                            WHERE date = ? AND peer = ? AND interface = ?""",
                            (date, peer, interface),
                        )
                        exists = cursor.fetchone()
                        
                        if exists:
                            cursor.execute(
                                """UPDATE wg_daily_stats
                                SET received = ?, sent = ?, client = ?
                                WHERE date = ? AND peer = ? AND interface = ?""",
                                (
                                    convert_to_bytes(received_diff),
                                    convert_to_bytes(sent_diff),
                                    client,
                                    date,
                                    peer,
                                    interface,
                                ),
                            )
                            updated_count += 1
                        else:
                            cursor.execute(
                                """INSERT INTO wg_daily_stats
                                (date, peer, client, received, sent, interface)
                                VALUES (?, ?, ?, ?, ?, ?)""",
                                (
                                    date,
                                    peer,
                                    client,
                                    convert_to_bytes(received_diff),
                                    convert_to_bytes(sent_diff),
                                    interface,
                                ),
                            )
                            inserted_count += 1
                
                conn.commit()
                logger.info(
                    f"✅ Ежедневная статистика сохранена: "
                    f"обновлено={updated_count}, добавлено={inserted_count}"
                )
                return True
    
    except Exception as e:
        logger.error(f"❌ Ошибка при ежедневном сохранении: {e}")
        return False

def sync_new_peers():
    """Добавляет новые peer+interface из wg_total_stats в wg_intermediate"""
    try:
        if not clear_wg_total_stats():
            logger.warning("⚠️ Не удалось очистить wg_total_stats, пропускаем синхронизацию")
            return
        
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        try:
            date = datetime.now().strftime("%Y-%m-%d")
            cursor.execute("""
            SELECT t.peer, t.interface
            FROM wg_total_stats t
            LEFT JOIN wg_intermediate i
            ON t.peer = i.peer AND t.interface = i.interface
            WHERE i.peer IS NULL
            GROUP BY t.peer, t.interface
            """)
            new_combinations = cursor.fetchall()
            
            for peer, interface in new_combinations:
                cursor.execute("""
                INSERT INTO wg_intermediate
                (peer, interface, last_received, last_sent, date)
                VALUES (?, ?, 0, 0, ?)
                """, (peer, interface, date))
            
            conn.commit()
            
            if new_combinations:
                logger.info(f"🔄 Синхронизировано {len(new_combinations)} новых пиров")
            else:
                logger.debug("ℹ️ Новых пиров для синхронизации не найдено")
        
        except sqlite3.Error as e:
            logger.error(f"❌ Ошибка при синхронизации новых клиентов: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    except Exception as e:
        logger.error(f"❌ Ошибка в sync_new_peers: {e}")

# =============================================================================
# ФУНКЦИИ ОЧИСТКИ
# =============================================================================
def clean_old_daily_stats(days=7):
    """Удаление старых записей из wg_daily_stats"""
    try:
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """SELECT COUNT(*) FROM wg_daily_stats WHERE date < ?""",
                (cutoff_date,),
            )
            count = cursor.fetchone()[0]
            
            if count > 0:
                cursor.execute(
                    """DELETE FROM wg_daily_stats WHERE date < ?""", 
                    (cutoff_date,)
                )
                conn.commit()
                logger.info(f"🧹 Удалено {count} записей старше {cutoff_date}")
            else:
                logger.debug(f"ℹ️ Нет записей старше {cutoff_date} для удаления")
    
    except sqlite3.Error as e:
        logger.error(f"❌ Ошибка при очистке старых записей: {e}")
    except Exception as e:
        logger.error(f"❌ Ошибка в clean_old_daily_stats: {e}")

# =============================================================================
# ТАЙМЕРЫ И ЗАПУСК
# =============================================================================
timer_1 = None
timer_2 = None
timer_3 = None

def start_timers():
    """Запуск таймеров"""
    global timer_1, timer_2, timer_3
    timer_1 = schedule.every(EVERY_TIME).seconds.do(save_daily_stats)
    timer_2 = schedule.every(EVERY_TIME).seconds.do(save_wg_stats)
    timer_3 = schedule.every(SYNS_TIME).minutes.do(sync_new_peers)
    logger.info(f"✅ Таймеры запущены: каждые {EVERY_TIME} сек (статистика), каждые {SYNS_TIME} мин (синхронизация)")

def stop_timers():
    """Остановка таймеров на время фиксирования ежедневной статистики"""
    global timer_1, timer_2, timer_3
    schedule.cancel_job(timer_1)
    schedule.cancel_job(timer_2)
    schedule.cancel_job(timer_3)
    logger.info("⏸️ Таймеры остановлены для фиксирования дневной статистики")
    time.sleep(2)
    save_daily_stats(True)
    logger.info("▶️ Таймеры будут перезапущены в 00:00")

def main():
    """Основная функция"""
    logger.info("=" * 60)
    logger.info("🚀 ЗАПУСК WG_STATS.PY (Сбор статистики WireGuard)")
    logger.info("=" * 60)
    logger.info(f"📍 Версия Python: {__import__('sys').version.split()[0]}")
    logger.info(f"📁 Путь к БД: {DB_PATH}")
    logger.info(f"📁 Путь к логам: {LOG_DIR}")
    logger.info(f"⏰ Интервал сохранения: {EVERY_TIME} сек")
    logger.info(f"⏰ Интервал синхронизации: {SYNS_TIME} мин")
    logger.info(f"📅 Срок хранения статистики: {get_stats_retention_days(default_days=365)} дней")
    
    try:
        init_db()
        
        inter_date = get_wg_intermediate("date")
        if not inter_date:
            inter_date = datetime.now().strftime("%Y-%m-%d")
            logger.info(f"ℹ️ Дата в wg_intermediate не найдена, используем текущую: {inter_date}")
        
        today_date = datetime.now().strftime("%Y-%m-%d")
        if inter_date != today_date:
            logger.info(f"📊 Обнаружена новая дата ({today_date}), фиксируем дневную статистику")
            save_daily_stats(True)
            time.sleep(3)
        
        clean_old_daily_stats(days=get_stats_retention_days(default_days=365))
        time.sleep(3)
        
        start_timers()
        
        logger.info("🔄 Запуск основного цикла...")
        while True:
            schedule.run_pending()
            time.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("🛑 Получен сигнал остановки, завершаем работу")
    except Exception as e:
        logger.critical(f"❌ Фатальная ошибка: {e}", exc_info=True)
        raise
    finally:
        logger.info("=" * 60)
        logger.info("🏁 WG_STATS.PY завершён")
        logger.info("=" * 60)

if __name__ == "__main__":
    main()