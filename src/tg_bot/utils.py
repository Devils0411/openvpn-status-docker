"""Вспомогательные функции для Telegram-бота."""
import os
import asyncio
import datetime
import re
import logging
logger = logging.getLogger("tg_bot")
_server_ip_cache = None

def get_external_ip():
    """Получить внешний IP-адрес (с кэшированием)."""
    global _server_ip_cache
    logger.debug("🔍 Запрос внешнего IP-адреса")
    if _server_ip_cache is not None:
        logger.debug("✅ IP возвращён из кэша: %s", _server_ip_cache)
        return _server_ip_cache
    
    import requests
    try:
        response = requests.get("https://api.ipify.org", timeout=10)
        if response.status_code == 200:
            _server_ip_cache = response.text
            logger.debug("✅ Внешний IP получен: %s", _server_ip_cache)
            return _server_ip_cache
        logger.warning("⚠️ Не удалось получить внешний IP. Статус: %s", response.status_code)
        return "IP не найден"
    except requests.Timeout:
        logger.error("❌ Ошибка: запрос превысил время ожидания при получении IP.")
        return "Ошибка: запрос превысил время ожидания."
    except requests.ConnectionError:
        logger.error("❌ Ошибка: нет подключения к интернету при получении IP.")
        return "Ошибка: нет подключения к интернету."
    except requests.RequestException as e:
        logger.error("❌ Ошибка при запросе внешнего IP: %s", e)
        return f"Ошибка при запросе: {e}"

async def execute_script(option: str, client_name: str = None, days: str = None):
    """Выполнить shell-скрипт управления VPN."""
    script_path = "/root/web/scripts/client.sh"
    logger.debug("🔧 Выполнение скрипта: option=%s, client=%s, days=%s", option, client_name, days)
    
    if not os.path.exists(script_path):
        logger.error("❌ Файл %s не найден!", script_path)
        return {"returncode": 1, "stdout": "", "stderr": f"❌ Файл {script_path} не найден!"}

    command = f"{script_path} {option}"
    if option not in ["8", "7"] and client_name:
        command += f" {client_name}"
        if days and option == "1":
            command += f" {days}"

    logger.debug("📝 Команда: %s", command)

    try:
        env = os.environ.copy()
        env["PATH"] = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
        
        logger.debug(f"🚀 Запуск процесса с PATH={env['PATH']}")
        process = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
         
        stdout, stderr = await process.communicate()
        stdout_decoded = stdout.decode().strip()
        stderr_decoded = stderr.decode().strip()
        
        logger.debug("✅ Скрипт выполнен: option=%s, returncode=%s", option, process.returncode)
        
        if process.returncode != 0:
            logger.error("❌ Ошибка скрипта: %s", stderr_decoded)
        elif stderr_decoded:
            logger.warning("⚠️ Скрипт выполнен с предупреждениями: %s", stderr_decoded)
        
        return {"returncode": process.returncode, "stdout": stdout_decoded, "stderr": stderr_decoded}
    except Exception as e:
        logger.error("❌ Ошибка при выполнении скрипта: %s", e, exc_info=True)
        return {"returncode": 1, "stdout": "", "stderr": f"❌ Ошибка при выполнении скрипта: {str(e)}"}

async def get_clients(vpn_type: str):
    """Получить список клиентов VPN."""
    logger.debug("📋 Запрос списка клиентов VPN типа: %s", vpn_type)
    option = "3" if vpn_type == "openvpn" else "6"
    result = await execute_script(option)
    
    if result["returncode"] == 0:
        clients = [
            c.strip()
            for c in result["stdout"].split("\n")
            if c.strip()
            and not c.startswith("OpenVPN client names:")
            and not c.startswith("WireGuard/AmneziaWG client names:")
            and not c.startswith("OpenVPN - List clients")
            and not c.startswith("WireGuard/AmneziaWG - List clients")
        ]
        logger.debug("✅ Получено %d клиентов %s", len(clients), vpn_type)
        return clients
    
    logger.warning("⚠️ Не удалось получить список клиентов %s: %s", vpn_type, result['stderr'])
    return []

async def get_all_clients_unique():
    """Объединённый отсортированный список уникальных имён клиентов (OpenVPN + WireGuard)."""
    logger.debug("📋 Получение объединённого списка всех клиентов")
    ovpn = await get_clients("openvpn")
    wg = await get_clients("wireguard")
    all_clients = sorted(set(ovpn) | set(wg))
    logger.debug("✅ Всего уникальных клиентов: %d", len(all_clients))
    return all_clients

async def cleanup_openvpn_files(client_name: str):
    """Удалить файлы OpenVPN после удаления клиента."""
    logger.debug("🗑️ Очистка файлов OpenVPN для клиента: %s", client_name)  # DEBUG вместо INFO
    clean_name = client_name.replace("antizapret-", "").replace("vpn-", "")
    dirs_to_check = ["/root/web/openvpn/clients/"]
    deleted_files = []
    for dir_path in dirs_to_check:
        if not os.path.exists(dir_path):
            logger.debug("⚠️ Каталог не найден: %s", dir_path)
            continue
        for filename in os.listdir(dir_path):
            if clean_name in filename:
                try:
                    file_path = os.path.join(dir_path, filename)
                    os.remove(file_path)
                    deleted_files.append(file_path)
                    logger.debug("🗑️ Удалён файл: %s", file_path)
                except Exception as e:
                    logger.error("❌ Ошибка удаления %s: %s", file_path, e, exc_info=True)
    logger.debug("✅ Удалено файлов: %d", len(deleted_files))  # DEBUG вместо INFO
    return deleted_files

def get_color_by_percent(percent):
    """Вернуть эмодзи-цвет по проценту."""
    if percent < 50:
        return "🟢"
    elif percent < 80:
        return "🟡"
    else:
        return "🔴"

def format_vpn_clients(clients_dict):
    """Форматировать словарь клиентов VPN в строку."""
    total = clients_dict['WireGuard'] + clients_dict['OpenVPN']
    if total == 0:
        return "0 шт."
    return f"""
├ WireGuard: {clients_dict['WireGuard']} шт.
└ OpenVPN: {clients_dict['OpenVPN']} шт."""

def parse_handshake_time(raw_value: str):
    """Разобрать строку времени handshake WireGuard."""
    value = (raw_value or "").strip()
    if not value:
        return None
    if value.lower() == "now":
        return datetime.datetime.now()
    if value.lower() in ["never", "n/a", "(none)"]:
        return None
    if any(unit in value for unit in ["мин", "час", "сек", "minute", "hour", "second", "day", "week"]):
        return _parse_relative_time(value)

    try:
        parsed = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        logger.debug("🕐 Распаршено время handshake: %s", parsed)
        return parsed
    except ValueError:
        logger.debug("⚠️ Не удалось распарсить время handshake: %s", value)
        return None

def _parse_relative_time(relative_time):
    """Преобразовать строку относительного времени в datetime."""
    logger.debug("🕐 Парсинг относительного времени: %s", relative_time)
    now = datetime.datetime.now()
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

    delta = datetime.timedelta(
        days=time_deltas["days"],
        hours=time_deltas["hours"],
        minutes=time_deltas["minutes"],
        seconds=time_deltas["seconds"],
    )
    result = now - delta
    logger.debug("🕐 Результат парсинга: %s", result)
    return result

def is_peer_online(last_handshake):
    """Проверить, онлайн ли пир WireGuard."""
    if not last_handshake:
        return False
    is_online = datetime.datetime.now() - last_handshake < datetime.timedelta(minutes=3)
    logger.debug("🟢 Пир онлайн: %s (last_handshake: %s)", is_online, last_handshake)
    return is_online

def read_wg_config(file_path):
    """Прочитать привязку клиентов из конфига WireGuard."""
    logger.debug("📖 Чтение конфига WireGuard: %s", file_path)
    client_mapping = {}
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            current_client_name = None
            peers_count = 0
            
            for line in file:
                line = line.strip()
                if line.startswith("# Client ="):
                    current_client_name = line.split("=", 1)[1].strip()
                elif line.startswith("[Peer]"):
                    current_client_name = current_client_name or "N/A"
                    peers_count += 1
                elif line.startswith("PublicKey =") and current_client_name:
                    public_key = line.split("=", 1)[1].strip()
                    client_mapping[public_key] = current_name
            
            logger.debug("✅ Прочитано %d пиров из %s, маппингов: %d", peers_count, file_path, len(client_mapping))
    except FileNotFoundError:
        logger.debug("⚠️ Файл конфига не найден: %s", file_path)
    except Exception as e:
        logger.error("❌ Ошибка чтения конфига %s: %s", file_path, e, exc_info=True)
    return client_mapping

def find_config_file(dir_path: str, pattern) -> str:
    """Найти файл конфигурации по шаблону в каталоге."""
    logger.debug("🔍 Поиск файла по шаблону %s в %s", pattern, dir_path)
    if not os.path.exists(dir_path):
        logger.debug("⚠️ Каталог не найден: %s", dir_path)
        return None
    for filename in os.listdir(dir_path):
        if pattern.fullmatch(filename):
            file_path = os.path.join(dir_path, filename)
            logger.debug("✅ Файл найден: %s", file_path)  # DEBUG вместо INFO
            return file_path
    logger.debug("⚠️ Файл не найден в %s", dir_path)
    return None

def format_days(days: int) -> str:
    """Форматирует количество дней с правильным окончанием на русском языке."""
    days = int(days)
    if days % 10 == 1 and days % 100 != 11:
        return f"{days} день"
    elif 2 <= days % 10 <= 4 and (days % 100 < 10 or days % 100 >= 20):
        return f"{days} дня"
    else:
        return f"{days} дней"