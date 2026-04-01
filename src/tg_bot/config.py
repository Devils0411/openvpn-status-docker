"""Управление конфигурацией Telegram-бота."""

import os
import json
import logging
from config import Config

logger = logging.getLogger("tg_bot")
_settings_cache = None
_settings_mtime = 0

SETTINGS_PATH = Config.SETTINGS_PATH
ENV_PATH = Config.ENV_PATH
CLIENT_MAPPING_KEY = "CLIENT_MAPPING"

ITEMS_PER_PAGE = 5
DEFAULT_CPU_ALERT_THRESHOLD = 80
DEFAULT_MEMORY_ALERT_THRESHOLD = 80
LOAD_CHECK_INTERVAL = 60
LOAD_ALERT_COOLDOWN = 30 * 60


def get_bot_token():
    """Получить токен бота из окружения (ленивая загрузка)."""
    from dotenv import load_dotenv
    load_dotenv(ENV_PATH)
    return os.getenv("BOT_TOKEN")

def get_admin_ids():
    """Получить ID администраторов из окружения (ленивая загрузка)."""
    from dotenv import load_dotenv
    load_dotenv(ENV_PATH)
    raw = os.getenv("ADMIN_ID", "")
    return [int(x) for x in raw.split(",") if x.strip().isdigit()]

def load_settings():
    """Загрузить настройки из JSON-файла (с кэшированием)."""
    global _settings_cache, _settings_mtime
    try:
        current_mtime = os.path.getmtime(SETTINGS_PATH)
        if _settings_cache is not None and current_mtime == _settings_mtime:
            return _settings_cache.copy()
    except OSError:
        pass

    try:
        with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.warning("Файл настроек не найден: %s. Создаю новый. Ошибка: %s", SETTINGS_PATH, e)
        data = {}

    if not isinstance(data, dict):
        data = {}

    data.setdefault("telegram_admins", {})
    data.setdefault("telegram_clients", {})

    if not isinstance(data.get("telegram_admins"), dict):
        data["telegram_admins"] = {}
    if not isinstance(data.get("telegram_clients"), dict):
        data["telegram_clients"] = {}

    _settings_cache = data
    try:
        _settings_mtime = os.path.getmtime(SETTINGS_PATH)
    except OSError:
        _settings_mtime = 0

    return data.copy()

def save_settings(data):
    """Сохранить настройки в JSON-файл."""
    global _settings_cache, _settings_mtime
    with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
        f.write("\n")

    _settings_cache = data.copy()
    try:
        _settings_mtime = os.path.getmtime(SETTINGS_PATH)
    except OSError:
        _settings_mtime = 0

def read_env_values():
    """Прочитать все значения из файла .env."""
    values = {}
    try:
        with open(ENV_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip()
    except FileNotFoundError:
        pass
    return values

def update_env_values(updates):
    """Обновить значения в файле .env."""
    updates = {k: v for k, v in updates.items() if k}
    if not updates:
        return
    updated_keys = set()
    try:
        with open(ENV_PATH, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except FileNotFoundError:
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

    with open(ENV_PATH, "w", encoding="utf-8") as f:
        f.writelines(new_lines)

def get_client_mapping():
    """Получить привязку клиентов к ID Telegram (поддержка нескольких профилей)."""
    env_values = read_env_values()
    raw_value = env_values.get(CLIENT_MAPPING_KEY, "")
    mapping = {}
    if not raw_value:
        return mapping
    try:
        for item in raw_value.split(","):
            item = item.strip()
            if not item or ":" not in item:
                continue
            telegram_id, client_name = item.split(":", 1)
            telegram_id = telegram_id.strip()
            client_name = client_name.strip()
            if not telegram_id or not client_name:
                continue
            
            # Если такой ID уже есть, добавляем профиль в список, иначе создаем новый
            if telegram_id in mapping:
                if isinstance(mapping[telegram_id], list):
                    mapping[telegram_id].append(client_name)
                else:
                    mapping[telegram_id] = [mapping[telegram_id], client_name]
            else:
                mapping[telegram_id] = [client_name]
    except Exception as e:
        logger.error(f"Ошибка парсинга CLIENT_MAPPING: {e}")

    return mapping

def get_client_name_for_user(user_id: int):
    """Получить имя клиента по ID пользователя Telegram (первый профиль)."""
    profiles = get_client_mapping().get(str(user_id), [])
    if isinstance(profiles, list):
        return profiles[0] if profiles else None
    return profiles


async def get_client_info_for_user(user_id: int):
    """Получает полную информацию о профилях пользователя с датами истечения."""
    profiles = get_client_mapping().get(str(user_id), [])
    if not profiles:
        return []
    
    if not isinstance(profiles, list):
        profiles = [profiles]
    
    # Получаем список всех клиентов OpenVPN с датами
    try:
        clients = await get_clients("openvpn")
    except:
        logger.error(f"Ошибка получения клиентов: {e}")
        clients = []
    
    result = []
    for profile in profiles:
        client_info = {
            "name": profile,
            "expire": None
        }
        # Ищем совпадение в списке клиентов
        for client in clients:
            if isinstance(client, dict) and client.get("name") == profile:
                client_info["expire"] = client.get("expire")
                break
        result.append(client_info)
    
    return result


def get_all_profiles_for_user(user_id: int):
    """Получить все профили пользователя (список)."""
    profiles = get_client_mapping().get(str(user_id), [])
    if isinstance(profiles, list):
        return profiles
    return [profiles] if profiles else []

def set_client_mapping(telegram_id: str, client_name: str):
    """Установить привязку клиента для пользователя Telegram (добавляет к существующим)."""
    client_map = get_client_mapping()
    tid = str(telegram_id)
    if tid not in client_map:
        client_map[tid] = []
    elif isinstance(client_map[tid], str):
        client_map[tid] = [client_map[tid]]
    
    if client_name not in client_map[tid]:
        client_map[tid].append(client_name)
    
    serialized_items = []
    for tid, profiles in client_map.items():
        if isinstance(profiles, list):
            for prof in profiles:
                serialized_items.append(f"{tid}:{prof}")
        else:
            serialized_items.append(f"{tid}:{profiles}")
    
    serialized = ",".join(serialized_items)
    update_env_values({CLIENT_MAPPING_KEY: serialized})
    logger.info(f"Добавлена привязка клиента: {telegram_id} → {client_name}")

def remove_client_mapping(telegram_id: str, client_name: str = None):
    """Удаляет привязку клиента."""
    try:
        client_map = get_client_mapping()
        telegram_id = str(telegram_id)
        
        if telegram_id not in client_map:
            logger.warning(f"⚠️ Привязка не найдена для удаления: {telegram_id}")
            return False
            
        existing_profiles = client_map[telegram_id]
        if not isinstance(existing_profiles, list):
            existing_profiles = [existing_profiles]
        
        if client_name:
            # Удаляем конкретный профиль
            if client_name in existing_profiles:
                existing_profiles.remove(client_name)
                logger.info(f"✅ Удалена привязка: {telegram_id} → {client_name}")
            else:
                logger.warning(f"⚠️ Профиль не найден: {client_name}")
                return False
        else:
            # Если профиль не указан, удаляем все привязки пользователя
            logger.info(f"✅ Удалены все привязки пользователя: {telegram_id}")
            existing_profiles = []
        
        if existing_profiles:
            client_map[telegram_id] = existing_profiles
        else:
            client_map.pop(telegram_id, None)
        
        # Сериализуем обратно
        serialized_items = []
        for tid, profiles in client_map.items():
            if isinstance(profiles, list):
                for prof in profiles:
                    serialized_items.append(f"{tid}:{prof}")
            else:
                serialized_items.append(f"{tid}:{profiles}")
        
        serialized = ",".join(serialized_items) if serialized_items else ""
        update_env_values({CLIENT_MAPPING_KEY: serialized})
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка удаления привязки клиента: {e}")
        return False

def get_load_thresholds():
    """Получить пороги оповещения по CPU и памяти."""
    data = load_settings()
    thresholds = data.get("load_thresholds") or {}
    if not isinstance(thresholds, dict):
        thresholds = {}
    cpu = thresholds.get("cpu", DEFAULT_CPU_ALERT_THRESHOLD)
    memory = thresholds.get("memory", DEFAULT_MEMORY_ALERT_THRESHOLD)
    return cpu, memory

def set_load_thresholds(cpu_threshold: int = None, memory_threshold: int = None):
    """Установить пороги оповещения по CPU и/или памяти."""
    data = load_settings()
    thresholds = data.get("load_thresholds") or {}
    if not isinstance(thresholds, dict):
        thresholds = {}
    if cpu_threshold is not None:
        thresholds["cpu"] = int(cpu_threshold)
    if memory_threshold is not None:
        thresholds["memory"] = int(memory_threshold)
    data["load_thresholds"] = thresholds
    save_settings(data)