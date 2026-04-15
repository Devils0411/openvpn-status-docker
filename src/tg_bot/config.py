"""Управление конфигурацией Telegram-бота."""
import os
import json
import time
import logging
from config import Config
from types import MappingProxyType

logger = logging.getLogger("tg_bot")

# Кэш конфигураций
_config_cache = {
    "allowed_users": set(),
    "client_map": {},
    "settings": {},
    "admin_ids": [],
    "env_mtime": 0,
    "settings_mtime": 0,
    "expires": 0,
    "TTL": 15  # секунд
}

SETTINGS_PATH = Config.SETTINGS_PATH
ENV_PATH = Config.ENV_PATH
CLIENT_MAPPING_KEY = "CLIENT_MAPPING"
TG_BOT_PROFILE_SEEDED_KEY = "tg_bot_profile_seeded"
ITEMS_PER_PAGE = 5
DEFAULT_CPU_ALERT_THRESHOLD = 80
DEFAULT_MEMORY_ALERT_THRESHOLD = 80
LOAD_CHECK_INTERVAL = 60
LOAD_ALERT_COOLDOWN = 30 * 60

def _refresh_cache():
    """Обновляет кэш только если файлы изменились или истёк TTL."""
    now = time.time()
    if now < _config_cache["expires"]:
        return

    try:
        env_mtime = os.path.getmtime(ENV_PATH)
        settings_mtime = os.path.getmtime(SETTINGS_PATH)
    except OSError:
        env_mtime = settings_mtime = None

    # Если файлы не менялись, просто продлеваем TTL
    if env_mtime == _config_cache["env_mtime"] and settings_mtime == _config_cache["settings_mtime"]:
        _config_cache["expires"] = now + _config_cache["TTL"]
        return

    # 1. Читаем .env
    env_vals = {}
    try:
        with open(ENV_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line: continue
                k, v = line.split("=", 1)
                env_vals[k.strip()] = v.strip()
    except FileNotFoundError: pass

    # 2. Читаем settings.json
    settings = {}
    try:
        with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
            settings = json.load(f)
        if not isinstance(settings, dict): settings = {}
    except (FileNotFoundError, json.JSONDecodeError): pass

    settings.setdefault("telegram_admins", {})
    settings.setdefault("telegram_clients", {})

    # 3. Парсим админов
    raw_admins = env_vals.get("ADMIN_ID", "")
    admin_ids = [int(x) for x in raw_admins.split(",") if x.strip().isdigit()]

    # 4. Парсим маппинг клиентов
    raw_mapping = env_vals.get(CLIENT_MAPPING_KEY, "")
    client_map = {}
    if raw_mapping:
        for item in raw_mapping.split(","):
            if ":" not in item: continue
            tid, name = item.split(":", 1)
            tid, name = tid.strip(), name.strip()
            if tid.isdigit():
                client_map.setdefault(tid, []).append(name)

    # 5. Формируем set разрешённых
    allowed = set(admin_ids) | {int(k) for k in client_map.keys()}

    # Обновляем кэш
    _config_cache.update({
        "settings": settings,
        "admin_ids": admin_ids,
        "client_map": client_map,
        "allowed_users": allowed,
        "env_mtime": env_mtime,
        "settings_mtime": settings_mtime,
        "expires": now + _config_cache["TTL"]
    })

def get_bot_token():
    _refresh_cache()
    from dotenv import load_dotenv
    load_dotenv(ENV_PATH) # Оставляем для совместимости, но кэш уже загрузил данные
    return os.getenv("BOT_TOKEN") or _config_cache.get("settings", {}).get("bot_token")

def get_admin_ids():
    _refresh_cache()
    return _config_cache["admin_ids"]

def is_user_allowed_for_bot(user_id: int) -> bool:
    _refresh_cache()
    if not _config_cache["admin_ids"]: return True
    return int(user_id) in _config_cache["allowed_users"]

def get_client_mapping():
    _refresh_cache()
    return _config_cache["client_map"]

def get_client_name_for_user(user_id: int):
    profiles = get_client_mapping().get(str(user_id), [])
    return profiles[0] if isinstance(profiles, list) and profiles else (profiles if profiles else None)

def load_settings():
    _refresh_cache()
    return _config_cache["settings"].copy()

def save_settings(data):
    with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
        f.write("\n")
    _config_cache["settings"] = data.copy()
    try: _config_cache["settings_mtime"] = os.path.getmtime(SETTINGS_PATH)
    except OSError: pass
    _config_cache["expires"] = 0 # Сброс TTL для принудительного обновления

def set_client_mapping(telegram_id: str, client_name: str):
    client_map = get_client_mapping()
    tid = str(telegram_id)
    if tid not in client_map: client_map[tid] = []
    elif isinstance(client_map[tid], str): client_map[tid] = [client_map[tid]]
    if client_name not in client_map[tid]: client_map[tid].append(client_name)

    serialized = ",".join(f"{tid}:{p}" for profiles in client_map.values() for p in (profiles if isinstance(profiles, list) else [profiles]))
    update_env_values({CLIENT_MAPPING_KEY: serialized})
    _config_cache["client_map"] = client_map
    _config_cache["expires"] = 0

def remove_client_mapping(telegram_id: str, client_name: str = None):
    client_map = get_client_mapping()
    tid = str(telegram_id)
    if tid not in client_map: return False
    existing = client_map[tid] if isinstance(client_map[tid], list) else [client_map[tid]]
    if client_name:
        if client_name in existing: existing.remove(client_name)
        else: return False
    else: existing = []

    if existing: client_map[tid] = existing
    else: client_map.pop(tid, None)

    serialized = ",".join(f"{t}:{p}" for t, profiles in client_map.items() for p in (profiles if isinstance(profiles, list) else [profiles]))
    update_env_values({CLIENT_MAPPING_KEY: serialized or ""})
    _config_cache["client_map"] = client_map
    _config_cache["expires"] = 0
    return True

def update_env_values(updates):
    updates = {k: v for k, v in updates.items() if k}
    if not updates: return
    updated_keys = set()
    try:
        with open(ENV_PATH, "r", encoding="utf-8") as f: lines = f.readlines()
    except FileNotFoundError: lines = []
    new_lines = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            new_lines.append(line); continue
        key, _ = line.split("=", 1)
        if key.strip() in updates:
            new_lines.append(f"{key.strip()}={updates[key.strip()]}\n")
            updated_keys.add(key.strip())
        else: new_lines.append(line)
    for k, v in updates.items():
        if k not in updated_keys: new_lines.append(f"{k}={v}\n")
    with open(ENV_PATH, "w", encoding="utf-8") as f: f.writelines(new_lines)
    _config_cache["env_mtime"] = 0 # Сброс для принудительного перечитывания

# Остальные функции оставляем без изменений, они автоматически подхватят кэш
def get_load_thresholds():
    data = load_settings()
    thresholds = data.get("load_thresholds") or {}
    if not isinstance(thresholds, dict): thresholds = {}
    return thresholds.get("cpu", DEFAULT_CPU_ALERT_THRESHOLD), thresholds.get("memory", DEFAULT_MEMORY_ALERT_THRESHOLD)

def set_load_thresholds(cpu_threshold: int = None, memory_threshold: int = None):
    data = load_settings()
    thresholds = data.get("load_thresholds") or {}
    if not isinstance(thresholds, dict): thresholds = {}
    if cpu_threshold is not None: thresholds["cpu"] = int(cpu_threshold)
    if memory_threshold is not None: thresholds["memory"] = int(memory_threshold)
    data["load_thresholds"] = thresholds
    save_settings(data)

def is_tg_bot_profile_seeded() -> bool:
    """Уже выполнялась однократная установка описания и «о боте» через API."""
    # load_settings() теперь автоматически использует кэш
    return bool(load_settings().get(TG_BOT_PROFILE_SEEDED_KEY))

def mark_tg_bot_profile_seeded() -> None:
    """Пометить, что описание и «о боте» заданы (чтобы не перезаписывать при каждом запуске)."""
    data = load_settings()
    data[TG_BOT_PROFILE_SEEDED_KEY] = True
    save_settings(data)

def get_user_timezone(user_id: int) -> str:
    """Получить временную зону пользователя (по умолчанию Europe/Moscow)."""
    data = load_settings()
    user_settings = data.get("user_settings", {})
    return user_settings.get(str(user_id), {}).get("timezone", "Europe/Moscow")

def set_user_timezone(user_id: int, timezone: str):
    """Установить временную зону пользователя."""
    data = load_settings()
    user_settings = data.get("user_settings", {})
    if str(user_id) not in user_settings:
        user_settings[str(user_id)] = {}
    user_settings[str(user_id)]["timezone"] = timezone
    data["user_settings"] = user_settings
    save_settings(data)