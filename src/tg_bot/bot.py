"""Управление экземпляром бота с ленивой инициализацией."""

import asyncio
import time
import datetime
import logging
from src.config import Config
logger = logging.getLogger("tg_bot")

_bot = None
_dp = None


def get_bot():
    """Получить или создать экземпляр бота (ленивая инициализация)."""
    global _bot
    if _bot is None:
        from aiogram import Bot
        from aiogram.client.default import DefaultBotProperties
        from aiogram.enums import ParseMode
        from .config import get_bot_token
        
        token = get_bot_token()
        _bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        logger.debug("✅ Бот инициализирован")
    return _bot


def get_dispatcher():
    """Получить или создать экземпляр диспетчера (ленивая инициализация)."""
    global _dp
    if _dp is None:
        from aiogram import Dispatcher
        _dp = Dispatcher()
        _register_handlers(_dp)
        logger.debug("✅ Диспетчер инициализирован")
    return _dp


def _register_handlers(dp):
    """Зарегистрировать все обработчики в диспетчере."""
    from .handlers import common, menus, server, vpn, admin
    
    dp.include_router(common.router)
    dp.include_router(menus.router)
    dp.include_router(server.router)
    dp.include_router(vpn.router)
    dp.include_router(admin.router)
    logger.debug("✅ Обработчики зарегистрированы")


async def notify_admin_server_online():
    """Отправить уведомление о запуске бота."""
    from .config import get_admin_ids
    from .admin import is_admin_notification_enabled
    from .utils import get_external_ip
    
    bot = get_bot()
    admin_ids = get_admin_ids()
    server_ip = get_external_ip()
    
    try:
        with open('/proc/uptime', 'r') as f:
            uptime_seconds = float(f.readline().split()[0])
        
        if uptime_seconds < 120:
            event = "🔄 <b>Сервер был перезагружен!</b>"
        else:
            event = "⚡ <b>Бот был перезагружен!</b>"
    except Exception as e:
        logger.error(f"Ошибка получения uptime: {e}")
        event = "📱 <b>Бот запущен</b>"
    
    text = f"{event}\n<b>IP адрес сервера: </b> <code>{server_ip}</code>\nИспользуйте /start для начала работы."

    
    for admin in admin_ids:
        try:
            if not is_admin_notification_enabled(admin):
                continue
            await bot.send_message(admin, text, parse_mode="HTML")
            logger.info(f"✅ Уведомление отправлено админу {admin}")
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления админу {admin}: {e}")


async def update_bot_description():
    """Обновить описание бота."""
    from aiogram import Bot
    from .config import get_bot_token
    
    description = """

Привет! Я бот для управления OpenVPN и WireGuard. 
Вот что я могу сделать:
- Управлять пользователями (удаление/добавление).
- Генерировать и выдавать конфигурационные файлы.

Перейдите в главное меню (/start), чтобы начать.

"""
    token = get_bot_token()
    async with Bot(token=token) as bot:
        await bot.set_my_description(description, language_code="ru")
        logger.debug("✅ Описание бота обновлено")


async def update_bot_about():
    """Обновить раздел «О боте»."""
    from aiogram import Bot
    from .config import get_bot_token
    
    about = "Бот для управления OpenVPN и WireGuard."

    token = get_bot_token()
    async with Bot(token=token) as bot:
        await bot.set_my_short_description(about, language_code="ru")
        logger.debug("✅ Раздел «О боте» обновлён")


async def set_bot_commands():
    """Установить команды бота."""
    from aiogram import Bot
    from aiogram.types import BotCommand
    from .config import get_bot_token

    token = get_bot_token()
    async with Bot(token=token) as bot:
        commands = [
            BotCommand(command="start", description="Запустить бота"),
            BotCommand(command="id", description="Показать ваш Telegram ID"),
            BotCommand(command="request", description="Запросить доступ к боту"),
            BotCommand(command="client", description="Привязать клиента к ID"),
        ]
        await bot.set_my_commands(commands)
        logger.debug("✅ Команды бота установлены")

_last_load_alerts = {}


def _check_sustained_high_load(cpu_threshold: int, memory_threshold: int) -> tuple:
    """
    Проверить, была ли нагрузка выше порога последние 5 минут (по данным БД).
    Возвращает (is_sustained, avg_cpu, avg_ram) или (False, None, None), если порог не превышен.
    """
    import sqlite3
    
    db_path = Config.SYSTEM_STATS_PATH
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        now = datetime.datetime.now()
        five_minutes_ago = now - datetime.timedelta(minutes=5)
        
        cursor.execute(
            """
            SELECT cpu_percent, ram_percent, timestamp
            FROM system_stats
            WHERE timestamp >= ?
            ORDER BY timestamp DESC
            """,
            (five_minutes_ago.strftime("%Y-%m-%d %H:%M:%S"),)
        )
        
        rows = cursor.fetchall()
        conn.close()
        
        if not rows:
            return False, None, None
        
        cpu_values = [row[0] for row in rows]
        ram_values = [row[1] for row in rows]
        
        cpu_above = sum(1 for cpu in cpu_values if cpu >= cpu_threshold)
        ram_above = sum(1 for ram in ram_values if ram >= memory_threshold)
        
        total_records = len(rows)
        
        cpu_sustained = cpu_above == total_records and total_records > 0
        ram_sustained = ram_above == total_records and total_records > 0
        
        if cpu_sustained or ram_sustained:
            avg_cpu = sum(cpu_values) / len(cpu_values)
            avg_ram = sum(ram_values) / len(ram_values)
            return True, avg_cpu, avg_ram
        
        return False, None, None
        
    except Exception as e:
        logger.error(f"Ошибка чтения system_stats.db: {e}")
        return False, None, None


async def monitor_server_load():
    """Фоновая задача мониторинга нагрузки сервера по данным БД."""
    from .config import (
        get_admin_ids,
        get_load_thresholds,
        LOAD_CHECK_INTERVAL,
        LOAD_ALERT_COOLDOWN,
    )
    from .admin import is_admin_notification_enabled, is_admin_load_notification_enabled
    from .utils import get_color_by_percent

    logger.info("📊 Задача мониторинга нагрузки запущена")
    
    while True:
        await asyncio.sleep(LOAD_CHECK_INTERVAL)
        admin_ids = get_admin_ids()
        
        if not admin_ids:
            continue
        
        cpu_threshold, memory_threshold = get_load_thresholds()
        
        is_sustained, avg_cpu, avg_ram = await asyncio.to_thread(
            _check_sustained_high_load, cpu_threshold, memory_threshold
        )
        
        if not is_sustained:
            continue

        logger.warning(f"⚠️ Высокая нагрузка: CPU={avg_cpu:.1f}%, RAM={avg_ram:.1f}%")
        
        now_ts = time.time()
        alert_text = (
            "<b>⚠️ Высокая нагрузка на сервер</b>\n"
            "<i>(держится более 5 минут)</i>\n\n"
            f"{get_color_by_percent(avg_cpu)} <b>ЦП:</b> {avg_cpu:>5.1f}%\n"
            f"{get_color_by_percent(avg_ram)} <b>ОЗУ:</b> {avg_ram:>5.1f}%"
        )
        
        bot = get_bot()
        for admin in admin_ids:
            if not is_admin_notification_enabled(admin):
                continue
            if not is_admin_load_notification_enabled(admin):
                continue
            last_sent = _last_load_alerts.get(admin, 0)
            if now_ts - last_sent < LOAD_ALERT_COOLDOWN:
                continue
            try:
                await bot.send_message(admin, alert_text, parse_mode="HTML")
                _last_load_alerts[admin] = now_ts
                logger.info(f"✅ Уведомление о нагрузке отправлено админу {admin}")
            except Exception as e:
                logger.error(f"Ошибка отправки уведомления о нагрузке: {e}")