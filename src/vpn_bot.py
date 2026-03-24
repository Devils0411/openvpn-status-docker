"""Точка входа в Telegram bot для службы."""

import asyncio
import os
import sys
import logging

_script_dir = os.path.dirname(os.path.abspath(__file__))
_root = os.path.dirname(_script_dir)
if _root not in sys.path:
    sys.path.insert(0, _root)

from src.tg_bot.logging_config import setup_logging
setup_logging()

from src.tg_bot.bot import (
    get_bot,
    get_dispatcher,
    notify_admin_server_online,
    update_bot_description,
    update_bot_about,
    set_bot_commands,
    monitor_server_load,
)


async def main():
    """Главная функция для запуска бота."""

    logger = logging.getLogger("tg_bot")
    logger.info("✅ Бот успешно запущен!")
    bot = get_bot()
    dp = get_dispatcher()
    try:
        await update_bot_description()
        await notify_admin_server_online()
        await update_bot_about()
        await set_bot_commands()
        asyncio.create_task(monitor_server_load())
        await dp.start_polling(bot)
    except KeyboardInterrupt:
        logger.warning("🛑 Бот остановлен!")


if __name__ == "__main__":
    asyncio.run(main())