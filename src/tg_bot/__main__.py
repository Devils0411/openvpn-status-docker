"""Точка входа для запуска Telegram-бота"""
import asyncio
import logging

try:
    from .logging_config import setup_logging
except ImportError:
    from logging_config import setup_logging

logger = None


async def main():
    """Запустить бота в режиме long polling."""
    global logger
    logger = logging.getLogger("tg_bot")
    logger.info("✅ Бот успешно запущен!")

    from .bot import (
        get_bot,
        get_dispatcher,
        notify_admin_server_online,
        update_bot_description,
        update_bot_about,
        set_bot_commands,
        monitor_server_load,
    )

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
    except Exception as e:
        logger.critical(f"❌ Критическая ошибка: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())