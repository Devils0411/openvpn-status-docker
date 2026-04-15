"""Точка входа для запуска Telegram-бота"""
import asyncio
import logging

try:
    from .logging_config import setup_logging
except ImportError:
    from logging_config import setup_logging

logger = None


async def main():
    logger = logging.getLogger("tg_bot")
    logger.info("✅ Бот успешно запущен!")
    
    bot = get_bot()
    dp = get_dispatcher()
    
    # 1. Сохраняем ссылку на задачу, чтобы потом корректно её остановить
    monitor_task = asyncio.create_task(monitor_server_load())

    try:
        await seed_bot_profile_if_needed()
        await notify_admin_server_online()
        await set_bot_commands()
        await dp.start_polling(bot)
    except KeyboardInterrupt:
        logger.warning("🛑 Бот остановлен вручную!")
    except Exception as e:
        logger.critical(f"❌ Критическая ошибка: {e}", exc_info=True)
    finally:
        # 2. Гарантированно отменяем фоновую задачу при любом выходе
        if not monitor_task.done():
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass
        
        # 3. Очищаем кэш уведомлений, чтобы не удерживать память
        # ⚠️ В vpn_bot.py оставьте: from src.tg_bot.bot import _last_load_alerts
        # ⚠️ В __main__.py оставьте: from .bot import _last_load_alerts
        from src.tg_bot.bot import _last_load_alerts 
        _last_load_alerts.clear()
        
        logger.info("🧹 Фоновые задачи остановлены, кэш очищен. Ресурсы освобождены.")


if __name__ == "__main__":
    asyncio.run(main())