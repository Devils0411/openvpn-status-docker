"""Настройка логирования с ротацией файлов."""
import logging
import os
from src.config import Config
from logging.handlers import RotatingFileHandler

# Берем путь к папке Logs из конфиг файла
LOG_DIR = Config.LOGS_PATH
os.makedirs(LOG_DIR, exist_ok=True)

STDOUT_LOG = os.path.join(LOG_DIR, "vpn_bot.stdout.log")
STDERR_LOG = os.path.join(LOG_DIR, "vpn_bot.stderr.log")
MAX_LOG_SIZE = 10 * 1024 * 1024  # 10 МБ
BACKUP_COUNT = 5

class LevelFilter(logging.Filter):
    """Фильтр для разделения логов по уровням."""
    def __init__(self, min_level, max_level=None):
        super().__init__()
        self.min_level = min_level
        self.max_level = max_level if max_level is not None else min_level

    def filter(self, record):
        return self.min_level <= record.levelno <= self.max_level


def setup_logging():
    """Настроить логирование с ротацией и разделением по уровням."""
    logger = logging.getLogger("tg_bot")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.propagate = False

    # Формат для файлов
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%d-%m-%Y %H:%M:%S'
    )

    # stderr: WARNING и выше
    stderr_handler = RotatingFileHandler(
        STDERR_LOG,
        maxBytes=MAX_LOG_SIZE,
        backupCount=BACKUP_COUNT,
        encoding='utf-8',
        delay=True
    )
    stderr_handler.setLevel(logging.WARNING)
    stderr_handler.addFilter(LevelFilter(logging.WARNING, logging.CRITICAL))
    stderr_handler.setFormatter(file_formatter)

    # stdout: DEBUG и INFO
    stdout_handler = RotatingFileHandler(
        STDOUT_LOG,
        maxBytes=MAX_LOG_SIZE,
        backupCount=BACKUP_COUNT,
        encoding='utf-8',
        delay=True
    )
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.addFilter(LevelFilter(logging.DEBUG, logging.INFO))
    stdout_handler.setFormatter(file_formatter)

    logger.addHandler(stderr_handler)
    logger.addHandler(stdout_handler)

    # Снижаем уровень для шумных библиотек
    logging.getLogger("aiogram").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    logger.info("📋 Логирование настроено")
    return logger


def get_logger(name: str = "tg_bot"):
    """Получить настроенный логгер."""
    return logging.getLogger(name)