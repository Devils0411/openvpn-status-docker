from datetime import timedelta
import os

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

class Config:
    SECRET_KEY = os.environ.get("SECRET_KEY") or "supersecretkey"
    DATABASE_PATH = os.path.join(BASE_DIR, "data", "databases", "db.db")
    LOGS_DATABASE_PATH = os.path.join(BASE_DIR, "data", "databases", "openvpn_logs.db")
    LOGS_PATH = os.path.join(BASE_DIR, "data", "logs")
    WG_STATS_PATH = os.path.join(BASE_DIR, "data", "databases", "wireguard_stats.db")
    SYSTEM_STATS_PATH = os.path.join(BASE_DIR, "data", "databases", "system_stats.db")
    ENV_PATH = os.path.join(BASE_DIR, "data", ".env")
    SETTINGS_PATH = os.path.join(BASE_DIR, "data", "settings.json")
    LEGACY_ADMIN_INFO_PATH = os.path.join(BASE_DIR, "data", "telegram_admins.json")
    PERMANENT_SESSION_LIFETIME=timedelta(minutes=5)
    REMEMBER_COOKIE_DURATION = timedelta(days=30)
    SESSION_REFRESH_EACH_REQUEST = False
    LOG_FILES = [
        ("/etc/openvpn/server/logs/openvpn-status.log", "VPN-UDP"),
    ]

class DevelopmentConfig(Config):
    DEBUG = True
class ProductionConfig(Config):
    DEBUG = False
