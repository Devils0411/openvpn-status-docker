"""Клиент для взаимодействия с контейнером AmneziaWG Easy."""
import logging
import re
import time
import aiohttp
from aiohttp import CookieJar, BasicAuth
from typing import Optional, Dict, Any, Tuple
import docker

logger = logging.getLogger("tg_bot")

# Простой кэш статуса контейнера
_amnezia_cache = {"is_running": None, "ts": 0.0, "ttl": 15}



class AmneziaClient:
    """Клиент для управления AmneziaWG Easy через API."""
    
    def __init__(self, base_url: str, password: str, username: str = "admin"):
        self.base_url = base_url.rstrip("/")
        self.password = password
        self.username = username
        self.session: Optional[aiohttp.ClientSession] = None
        
    async def __aenter__(self):
        # 🔑 Ключевое исправление: unsafe=True разрешает сохранение куки для IP-адресов (10.x.x.x)
        self.session = aiohttp.ClientSession(cookie_jar=CookieJar(unsafe=True))
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session and not self.session.closed:
            await self.session.close()
            
    async def _get_auth(self):
        """Возвращает объект авторизации для запросов."""
        return BasicAuth(self.username, self.password)
    
    async def create_client(self, name: str, expire_date: Optional[str] = None) -> Dict[str, Any]:
        """Создать нового клиента WireGuard."""
        payload = {"name": name}
        if expire_date:
            payload["expiredDate"] = expire_date

        async with self.session.post(
            f"{self.base_url}/api/client",
            json=payload,
            headers={"Content-Type": "application/json"},
            auth=await self._get_auth()
        ) as resp:
            if resp.status == 200:
                return await resp.json()
            error_body = await resp.text()
            logger.error(f"❌ Create client failed {resp.status}: {error_body}")
            raise RuntimeError(f"Create client failed: {resp.status}. Body: {error_body}")

    async def download_config(self, client_id: str) -> bytes:
        """Скачать конфигурационный файл клиента."""
        async with self.session.get(
            f"{self.base_url}/api/client/{client_id}/configuration",
            auth=await self._get_auth()
        ) as resp:
            if resp.status == 200:
                return await resp.read()
            error_body = await resp.text()
            logger.error(f"❌ Download config failed {resp.status}: {error_body}")
            raise RuntimeError(f"Download config failed: {resp.status}. Body: {error_body}")

    async def get_clients(self) -> list:
        """Получить список всех клиентов с деталями."""
        async with self.session.get(
            f"{self.base_url}/api/client",
            headers={"Accept": "application/json", "User-Agent": "AmneziaBot/1.0"},
            auth=await self._get_auth()
        ) as resp:
            if resp.status == 200:
                clients = await resp.json()
                for client in clients:
                    client.setdefault("expire", "unknown")
                    client.setdefault("name", client.get("id", "unknown"))
                return clients
            error_body = await resp.text()
            logger.error(f"❌ API Error {resp.status}: {error_body}")
            raise RuntimeError(f"Get clients failed: {resp.status}. Body: {error_body}")

    async def delete_client(self, client_id: str) -> bool:
        """Удалить клиента по ID."""
        async with self.session.delete(
            f"{self.base_url}/api/client/{client_id}",
            auth=await self._get_auth()
        ) as resp:
            if resp.status in (200, 204):
                logger.debug("✅ Клиент %s удалён через API", client_id)
                return True
            error_body = await resp.text()
            logger.error(f"❌ Delete client failed {resp.status}: {error_body}")
            return False


class AmneziaContainerFinder:
    """Поиск и извлечение данных из контейнера AmneziaWG."""
    
    CONTAINER_NAME_PATTERN = re.compile(r"amnezia", re.I)
    PASSWORD_PATTERN = re.compile(r"^WIREGUARD_PASSWORD=(.+)$", re.M)
    
    @staticmethod
    def find_amnezia_container() -> Optional[docker.models.containers.Container]:
        try:
            client = docker.from_env()
            for container in client.containers.list():
                if AmneziaContainerFinder.CONTAINER_NAME_PATTERN.search(container.name):
                    logger.info(f"✅ Найден контейнер Amnezia: {container.name}")
                    return container
        except docker.errors.DockerException as e:
            logger.error(f"❌ Ошибка подключения к Docker: {e}")
        return None
    
    @staticmethod
    def get_container_ip(container: docker.models.containers.Container) -> Optional[str]:
        try:
            attrs = container.attrs
            networks = attrs.get("NetworkSettings", {}).get("Networks", {})
            for net_data in networks.values():
                ip = net_data.get("IPAddress")
                if ip:
                    logger.debug(f"🌐 IP контейнера: {ip}")
                    return ip
        except Exception as e:
            logger.error(f"❌ Ошибка получения IP: {e}")
        return None
    
    @staticmethod
    def get_wireguard_password(container: docker.models.containers.Container) -> Optional[str]:
        try:
            config = container.attrs.get("Config", {})
            env_vars = config.get("Env", []) or []
            for env in env_vars:
                match = AmneziaContainerFinder.PASSWORD_PATTERN.match(env)
                if match:
                    password = match.group(1).strip()
                    logger.debug("🔑 Пароль AmneziaWG получен")
                    return password
        except Exception as e:
            logger.error(f"❌ Ошибка получения пароля: {e}")
        return None
    
    @classmethod
    def discover(cls) -> Optional[Tuple[str, str]]:
        container = cls.find_amnezia_container()
        if not container:
            logger.debug("⚠️ Контейнер Amnezia не найден")
            return None
            
        ip = cls.get_container_ip(container)
        password = cls.get_wireguard_password(container)
        
        if ip and password:
            return ip, password
        logger.warning(f"⚠️ Не удалось получить данные: IP={ip}, PASS={'***' if password else None}")
        return None

def is_amnezia_running() -> bool:
    """Проверяет наличие запущенного контейнера Amnezia. Кэширует результат на 15 сек."""
    now = time.time()
    if _amnezia_cache["is_running"] is not None and (now - _amnezia_cache["ts"]) < _amnezia_cache["ttl"]:
        return _amnezia_cache["is_running"]

    try:
        client = docker.from_env()
        for c in client.containers.list():
            if "amnezia" in c.name.lower():
                _amnezia_cache["is_running"] = True
                _amnezia_cache["ts"] = now
                return True
        _amnezia_cache["is_running"] = False
        _amnezia_cache["ts"] = now
        return False
    except Exception as e:
        logger.debug(f"⚠️ Проверка контейнера Amnezia: {e}")
        _amnezia_cache["is_running"] = False
        _amnezia_cache["ts"] = now
        return False