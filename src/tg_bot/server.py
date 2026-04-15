"""Функции мониторинга и статистики сервера."""

import asyncio
import re
import subprocess
import logging

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Optional, Tuple
from src.config import Config
from .config import get_user_timezone
from .utils import get_color_by_percent, format_vpn_clients, parse_handshake_time, is_peer_online, read_wg_config

logger = logging.getLogger("tg_bot")


def _lazy_psutil():
    """Ленивый импорт psutil."""
    import psutil
    return psutil

def format_speed(bits_per_second):
    """Форматирует скорость в битах."""
    if bits_per_second < 1000:
        return f"{bits_per_second:.1f} бит/с"
    elif bits_per_second < 1000**2:
        return f"{bits_per_second / 1000:.1f} Кбит/с"
    elif bits_per_second < 1000**3:
        return f"{bits_per_second / 1000**2:.1f} Мбит/с"
    else:
        return f"{bits_per_second / 1000**3:.2f} Гбит/с"


async def get_network_speed(interface: str = None, interval: float = 1.0):
    """Измеряет текущую скорость сети."""
    try:
        psutil = _lazy_psutil()
        if not interface:
            interfaces = psutil.net_io_counters(pernic=True)
            if not interfaces:
                return 0, 0
            interface = max(interfaces.items(), key=lambda x: x[1].bytes_recv + x[1].bytes_sent)[0]
        
        net_start = psutil.net_io_counters(pernic=True).get(interface)
        if not net_start:
            return 0, 0
        
        await asyncio.sleep(interval)
        
        net_end = psutil.net_io_counters(pernic=True).get(interface)
        if not net_end:
            return 0, 0

        download_bits = ((net_end.bytes_recv - net_start.bytes_recv) / interval) * 8
        upload_bits = ((net_end.bytes_sent - net_start.bytes_sent) / interval) * 8

        return max(0, download_bits), max(0, upload_bits)
    except Exception as e:
        logger.error("Ошибка измерения скорости сети: %s", e)
        return 0, 0


async def get_server_stats():
    """Получить статистику сервера."""
    try:
        psutil = _lazy_psutil()
        
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        memory_percent = memory.percent
        disk = psutil.disk_usage("/")
        disk_total = disk.total / (1024**3)
        disk_used = disk.used / (1024**3)
        
        uptime = _get_uptime()
        formatted_uptime = _format_uptime(uptime)
        main_interface = _get_main_interface()

        logger.debug("📊 Статистика: CPU=%d%%, RAM=%d%%", cpu_percent, memory_percent)
        
        traffic_text = ""
        if main_interface:
            stats = psutil.net_io_counters(pernic=True).get(main_interface)
            if stats:
                traffic_text = f"\n<b>💾 Всего:</b> ⬇ {stats.bytes_recv / (1024**3):.2f} GB / ⬆ {stats.bytes_sent / (1024**3):.2f} GB"

        download_speed, upload_speed = await get_network_speed(main_interface, interval=1.0)
        
        vpn_clients = await _count_online_clients()
        clients_section = format_vpn_clients(vpn_clients)
        
        stats_text = f"""
<b>📊 Статистика сервера: </b>

{get_color_by_percent(cpu_percent)} <b>ЦП:</b> {cpu_percent:>5}%
{get_color_by_percent(memory_percent)} <b>ОЗУ:</b> {memory_percent:>5}%
<b>👥 Онлайн: </b> {clients_section}
<b>💿 Диск:</b> {disk_used:.1f}/{disk_total:.1f} GB
<b>⏱️ Uptime:</b> {formatted_uptime}
🌐 Сеть ({main_interface or 'N/A'}):
⬇ Скорость: {format_speed(download_speed)}
⬆ Скорость: {format_speed(upload_speed)}
{traffic_text}

"""
        return stats_text
    except Exception as e:
        logger.error("Ошибка получения статистики сервера: %s", e)
        return f"❌ Ошибка получения статистики: {str(e)}"


async def get_service_state(service_name: str) -> str:
    try:
        process = await asyncio.create_subprocess_exec(
            "supervisorctl",
            "status",
            service_name,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        state = stdout.decode().strip()
        
        if "RUNNING" in state:
            return "активен"
        elif "STARTING" in state:
            return "запускается"
        elif "STOPPED" in state:
            return "неактивен"
        elif "FATAL" in state or "BACKOFF" in state:
            return "ошибка"
        else:
            return "неизвестно"
    
    except Exception as e:
        logger.error("Ошибка проверки службы %s: %s", service_name, e)
        return "неизвестно"


async def get_services_status_text():
    services = [("StatusOpenVPN", "logs"), ("Telegram bot", "telegram-bot")]
    lines = ["⚙️ Службы StatusOpenVPN:", ""]
    
    for label, service in services:
        state = await get_service_state(service)
        icon = "🟢" if state == "активен" else "🔴" if state == "неактивен" else "🟡"
        lines.append(f"{icon} {label}: {state}")
    
    return "\n".join(lines)


def _format_connected_dt(dt: Optional[datetime], user_id: int = None) -> str:
    """Краткая строка времени с учетом временной зоны пользователя."""
    if not dt:
        return "—"
    
    # Делаем datetime timezone-aware (предполагаем, что dt в UTC)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    
    # Получаем временную зону пользователя
    user_tz_name = get_user_timezone(user_id) if user_id else "Europe/Moscow"
    try:
        user_tz = ZoneInfo(user_tz_name)
        local_dt = dt.astimezone(user_tz)
        return local_dt.strftime("%d.%m.%Y %H:%M")
    except Exception:
        # Fallback на Москву при ошибке
        moscow_tz = ZoneInfo("Europe/Moscow")
        local_dt = dt.astimezone(moscow_tz)
        return local_dt.strftime("%d.%m.%Y %H:%M")


def get_openvpn_online_entries(user_id: int = None):
    """Получает список активных клиентов OpenVPN из логов."""
    entries = []
    file_paths = Config.LOG_FILES
    
    for file_path, protocol in file_paths:
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                for line in file:
                    line = line.strip()
                    if not line.startswith("CLIENT_LIST"):
                        continue
                    parts = line.split(",")
                    if len(parts) < 2:
                        continue
                    client_name = parts[1].strip()
                    if client_name and client_name not in ["UNDEF", "Common Name"]:
                        connected = "—"
                    if len(parts) > 7:
                        try:
                            raw = parts[7].strip()
                            start_dt = datetime.strptime(
                                raw, "%Y-%m-%d %H:%M:%S"
                            )
                            connected = _format_connected_dt(start_dt, user_id=user_id)
                        except (ValueError, IndexError):
                            pass
                    entries.append(
                        {
                            "name": client_name,
                            "protocol": f"OpenVPN · {protocol}",
                            "connected": connected,
                        }
                    )
        except FileNotFoundError:
            logger.debug("Файл логов не найден: %s", file_path)
            continue
        except Exception as e:
            logger.error("Ошибка чтения %s: %s", file_path, e)

    entries.sort(key=lambda x: (x["name"].lower(), x["protocol"]))
    return entries


def _wg_online_proto_and_name(
    public_key: str,
    iface: Optional[str],
    vpn_mapping: dict,
    antizapret_mapping: dict,
) -> Tuple[str, str]:
    """
    Подпись и имя для пира WireGuard.

    Главный критерий — интерфейс из `wg show` (фактический туннель), чтобы не
    путать VPN и Antizapret при дубликате одного PublicKey в обоих .conf.
    Если интерфейс не распознан — fallback по наличию ключа в конфигах.
    """
    n = (iface or "").strip().lower()

    def name_vpn_first() -> str:
        return (
            vpn_mapping.get(public_key)
            or antizapret_mapping.get(public_key)
            or public_key
        )

    def name_az_first() -> str:
        return (
            antizapret_mapping.get(public_key)
            or vpn_mapping.get(public_key)
            or public_key
        )

    if n == "vpn":
        return "WireGuard · VPN", name_vpn_first()
    if n == "antizapret":
        return "WireGuard · Antizapret", name_az_first()

    if public_key in vpn_mapping:
        return "WireGuard · VPN", name_vpn_first()
    if public_key in antizapret_mapping:
        return "WireGuard · Antizapret", name_az_first()
    return "WireGuard", public_key


def _parse_wireguard_online_entries(output: str):
    """Разобрать вывод WireGuard для онлайн-клиентов."""
    entries = []
    lines = (output or "").splitlines()
    
    antizapret_mapping = read_wg_config("/root/web/awg/wg0.conf")
    
    current_peer = None
    current_interface: Optional[str] = None
    for line in lines:
        line = line.strip()
        if line.startswith("interface:"):
            current_interface = line.split(":", 1)[1].strip()
            continue
        if line.startswith("peer:"):
            current_peer = line.split(":", 1)[1].strip()
            continue
        if line.startswith("latest handshake:") and current_peer:
            handshake_raw = line.split(":", 1)[1].strip()
            handshake_time = parse_handshake_time(handshake_raw)
            if handshake_time and is_peer_online(handshake_time):
                proto, name = _wg_online_proto_and_name(
                    current_peer,
                    current_interface,
                    vpn_mapping,
                    antizapret_mapping,
                )
                entries.append(
                    {
                        "name": name,
                        "protocol": proto,
                        "connected": _format_connected_dt(handshake_time),
                    }
                )
            current_peer = None

    entries.sort(key=lambda x: (x["name"].lower(), x["protocol"]))
    return entries


async def get_amnezia_online_entries(user_id: int = None):
    """Получить список онлайн-клиентов AmneziaWG через API."""
    from .bot import get_amnezia_client
    amnezia = await get_amnezia_client()
    
    if not amnezia:
        return []
    
    try:
        clients = await amnezia.get_clients()
        online_clients = []
        
        for client in clients:
            # Проверяем latestHandshakeAt - если handshake был недавно, клиент онлайн
            if client.get("latestHandshakeAt"):
                try:
                    # Парсим время последнего handshake
                    handshake_time = datetime.fromisoformat(
                        client["latestHandshakeAt"].replace("Z", "+00:00")
                    )
                    # Если handshake был в последние 3 минуты - клиент онлайн
                    if datetime.now(handshake_time.tzinfo) - handshake_time < timedelta(minutes=3):
                        online_clients.append({
                            "name": client.get("name", "Unknown"),
                            "protocol": "AmneziaWG",
                            "connected": _format_connected_dt(handshake_time, user_id=user_id)
                        })
                except Exception as e:
                    logger.debug(f"⚠️ Ошибка парсинга времени handshake: {e}")
                    continue
        
        return online_clients
    except Exception as e:
        logger.error(f"❌ Ошибка получения онлайн-клиентов Amnezia: {e}")
        return []


def _format_online_line(entry: dict) -> str:
    """Одна строка списка «кто онлайн»."""
    return f"• <b>{entry['name']}</b> · с {entry['connected']}"


async def get_online_clients_text(user_id: int = None):
    """Получить отформатированный текст онлайн-клиентов."""
    openvpn_entries = get_openvpn_online_entries(user_id=user_id)
    amnezia_entries = await get_amnezia_online_entries(user_id=user_id)
    
    lines = ["<b>👥 Кто онлайн:</b>", ""]
    
    if openvpn_entries:
        lines.append("<b>OpenVPN:</b>")
        lines.extend(_format_online_line(e) for e in openvpn_entries)
    else:
        lines.append("<b>OpenVPN:</b> нет активных клиентов")
    
    lines.append("")
    
    if amnezia_entries:
        lines.append("<b>AmneziaWG:</b>")
        lines.extend(_format_online_line(e) for e in amnezia_entries)
    else:
        lines.append("<b>AmneziaWG:</b> нет активных клиентов")
    
    return "\n".join(lines)


def _get_main_interface():
    """Получить основной сетевой интерфейс."""
    psutil = _lazy_psutil()
    interfaces = psutil.net_io_counters(pernic=True)
    if not interfaces:
        return None
    return max(interfaces.items(), key=lambda x: x[1].bytes_recv + x[1].bytes_sent)[0]


def _get_uptime():
    """Получить строку времени работы системы."""
    try:
        return subprocess.check_output("/usr/bin/uptime -p", shell=True).decode().strip()
    except subprocess.CalledProcessError as e:
        logger.error("❌ Ошибка получения uptime: %s", e)
        return "Не удалось получить время работы"


def _format_uptime(uptime_string):
    """Форматировать строку uptime на русский."""
    pattern = r"(?:(\d+)\syears?|(\d+)\smonths?|(\d+)\sweeks?|(\d+)\sdays?|(\d+)\shours?|(\d+)\sminutes?)"
    years = months = weeks = days = hours = minutes = 0
    for match in re.findall(pattern, uptime_string):
        if match[0]: years = int(match[0])
        elif match[1]: months = int(match[1])
        elif match[2]: weeks = int(match[2])
        elif match[3]: days = int(match[3])
        elif match[4]: hours = int(match[4])
        elif match[5]: minutes = int(match[5])

    result = []
    if years > 0: result.append(f"{years} г.")
    if months > 0: result.append(f"{months} мес.")
    if weeks > 0: result.append(f"{weeks} нед.")
    if days > 0: result.append(f"{days} дн.")
    if hours > 0: result.append(f"{hours} ч.")
    if minutes > 0: result.append(f"{minutes} мин.")
    return " ".join(result)


async def _count_online_clients():
    """Асинхронный подсчёт онлайн-клиентов."""
    # 1. Считаем OpenVPN синхронно в отдельном потоке (не блокирует event loop)
    def _count_ovpn_sync():
        total = 0
        for path, _ in Config.LOG_FILES:
            try:
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.startswith("CLIENT_LIST"):
                            total += 1
            except Exception:
                continue
        return total

    try:
        total_openvpn = await asyncio.to_thread(_count_ovpn_sync)
    except Exception as e:
        logger.error(f"Ошибка подсчёта OpenVPN: {e}")
        total_openvpn = 0

    # 2. Считаем AmneziaWG асинхронно в основном цикле (без to_thread!)
    online_amnezia = 0
    try:
        from src.tg_bot.bot import get_amnezia_client
        amnezia = await get_amnezia_client()
        if amnezia:
            clients = await amnezia.get_clients()
            for client in clients:
                if client.get("latestHandshakeAt"):
                    try:
                        handshake_time = datetime.fromisoformat(
                            client["latestHandshakeAt"].replace("Z", "+00:00")
                        )
                        if datetime.now(handshake_time.tzinfo) - handshake_time < timedelta(minutes=3):
                            online_amnezia += 1
                    except Exception:
                        continue
    except Exception as e:
        logger.debug(f"⚠️ Не удалось получить клиентов Amnezia: {e}")

    return {"OpenVPN": total_openvpn, "AmneziaWG": online_amnezia}