"""Обработчики управления сервером."""
import asyncio
import logging
from aiogram import Router, types
from aiogram.fsm.context import FSMContext
from ..config import get_admin_ids, set_load_thresholds
from ..keyboards import create_server_menu, create_thresholds_menu, create_reboot_confirm_menu, create_back_keyboard
from ..states import VPNSetup
from ..server import get_server_stats, get_services_status_text, get_online_clients_text
from ..audit import log_action, notify_admins

logger = logging.getLogger("tg_bot")
router = Router()

async def _safe_answer(callback: types.CallbackQuery, text: str = "", show_alert: bool = False):
    """Безопасный ответ на callback, игнорирует ошибки 'query already answered' или 'expired'."""
    try:
        await callback.answer(text, show_alert=show_alert)
    except Exception as e:
        logger.debug(f"Callback already handled/expired: {e}")

@router.callback_query(lambda c: c.data == "server_stats")
async def handle_server_stats(callback: types.CallbackQuery):
    admin_ids = get_admin_ids()
    if callback.from_user.id not in admin_ids:
        await _safe_answer(callback, "Доступ запрещен!", True)
        return

    try:
        stats_text = await get_server_stats()
        await callback.message.edit_text(stats_text, reply_markup=create_back_keyboard("server_menu"))
    except Exception as e:
        logger.error(f"Ошибка статистики: {e}", exc_info=True)
        await callback.message.edit_text("❌ Не удалось получить статистику.", reply_markup=create_back_keyboard("server_menu"))
    finally:
        await _safe_answer(callback)

@router.callback_query(lambda c: c.data == "server_reboot")
async def handle_server_reboot(callback: types.CallbackQuery):
    admin_ids = get_admin_ids()
    if callback.from_user.id not in admin_ids:
        await _safe_answer(callback, "Доступ запрещен!", True)
        return
    await callback.message.edit_text("⚠️ Внимание!\n\nПерезагрузка сервера прервет активные подключения.", reply_markup=create_reboot_confirm_menu())
    await _safe_answer(callback)

@router.callback_query(lambda c: c.data == "server_reboot_confirm")
async def handle_server_reboot_confirm(callback: types.CallbackQuery):
    admin_ids = get_admin_ids()
    if callback.from_user.id not in admin_ids:
        await _safe_answer(callback, "Доступ запрещен!", True)
        return
    
    await callback.message.edit_text("⏳ Перезагрузка сервера...")
    log_action("bot", callback.from_user.id, callback.from_user.full_name, "server_reboot", " ")
    await notify_admins(callback.from_user.id, callback.from_user.full_name, "перезагрузил сервер")

    try:
        await asyncio.create_subprocess_exec("/sbin/shutdown", "-r", "now")
    except Exception as e:
        logger.error("❌ Ошибка запуска перезагрузки: %s", e, exc_info=True)
        await callback.message.edit_text(f"❌ Ошибка запуска перезагрузки:\n{e}", reply_markup=create_server_menu())
        await _safe_answer(callback)
        return
    await _safe_answer(callback)

@router.callback_query(lambda c: c.data == "server_services")
async def handle_server_services(callback: types.CallbackQuery):
    admin_ids = get_admin_ids()
    if callback.from_user.id not in admin_ids:
        await _safe_answer(callback, "Доступ запрещен!", True)
        return
    try:
        services_text = await get_services_status_text()
        await callback.message.edit_text(services_text, reply_markup=create_back_keyboard("server_menu"))
    except Exception as e:
        await callback.message.edit_text("❌ Ошибка получения статуса служб.", reply_markup=create_back_keyboard("server_menu"))
    finally:
        await _safe_answer(callback)

@router.callback_query(lambda c: c.data == "server_online")
async def handle_server_online(callback: types.CallbackQuery):
    admin_ids = get_admin_ids()
    if callback.from_user.id not in admin_ids:
        await _safe_answer(callback, "Доступ запрещен!", True)
        return
    try:
        online_text = await get_online_clients_text(user_id=callback.from_user.id)
        await callback.message.edit_text(online_text, reply_markup=create_back_keyboard("server_menu"))
    except Exception as e:
        logger.error(f"Ошибка онлайн-клиентов: {e}", exc_info=True)
        await callback.message.edit_text("❌ Не удалось получить список клиентов.", reply_markup=create_back_keyboard("server_menu"))
    finally:
        await _safe_answer(callback)

@router.callback_query(lambda c: c.data == "server_thresholds")
async def handle_server_thresholds(callback: types.CallbackQuery):
    admin_ids = get_admin_ids()
    if callback.from_user.id not in admin_ids:
        await _safe_answer(callback, "Доступ запрещен!", True)
        return
    await callback.message.edit_text("Пороги нагрузки:", reply_markup=create_thresholds_menu())
    await _safe_answer(callback)

@router.callback_query(lambda c: c.data in ["set_cpu_threshold", "set_memory_threshold"])
async def handle_set_threshold_prompt(callback: types.CallbackQuery, state: FSMContext):
    admin_ids = get_admin_ids()
    if callback.from_user.id not in admin_ids:
        await _safe_answer(callback, "Доступ запрещен!", True)
        return

    if callback.data == "set_cpu_threshold":
        await callback.message.edit_text("Введите порог CPU (1-100):", reply_markup=create_back_keyboard("server_thresholds"))
        await state.set_state(VPNSetup.entering_cpu_threshold)
    else:
        await callback.message.edit_text("Введите порог RAM (1-100):", reply_markup=create_back_keyboard("server_thresholds"))
        await state.set_state(VPNSetup.entering_memory_threshold)
    await _safe_answer(callback)

@router.message(VPNSetup.entering_cpu_threshold)
async def handle_cpu_threshold_input(message: types.Message, state: FSMContext):
    admin_ids = get_admin_ids()
    if message.from_user.id not in admin_ids:
        await message.answer("Доступ запрещен")
        await state.clear()
        return

    value = message.text.strip()
    if not value.isdigit() or not (1 <= int(value) <= 100):
        await message.answer("Введите число от 1 до 100.")
        return

    set_load_thresholds(cpu_threshold=int(value))
    logger.info("⚙️ Порог CPU обновлён до %s%% админом %s", value, message.from_user.id)
    await message.answer("Порог CPU обновлен.", reply_markup=create_server_menu())
    await state.clear()

@router.message(VPNSetup.entering_memory_threshold)
async def handle_memory_threshold_input(message: types.Message, state: FSMContext):
    admin_ids = get_admin_ids()
    if message.from_user.id not in admin_ids:
        await message.answer("Доступ запрещен")
        await state.clear()
        return

    value = message.text.strip()
    if not value.isdigit() or not (1 <= int(value) <= 100):
        await message.answer("Введите число от 1 до 100.")
        return

    set_load_thresholds(memory_threshold=int(value))
    logger.info("⚙️ Порог RAM обновлён до %s%% админом %s", value, message.from_user.id)
    await message.answer("Порог RAM обновлен.", reply_markup=create_server_menu())
    await state.clear()