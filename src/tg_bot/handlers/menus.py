"""Обработчики навигации по меню."""

from aiogram import Router, types
from aiogram.fsm.context import FSMContext
import logging

from ..config import get_admin_ids, get_client_mapping, remove_client_mapping
from ..admin import (
    is_admin_notification_enabled,
    set_admin_notification,
    is_admin_load_notification_enabled,
    set_admin_load_notification,
    is_admin_request_notification_enabled,
    set_admin_request_notification,
    get_user_label,
)
from ..keyboards import (
    create_main_menu,
    create_openvpn_menu,
    create_wireguard_menu,
    create_server_menu,
    create_clients_menu,
    create_admins_menu,
    create_notifications_menu,
    create_clientmap_delete_menu,
    create_back_keyboard,
)
from ..states import VPNSetup
from ..utils import get_external_ip

logger = logging.getLogger("tg_bot")
router = Router()


def _get_server_ip():
    return get_external_ip()


@router.callback_query(lambda c: c.data in ["main_menu", "openvpn_menu", "wireguard_menu", "server_menu", "clients_menu", "admins_menu"])
async def handle_main_menus(callback: types.CallbackQuery):
    """Обработка навигации по главному меню."""
    admin_ids = get_admin_ids()
    
    if callback.from_user.id not in admin_ids:
        logger.warning("❌ Отказано в доступе к меню %s для %s", callback.data, callback.from_user.id)
        await callback.answer("Доступ запрещен!", show_alert=True)
        return
    
    if callback.data == "main_menu":
        server_ip = _get_server_ip()
        await callback.message.edit_text("Главное меню:", reply_markup=create_main_menu(server_ip))
    elif callback.data == "openvpn_menu":
        await callback.message.edit_text("Меню OpenVPN:", reply_markup=create_openvpn_menu())
    elif callback.data == "server_menu":
        await callback.message.edit_text("Меню сервера:", reply_markup=create_server_menu())
    elif callback.data == "clients_menu":
        await callback.message.edit_text(
            "Привязки клиентов:\n\n"
            "Чтобы удалить привязку — нажмите на неё в списке и подтвердите удаление.",
            reply_markup=create_clients_menu(admin_ids),
        )
    elif callback.data == "admins_menu":
        await callback.message.edit_text("Администраторы:", reply_markup=create_admins_menu(admin_ids))
    else:
        await callback.message.edit_text("Меню WireGuard:", reply_markup=create_wireguard_menu())
    
    await callback.answer()

def get_contact_request_keyboard():
    """Создает клавиатуру с кнопкой запроса контакта."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📱 Отправить контакт",
                    switch_inline_query="",  # Альтернатива: использовать request_contact в ReplyKeyboard
                )
            ],
            [
                InlineKeyboardButton(
                    text="📨 Переслать сообщение от пользователя",
                    callback_data="clientmap_forward_info"
                )
            ],
            [
                InlineKeyboardButton(text="⬅️ Назад", callback_data="clients_menu")
            ]
        ]
    )


@router.callback_query(lambda c: c.data.startswith("clientmap_"))
async def handle_clientmap_actions(callback: types.CallbackQuery, state: FSMContext):
    """Обработка действий с привязками клиентов."""
    admin_ids = get_admin_ids()
    
    if callback.from_user.id not in admin_ids:
        logger.warning("❌ Несанкционированный доступ к clientmap от %s", callback.from_user.id)
        await callback.answer("Доступ запрещен!", show_alert=True)
        return
    
    data = callback.data
    
    if data == "clientmap_add":
        logger.debug("➕ Админ %s начал добавление привязки", callback.from_user.id)
        await callback.message.edit_text("Отправьте привязку в формате:\n<code>client_id:имя_клиента</code>", reply_markup=create_back_keyboard("clients_menu"))
        await state.set_state(VPNSetup.entering_client_mapping)
        await callback.answer()
        return

    if data == "clientmap_select_user":
        logger.debug("👤 Админ начал процесс выбора пользователя для привязки")
        await callback.message.edit_text(
            "👤 <b>Привязка профиля к пользователю</b>\n\n"
            "Чтобы получить ID пользователя:\n"
            "1️⃣ Попросите пользователя написать боту /start\n"
            "2️⃣ <b>Перешлите</b> любое сообщение от этого пользователя сюда\n\n"
            "<i>Бот автоматически считает ID из пересланного сообщения</i>",
            reply_markup=create_back_keyboard("clients_menu")
        )
        await state.set_state(VPNSetup.waiting_for_user_contact)
        await callback.answer()
        return
    
    if data.startswith("clientmap_delete_confirm_"):
        # Формат: clientmap_delete_confirm_telegram_id_profile_name
        parts = data.replace("clientmap_delete_confirm_", "").split("_", 1)
        if len(parts) == 2:
            telegram_id, profile_name = parts
            logger.info(f"🗑️ Удаление привязки: {telegram_id} → {profile_name}")
            remove_client_mapping(telegram_id, profile_name)
            await callback.message.edit_text(
                f"✅ Привязка удалена: <code>get_user_label(telegram_id)</code> → <b>{profile_name}</b>",
                reply_markup=create_clients_menu(admin_ids)
            )
        else:
            await callback.message.edit_text("❌ Ошибка при удалении привязки")
        await callback.answer()
        return
    
    if data.startswith("clientmap_"):
        parts = data.replace("clientmap_", "").split("_", 1)
        if len(parts) == 2:
            telegram_id, profile_name = parts

            client_map = get_client_mapping()
            user_profiles = client_map.get(telegram_id, [])
            
            if not isinstance(user_profiles, list):
                user_profiles = [user_profiles] if user_profiles else []
            
            if profile_name not in user_profiles:
                await callback.message.edit_text(
                    "❌ Привязка не найдена.",
                    reply_markup=create_clients_menu(admin_ids)
                )
                await callback.answer()
                return
            
            logger.debug(f"📋 Админ {callback.from_user.id} просматривает привязку: {telegram_id} → {profile_name}")
            await callback.message.edit_text(
                f"Удалить привязку <code>{get_user_label(telegram_id)}</code> → <b>{profile_name}</b>?",
                reply_markup=create_clientmap_delete_menu(telegram_id, profile_name)
            )
        else:
            await callback.message.edit_text("❌ Ошибка формата привязки")
        await callback.answer()


@router.callback_query(lambda c: c.data in ["notifications_menu", "toggle_notifications", "toggle_load_notifications", "toggle_request_notifications"])
async def handle_notifications_menu(callback: types.CallbackQuery):
    """Обработка меню уведомлений."""
    admin_ids = get_admin_ids()
    
    if callback.from_user.id not in admin_ids:
        await callback.answer("Доступ запрещен!", show_alert=True)
        return
    
    if callback.data == "toggle_notifications":
        current = is_admin_notification_enabled(callback.from_user.id)
        set_admin_notification(callback.from_user.id, not current)
        logger.info("🔔 Уведомления для %s: %s", callback.from_user.id, 'вкл' if not current else 'выкл')
    elif callback.data == "toggle_load_notifications":
        current = is_admin_load_notification_enabled(callback.from_user.id)
        set_admin_load_notification(callback.from_user.id, not current)
        logger.info("⚠️ Уведомления о нагрузке для %s: %s", callback.from_user.id, 'вкл' if not current else 'выкл')
    elif callback.data == "toggle_request_notifications":
        current = is_admin_request_notification_enabled(callback.from_user.id)
        set_admin_request_notification(callback.from_user.id, not current)
        logger.info("📩 Уведомления о запросах для %s: %s", callback.from_user.id, 'вкл' if not current else 'выкл')
    
    await callback.message.edit_text(
        "Настройка уведомлений:",
        reply_markup=create_notifications_menu(callback.from_user.id),
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "no_action")
async def handle_no_action(callback: types.CallbackQuery):
    """Обработка кнопок без действия."""
    await callback.answer("В разработке", show_alert=False)
