"""Обработчики общих команд."""

import re
import datetime
import logging

from aiogram import Router, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext

from ..admin import update_admin_info, is_admin_request_notification_enabled
from ..config import (
    get_admin_ids,
    get_client_name_for_user,
    set_client_mapping,
    get_client_mapping,
)
from ..keyboards import (
    create_main_menu,
    create_client_menu,
    create_request_access_keyboard,
    create_request_actions_keyboard,
    create_back_keyboard,
    create_user_profile_selection_keyboard,
    create_user_info_keyboard,
    create_forward_message_keyboard,
    create_profile_mapping_keyboard,
    create_no_profiles_keyboard,
)
from ..states import VPNSetup
from ..utils import get_external_ip, get_clients

logger = logging.getLogger("tg_bot")
router = Router()


def _suggest_client_name(user: types.User) -> str:
    """Предложить имя клиента по данным пользователя Telegram (username / first_name / user_id)."""
    name = (user.username or "").strip()
    if name and re.match(r"^[a-zA-Z0-9_.-]{1,32}$", name):
        return name
    name = " ".join([p for p in [user.first_name, user.last_name] if p]).strip()
    if name:
        sanitized = re.sub(r"[^a-zA-Z0-9_.-]", "_", name)[:32]
        if sanitized:
            return sanitized
    return f"user_{user.id}"


def _get_server_ip():
    return get_external_ip()


async def get_client_info_for_user(user_id: int):
    """Получает полную информацию о профилях пользователя с датами истечения."""
    profiles = get_client_mapping().get(str(user_id), [])
    if not profiles:
        return []
    if not isinstance(profiles, list):
        profiles = [profiles]

    try:
        clients = await get_clients("openvpn")
    except Exception as e:
        logger.error("Ошибка получения клиентов: %s", e)
        clients = []

    result = []
    for profile in profiles:
        client_info = {"name": profile, "expire": None}
        for client in clients:
            if isinstance(client, dict) and client.get("name") == profile:
                client_info["expire"] = client.get("expire")
                break
        result.append(client_info)

    return result


async def show_client_menu(message: types.Message, user_id: int):
    """Показать меню клиента для пользователей не-администраторов с поддержкой множественных профилей."""
    client_info = await get_client_info_for_user(user_id)
    
    if not client_info:
        await message.answer(
            "У вас ещё нет доступа к боту.\n\n"
            f"Ваш ID: `<code>{user_id}</code>`\n\n"
            "Нажмите кнопку ниже — администратор получит запрос и сможет "
            "подтвердить или отклонить доступ.",
            reply_markup=create_request_access_keyboard(),
        )
        return

    # Формируем текст с информацией о профилях
    profiles_text = ""
    today = datetime.datetime.now().date()
    
    for i, info in enumerate(client_info, 1):
        profile_name = info.get("name", "Неизвестно")
        expire_date = info.get("expire")
        
        if expire_date and expire_date != "unknown":
            try:
                exp_date = datetime.datetime.strptime(expire_date, "%d-%m-%Y").date()
                days_left = (exp_date - today).days
                
                if days_left < 0:
                    status = "❌ Истёк"
                elif days_left <= 7:
                    status = f"⚠️ {days_left} дн."
                elif days_left <= 30:
                    status = f"⏳ {days_left} дн."
                else:
                    status = f"✅ {days_left} дн."
                
                profiles_text += f"{i}. 🔐 {profile_name} (до {expire_date}) {status}\n"
            except:
                profiles_text += f"{i}. 🔐 {profile_name} (до {expire_date})\n"
        else:
            profiles_text += f"{i}. 🔐 {profile_name} (срок не указан)\n"

    if len(client_info) == 1:
        client_name = client_info[0]["name"]
        await message.answer(
            f'✅ <b>Ваш профиль: </b>\n{profiles_text}\n'
            f'Ваш Telegram ID: <code>{user_id}</code>\n\n'
            f'<b>Выберите действие: </b>',
            reply_markup=create_client_menu(client_name)
        )
    else:
        # ← ИСПОЛЬЗУЕМ ФУНКЦИЮ ИЗ keyboards.py
        await message.answer(
            f'✅ <b>Найдено профилей: </b> {len(client_info)}\n\n'
            f'{profiles_text}\n'
            f'Ваш Telegram ID: <code>{user_id}</code>\n\n'
            f'<b>Выберите профиль для подключения: </b>',
            reply_markup=create_user_profile_selection_keyboard(client_info)
        )


@router.message(Command("start"))
async def start(message: types.Message, state: FSMContext):
    """Обработка команды /start."""
    update_admin_info(message.from_user)
    admin_ids = get_admin_ids()
    
    if not admin_ids:
        await message.answer(
            "Администраторы еще не настроены.\n"
            "Ваш ID для настройки: "
            f"<code>{message.from_user.id}</code>\n"
            "Добавьте его в переменную <b>ADMIN_ID</b> в .env."
        )
        await state.clear()
        return

    # Неадмин без привязки — показываем кнопку «Запросить доступ»
    if message.from_user.id not in admin_ids:
        client_name = get_client_name_for_user(message.from_user.id)
        if not client_name:
            await message.answer(
                "У вас ещё нет доступа к боту.\n\n"
                f"Ваш ID: <code>{message.from_user.id}</code>\n\n"
                "Нажмите кнопку ниже — администратор получит запрос и сможет "
                "подтвердить или отклонить доступ.",
                reply_markup=create_request_access_keyboard(),
            )
            await state.clear()
            return
        await show_client_menu(message, message.from_user.id)
        await state.clear()
        return
    
    # Админ
    server_ip = _get_server_ip()
    await message.answer("Главное меню:", reply_markup=create_main_menu(server_ip))
    await state.set_state(VPNSetup.choosing_option)


@router.message(Command("id"))
async def show_user_id(message: types.Message):
    """Обработка команды /id."""
    update_admin_info(message.from_user)
    await message.answer(f"Ваш ID: <code>{message.from_user.id}</code>")


@router.message(Command("request"))
async def request_access_command(message: types.Message):
    """Обработка /request — запрос доступа (то же, что кнопка «Запросить доступ»)."""
    update_admin_info(message.from_user)
    admin_ids = get_admin_ids()
    if not admin_ids:
        await message.answer("Администраторы не настроены.")
        return
    if message.from_user.id in admin_ids:
        await message.answer("Вы уже администратор.")
        return
    if get_client_name_for_user(message.from_user.id):
        await message.answer("У вас уже есть доступ. Используйте /start.")
        return
    user = message.from_user
    label = " ".join([p for p in [user.first_name, user.last_name] if p]).strip() or "—"
    username_part = f" @{user.username}" if user.username else ""
    text = (
        f"Клиент: {label}{username_part}\n"
        f"ID: <code>{user.id}</code>\n\n"
        "Выберите клиента, введите имя клиента или отклоните запрос."
    )
    keyboard = create_request_actions_keyboard(user.id)
    from ..bot import get_bot
    bot = get_bot()
    for admin_id in admin_ids:
        if not is_admin_request_notification_enabled(admin_id):
            continue
        try:
            await bot.send_message(admin_id, text, reply_markup=keyboard)
        except Exception:
            pass
    await message.answer("Запрос отправлен администраторам.")
    logger.info("📩 Запрос доступа от пользователя %s", message.from_user.id)


@router.message(Command("client"))
async def handle_client_mapping_command(message: types.Message, state: FSMContext):
    """Обработка команды /client для привязки клиентов."""
    update_admin_info(message.from_user)
    admin_ids = get_admin_ids()
    
    if message.from_user.id not in admin_ids:
        await message.answer("Доступ запрещен")
        return
    
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer(
            "Отправьте привязку в формате:\n"
            "<code>client_id:имя_клиента</code>\n"
            "Например: <code>123456789:vpn-user</code>"
        )
        await state.set_state(VPNSetup.entering_client_mapping)
        return
    
    await process_client_mapping(message, parts[1], state)


@router.message(VPNSetup.entering_client_mapping)
async def handle_client_mapping_state(message: types.Message, state: FSMContext):
    """Обработка ввода привязки клиента (состояние FSM)."""
    update_admin_info(message.from_user)
    admin_ids = get_admin_ids()
    
    if message.from_user.id not in admin_ids:
        await message.answer("Доступ запрещен")
        await state.clear()
        return
    
    from ..keyboards import create_clients_menu
    success = await process_client_mapping(message, message.text, state)
    if success:
        await message.answer(
            "Привязки клиентов:\n\n"
            "Чтобы удалить привязку — нажмите на неё в списке и подтвердите удаление.",
            reply_markup=create_clients_menu(admin_ids),
        )


async def process_client_mapping(message: types.Message, raw_text: str, state: FSMContext):
    """Обработать введённую привязку клиента."""
    payload = raw_text.strip()
    match = re.match(r"^(\d+)\s*:\s*([a-zA-Z0-9_.-]{1,32})$", payload)
    if not match:
        await message.answer(
            "❌ Некорректный формат. Используйте:\n"
            "<code>client_id:имя_клиента</code>"
        )
        return False
    
    telegram_id, client_name = match.groups()
    set_client_mapping(telegram_id, client_name)
    # Уведомляем клиента о привязке
    try:
        from ..bot import get_bot
        bot = get_bot()
        await bot.send_message(
            int(telegram_id),
            f"✅ Вам предоставлен доступ к боту. Ваш клиент: <b>{client_name}</b>. Нажмите /start для входа.",
        )
    except Exception:
        pass
    await message.answer(
        f"✅ Привязка сохранена: <code>{telegram_id}</code> → <b>{client_name}</b>"
    )
    logger.info("✅ Привязка клиента сохранена: %s → %s", telegram_id, client_name)  # INFO - важное действие
    await state.clear()
    return True


@router.callback_query(lambda c: c.data == "request_access")
async def handle_request_access(callback: types.CallbackQuery):
    """При нажатии неавторизованным пользователем «Запросить доступ» — уведомить всех админов."""
    user = callback.from_user
    admin_ids = get_admin_ids()
    if not admin_ids:
        await callback.answer("Администраторы не настроены.", show_alert=True)
        return
    if user.id in admin_ids:
        await callback.answer("Вы уже администратор.", show_alert=True)
        return
    if get_client_name_for_user(user.id):
        await callback.answer("У вас уже есть доступ.", show_alert=True)
        return

    label = " ".join([p for p in [user.first_name, user.last_name] if p]).strip() or "—"
    username_part = f" @{user.username}" if user.username else ""
    text = (
        f"Клиент: {label}{username_part}\n"
        f"ID: <code>{user.id}</code>\n\n"
        "Выберите клиента, введите имя клиента или отклоните запрос."
    )
    keyboard = create_request_actions_keyboard(user.id)

    from ..bot import get_bot
    bot = get_bot()
    sent = 0
    for admin_id in admin_ids:
        if not is_admin_request_notification_enabled(admin_id):
            continue
        try:
            await bot.send_message(admin_id, text, reply_markup=keyboard)
            sent += 1
        except Exception:
            pass
    if sent:
        await callback.answer("Запрос отправлен администраторам.")
        logger.info("📩 Запрос доступа отправлен от %s", user.id)
    else:
        await callback.answer("Не удалось отправить запрос.\nЗапросы временно отключены.", show_alert=True)


@router.callback_query(lambda c: c.data.startswith("user_select_profile_"))
async def handle_user_profile_selection(callback: types.CallbackQuery, state: FSMContext):
    """Обрабатывает выбор профиля обычным пользователем."""
    admin_ids = get_admin_ids()
    if callback.from_user.id in admin_ids:
        await callback.answer("Это меню для клиентов", show_alert=True)
        return
    
    profile_name = callback.data.replace("user_select_profile_", "", 1)

    # Проверяем, что профиль действительно привязан к этому пользователю
    client_info = await get_client_info_for_user(callback.from_user.id) 
    profile_info = None
    for info in client_info:
        if info.get("name") == profile_name:
            profile_info = info
            break

    if not profile_info:
        await callback.answer("❌ Доступ запрещен!", show_alert=True)
        return

    # Формируем текст с информацией о сроке действия
    expire_date = profile_info.get("expire")
    expire_text = ""
    today = datetime.datetime.now().date()
    
    if expire_date and expire_date != "unknown":
        try:
            exp_date = datetime.datetime.strptime(expire_date, "%d-%m-%Y").date()
            days_left = (exp_date - today).days
            
            if days_left < 0:
                expire_text = f"📅 Срок действия: <b>❌ Истёк</b> ({expire_date})"
            elif days_left <= 7:
                expire_text = f"📅 Срок действия: <b>⚠️ {days_left} дн.</b> ({expire_date})"
            elif days_left <= 30:
                expire_text = f"📅 Срок действия: <b>⏳ {days_left} дн.</b> ({expire_date})"
            else:
                expire_text = f"📅 Срок действия: <b>✅ {days_left} дн.</b> ({expire_date})"
        except:
            expire_text = f"📅 Срок действия: {expire_date}"
    else:
        expire_text = "📅 Срок действия: не указан"

    await callback.message.edit_text(
        f'✅ <b>Выбран профиль:</b> {profile_name}\n\n'
        f'Ваш Telegram ID: <code>{callback.from_user.id}</code>\n\n'
        f'{expire_text}\n\n'
        f'<b>Выберите протокол подключения:</b>',
        reply_markup=create_client_menu(profile_name)
    )
    await callback.answer()

@router.callback_query(lambda c: c.data == "user_my_info")
async def handle_user_my_info(callback: types.CallbackQuery):
    """Показывает пользователю информацию о его привязках."""
    admin_ids = get_admin_ids()
    if callback.from_user.id in admin_ids:
        await callback.answer("Это меню для клиентов", show_alert=True)
        return
    
    # Получаем ВСЕ профили пользователя напрямую из маппинга
    client_map = get_client_mapping()
    profiles = client_map.get(str(callback.from_user.id), [])
    
    if not profiles:
        await callback.answer("❌ Профили не найдены", show_alert=True)
        return
    
    if not isinstance(profiles, list):
        profiles = [profiles]

    info_text = (
        f"ℹ️ <b>Ваша информация</b>\n\n"
        f"🔢 Telegram ID: <code>{callback.from_user.id}</code>\n"
        f"👤 Имя: {callback.from_user.first_name}\n"
        f"📊 Количество профилей: {len(profiles)}\n\n"
        f"<b>Ваши профили:</b>\n"
    )

    for i, profile in enumerate(profiles, 1):
        info_text += f"{i}. 🔐 {profile}\n"

    await callback.message.answer(
        info_text,
        reply_markup=create_user_info_keyboard()
    )
    await callback.answer()


@router.message(VPNSetup.waiting_for_user_contact)
async def handle_user_contact(message: types.Message, state: FSMContext):
    """Обрабатывает пересланное сообщение для получения ID пользователя."""
    admin_ids = get_admin_ids()
    if message.from_user.id not in admin_ids:
        await message.answer("Доступ запрещен")
        await state.clear()
        return

    if message.forward_from:
        user_id = message.forward_from.id
        first_name = message.forward_from.first_name or ""
        last_name = message.forward_from.last_name or ""
        username = message.forward_from.username or ""

        full_name = " ".join([first_name, last_name]).strip()
        user_display = full_name if full_name else f"@{username}" if username else f"ID: {user_id}"
        update_admin_info(message.forward_from) 
        await state.update_data(
            target_user_id=str(user_id),
            target_user_name=user_display
        )

        clients = await get_clients("openvpn")

        if not clients:
            await message.answer(
                "❌ <b>Нет доступных профилей OpenVPN</b>\n\n"
                "Сначала создайте клиентов OpenVPN.",
                reply_markup=create_no_profiles_keyboard()
            )
            await state.clear()
            return

        await message.answer(
            f"✅ <b>Пользователь выбран:</b>\n"
            f"👤 {user_display}\n"
            f"🔢 ID: <code>{user_id}</code>\n\n"
            f"<b>Выберите профиль OpenVPN для привязки:</b>",
            reply_markup=create_profile_mapping_keyboard(clients)
        )
        await state.set_state(VPNSetup.selecting_profile_for_mapping)
    else:
        await message.answer(
            "❌ <b>Это не пересланное сообщение</b>\n\n"
            "Пожалуйста, перешлите сообщение от пользователя.\n\n"
            "<i>Или нажмите «Назад» и выберите «Добавить вручную»</i>",
            reply_markup=create_forward_message_keyboard()
        )


@router.callback_query(lambda c: c.data.startswith("map_profile_"))
async def handle_profile_selection(callback: types.CallbackQuery, state: FSMContext):
    """Обрабатывает выбор профиля для привязки к пользователю."""
    admin_ids = get_admin_ids()
    if callback.from_user.id not in admin_ids:
        await callback.answer("Доступ запрещен!", show_alert=True)
        return

    profile_name = callback.data.replace("map_profile_", "", 1)
    state_data = await state.get_data()
    user_id = state_data.get("target_user_id")
    user_name = state_data.get("target_user_name")

    if not user_id:
        await callback.answer("❌ Ошибка: данные пользователя не найдены", show_alert=True)
        await state.clear()
        return

    logger.info(f"✅ Привязка профиля: {user_id} → {profile_name}")
    client_map = get_client_mapping()
    existing = client_map.get(str(user_id), [])
    if not isinstance(existing, list):
        existing = [existing] if existing else []

    is_new = set_client_mapping(user_id, profile_name)

    if is_new:
        status_text = "✅ <b>Новый профиль добавлен!</b>"
        if existing:
            status_text += f"\n\n📋 У пользователя уже было профилей: {len(existing)}"
    else:
        status_text = "⚠️ <b>Такой профиль уже привязан!</b>"

    from ..keyboards import create_clients_menu
    await callback.message.answer(
        f"{status_text}\n\n"
        f"👤 Пользователь: {user_name}\n"
        f"🔢 Telegram ID: <code>{user_id}</code>\n"
        f"🔐 Профиль OpenVPN: <b>{profile_name}</b>",
        reply_markup=create_clients_menu(admin_ids)
    )
    await state.clear()
    await callback.answer()