import asyncio
import json
import logging
import os
import socket
from datetime import datetime, timezone
from collections import defaultdict

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv

from db_manager import (
    find_participant_by_telegram_id,
    find_participant_by_ozon_id,
    find_participant_by_username,
    create_participant,
    deactivate_participant,
    create_database,
    get_user_orders_stats,
    get_user_orders_summary,
    get_referrals_by_level,
    get_referrals_orders_stats,
    get_user_bonuses,
    get_referrals_bonuses_stats,
    get_bonus_settings,
    update_bonus_settings,
    get_available_bonuses_for_withdrawal,
    clear_bonus_settings_cache,
    get_last_sync_timestamp,
    get_daily_bonus_summary,
    get_all_participants,
    get_withdrawal_settings,
    update_withdrawal_settings,
    clear_withdrawal_settings_cache,
    get_user_available_balance,
    get_user_total_balance,
    has_active_withdrawal_request,
    get_active_withdrawal_request,
    check_withdrawal_period,
    create_withdrawal_request,
    get_user_withdrawal_requests,
    get_pending_withdrawal_requests,
    get_withdrawal_request_by_id,
    cancel_withdrawal_request,
    approve_withdrawal_request,
    reject_withdrawal_request,
    complete_withdrawal_request,
    SessionLocal,
    Posting,
    BonusTransaction,
    Participant,
)

from states import Registration, BonusSettings, LeavingProgram, Withdrawal, WithdrawalRejection, ParticipantAnalytics, WithdrawalSettings
# ИМПОРТ ДЛЯ СИНХРОНИЗАЦИИ ЗАКАЗОВ
from orders_updater import update_orders_sheet 

# грузим переменные из .env
from datetime import datetime, timedelta, timezone
load_dotenv()
API_TOKEN = os.getenv("BOT_TOKEN")

if not API_TOKEN:
    raise RuntimeError("Нет BOT_TOKEN в .env, проверь файл .env")

# Список ID администраторов (можно загружать из .env)
# Формат в .env: ADMIN_IDS=123456789,987654321
admin_ids_str = os.getenv("ADMIN_IDS", "")
if admin_ids_str:
    ADMIN_IDS = [int(x.strip()) for x in admin_ids_str.split(",") if x.strip()]
else:
    # Если не указано в .env, можно задать здесь
    ADMIN_IDS = [419985638]  # Artem (ID: 419985638)

logging.basicConfig(level=logging.INFO)

# Создаем Bot без кастомной сессии (сессия будет создана внутри async контекста в main())
bot = Bot(token=API_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# =========================================================
# СИСТЕМА ПРОВЕРКИ ПРАВ АДМИНИСТРАТОРА
# =========================================================
def is_admin(user_id: int) -> bool:
    """Проверяет, является ли пользователь администратором."""
    return user_id in ADMIN_IDS

# =========================================================
# КОНСТАНТЫ ДЛЯ ВАЛИДАЦИИ
# =========================================================
MAX_TEXT_LENGTH = 1000  # Максимальная длина текстовых полей
MAX_OZON_ID_LENGTH = 50  # Максимальная длина Ozon ID
MAX_USERNAME_LENGTH = 100  # Максимальная длина username
MAX_WITHDRAWAL_AMOUNT = 1000000.0  # Максимальная сумма вывода
MIN_WITHDRAWAL_AMOUNT = 0.01  # Минимальная сумма вывода
MAX_BONUS_PERCENT = 100.0  # Максимальный процент бонуса
MIN_BONUS_PERCENT = 0.0  # Минимальный процент бонуса
MAX_LEVELS = 5  # Максимальное количество уровней
MIN_LEVELS = 1  # Минимальное количество уровней

# =========================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ БЕЗОПАСНОСТИ
# =========================================================
def safe_extract_id(callback_data: str, prefix: str) -> int | None:
    """
    Безопасно извлекает ID из callback_data.
    
    Args:
        callback_data: Данные callback (например, "admin_withdrawal_123")
        prefix: Префикс для проверки (например, "admin_withdrawal_")
        
    Returns:
        int | None: Извлеченный ID или None при ошибке
    """
    try:
        if not callback_data.startswith(prefix):
            return None
        
        # Извлекаем ID после последнего подчеркивания
        id_str = callback_data.split("_")[-1]
        if not id_str.isdigit():
            return None
        
        return int(id_str)
    except (ValueError, AttributeError, IndexError):
        return None

def sanitize_html(text: str) -> str:
    """
    Экранирует HTML-символы для безопасного отображения.
    
    Args:
        text: Текст для экранирования
        
    Returns:
        str: Экранированный текст
    """
    if not text:
        return ""
    
    # Экранируем основные HTML-символы
    text = text.replace("&", "&amp;")
    text = text.replace("<", "&lt;")
    text = text.replace(">", "&gt;")
    text = text.replace('"', "&quot;")
    text = text.replace("'", "&#x27;")
    
    return text

def validate_text_length(text: str, max_length: int, field_name: str = "Текст") -> tuple[bool, str | None]:
    """
    Проверяет длину текста.
    
    Args:
        text: Текст для проверки
        max_length: Максимальная допустимая длина
        field_name: Название поля для сообщения об ошибке
        
    Returns:
        tuple[bool, str | None]: (валидно, сообщение об ошибке)
    """
    if not text or not text.strip():
        return False, f"{field_name} не может быть пустым."
    
    if len(text) > max_length:
        return False, f"{field_name} слишком длинный. Максимальная длина: {max_length} символов."
    
    return True, None

def validate_numeric_range(value: float, min_val: float, max_val: float, field_name: str = "Значение") -> tuple[bool, str | None]:
    """
    Проверяет, находится ли числовое значение в допустимом диапазоне.
    
    Args:
        value: Значение для проверки
        min_val: Минимальное допустимое значение
        max_val: Максимальное допустимое значение
        field_name: Название поля для сообщения об ошибке
        
    Returns:
        tuple[bool, str | None]: (валидно, сообщение об ошибке)
    """
    if value < min_val:
        return False, f"{field_name} должно быть не меньше {min_val}."
    
    if value > max_val:
        return False, f"{field_name} должно быть не больше {max_val}."
    
    return True, None

# =========================================================
# СОЗДАНИЕ КЛАВИАТУР С КНОПКАМИ
# =========================================================
async def get_referral_link(bot: Bot, telegram_id: int) -> str:
    """Генерирует реферальную ссылку для пользователя."""
    me = await bot.get_me()
    bot_username = me.username
    return f"https://t.me/{bot_username}?start={telegram_id}"

async def get_admin_contact_info(bot: Bot, admin_id: int) -> dict:
    """Получает информацию об админе для отправки контакта."""
    try:
        chat = await bot.get_chat(admin_id)
        return {
            "user_id": admin_id,
            "username": chat.username,
            "first_name": chat.first_name,
            "last_name": chat.last_name,
            "has_username": chat.username is not None
        }
    except Exception as e:
        print(f"Ошибка при получении информации об админе: {e}")
        return None

def get_user_keyboard() -> ReplyKeyboardMarkup:
    """Создает клавиатуру для обычных пользователей."""
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="📊 Моя статистика"),
                KeyboardButton(text="📦 Мои заказы"),
            ],
            [
                KeyboardButton(text="👥 Пригласить друга"),
                KeyboardButton(text="❓ Помощь"),
            ],
            [
                KeyboardButton(text="💸 Вывести бонусы"),
            ],
            [
                KeyboardButton(text="🚪 Выйти из программы"),
            ],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder="Выберите команду или введите Ozon ID"
    )
    return keyboard

def get_admin_keyboard() -> ReplyKeyboardMarkup:
    """Создает клавиатуру для администраторов."""
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="📊 Моя статистика"),
                KeyboardButton(text="📦 Мои заказы"),
            ],
            [
                KeyboardButton(text="👥 Управление"),
                KeyboardButton(text="📈 Аналитика"),
            ],
            [
                KeyboardButton(text="⚙️ Настройки"),
                KeyboardButton(text="👥 Пригласить друга"),
            ],
            [
                KeyboardButton(text="💸 Вывести бонусы"),
            ],
            [
                KeyboardButton(text="❓ Помощь"),
            ],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder="Выберите команду или введите Ozon ID"
    )
    return keyboard

def get_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    """Возвращает клавиатуру в зависимости от роли пользователя."""
    if is_admin(user_id):
        return get_admin_keyboard()
    else:
        return get_user_keyboard()

# =========================================================
# 1. ОБРАБОТЧИК КОМАНДЫ /START (Начало регистрации)
# =========================================================
@dp.message(CommandStart())
async def start_handler(message: types.Message, state: FSMContext):
    user = message.from_user
    tg_id = user.id
    username = user.username
    first_name = user.first_name

    # парсим реферальный код, если он есть в /start <код>
    parts = message.text.split(maxsplit=1)
    referrer_telegram_id = None
    referrer_ozon_id = None
    
    if len(parts) == 2:
        referrer_telegram_id_str = parts[1].strip()
        # Проверяем, что код выглядит как Telegram ID (число)
        if referrer_telegram_id_str.isdigit():
            referrer_telegram_id = int(referrer_telegram_id_str)
            # Ищем участника по Telegram ID, чтобы получить его Ozon ID
            referrer_participant = await asyncio.to_thread(
                find_participant_by_telegram_id, referrer_telegram_id
            )
            if referrer_participant:
                referrer_ozon_id = referrer_participant.get("Ozon ID")
                print(f"✅ Реферер найден при /start: Telegram ID={referrer_telegram_id}, Ozon ID={referrer_ozon_id}")
            else:
                print(f"⚠️ Реферер не найден при /start: Telegram ID={referrer_telegram_id} (будет попытка найти при регистрации)")

    # пробуем найти участника по Telegram ID
    # ИСПРАВЛЕНО: Оборачиваем синхронную функцию Sheets в asyncio.to_thread
    participant = await asyncio.to_thread(find_participant_by_telegram_id, tg_id) 

    if participant:
        # уже есть в системе
        text = (
            f"Привет, {first_name or username or 'друг'}! 👋\n\n"
            "Ты уже зарегистрирован в реферальной программе.\n"
            "Используй кнопки ниже для навигации."
        )
        await state.clear()
        await message.answer(text, reply_markup=get_keyboard(tg_id))
        return

    # нового участника ещё нет — начинаем регистрацию
    # Сохраняем Ozon ID реферера (если найден) и Telegram ID (для повторной попытки поиска)
    await state.update_data(
        referrer_id=referrer_ozon_id,
        referrer_telegram_id=referrer_telegram_id
    )

    text = (
        f"Привет, {first_name or username or 'друг'}! 👋\n\n"
        "Чтобы зарегистрировать тебя в реферальной программе,\n"
        "мне нужен твой <b>Ozon ID</b>.\n\n"
        "📝 <b>Что это?</b> Первые цифры номера любого твоего заказа до тире.\n"
        "Можешь отправить полный номер заказа — я сам выделю нужные цифры.\n\n"
        "Если нужна помощь — нажми кнопку '❓ Помощь'."
    )
    await message.answer(text, parse_mode="HTML", reply_markup=get_keyboard(tg_id))
    await state.set_state(Registration.waiting_for_ozon_id)

# =========================================================
# 2. ОБРАБОТЧИК КОМАНДЫ /TEST_DB (Проверка подключения к БД)
# =========================================================
@dp.message(Command("test_db"))
async def test_db(message: types.Message):
    try:
        # Проверяем подключение к базе данных
        await asyncio.to_thread(create_database)
        
        # Пробуем найти участника (тестовый запрос)
        test_result = await asyncio.to_thread(find_participant_by_telegram_id, 0)

        await message.answer(
            f"Подключение к базе данных работает! 🎉\n"
            f"База данных готова к использованию.",
            parse_mode="HTML",
            reply_markup=get_keyboard(message.from_user.id)
        )
    except Exception as e:
        await message.answer(
            f"Ошибка при подключении к базе данных ❌\n<code>{e}</code>",
            parse_mode="HTML",
            reply_markup=get_keyboard(message.from_user.id)
        )

# =========================================================
# 3. ОБРАБОТЧИК КОМАНДЫ /SYNC_ORDERS
# =========================================================
@dp.message(Command("sync_orders"))
async def sync_orders_handler(message: types.Message):
    """Обновляет лист 'Заказы', вызывая функцию обновления."""
    
    # Проверка прав администратора
    if not is_admin(message.from_user.id):
        await message.answer(
            "❌ У тебя нет прав для выполнения этой команды.",
            reply_markup=get_keyboard(message.from_user.id)
        )
        return
    
    try:
        results = await asyncio.to_thread(update_orders_sheet)
        
        # Проверка структуры результата
        if not isinstance(results, dict):
            await message.answer(
                "❌ Ошибка: неверный формат результата синхронизации.",
                reply_markup=get_keyboard(message.from_user.id)
            )
            return
        
        # Формируем сообщение для каждого кабинета
        messages = []
        for cabinet_name, result in results.items():
            if not isinstance(result, dict):
                continue
            
            cabinet_name_display = result.get("cabinet_name", cabinet_name)
            client_id = result.get("client_id", "не указан")
            
            period_start = result.get("period_start")
            period_end = result.get("period_end")
            
            if period_start is None or period_end is None:
                period_start_str = "не указано"
                period_end_str = "не указано"
            else:
                period_start_str = period_start.strftime("%d.%m.%Y %H:%M")
                period_end_str = period_end.strftime("%d.%m.%Y %H:%M")
            
            # Получаем статистику по статусам за первый день периода
            first_day_stats = result.get("first_day_stats", {})
            
            # Формируем строку со статистикой по статусам
            status_stats_text = ""
            if first_day_stats and first_day_stats.get("total", 0) > 0:
                first_day_date = period_start_str.split()[0] if period_start_str != "не указано" else ""
                if first_day_date:
                    status_stats_text = f"\n\n📊 <b>Статистика за {first_day_date}:</b>\n"
                    status_stats_text += f"Всего отправлений: <b>{first_day_stats['total']}</b>\n"
                    
                    statuses = first_day_stats.get("statuses", {})
                    if statuses:
                        # Сортируем по количеству (от большего к меньшему)
                        sorted_statuses = sorted(statuses.items(), key=lambda x: x[1], reverse=True)
                        for status, count in sorted_statuses:
                            percentage = (count / first_day_stats['total']) * 100
                            status_name = {
                                "delivered": "✅ Доставлено",
                                "delivering": "🚚 В доставке",
                                "awaiting_packaging": "📦 Ожидает упаковки",
                                "awaiting_deliver": "⏳ Ожидает доставки",
                                "cancelled": "❌ Отменено"
                            }.get(status, status)
                            status_stats_text += f"{status_name}: <b>{count}</b> ({percentage:.1f}%)\n"
                    
                    if first_day_stats.get("active_count", 0) > 0:
                        status_stats_text += f"\n⚠️ Активных отправлений: <b>{first_day_stats['active_count']}</b>"
            
            if result.get("error"):
                # Экранируем HTML в сообщении об ошибке
                error_msg = str(result.get('error', 'Неизвестная ошибка'))
                # Ограничиваем длину и экранируем специальные символы
                if len(error_msg) > 300:
                    error_msg = error_msg[:300] + "..."
                # Заменяем HTML-специальные символы
                error_msg = error_msg.replace('<', '&lt;').replace('>', '&gt;')
                
                text = (
                    f"🤖 <b>Синхронизация кабинета \"{cabinet_name_display}\"</b>\n\n"
                    f"❌ Ошибка синхронизации\n"
                    f"⚠️ Ошибка: <code>{error_msg}</code>\n"
                    f"💡 Проверьте подключение к API Ozon и наличие таблиц в БД\n\n"
                    f"⏰ Время попытки: {period_end_str}"
                )
            elif result.get("count", 0) > 0:
                text = (
                    f"🎉 Синхронизация завершена! 🎉\n\n"
                    f"📊 Кабинет: {cabinet_name_display} (Client ID: {client_id})\n\n"
                    f"✅ Добавлено <b>{result['count']}</b> новых отправлений\n"
                    f"👥 Обработано <b>{result['customers_count']}</b> клиентов "
                    f"(новых: <b>{result['new_customers_count']}</b>)\n"
                    f"🎯 Участников программы совершивших покупку: <b>{result.get('participants_with_orders_count', 0)}</b>\n\n"
                    f"📅 <b>Период синхронизации:</b>\n"
                    f"С: {period_start_str}\n"
                    f"По: {period_end_str}"
                    f"{status_stats_text}"
                )
            else:
                text = (
                    f"✅ Синхронизация завершена!\n\n"
                    f"📊 Кабинет: {cabinet_name_display} (Client ID: {client_id})\n\n"
                    f"Новых отправлений не найдено.\n\n"
                    f"📅 <b>Период проверки:</b>\n"
                    f"С: {period_start_str}\n"
                    f"По: {period_end_str}"
                    f"{status_stats_text}"
                )
            
            messages.append(text)
        
        # Отправляем все сообщения
        if messages:
            for msg in messages:
                await message.answer(msg, parse_mode="HTML", reply_markup=get_keyboard(message.from_user.id))
        else:
            await message.answer(
                "❌ Не удалось синхронизировать ни один кабинет.",
                reply_markup=get_keyboard(message.from_user.id)
            )
        
    except Exception as e:
        await message.answer(
            f"❌ Ошибка при синхронизации заказов ❌\n"
            f"<code>{e}</code>",
            parse_mode="HTML",
            reply_markup=get_keyboard(message.from_user.id)
        )

# =========================================================
# ОБРАБОТЧИКИ КНОПОК КЛАВИАТУРЫ
# =========================================================
@dp.message(lambda message: message.text == "📊 Моя статистика")
async def my_stats_handler(message: types.Message):
    """Обработчик кнопки 'Моя статистика'."""
    
    user = message.from_user
    participant = await asyncio.to_thread(find_participant_by_telegram_id, user.id)
    
    if not participant:
        await message.answer(
            "❌ Ты еще не зарегистрирован в программе.\n"
            "Используй команду /start для регистрации.",
            reply_markup=get_keyboard(user.id)
        )
        return
    
    ozon_id = participant.get('Ozon ID')
    if not ozon_id:
        await message.answer(
            "❌ Ошибка: Ozon ID не найден.",
            reply_markup=get_keyboard(user.id)
        )
        return
    
    # Форматируем дату регистрации
    reg_date = participant.get('Дата регистрации', 'Не указана')
    if reg_date and reg_date != 'Не указана':
        try:
            # Преобразуем YYYY-MM-DD в DD.MM.YYYY
            from datetime import datetime
            dt = datetime.strptime(reg_date, "%Y-%m-%d")
            reg_date = dt.strftime("%d.%m.%Y")
        except:
            pass
    
    try:
        # Получаем статистику
        user_stats = await asyncio.to_thread(get_user_orders_stats, ozon_id)
        
        referrals_by_level = await asyncio.to_thread(get_referrals_by_level, ozon_id, max_level=3)
        
        # Функция для форматирования чисел с пробелами
        def format_number(num):
            try:
                return f"{int(num):,}".replace(',', ' ')
            except (ValueError, TypeError) as e:
                return "0"
        
        # Получаем бонусы пользователя
        user_bonuses = await asyncio.to_thread(get_user_bonuses, ozon_id)
        
        # Получаем доступные к выводу бонусы
        available_bonuses = await asyncio.to_thread(get_available_bonuses_for_withdrawal, ozon_id)
        
        # Формируем текст
        text = (
            f"📊 Моя статистика\n\n"
            f"👤 Информация:\n"
            f"• Ozon ID: {ozon_id}\n"
            f"• Дата регистрации: {reg_date}\n\n"
            f"📦 Мои товары:\n"
            f"• Всего доставлено товаров: {user_stats['delivered_count']}\n"
            f"• Общая сумма: {format_number(user_stats['total_sum'])} ₽\n"
            f"• Начислено бонусов: {format_number(user_bonuses)} ₽\n"
            f"• Доступно к выводу: {format_number(available_bonuses)} ₽\n\n"
            f"👥 Реферальная программа:\n\n"
        )
        
        # Статистика по уровням
        total_referrals = 0
        total_referral_orders = 0
        total_referral_sum = 0.0
        total_bonuses = 0.0
        
        # Получаем максимальное количество уровней из настроек
        from db_manager import get_bonus_settings
        settings = await asyncio.to_thread(get_bonus_settings)
        max_levels = settings.max_levels if settings else 3
        
        for level in range(1, max_levels + 1):
            referral_ids = referrals_by_level.get(level, [])
            
            level_name = {
                1: "Уровень 1 (прямые друзья)",
                2: "Уровень 2 (друзья друзей)",
                3: "Уровень 3 (друзья друзей друзей)"
            }.get(level, f"Уровень {level}")
            
            if referral_ids:
                referrals_stats = await asyncio.to_thread(get_referrals_orders_stats, referral_ids)
                referrals_bonuses = await asyncio.to_thread(get_referrals_bonuses_stats, referral_ids, level)
                
                total_referrals += len(referral_ids)
                total_referral_orders += referrals_stats['orders_count']
                total_referral_sum += referrals_stats['total_sum']
                total_bonuses += referrals_bonuses
                
                text += (
                    f"{level_name}:\n"
                    f"• Участников: {len(referral_ids)}\n"
                    f"• Кол-во товаров: {referrals_stats['orders_count']}\n"
                    f"• Их сумма: {format_number(referrals_stats['total_sum'])} ₽\n"
                    f"• Начислено бонусов: {format_number(referrals_bonuses)} ₽\n\n"
                )
            else:
                text += (
                    f"{level_name}:\n"
                    f"• Участников: 0\n"
                    f"• Кол-во товаров: 0\n"
                    f"• Их сумма: 0 ₽\n"
                    f"• Начислено бонусов: 0 ₽\n\n"
                )
        
        text += f"Всего бонусов от программы: {format_number(total_bonuses)} ₽"
        
        await message.answer(text, reply_markup=get_keyboard(user.id))
    except Exception as e:
        await message.answer(
            f"❌ Произошла ошибка при получении статистики: {str(e)}",
            reply_markup=get_keyboard(user.id)
        )

@dp.message(lambda message: message.text == "📦 Мои заказы")
async def my_orders_handler(message: types.Message):
    """Обработчик кнопки 'Мои заказы'."""
    user = message.from_user
    participant = await asyncio.to_thread(find_participant_by_telegram_id, user.id)
    
    if not participant:
        await message.answer(
            "❌ Ты еще не зарегистрирован в программе.\n"
            "Используй команду /start для регистрации.",
            reply_markup=get_keyboard(user.id)
        )
        return
    
    ozon_id = participant.get('Ozon ID')
    if not ozon_id:
        await message.answer(
            "❌ Ошибка: Ozon ID не найден.",
            reply_markup=get_keyboard(user.id)
        )
        return
    
    try:
        # Получаем сводку по заказам
        summary = await asyncio.to_thread(get_user_orders_summary, ozon_id)
        
        # Функция для форматирования чисел с пробелами
        def format_number(num):
            try:
                return f"{int(num):,}".replace(',', ' ')
            except (ValueError, TypeError):
                return "0"
        
        def format_float(num):
            try:
                return f"{float(num):,.2f}".replace(',', ' ').replace('.', ',')
            except (ValueError, TypeError):
                return "0,00"
        
        # Форматируем дату регистрации
        reg_date = summary.get("registration_date")
        if reg_date:
            try:
                from datetime import datetime
                dt = datetime.strptime(reg_date, "%Y-%m-%d")
                reg_date_str = dt.strftime("%d.%m.%Y")
            except:
                reg_date_str = reg_date
        else:
            reg_date_str = "не указана"
        
        total_orders = summary.get("total_orders", 0)
        total_sum = summary.get("total_sum", 0.0)
        by_status = summary.get("by_status", {})
        
        if total_orders == 0:
            text = (
                f"📦 <b>Твои заказы</b>\n\n"
                f"Ozon ID: <code>{ozon_id}</code>\n"
                f"Дата регистрации: {reg_date_str}\n\n"
                f"У тебя пока нет заказов с даты регистрации в программе."
            )
        else:
            text = (
                f"📦 <b>Твои заказы</b>\n\n"
                f"Ozon ID: <code>{ozon_id}</code>\n"
                f"Дата регистрации: {reg_date_str}\n\n"
                f"📊 <b>Общая статистика:</b>\n"
                f"• Всего товаров: <b>{total_orders}</b>\n"
                f"• Общая сумма: <b>{format_float(total_sum)}</b> ₽\n\n"
            )
            
            # Словарь для перевода статусов на русский
            status_names = {
                "delivered": "✅ Доставлено",
                "delivering": "🚚 В доставке",
                "awaiting_packaging": "📦 Ожидает упаковки",
                "awaiting_deliver": "⏳ Ожидает доставки",
                "cancelled": "❌ Отменено",
                "unknown": "❓ Неизвестный статус"
            }
            
            # Показываем разбивку по статусам
            if by_status:
                text += f"📋 <b>По статусам:</b>\n"
                
                # Сортируем статусы по количеству товаров (от большего к меньшему)
                sorted_statuses = sorted(
                    by_status.items(),
                    key=lambda x: x[1]["count"],
                    reverse=True
                )
                
                for status, data in sorted_statuses:
                    status_name = status_names.get(status, f"❓ {status}")
                    count = data.get("count", 0)
                    sum_amount = data.get("sum", 0.0)
                    text += f"• {status_name}: <b>{count}</b> заказ"
                    
                    # Правильное склонение слова "заказ"
                    if count == 1:
                        text += f" — {format_float(sum_amount)} ₽\n"
                    elif count < 5:
                        text += f"а — {format_float(sum_amount)} ₽\n"
                    else:
                        text += f"ов — {format_float(sum_amount)} ₽\n"
        
        await message.answer(text, parse_mode="HTML", reply_markup=get_keyboard(user.id))
    except Exception as e:
        await message.answer(
            f"❌ Произошла ошибка при получении информации о заказах: {str(e)}",
            reply_markup=get_keyboard(user.id)
        )

@dp.message(lambda message: message.text == "👥 Пригласить друга")
async def invite_friend_handler(message: types.Message):
    """Обработчик кнопки 'Пригласить друга'."""
    user = message.from_user
    participant = await asyncio.to_thread(find_participant_by_telegram_id, user.id)
    
    if not participant:
        await message.answer(
            "❌ Ты еще не зарегистрирован в программе.\n\n"
            "Сначала зарегистрируйся через команду /start, чтобы получить реферальную ссылку.",
            reply_markup=get_keyboard(user.id)
        )
        return
    
    # Генерируем реферальную ссылку
    referral_link = await get_referral_link(bot, user.id)
    
    # Первое сообщение - для пересылки другу
    invite_text = (
        f"Привет! 👋\n\n"
        f"Приглашаю тебя присоединиться к нашей реферальной программе! 🎉\n\n"
        f"Переходи по ссылке и регистрируйся:\n"
        f"{referral_link}\n\n"
        f"Это займет всего минуту, а потом ты сможешь получать бонусы за покупки! 💰"
    )
    
    # Второе сообщение - инструкция
    instruction_text = (
        f"Перешли это сообщение своему другу или просто отправь ему ссылку выше.\n\n"
        f"Когда он зарегистрируется по твоей ссылке, ты автоматически станешь его реферером! 🎯"
    )
    
    # Отправляем два сообщения
    await message.answer(invite_text, reply_markup=get_keyboard(user.id))
    await message.answer(instruction_text, reply_markup=get_keyboard(user.id))

@dp.message(lambda message: message.text == "💸 Вывести бонусы")
async def withdrawal_bonuses_handler(message: types.Message, state: FSMContext):
    """Обработчик кнопки 'Вывести бонусы'."""
    user = message.from_user
    participant = await asyncio.to_thread(find_participant_by_telegram_id, user.id)
    
    if not participant:
        await message.answer(
            "❌ Ты еще не зарегистрирован в программе.\n\n"
            "Сначала зарегистрируйся через команду /start.",
            reply_markup=get_keyboard(user.id)
        )
        return
    
    ozon_id = participant.get('Ozon ID')
    if not ozon_id:
        await message.answer(
            "❌ Ошибка: Ozon ID не найден.",
            reply_markup=get_keyboard(user.id)
        )
        return
    
    # Проверка активной заявки
    has_active = await asyncio.to_thread(has_active_withdrawal_request, ozon_id)
    if has_active:
        active_request = await asyncio.to_thread(get_active_withdrawal_request, ozon_id)
        if active_request:
            status_text = {
                "processing": "Обрабатывается",
                "approved": "Одобрена"
            }.get(active_request.get("status"), active_request.get("status"))
            
            text = (
                f"💸 <b>Вывод бонусов</b>\n\n"
                f"❌ У тебя уже есть активная заявка на вывод.\n\n"
                f"Сумма: {active_request.get('amount', 0):,.2f} ₽\n"
                f"Статус: {status_text}\n"
                f"Дата создания: {active_request.get('created_at').strftime('%d.%m.%Y %H:%M') if active_request.get('created_at') else 'Не указана'}\n\n"
                f"Дождись обработки текущей заявки перед созданием новой."
            )
            await message.answer(text, parse_mode="HTML", reply_markup=get_keyboard(user.id))
            return
    
    # Получаем настройки и баланс
    settings = await asyncio.to_thread(get_withdrawal_settings)
    available_balance = await asyncio.to_thread(get_user_available_balance, ozon_id)
    
    # Функция для форматирования чисел
    def format_number(num):
        try:
            return f"{float(num):,.2f}".replace(',', ' ').replace('.', ',')
        except (ValueError, TypeError):
            return "0,00"
    
    text = (
        f"💸 <b>Вывод бонусов</b>\n\n"
        f"💰 Доступный баланс: <b>{format_number(available_balance)}</b> ₽\n"
        f"📊 Минимальная сумма вывода: <b>{format_number(settings.min_withdrawal_amount)}</b> ₽\n\n"
        f"Введи сумму, которую хочешь вывести:"
    )
    
    await message.answer(text, parse_mode="HTML", reply_markup=get_keyboard(user.id))
    await state.set_state(Withdrawal.entering_amount)

# Список всех кнопок для исключения из обработки состояния
WITHDRAWAL_BUTTON_TEXTS = [
    "📊 Моя статистика", "📦 Мои заказы", "👥 Пригласить друга", 
    "❓ Помощь", "💸 Вывести бонусы", "🚪 Выйти из программы",
    "👥 Управление", "📈 Аналитика", "⚙️ Настройки"
]

@dp.message(Withdrawal.entering_amount, F.text.in_(WITHDRAWAL_BUTTON_TEXTS))
async def process_withdrawal_button_in_state(message: types.Message, state: FSMContext):
    """Обработчик кнопок в состоянии ввода суммы вывода - очищает состояние и обрабатывает кнопку."""
    await state.clear()
    
    # Вызываем обработчик кнопки через диспетчер
    from aiogram.types import Update
    
    new_update = Update(update_id=message.message_id, message=message)
    
    try:
        await dp.feed_update(bot, new_update)
    except Exception:
        # Если feed_update не работает, состояние уже очищено
        # Пользователю нужно будет нажать кнопку еще раз
        pass

@dp.message(Withdrawal.entering_amount, ~F.text.in_(WITHDRAWAL_BUTTON_TEXTS))
async def process_withdrawal_amount(message: types.Message, state: FSMContext):
    """Обработчик ввода суммы вывода (не обрабатывает кнопки)."""
    user = message.from_user
    participant = await asyncio.to_thread(find_participant_by_telegram_id, user.id)
    
    if not participant:
        await state.clear()
        await message.answer(
            "❌ Ты еще не зарегистрирован в программе.",
            reply_markup=get_keyboard(user.id)
        )
        return
    
    ozon_id = participant.get('Ozon ID')
    if not ozon_id:
        await state.clear()
        await message.answer(
            "❌ Ошибка: Ozon ID не найден.",
            reply_markup=get_keyboard(user.id)
        )
        return
    
    # Парсим сумму
    try:
        # Убираем пробелы и заменяем запятую на точку
        amount_str = message.text.strip().replace(' ', '').replace(',', '.')
        amount = float(amount_str)
    except ValueError:
        settings = await asyncio.to_thread(get_withdrawal_settings)
        await message.answer(
            f"❌ Неверный формат суммы. Введи число (например: 1000 или 1000.50).\n\n"
            f"Минимальная сумма вывода: {settings.min_withdrawal_amount:,.2f} ₽",
            reply_markup=get_keyboard(user.id)
        )
        return
    
    # Получаем настройки и баланс
    settings = await asyncio.to_thread(get_withdrawal_settings)
    available_balance = await asyncio.to_thread(get_user_available_balance, ozon_id)
    
    # Валидация суммы - проверка диапазона
    max_allowed = min(available_balance, MAX_WITHDRAWAL_AMOUNT)
    is_valid, error_msg = validate_numeric_range(amount, MIN_WITHDRAWAL_AMOUNT, max_allowed, "Сумма вывода")
    if not is_valid:
        await message.answer(
            f"❌ {error_msg}\n\n"
            f"Доступный баланс: <b>{available_balance:,.2f}</b> ₽\n"
            f"Минимальная сумма: <b>{settings.min_withdrawal_amount:,.2f}</b> ₽\n\n"
            f"Попробуй еще раз:",
            parse_mode="HTML",
            reply_markup=get_keyboard(user.id)
        )
        return
    
    # Дополнительная проверка минимальной суммы из настроек
    if amount < settings.min_withdrawal_amount:
        await message.answer(
            f"❌ Минимальная сумма вывода: <b>{settings.min_withdrawal_amount:,.2f}</b> ₽\n\n"
            f"Попробуй еще раз:",
            parse_mode="HTML",
            reply_markup=get_keyboard(user.id)
        )
        return
    
    if amount > available_balance:
        await message.answer(
            f"❌ Недостаточно средств.\n\n"
            f"Доступный баланс: <b>{available_balance:,.2f}</b> ₽\n\n"
            f"Попробуй еще раз:",
            parse_mode="HTML",
            reply_markup=get_keyboard(user.id)
        )
        return
    
    # Сохраняем сумму в состоянии
    await state.update_data(amount=amount, ozon_id=ozon_id)
    
    # Переходим к подтверждению
    def format_number(num):
        try:
            return f"{float(num):,.2f}".replace(',', ' ').replace('.', ',')
        except (ValueError, TypeError):
            return "0,00"
    
    remaining_balance = available_balance - amount
    
    text = (
        f"💸 <b>Подтверждение заявки на вывод</b>\n\n"
        f"Сумма вывода: <b>{format_number(amount)}</b> ₽\n"
        f"Доступный баланс: {format_number(available_balance)} ₽\n"
        f"После вывода останется: <b>{format_number(remaining_balance)}</b> ₽\n\n"
        f"После подтверждения администратор свяжется с тобой для уточнения способа выплаты."
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Подтвердить", callback_data="withdrawal_confirm"),
            InlineKeyboardButton(text="❌ Отменить", callback_data="withdrawal_cancel"),
        ]
    ])
    
    await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
    await state.set_state(Withdrawal.confirming)

@dp.callback_query(lambda c: c.data == "withdrawal_confirm")
async def withdrawal_confirm_handler(callback: types.CallbackQuery, state: FSMContext):
    """Обработчик подтверждения заявки на вывод."""
    await callback.answer()
    
    user = callback.from_user
    data = await state.get_data()
    amount = data.get("amount")
    ozon_id = data.get("ozon_id")
    
    if not amount or not ozon_id:
        await callback.message.edit_text(
            "❌ Ошибка: данные не найдены. Попробуй создать заявку заново.",
            reply_markup=None
        )
        await state.clear()
        return
    
    try:
        # Создаем заявку
        request = await asyncio.to_thread(
            create_withdrawal_request,
            ozon_id,
            str(user.id),
            amount
        )
        
        # Уведомление пользователю
        def format_number(num):
            try:
                return f"{float(num):,.2f}".replace(',', ' ').replace('.', ',')
            except (ValueError, TypeError):
                return "0,00"
        
        text = (
            f"✅ <b>Заявка на вывод создана!</b>\n\n"
            f"Сумма: <b>{format_number(amount)}</b> ₽\n"
            f"Статус: Обрабатывается\n\n"
            f"Администратор свяжется с тобой в ближайшее время для уточнения способа выплаты."
        )
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=None)
        await state.clear()
        
        # Уведомление первому админу
        if ADMIN_IDS:
            admin_id = ADMIN_IDS[0]
            participant = await asyncio.to_thread(find_participant_by_telegram_id, user.id)
            user_name = participant.get('Имя / ник', '') if participant else user.first_name or 'Пользователь'
            user_username = participant.get('Телеграм @', '') if participant else (f"@{user.username}" if user.username else "")
            
            admin_text = (
                f"💸 <b>Новая заявка на вывод бонусов</b>\n\n"
                f"👤 Пользователь: {user_name} {user_username}\n"
                f"🆔 Ozon ID: {ozon_id}\n"
                f"💰 Сумма: <b>{format_number(amount)}</b> ₽\n"
                f"📅 Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
                f"Свяжись с пользователем для уточнения способа выплаты."
            )
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📋 Просмотреть заявки", callback_data="admin_withdrawals_list")]
            ])
            
            try:
                await bot.send_message(admin_id, admin_text, parse_mode="HTML", reply_markup=keyboard)
            except Exception as e:
                print(f"⚠️ Не удалось отправить уведомление админу: {e}")
        
    except ValueError as e:
        # Ошибка валидации
        await callback.message.edit_text(
            f"❌ {str(e)}\n\nПопробуй создать заявку заново.",
            reply_markup=None
        )
        await state.clear()
    except Exception as e:
        await callback.message.edit_text(
            f"❌ Произошла ошибка при создании заявки: {str(e)}\n\nПопробуй позже.",
            reply_markup=None
        )
        await state.clear()

@dp.callback_query(lambda c: c.data == "withdrawal_cancel")
async def withdrawal_cancel_handler(callback: types.CallbackQuery, state: FSMContext):
    """Обработчик отмены создания заявки на вывод."""
    await callback.answer("Отменено")
    
    text = (
        "❌ <b>Создание заявки отменено</b>\n\n"
        "Ты можешь создать новую заявку в любое время."
    )
    
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=None)
    await state.clear()

@dp.message(lambda message: message.text == "❓ Помощь")
async def help_handler(message: types.Message):
    """Обработчик кнопки 'Помощь' - показывает главное меню помощи."""
    await show_help_main_menu(message)

@dp.message(lambda message: message.text == "💬 Чат с админом")
async def chat_with_admin_handler(message: types.Message):
    """Обработчик кнопки 'Чат с админом'."""
    user = message.from_user
    participant = await asyncio.to_thread(find_participant_by_telegram_id, user.id)
    
    if not participant:
        await message.answer(
            "❌ Ты еще не зарегистрирован в программе.\n\n"
            "Сначала зарегистрируйся через команду /start.",
            reply_markup=get_keyboard(user.id)
        )
        return
    
    # Получаем информацию о первом админе
    if not ADMIN_IDS:
        await message.answer(
            "❌ Администратор временно недоступен. Попробуй позже.",
            reply_markup=get_keyboard(user.id)
        )
        return
    
    admin_id = ADMIN_IDS[0]  # Берем первого админа
    admin_info = await get_admin_contact_info(message.bot, admin_id)
    
    if not admin_info:
        await message.answer(
            "❌ Не удалось получить контакт администратора. Попробуй позже.",
            reply_markup=get_keyboard(user.id)
        )
        return
    
    # Если у админа есть username
    if admin_info["has_username"]:
        username = admin_info["username"]
        text = (
            f"💬 <b>Чат с администратором</b>\n\n"
            f"Нажми на кнопку ниже, чтобы написать администратору напрямую:\n\n"
            f"Или напиши ему в Telegram: @{username}"
        )
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="💬 Написать администратору",
                url=f"https://t.me/{username}"
            )]
        ])
        
        await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
    
    # Если username нет - отправляем инструкцию
    else:
        admin_name = admin_info["first_name"] or "Администратор"
        if admin_info.get("last_name"):
            admin_name += f" {admin_info['last_name']}"
        
        text = (
            f"💬 <b>Чат с администратором</b>\n\n"
            f"Администратор: <b>{admin_name}</b>\n\n"
            f"Чтобы связаться с администратором:\n"
            f"1. Открой Telegram\n"
            f"2. Найди пользователя по имени: <b>{admin_name}</b>\n"
            f"3. Напиши ему напрямую\n\n"
            f"Или попроси администратора добавить username в настройках Telegram для более удобной связи."
        )
        await message.answer(text, parse_mode="HTML")
    
    # Инструкция
    instruction_text = (
        f"\n\n💡 <b>Как это работает:</b>\n"
        f"• Нажми на кнопку выше или напиши администратору напрямую\n"
        f"• Вся переписка будет в вашем личном чате в Telegram\n"
        f"• Администратор ответит в ближайшее время"
    )
    await message.answer(instruction_text, parse_mode="HTML", reply_markup=get_keyboard(user.id))
    
    # Уведомляем админа о новом запросе
    await notify_admin_about_chat_request(admin_id, user, participant)

async def show_help_main_menu(message_or_callback):
    """Показывает главное меню помощи с подразделами."""
    text = (
        "❓ <b>Помощь</b>\n\n"
        "Выбери интересующий раздел:"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="ℹ️ Общая информация", callback_data="help_general_info"),
        ],
        [
            InlineKeyboardButton(text="📝 Как найти Ozon ID", callback_data="help_find_ozon_id"),
        ],
        [
            InlineKeyboardButton(text="💰 Бонусные ставки", callback_data="help_bonus_rates"),
        ],
        [
            InlineKeyboardButton(text="💬 Чат с админом", callback_data="help_chat_with_admin"),
        ],
    ])
    
    if isinstance(message_or_callback, types.Message):
        await message_or_callback.answer(text, parse_mode="HTML", reply_markup=keyboard)
    else:
        await message_or_callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
        await message_or_callback.answer()

@dp.message(lambda message: message.text == "🚪 Выйти из программы")
async def leave_program_handler(message: types.Message, state: FSMContext):
    """Обработчик кнопки 'Выйти из программы'."""
    user = message.from_user
    participant = await asyncio.to_thread(find_participant_by_telegram_id, user.id)
    
    if not participant:
        await message.answer(
            "❌ Ты еще не зарегистрирован в программе.\n\n"
            "Сначала зарегистрируйся через команду /start.",
            reply_markup=get_keyboard(user.id)
        )
        return
    
    # Получаем количество рефералов
    ozon_id = participant.get("Ozon ID")
    referrals_by_level = await asyncio.to_thread(get_referrals_by_level, ozon_id, max_level=3)
    
    # Подсчитываем общее количество рефералов
    # referrals_by_level имеет структуру: {1: [ozon_id, ...], 2: [ozon_id, ...], ...}
    total_referrals = 0
    for level_data in referrals_by_level.values():
        # level_data - это список ozon_id, а не словарь
        if isinstance(level_data, list):
            total_referrals += len(level_data)
        elif isinstance(level_data, dict):
            # На случай, если структура изменится в будущем
            total_referrals += len(level_data.get("referrals", []))
    
    # Формируем сообщение с предупреждением
    referrals_text = ""
    if total_referrals > 0:
        referrals_text = f"\n\n⚠️ <b>Внимание!</b> У тебя есть <b>{total_referrals}</b> реферал"
        if total_referrals == 1:
            referrals_text += ". "
        elif total_referrals < 5:
            referrals_text += "а. "
        else:
            referrals_text += "ов. "
        referrals_text += "При выходе из программы ты потеряешь всех своих рефералов, и они потеряют связь с тобой."
    else:
        referrals_text = "\n\n⚠️ <b>Внимание!</b> Это действие необратимо."
    
    text = (
        f"🚪 <b>Выход из программы</b>\n\n"
        f"Ты действительно хочешь выйти из бонусной программы?{referrals_text}\n\n"
        f"После выхода:\n"
        f"• Твой аккаунт будет удален из программы\n"
        f"• Ты потеряешь всех своих рефералов\n"
        f"• Ты сможешь заново зарегистрироваться через /start"
    )
    
    # Создаем InlineKeyboard для подтверждения/отмены
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да, выйти", callback_data="leave_program_confirm"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="leave_program_cancel"),
        ]
    ])
    
    await state.set_state(LeavingProgram.confirming_leave)
    await message.answer(text, parse_mode="HTML", reply_markup=keyboard)

@dp.message(lambda message: message.text == "👥 Управление")
async def management_handler(message: types.Message):
    """Обработчик кнопки 'Управление' (только для админов)."""
    user_id = message.from_user.id
    
    if not is_admin(user_id):
        await message.answer(
            "❌ У тебя нет прав для выполнения этой команды.",
            reply_markup=get_keyboard(user_id)
        )
        return
    
    text = (
        "👥 <b>Управление пользователями</b>\n\n"
        "Функция управления пользователями будет доступна в ближайшее время.\n\n"
        "Здесь можно будет:\n"
        "• Просматривать список всех участников\n"
        "• Управлять правами доступа\n"
        "• Редактировать данные участников"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=get_keyboard(user_id))

@dp.callback_query(lambda c: c.data == "admin_withdrawals_list")
async def admin_withdrawals_list_handler(callback: types.CallbackQuery):
    """Обработчик просмотра списка заявок на вывод (для админов)."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ У тебя нет прав для выполнения этой команды.", show_alert=True)
        return
    
    await callback.answer()
    
    # Получаем список заявок
    requests = await asyncio.to_thread(get_pending_withdrawal_requests)
    
    if not requests:
        text = (
            "💸 <b>Заявки на вывод бонусов</b>\n\n"
            "✅ Нет заявок, ожидающих обработки."
        )
        await callback.message.edit_text(text, parse_mode="HTML")
        return
    
    # Формируем список заявок
    text = "💸 <b>Заявки на вывод бонусов</b>\n\n"
    
    def format_number(num):
        try:
            return f"{float(num):,.2f}".replace(',', ' ').replace('.', ',')
        except (ValueError, TypeError):
            return "0,00"
    
    keyboard_buttons = []
    for req in requests[:10]:  # Ограничиваем до 10 заявок
        user_display = req.get("user_name", "Пользователь")
        if req.get("user_username"):
            user_display += f" {req['user_username']}"
        
        text += (
            f"<b>Заявка #{req['id']}</b>\n"
            f"👤 {user_display}\n"
            f"🆔 Ozon ID: {req['user_ozon_id']}\n"
            f"💰 Сумма: {format_number(req['amount'])} ₽\n"
            f"📅 {req['created_at'].strftime('%d.%m.%Y %H:%M')}\n\n"
        )
        
        keyboard_buttons.append([
            InlineKeyboardButton(
                text=f"Заявка #{req['id']} - {format_number(req['amount'])} ₽",
                callback_data=f"admin_withdrawal_{req['id']}"
            )
        ])
    
    keyboard_buttons.append([InlineKeyboardButton(text="❌ Закрыть", callback_data="admin_withdrawals_close")])
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)

@dp.callback_query(lambda c: c.data == "admin_withdrawals_close")
async def admin_withdrawals_close_handler(callback: types.CallbackQuery):
    """Закрыть список заявок."""
    await callback.answer()
    await callback.message.delete()

@dp.callback_query(lambda c: c.data.startswith("admin_withdrawal_"))
async def admin_withdrawal_detail_handler(callback: types.CallbackQuery, state: FSMContext):
    """Обработчик просмотра деталей заявки на вывод."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ У тебя нет прав для выполнения этой команды.", show_alert=True)
        return
    
    await callback.answer()
    
    request_id = safe_extract_id(callback.data, "admin_withdrawal_")
    if request_id is None:
        await callback.message.edit_text(
            "❌ Ошибка: неверный формат данных.",
            reply_markup=None
        )
        return
    
    request = await asyncio.to_thread(get_withdrawal_request_by_id, request_id)
    
    if not request:
        await callback.message.edit_text(
            "❌ Заявка не найдена.",
            reply_markup=None
        )
        return
    
    def format_number(num):
        try:
            return f"{float(num):,.2f}".replace(',', ' ').replace('.', ',')
        except (ValueError, TypeError):
            return "0,00"
    
    user_display = request.get("user_name", "Пользователь")
    if request.get("user_username"):
        user_display += f" {request['user_username']}"
    
    status_text = {
        "processing": "Обрабатывается",
        "approved": "Одобрена",
        "rejected": "Отклонена",
        "completed": "Выполнена"
    }.get(request.get("status"), request.get("status"))
    
    text = (
        f"💸 <b>Заявка #{request_id}</b>\n\n"
        f"👤 Пользователь: {user_display}\n"
        f"📱 Telegram ID: {request['user_telegram_id']}\n"
        f"🆔 Ozon ID: {request['user_ozon_id']}\n"
        f"💰 Сумма: <b>{format_number(request['amount'])}</b> ₽\n"
        f"📊 Статус: {status_text}\n"
        f"📅 Дата: {request['created_at'].strftime('%d.%m.%Y %H:%M')}\n"
    )
    
    if request.get("admin_comment"):
        text += f"\n💬 Комментарий: {request['admin_comment']}"
    
    keyboard_buttons = []
    
    # Кнопки действий в зависимости от статуса
    if request.get("status") == "processing":
        keyboard_buttons.append([
            InlineKeyboardButton(text="✅ Одобрить", callback_data=f"admin_withdrawal_approve_{request_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_withdrawal_reject_{request_id}")
        ])
    elif request.get("status") == "approved":
        keyboard_buttons.append([
            InlineKeyboardButton(text="✅ Завершить выплату", callback_data=f"admin_withdrawal_complete_{request_id}")
        ])
    
    keyboard_buttons.append([
        InlineKeyboardButton(text="🔙 Назад к списку", callback_data="admin_withdrawals_list")
    ])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)

@dp.callback_query(lambda c: c.data.startswith("admin_withdrawal_approve_"))
async def admin_withdrawal_approve_handler(callback: types.CallbackQuery):
    """Обработчик одобрения заявки на вывод."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ У тебя нет прав для выполнения этой команды.", show_alert=True)
        return
    
    await callback.answer()
    
    request_id = safe_extract_id(callback.data, "admin_withdrawal_approve_")
    if request_id is None:
        await callback.message.edit_text(
            "❌ Ошибка: неверный формат данных.",
            reply_markup=None
        )
        return
    
    request = await asyncio.to_thread(get_withdrawal_request_by_id, request_id)
    
    if not request or request.get("status") != "processing":
        await callback.message.edit_text(
            "❌ Заявка не найдена или уже обработана.",
            reply_markup=None
        )
        return
    
    def format_number(num):
        try:
            return f"{float(num):,.2f}".replace(',', ' ').replace('.', ',')
        except (ValueError, TypeError):
            return "0,00"
    
    text = (
        f"✅ <b>Одобрить заявку на вывод?</b>\n\n"
        f"Пользователь: {request.get('user_name', 'Пользователь')}\n"
        f"Сумма: <b>{format_number(request['amount'])}</b> ₽\n\n"
        f"После одобрения бонусы будут списаны, и ты сможешь связаться с пользователем для уточнения способа выплаты."
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да, одобрить", callback_data=f"admin_withdrawal_approve_confirm_{request_id}"),
            InlineKeyboardButton(text="❌ Отмена", callback_data=f"admin_withdrawal_{request_id}")
        ]
    ])
    
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)

@dp.callback_query(lambda c: c.data.startswith("admin_withdrawal_approve_confirm_"))
async def admin_withdrawal_approve_confirm_handler(callback: types.CallbackQuery):
    """Обработчик подтверждения одобрения заявки."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ У тебя нет прав для выполнения этой команды.", show_alert=True)
        return
    
    await callback.answer()
    
    request_id = safe_extract_id(callback.data, "admin_withdrawal_approve_confirm_")
    if request_id is None:
        await callback.message.edit_text(
            "❌ Ошибка: неверный формат данных.",
            reply_markup=None
        )
        return
    
    try:
        success = await asyncio.to_thread(approve_withdrawal_request, request_id, str(callback.from_user.id))
        
        if success:
            request = await asyncio.to_thread(get_withdrawal_request_by_id, request_id)
            
            def format_number(num):
                try:
                    return f"{float(num):,.2f}".replace(',', ' ').replace('.', ',')
                except (ValueError, TypeError):
                    return "0,00"
            
            text = (
                f"✅ <b>Заявка одобрена!</b>\n\n"
                f"Пользователь: {request.get('user_name', 'Пользователь')}\n"
                f"Сумма: <b>{format_number(request['amount'])}</b> ₽\n\n"
                f"Бонусы списаны. Свяжись с пользователем для уточнения способа выплаты."
            )
            
            await callback.message.edit_text(text, parse_mode="HTML", reply_markup=None)
            
            # Уведомление пользователю
            user_telegram_id = request.get("user_telegram_id")
            if user_telegram_id:
                try:
                    user_text = (
                        f"✅ <b>Твоя заявка на вывод одобрена!</b>\n\n"
                        f"Сумма: <b>{format_number(request['amount'])}</b> ₽\n\n"
                        f"Администратор свяжется с тобой для уточнения реквизитов и способа выплаты."
                    )
                    await bot.send_message(int(user_telegram_id), user_text, parse_mode="HTML")
                except Exception as e:
                    print(f"⚠️ Не удалось отправить уведомление пользователю: {e}")
        else:
            await callback.message.edit_text(
                "❌ Не удалось одобрить заявку. Возможно, недостаточно средств на балансе пользователя.",
                reply_markup=None
            )
    except Exception as e:
        await callback.message.edit_text(
            f"❌ Ошибка при одобрении заявки: {str(e)}",
            reply_markup=None
        )

@dp.callback_query(lambda c: c.data.startswith("admin_withdrawal_reject_"))
async def admin_withdrawal_reject_handler(callback: types.CallbackQuery, state: FSMContext):
    """Обработчик отклонения заявки на вывод."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ У тебя нет прав для выполнения этой команды.", show_alert=True)
        return
    
    await callback.answer()
    
    request_id = safe_extract_id(callback.data, "admin_withdrawal_reject_")
    if request_id is None:
        await callback.message.edit_text(
            "❌ Ошибка: неверный формат данных.",
            reply_markup=None
        )
        return
    
    request = await asyncio.to_thread(get_withdrawal_request_by_id, request_id)
    
    if not request or request.get("status") != "processing":
        await callback.message.edit_text(
            "❌ Заявка не найдена или уже обработана.",
            reply_markup=None
        )
        return
    
    # Сохраняем ID заявки в состоянии
    await state.update_data(rejecting_request_id=request_id)
    
    text = (
        f"❌ <b>Отклонить заявку на вывод?</b>\n\n"
        f"Пользователь: {request.get('user_name', 'Пользователь')}\n"
        f"Сумма: {request['amount']:,.2f} ₽\n\n"
        f"Укажи причину отклонения:"
    )
    
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=None)
    await state.set_state(WithdrawalRejection.entering_reason)

@dp.callback_query(lambda c: c.data.startswith("admin_withdrawal_complete_"))
async def admin_withdrawal_complete_handler(callback: types.CallbackQuery):
    """Обработчик завершения выплаты."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ У тебя нет прав для выполнения этой команды.", show_alert=True)
        return
    
    await callback.answer()
    
    request_id = safe_extract_id(callback.data, "admin_withdrawal_complete_")
    if request_id is None:
        await callback.message.edit_text(
            "❌ Ошибка: неверный формат данных.",
            reply_markup=None
        )
        return
    
    try:
        success = await asyncio.to_thread(complete_withdrawal_request, request_id)
        
        if success:
            text = "✅ <b>Выплата завершена!</b>\n\nСтатус заявки изменен на 'Выполнена'."
            await callback.message.edit_text(text, parse_mode="HTML", reply_markup=None)
        else:
            await callback.message.edit_text(
                "❌ Не удалось завершить выплату. Заявка не найдена или имеет неверный статус.",
                reply_markup=None
            )
    except Exception as e:
        await callback.message.edit_text(
            f"❌ Ошибка при завершении выплаты: {str(e)}",
            reply_markup=None
        )

@dp.message(WithdrawalRejection.entering_reason)
async def process_withdrawal_rejection_reason(message: types.Message, state: FSMContext):
    """Обработчик ввода причины отклонения заявки."""
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    
    # Проверяем, не нажата ли кнопка
    button_texts = ["📊 Моя статистика", "📦 Мои заказы", "👥 Управление", 
                    "📈 Аналитика", "⚙️ Настройки", "👥 Пригласить друга", 
                    "💸 Вывести бонусы", "❓ Помощь"]
    if message.text in button_texts:
        await state.clear()
        return
    
    data = await state.get_data()
    request_id = data.get("rejecting_request_id")
    
    if not request_id:
        await message.answer(
            "❌ Ошибка: данные не найдены.",
            reply_markup=get_keyboard(message.from_user.id)
        )
        await state.clear()
        return
    
    reason = message.text.strip()
    
    # Валидация длины
    is_valid, error_msg = validate_text_length(reason, MAX_TEXT_LENGTH, "Причина отклонения")
    if not is_valid:
        await message.answer(
            f"❌ {error_msg}\n\nВведи причину отклонения:",
            reply_markup=get_keyboard(message.from_user.id)
        )
        return
    
    try:
        success = await asyncio.to_thread(
            reject_withdrawal_request,
            request_id,
            str(message.from_user.id),
            reason
        )
        
        if success:
            request = await asyncio.to_thread(get_withdrawal_request_by_id, request_id)
            
            def format_number(num):
                try:
                    return f"{float(num):,.2f}".replace(',', ' ').replace('.', ',')
                except (ValueError, TypeError):
                    return "0,00"
            
            # Экранируем HTML в причине отклонения
            safe_reason = sanitize_html(reason)
            
            text = (
                f"❌ <b>Заявка отклонена</b>\n\n"
                f"Пользователь: {request.get('user_name', 'Пользователь')}\n"
                f"Сумма: {format_number(request['amount'])} ₽\n"
                f"Причина: {safe_reason}"
            )
            
            await message.answer(text, parse_mode="HTML", reply_markup=get_keyboard(message.from_user.id))
            
            # Уведомление пользователю
            user_telegram_id = request.get("user_telegram_id")
            if user_telegram_id:
                try:
                    # Экранируем HTML в причине отклонения
                    safe_reason = sanitize_html(reason)
                    
                    user_text = (
                        f"❌ <b>Твоя заявка на вывод отклонена</b>\n\n"
                        f"Сумма: <b>{format_number(request['amount'])}</b> ₽\n"
                        f"Причина: {safe_reason}\n\n"
                        f"Бонусы возвращены на твой баланс."
                    )
                    await bot.send_message(int(user_telegram_id), user_text, parse_mode="HTML")
                except Exception as e:
                    print(f"⚠️ Не удалось отправить уведомление пользователю: {e}")
        else:
            await message.answer(
                "❌ Не удалось отклонить заявку. Заявка не найдена или уже обработана.",
                reply_markup=get_keyboard(message.from_user.id)
            )
    except Exception as e:
        await message.answer(
            f"❌ Ошибка при отклонении заявки: {str(e)}",
            reply_markup=get_keyboard(message.from_user.id)
        )
    
    await state.clear()

@dp.message(lambda message: message.text == "📈 Аналитика")
async def analytics_handler(message: types.Message, state: FSMContext):
    """Обработчик кнопки 'Аналитика' (только для админов)."""
    user_id = message.from_user.id
    
    if not is_admin(user_id):
        await message.answer(
            "❌ У тебя нет прав для выполнения этой команды.",
            reply_markup=get_keyboard(user_id)
        )
        return
    
    text = (
        "📈 <b>Аналитика участника</b>\n\n"
        "Введи данные участника для получения подробной аналитики:\n\n"
        "• <b>Ozon ID</b> (например: 19632916)\n"
        "• <b>Telegram username</b> (например: @username или username)\n"
        "• <b>Telegram ID</b> (например: 123456789)\n\n"
        "Или отправь /cancel для отмены."
    )
    
    await state.set_state(ParticipantAnalytics.waiting_for_participant_data)
    await message.answer(text, parse_mode="HTML", reply_markup=get_keyboard(user_id))

# Список всех кнопок для исключения из обработки состояния аналитики
ANALYTICS_BUTTON_TEXTS = [
    "📊 Моя статистика", "📦 Мои заказы", "👥 Управление", 
    "📈 Аналитика", "⚙️ Настройки", "👥 Пригласить друга", 
    "💸 Вывести бонусы", "❓ Помощь", "💬 Чат с админом"
]

@dp.message(ParticipantAnalytics.waiting_for_participant_data, F.text.in_(ANALYTICS_BUTTON_TEXTS))
async def process_analytics_button_in_state(message: types.Message, state: FSMContext):
    """Обработчик кнопок в состоянии ввода данных аналитики - очищает состояние и обрабатывает кнопку."""
    await state.clear()
    
    # Вызываем обработчик кнопки через диспетчер
    from aiogram.types import Update
    
    new_update = Update(update_id=message.message_id, message=message)
    
    try:
        await dp.feed_update(bot, new_update)
    except Exception:
        # Если feed_update не работает, состояние уже очищено
        # Пользователю нужно будет нажать кнопку еще раз
        pass

@dp.message(ParticipantAnalytics.waiting_for_participant_data, ~F.text.in_(ANALYTICS_BUTTON_TEXTS))
async def process_participant_analytics_input(message: types.Message, state: FSMContext):
    """Обрабатывает ввод данных участника для аналитики (не обрабатывает кнопки)."""
    user_id = message.from_user.id
    
    if not is_admin(user_id):
        await state.clear()
        return
    
    # Проверяем команду отмены
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено.", reply_markup=get_keyboard(user_id))
        return
    
    user_input = message.text.strip()
    
    # Валидация длины
    max_length = max(MAX_OZON_ID_LENGTH, MAX_USERNAME_LENGTH)
    is_valid, error_msg = validate_text_length(user_input, max_length, "Ввод")
    if not is_valid:
        await message.answer(
            f"❌ {error_msg}\n\nПопробуй еще раз или отправь /cancel для отмены.",
            parse_mode="HTML",
            reply_markup=get_keyboard(user_id)
        )
        return
    
    participant = None
    
    # Определяем тип ввода и ищем участника
    if user_input.isdigit():
        # Может быть Ozon ID или Telegram ID
        # Сначала пробуем как Ozon ID
        participant = await asyncio.to_thread(find_participant_by_ozon_id, user_input)
        
        # Если не найден, пробуем как Telegram ID
        if not participant:
            try:
                telegram_id = int(user_input)
                participant = await asyncio.to_thread(find_participant_by_telegram_id, telegram_id)
            except ValueError:
                pass
    else:
        # Пробуем как username
        participant = await asyncio.to_thread(find_participant_by_username, user_input)
    
    if not participant:
        # Экранируем HTML в user_input
        safe_user_input = sanitize_html(user_input)
        
        await message.answer(
            f"❌ Участник не найден по запросу: <code>{safe_user_input}</code>\n\n"
            f"Проверь правильность ввода и попробуй еще раз.\n"
            f"Или отправь /cancel для отмены.",
            parse_mode="HTML",
            reply_markup=get_keyboard(user_id)
        )
        return
    
    # Участник найден - генерируем аналитику
    ozon_id = participant.get("Ozon ID")
    await state.clear()
    
    # Показываем сообщение о загрузке
    loading_msg = await message.answer("⏳ Генерирую аналитику...", reply_markup=get_keyboard(user_id))
    
    try:
        # Генерируем аналитику
        analytics_parts = await generate_participant_analytics(ozon_id)
        
        # Удаляем сообщение о загрузке
        await loading_msg.delete()
        
        # Отправляем части аналитики
        for i, part in enumerate(analytics_parts, 1):
            if i == 1:
                await message.answer(part, parse_mode="HTML", reply_markup=get_keyboard(user_id))
            else:
                await message.answer(part, parse_mode="HTML")
    except Exception as e:
        await loading_msg.delete()
        await message.answer(
            f"❌ Ошибка при генерации аналитики: {str(e)}",
            reply_markup=get_keyboard(user_id)
        )

@dp.message(lambda message: message.text == "⚙️ Настройки")
async def settings_handler(message: types.Message):
    """Обработчик кнопки 'Настройки' (только для админов)."""
    user_id = message.from_user.id
    
    if not is_admin(user_id):
        await message.answer(
            "❌ У тебя нет прав для выполнения этой команды.",
            reply_markup=get_keyboard(user_id)
        )
        return
    
    # Получаем текущие настройки бонусов
    bonus_settings = await asyncio.to_thread(get_bonus_settings)
    withdrawal_settings = await asyncio.to_thread(get_withdrawal_settings)
    
    text = (
        "⚙️ <b>Настройки</b>\n\n"
        "💰 <b>Настройки бонусной программы:</b>\n\n"
        f"Количество уровней: <b>{bonus_settings.max_levels}</b>\n\n"
    )
    
    # Показываем уровень 0 (покупки самого участника)
    level_0_percent = getattr(bonus_settings, 'level_0_percent', 0.0)
    if level_0_percent is not None:
        text += f"Уровень 0 (покупки участника): <b>{level_0_percent}%</b>\n"
    
    # Показываем уровни 1-5
    for level in range(1, min(bonus_settings.max_levels + 1, 6)):  # Ограничиваем до 5 уровней
        percent = getattr(bonus_settings, f'level_{level}_percent', 0.0)
        if percent is not None:
            text += f"Уровень {level}: <b>{percent}%</b>\n"
    
    # Добавляем настройки вывода
    text += (
        "\n💸 <b>Настройки вывода бонусов:</b>\n\n"
        f"Минимальная сумма вывода: <b>{withdrawal_settings.min_withdrawal_amount} ₽</b>\n"
    )
    
    # Создаем inline-клавиатуру
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Изменить количество уровней", callback_data="bonus_edit_levels")],
        [InlineKeyboardButton(text="📝 Изменить проценты бонусов", callback_data="bonus_edit_percents")],
        [InlineKeyboardButton(text="📝 Изменить минимальную сумму вывода", callback_data="withdrawal_edit_min_amount")],
        [InlineKeyboardButton(text="❌ Закрыть", callback_data="bonus_settings_close")]
    ])
    
    await message.answer(text, parse_mode="HTML", reply_markup=keyboard)

@dp.callback_query(lambda c: c.data == "bonus_settings_close")
async def bonus_settings_close_handler(callback: types.CallbackQuery):
    """Закрыть настройки бонусов."""
    await callback.answer()
    await callback.message.delete()

# =========================================================
# ОБРАБОТЧИКИ РАЗДЕЛА "ПОМОЩЬ"
# =========================================================

@dp.callback_query(lambda c: c.data == "help_main")
async def help_main_handler(callback: types.CallbackQuery):
    """Вернуться в главное меню помощи."""
    await show_help_main_menu(callback)

@dp.callback_query(lambda c: c.data == "help_general_info")
async def help_general_info_handler(callback: types.CallbackQuery):
    """Обработчик подраздела 'Общая информация'."""
    await callback.answer()
    
    text = (
        "ℹ️ <b>Общая информация</b>\n\n"
        "🎉 Добро пожаловать в реферальную программу <b>Wistery</b>!\n\n"
        "💰 <b>Как это работает:</b>\n"
        "• Покупай товары Wistery на Ozon и получай скидки\n"
        "• Приглашай друзей по своей реферальной ссылке\n"
        "• Получай бонусы с покупок твоих друзей и их друзей\n"
        "• Чем больше друзей пригласишь, тем больше бонусов!\n\n"
        "🎯 <b>Преимущества:</b>\n"
        "• Автоматическое начисление бонусов\n"
        "• Многоуровневая система вознаграждений\n"
        "• Простая регистрация - нужен только Ozon ID\n"
        "• Отслеживание статистики в реальном времени\n\n"
        "💡 <b>Начни прямо сейчас:</b>\n"
        "Зарегистрируйся через /start и получи свою реферальную ссылку!"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔙 Назад", callback_data="help_main"),
        ]
    ])
    
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)

@dp.callback_query(lambda c: c.data == "help_find_ozon_id")
async def help_find_ozon_id_handler(callback: types.CallbackQuery):
    """Обработчик подраздела 'Как найти Ozon ID'."""
    await callback.answer()
    
    text = (
        "📝 <b>Как найти свой Ozon ID?</b>\n\n"
        "Твой Ozon ID — это первые цифры номера любого твоего заказа до тире.\n\n"
        "<b>Пример:</b>\n"
        "• Номер заказа: 10054917-1093-1\n"
        "• Твой Ozon ID: <b>10054917</b>\n\n"
        "💡 <b>Совет:</b> Можешь отправить полный номер заказа, я сам выделю нужные цифры.\n\n"
        "📋 <b>Где найти номер заказа:</b>\n"
        "• В личном кабинете Ozon\n"
        "• В письме на email о заказе\n"
        "• В мобильном приложении Ozon\n"
        "• В SMS о статусе заказа"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔙 Назад", callback_data="help_main"),
        ]
    ])
    
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)

@dp.callback_query(lambda c: c.data == "help_bonus_rates")
async def help_bonus_rates_handler(callback: types.CallbackQuery):
    """Обработчик подраздела 'Бонусные ставки'."""
    await callback.answer()
    
    # Получаем текущие настройки бонусов
    settings = await asyncio.to_thread(get_bonus_settings)
    
    text = "💰 <b>Бонусные ставки</b>\n\n"
    text += "Текущие бонусные проценты:\n\n"
    
    # Показываем проценты для каждого уровня
    for level in range(1, settings.max_levels + 1):
        percent = getattr(settings, f'level_{level}_percent', 0.0)
        if percent is None:
            percent = 0.0
        text += f"Уровень {level}: <b>{percent}%</b>\n"
    
    text += "\n💡 <b>Как это работает:</b>\n"
    text += "• Уровень 1 - бонус с покупок твоих прямых рефералов\n"
    if settings.max_levels > 1:
        text += "• Уровень 2 - бонус с покупок рефералов твоих рефералов\n"
    if settings.max_levels > 2:
        text += "• И так далее до уровня " + str(settings.max_levels) + "\n"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔙 Назад", callback_data="help_main"),
        ]
    ])
    
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)

@dp.callback_query(lambda c: c.data == "help_chat_with_admin")
async def help_chat_with_admin_handler(callback: types.CallbackQuery):
    """Обработчик кнопки 'Чат с админом' в разделе помощи."""
    await callback.answer()
    
    user = callback.from_user
    participant = await asyncio.to_thread(find_participant_by_telegram_id, user.id)
    
    if not participant:
        text = (
            "❌ Ты еще не зарегистрирован в программе.\n\n"
            "Сначала зарегистрируйся через команду /start."
        )
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🔙 Назад", callback_data="help_main"),
            ]
        ])
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
        return
    
    # Получаем информацию о первом админе
    if not ADMIN_IDS:
        text = "❌ Администратор временно недоступен. Попробуй позже."
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🔙 Назад", callback_data="help_main"),
            ]
        ])
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
        return
    
    admin_id = ADMIN_IDS[0]  # Берем первого админа
    admin_info = await get_admin_contact_info(callback.message.bot, admin_id)
    
    if not admin_info:
        text = "❌ Не удалось получить контакт администратора. Попробуй позже."
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🔙 Назад", callback_data="help_main"),
            ]
        ])
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
        return
    
    # Если у админа есть username
    if admin_info["has_username"]:
        username = admin_info["username"]
        text = (
            f"💬 <b>Чат с администратором</b>\n\n"
            f"Нажми на кнопку ниже, чтобы написать администратору напрямую:\n\n"
            f"Или напиши ему в Telegram: @{username}\n\n"
            f"💡 <b>Как это работает:</b>\n"
            f"• Нажми на кнопку выше или напиши администратору напрямую\n"
            f"• Вся переписка будет в вашем личном чате в Telegram\n"
            f"• Администратор ответит в ближайшее время"
        )
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="💬 Написать администратору",
                    url=f"https://t.me/{username}"
                )
            ],
            [
                InlineKeyboardButton(text="🔙 Назад", callback_data="help_main"),
            ]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    
    # Если username нет - отправляем инструкцию
    else:
        admin_name = admin_info["first_name"] or "Администратор"
        if admin_info.get("last_name"):
            admin_name += f" {admin_info['last_name']}"
        
        text = (
            f"💬 <b>Чат с администратором</b>\n\n"
            f"Администратор: <b>{admin_name}</b>\n\n"
            f"Чтобы связаться с администратором:\n"
            f"1. Открой Telegram\n"
            f"2. Найди пользователя по имени: <b>{admin_name}</b>\n"
            f"3. Напиши ему напрямую\n\n"
            f"Или попроси администратора добавить username в настройках Telegram для более удобной связи.\n\n"
            f"💡 <b>Как это работает:</b>\n"
            f"• Напиши администратору напрямую в Telegram\n"
            f"• Вся переписка будет в вашем личном чате в Telegram\n"
            f"• Администратор ответит в ближайшее время"
        )
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🔙 Назад", callback_data="help_main"),
            ]
        ])
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    
    # Уведомляем админа о новом запросе
    await notify_admin_about_chat_request(admin_id, user, participant)

@dp.callback_query(lambda c: c.data == "bonus_edit_levels")
async def bonus_edit_levels_handler(callback: types.CallbackQuery, state: FSMContext):
    """Начать редактирование количества уровней."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ У тебя нет прав для выполнения этой команды.", show_alert=True)
        return
    
    await callback.answer()
    await state.set_state(BonusSettings.editing_levels)
    
    text = (
        "📝 <b>Редактирование количества уровней</b>\n\n"
        "Введи новое количество уровней (от 1 до 10):"
    )
    
    await callback.message.edit_text(text, parse_mode="HTML")

@dp.callback_query(lambda c: c.data == "bonus_edit_percents")
async def bonus_edit_percents_handler(callback: types.CallbackQuery, state: FSMContext):
    """Начать редактирование процентов бонусов."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ У тебя нет прав для выполнения этой команды.", show_alert=True)
        return
    
    await callback.answer()
    
    settings = await asyncio.to_thread(get_bonus_settings)
    
    text = "📝 <b>Редактирование процентов бонусов</b>\n\n"
    
    # Создаем кнопки для каждого уровня
    keyboard_buttons = []
    
    # Кнопка для уровня 0 (покупки самого участника)
    level_0_percent = getattr(settings, 'level_0_percent', 0.0)
    if level_0_percent is None:
        level_0_percent = 0.0
    keyboard_buttons.append([
        InlineKeyboardButton(
            text=f"Уровень 0 - покупки участника ({level_0_percent}%)",
            callback_data="bonus_edit_level_0"
        )
    ])
    
    # Кнопки для уровней 1-5 (ограничиваем до 5 уровней)
    for level in range(1, min(settings.max_levels + 1, 6)):
        percent = getattr(settings, f'level_{level}_percent', 0.0)
        if percent is None:
            percent = 0.0
        keyboard_buttons.append([
            InlineKeyboardButton(
                text=f"Уровень {level} ({percent}%)",
                callback_data=f"bonus_edit_level_{level}"
            )
        ])
    
    keyboard_buttons.append([InlineKeyboardButton(text="❌ Отмена", callback_data="bonus_settings_close")])
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)

@dp.callback_query(lambda c: c.data.startswith("bonus_edit_level_"))
async def bonus_edit_single_percent_handler(callback: types.CallbackQuery, state: FSMContext):
    """Начать редактирование процента для конкретного уровня."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ У тебя нет прав для выполнения этой команды.", show_alert=True)
        return
    
    level = safe_extract_id(callback.data, "bonus_edit_level_")
    if level is None or level < 0 or level > MAX_LEVELS:
        await callback.answer("❌ Ошибка: неверный формат данных.", show_alert=True)
        return
    
    await callback.answer()
    await state.set_state(BonusSettings.editing_percent)
    await state.update_data(editing_level=level)
    
    settings = await asyncio.to_thread(get_bonus_settings)
    current_percent = getattr(settings, f'level_{level}_percent', 0.0)
    
    # Формируем текст в зависимости от уровня
    if level == 0:
        level_text = "уровня 0 (покупки самого участника)"
    else:
        level_text = f"уровня {level}"
    
    text = (
        f"📝 <b>Редактирование процента для {level_text}</b>\n\n"
        f"Текущее значение: <b>{current_percent}%</b>\n\n"
        f"Введи новый процент (например: 5.5 для 5.5%):"
    )
    
    await callback.message.edit_text(text, parse_mode="HTML")

@dp.message(BonusSettings.editing_levels)
async def process_editing_levels(message: types.Message, state: FSMContext):
    """Обработать ввод количества уровней."""
    if not is_admin(message.from_user.id):
        await message.answer("❌ У тебя нет прав для выполнения этой команды.")
        await state.clear()
        return
    
    try:
        levels = int(message.text.strip())
        # Валидация диапазона
        is_valid, error_msg = validate_numeric_range(float(levels), MIN_LEVELS, MAX_LEVELS, "Количество уровней")
        if not is_valid:
            await message.answer(f"❌ {error_msg} Попробуй еще раз:")
            return
        
        # Обновляем настройки
        await asyncio.to_thread(update_bonus_settings, {"max_levels": levels})
        clear_bonus_settings_cache()
        
        await message.answer(
            f"✅ Количество уровней успешно изменено на <b>{levels}</b>",
            parse_mode="HTML",
            reply_markup=get_keyboard(message.from_user.id)
        )
        await state.clear()
    except ValueError:
        await message.answer("❌ Введи число от 1 до 5. Попробуй еще раз:")

@dp.message(BonusSettings.editing_percent)
async def process_editing_percent(message: types.Message, state: FSMContext):
    """Обработать ввод процента для уровня."""
    if not is_admin(message.from_user.id):
        await message.answer("❌ У тебя нет прав для выполнения этой команды.")
        await state.clear()
        return
    
    data = await state.get_data()
    level = data.get("editing_level")
    
    try:
        percent = float(message.text.strip().replace(',', '.'))
        # Валидация диапазона
        is_valid, error_msg = validate_numeric_range(percent, MIN_BONUS_PERCENT, MAX_BONUS_PERCENT, "Процент бонуса")
        if not is_valid:
            await message.answer(f"❌ {error_msg} Попробуй еще раз:")
            return
        
        # Обновляем настройки
        await asyncio.to_thread(update_bonus_settings, {f"level_{level}_percent": percent})
        clear_bonus_settings_cache()
        
        await message.answer(
            f"✅ Процент для уровня {level} успешно изменен на <b>{percent}%</b>",
            parse_mode="HTML",
            reply_markup=get_keyboard(message.from_user.id)
        )
        await state.clear()
    except ValueError:
        await message.answer("❌ Введи число (можно с точкой, например: 5.5). Попробуй еще раз:")

@dp.callback_query(lambda c: c.data == "withdrawal_edit_min_amount")
async def withdrawal_edit_min_amount_handler(callback: types.CallbackQuery, state: FSMContext):
    """Начать редактирование минимальной суммы вывода."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ У тебя нет прав для выполнения этой команды.", show_alert=True)
        return
    
    await callback.answer()
    
    settings = await asyncio.to_thread(get_withdrawal_settings)
    
    await state.set_state(WithdrawalSettings.editing_min_amount)
    
    text = (
        "📝 <b>Редактирование минимальной суммы вывода</b>\n\n"
        f"Текущее значение: <b>{settings.min_withdrawal_amount} ₽</b>\n\n"
        "Введи новую минимальную сумму вывода (например: 500 для 500 ₽):"
    )
    
    await callback.message.edit_text(text, parse_mode="HTML")

@dp.message(WithdrawalSettings.editing_min_amount)
async def process_editing_min_amount(message: types.Message, state: FSMContext):
    """Обработать ввод минимальной суммы вывода."""
    if not is_admin(message.from_user.id):
        await message.answer("❌ У тебя нет прав для выполнения этой команды.")
        await state.clear()
        return
    
    try:
        min_amount = float(message.text.strip().replace(',', '.'))
        # Валидация диапазона (максимальная сумма вывода как верхний предел)
        is_valid, error_msg = validate_numeric_range(min_amount, 0.0, MAX_WITHDRAWAL_AMOUNT, "Минимальная сумма вывода")
        if not is_valid:
            await message.answer(f"❌ {error_msg} Попробуй еще раз:")
            return
        
        # Обновляем настройки
        await asyncio.to_thread(update_withdrawal_settings, {"min_withdrawal_amount": min_amount})
        clear_withdrawal_settings_cache()
        
        await message.answer(
            f"✅ Минимальная сумма вывода успешно изменена на <b>{min_amount} ₽</b>",
            parse_mode="HTML",
            reply_markup=get_keyboard(message.from_user.id)
        )
        await state.clear()
    except ValueError:
        await message.answer("❌ Введи число (можно с точкой, например: 500.5). Попробуй еще раз:")

@dp.callback_query(lambda c: c.data == "leave_program_confirm")
async def leave_program_confirm_handler(callback: types.CallbackQuery, state: FSMContext):
    """Обработчик подтверждения выхода из программы."""
    await callback.answer()
    
    user = callback.from_user
    result = await asyncio.to_thread(deactivate_participant, user.id)
    
    if result.get("success"):
        referrals_count = result.get("referrals_count", 0)
        ozon_id = result.get("ozon_id", "")
        was_already_inactive = result.get("was_already_inactive", False)
        
        # Формируем сообщение о выходе
        referrals_text = ""
        if referrals_count > 0:
            referrals_text = f"\n\n📋 У тебя <b>{referrals_count}</b> реферал"
            if referrals_count == 1:
                referrals_text += "а"
            elif referrals_count < 5:
                referrals_text += "ов"
            else:
                referrals_text += "ов"
            referrals_text += ". Твоя реферальная сеть сохранена."
        
        if was_already_inactive:
            text = (
                f"ℹ️ <b>Ты уже неактивен в программе</b>\n\n"
                f"Твой аккаунт (Ozon ID: {ozon_id}) уже был деактивирован.{referrals_text}\n\n"
                f"💡 Чтобы вернуться, зарегистрируйся заново через команду /start."
            )
        else:
            text = (
                f"✅ <b>Ты успешно вышел из программы</b>\n\n"
                f"Твой аккаунт (Ozon ID: {ozon_id}) деактивирован.{referrals_text}\n\n"
                f"💡 Твоя реферальная сеть сохранена. Если захочешь вернуться, "
                f"зарегистрируйся заново через команду /start - все твои рефералы останутся на месте."
            )
        
        await callback.message.edit_text(text, parse_mode="HTML")
        await state.clear()
    else:
        text = (
            "❌ <b>Ошибка</b>\n\n"
            "Не удалось выйти из программы. Попробуй еще раз или обратись в поддержку."
        )
        await callback.message.edit_text(text, parse_mode="HTML")
        await state.clear()

@dp.callback_query(lambda c: c.data == "leave_program_cancel")
async def leave_program_cancel_handler(callback: types.CallbackQuery, state: FSMContext):
    """Обработчик отмены выхода из программы."""
    await callback.answer("Выход отменен")
    
    text = (
        "✅ <b>Выход отменен</b>\n\n"
        "Ты остаешься в программе. Если нужна помощь, используй кнопки ниже."
    )
    
    await callback.message.edit_text(text, parse_mode="HTML")
    await state.clear()
    
    # Отправляем клавиатуру отдельным сообщением
    await callback.message.answer(
        "Выбери команду:",
        reply_markup=get_keyboard(callback.from_user.id)
    )

# =========================================================
# 4. ОБРАБОТЧИК СОСТОЯНИЯ (Получение Ozon ID)
# =========================================================
@dp.message(Registration.waiting_for_ozon_id)
async def process_ozon_id(message: types.Message, state: FSMContext):
    # Проверяем, не нажата ли кнопка вместо ввода ID
    button_texts = ["📊 Моя статистика", "📦 Мои заказы", 
                     "❓ Помощь", "👥 Управление", "📈 Аналитика", "⚙️ Настройки", 
                     "👥 Пригласить друга", "🚪 Выйти из программы"]
    if message.text in button_texts:
        # Если нажата кнопка, обрабатываем её соответствующим обработчиком
        return
    
    user_input = message.text.strip()
    user = message.from_user
    
    # Валидация длины входных данных
    is_valid, error_msg = validate_text_length(user_input, MAX_OZON_ID_LENGTH * 3, "Ozon ID")
    if not is_valid:
        await message.answer(
            f"❌ {error_msg}\n\nПопробуй еще раз:",
            reply_markup=get_keyboard(user.id)
        )
        return
    
    # Извлекаем Ozon ID из ввода:
    # - Если есть тире, берем первые цифры до тире (например, "10054917-1093-1" -> "10054917")
    # - Если только цифры, используем как есть
    if "-" in user_input:
        ozon_id = user_input.split("-")[0].strip()
    else:
        ozon_id = user_input
    
    # Валидация длины извлеченного Ozon ID
    is_valid, error_msg = validate_text_length(ozon_id, MAX_OZON_ID_LENGTH, "Ozon ID")
    if not is_valid:
        await message.answer(
            f"❌ {error_msg}\n\nПопробуй еще раз:",
            reply_markup=get_keyboard(user.id)
        )
        return
    
    # Проверяем, что получили только цифры
    if not ozon_id.isdigit():
        await message.answer(
            "❌ Неверный формат. Ozon ID должен содержать только цифры.\n\n"
            "Можешь отправить:\n"
            "• Ozon ID (только цифры, например: 10054917)\n"
            "• Или полный номер заказа (например: 10054917-1093-1)",
            reply_markup=get_keyboard(user.id)
        )
        return

    # проверяем, нет ли такого участника уже
    exist = await asyncio.to_thread(find_participant_by_ozon_id, ozon_id) 
    if exist:
        await message.answer(
            "Такой Ozon ID уже есть в системе. Если ты считаешь, что это ошибка, напиши в поддержку.",
            reply_markup=get_keyboard(user.id)
        )
        await state.clear() 
        return

    # достаём сохранённый referrer_id
    data = await state.get_data()
    referrer_id = data.get("referrer_id")
    referrer_telegram_id = data.get("referrer_telegram_id")
    
    # Если Ozon ID реферера не был найден при /start, но есть Telegram ID,
    # пытаемся найти реферера еще раз (возможно, он зарегистрировался между /start и вводом Ozon ID)
    if not referrer_id and referrer_telegram_id:
        print(f"🔄 Повторная попытка найти реферера по Telegram ID={referrer_telegram_id}")
        referrer_participant = await asyncio.to_thread(
            find_participant_by_telegram_id, referrer_telegram_id
        )
        if referrer_participant:
            referrer_id = referrer_participant.get("Ozon ID")
            print(f"✅ Реферер найден при регистрации: Telegram ID={referrer_telegram_id}, Ozon ID={referrer_id}")
        else:
            print(f"⚠️ Реферер все еще не найден: Telegram ID={referrer_telegram_id}")
    
    print(f"🔍 Создание участника {ozon_id} с referrer_id={referrer_id}")

    # создаём участника
    await asyncio.to_thread( 
        create_participant,
        ozon_id=ozon_id,
        tg_id=user.id,
        username=user.username,
        first_name=user.first_name,
        referrer_id=referrer_id,
        language=message.from_user.language_code
    )

    # Отправляем уведомление рефереру, если он есть
    if referrer_id:
        try:
            referrer_participant = await asyncio.to_thread(find_participant_by_ozon_id, referrer_id)
            if referrer_participant:
                referrer_telegram_id_str = referrer_participant.get("Telegram ID")
                if referrer_telegram_id_str:
                    try:
                        referrer_telegram_id = int(referrer_telegram_id_str)
                        await notify_referrer_about_new_registration(
                            referrer_telegram_id=referrer_telegram_id,
                            new_participant_name=user.first_name or "друг",
                            new_participant_ozon_id=ozon_id,
                            new_participant_username=user.username
                        )
                    except (ValueError, Exception) as e:
                        # Не критично, просто логируем
                        print(f"⚠️ Не удалось отправить уведомление рефереру: {e}")
        except Exception as e:
            # Не критично, просто логируем
            print(f"⚠️ Ошибка при поиске реферера для уведомления: {e}")

    await state.clear()

    await message.answer(
        f"Готово, {user.first_name or 'друг'}! Ты успешно зарегистрирован в программе.\n"
        f"Твой Ozon ID: {ozon_id}\n\n"
        f"Теперь я смогу отслеживать твои покупки и начислять баллы 😊",
        reply_markup=get_keyboard(user.id)
    )

# =========================================================
# 5. ЗАПУСК БОТА
# =========================================================
# =========================================================
# АВТОМАТИЧЕСКАЯ ПЕРИОДИЧЕСКАЯ СИНХРОНИЗАЦИЯ
# =========================================================

# Глобальный флаг для отслеживания процесса синхронизации
_sync_in_progress = False
_sync_task: asyncio.Task = None
_notification_task: asyncio.Task = None

# Время синхронизации заказов: 12:00 и 19:30 по московскому времени каждый день
SYNC_TIMES = [
    (12, 0),   # 12:00 МСК
    (19, 30),  # 19:30 МСК
]

async def perform_auto_sync(notify_admins: bool = False) -> bool:
    """
    Выполняет автоматическую синхронизацию заказов.
    
    Args:
        notify_admins: Если True, отправляет уведомления админам о результате
    
    Returns:
        True если синхронизация успешна, False в случае ошибки
    """
    global _sync_in_progress
    
    # Проверяем, не идет ли уже синхронизация
    if _sync_in_progress:
        print("⚠️ Синхронизация уже выполняется, пропускаем...")
        return False
    
    _sync_in_progress = True
    
    try:
        print(f"🔄 Начало автоматической синхронизации в {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        results = await asyncio.to_thread(update_orders_sheet)
        
        if isinstance(results, dict):
            # Результаты по кабинетам
            total_count = sum(r.get("count", 0) for r in results.values() if isinstance(r, dict))
            print(f"✅ Автоматическая синхронизация завершена. Обработано кабинетов: {len(results)}, всего заказов: {total_count}")
            
            # Уведомляем админов всегда, если запрошено (даже если заказов нет)
            if notify_admins:
                await notify_admins_about_sync(results)
            
            return True
        else:
            print(f"⚠️ Автоматическая синхронизация завершена, но результат неожиданный: {results}")
            return False
            
    except Exception as e:
        error_msg = f"❌ Ошибка при автоматической синхронизации: {e}"
        print(error_msg)
        
        # Уведомляем админов об ошибке
        if notify_admins:
            await notify_admins_about_sync_error(str(e))
        
        return False
    finally:
        _sync_in_progress = False

async def notify_admins_about_sync(results: dict):
    """Отправляет уведомление админам об успешной синхронизации с детальной статистикой по каждому кабинету."""
    global bot
    try:
        # Отправляем отдельное сообщение для каждого кабинета
        for cabinet_name, result in results.items():
            if not isinstance(result, dict):
                continue
            
            cabinet_name_display = result.get("cabinet_name", cabinet_name)
            client_id = result.get("client_id", "не указан")
            
            period_start = result.get("period_start")
            period_end = result.get("period_end")
            
            if period_start is None or period_end is None:
                period_start_str = "не указано"
                period_end_str = "не указано"
            else:
                period_start_str = period_start.strftime("%d.%m.%Y %H:%M")
                period_end_str = period_end.strftime("%d.%m.%Y %H:%M")
            
            # Получаем статистику по статусам за первый день периода
            first_day_stats = result.get("first_day_stats", {})
            
            # Формируем строку со статистикой по статусам
            status_stats_text = ""
            if first_day_stats and first_day_stats.get("total", 0) > 0:
                # Извлекаем дату из period_start_str (формат: "DD.MM.YYYY HH:MM")
                if period_start_str != "не указано" and " " in period_start_str:
                    first_day_date = period_start_str.split()[0]
                elif period_start_str != "не указано":
                    first_day_date = period_start_str
                else:
                    first_day_date = ""
                
                if first_day_date:
                    status_stats_text = f"\n\n📊 <b>Статистика за {first_day_date}:</b>\n"
                    status_stats_text += f"Всего отправлений: <b>{first_day_stats['total']}</b>\n"
                    
                    statuses = first_day_stats.get("statuses", {})
                    if statuses:
                        # Сортируем по количеству (от большего к меньшему)
                        sorted_statuses = sorted(statuses.items(), key=lambda x: x[1], reverse=True)
                        for status, count in sorted_statuses:
                            percentage = (count / first_day_stats['total']) * 100
                            status_name = {
                                "delivered": "✅ Доставлено",
                                "delivering": "🚚 В доставке",
                                "awaiting_packaging": "📦 Ожидает упаковки",
                                "awaiting_deliver": "⏳ Ожидает доставки",
                                "cancelled": "❌ Отменено"
                            }.get(status, status)
                            status_stats_text += f"{status_name}: <b>{count}</b> ({percentage:.1f}%)\n"
                    
                    if first_day_stats.get("active_count", 0) > 0:
                        status_stats_text += f"\n⚠️ Активных отправлений: <b>{first_day_stats['active_count']}</b>"
            
            # Формируем основное сообщение
            if result.get("count", 0) > 0:
                text = (
                    f"🤖 <b>Автоматическая синхронизация завершена</b>\n\n"
                    f"📊 Кабинет: {cabinet_name_display} (Client ID: {client_id})\n\n"
                    f"🎉 Добавлено <b>{result.get('count', 0)}</b> новых отправлений\n"
                    f"👥 Обработано <b>{result.get('customers_count', 0)}</b> клиентов "
                    f"(новых: <b>{result.get('new_customers_count', 0)}</b>)\n"
                    f"🎯 Участников программы совершивших покупку: <b>{result.get('participants_with_orders_count', 0)}</b>\n\n"
                    f"📅 <b>Период синхронизации:</b>\n"
                    f"С: {period_start_str}\n"
                    f"По: {period_end_str}"
                    f"{status_stats_text}"
                )
            else:
                text = (
                    f"🤖 <b>Автоматическая синхронизация завершена</b>\n\n"
                    f"📊 Кабинет: {cabinet_name_display} (Client ID: {client_id})\n\n"
                    f"✅ Новых отправлений не найдено\n\n"
                    f"📅 <b>Период проверки:</b>\n"
                    f"С: {period_start_str}\n"
                    f"По: {period_end_str}\n\n"
                    f"💡 Все отправления уже синхронизированы"
                    f"{status_stats_text}"
                )
            
            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(admin_id, text, parse_mode="HTML")
                except Exception as e:
                    print(f"⚠️ Не удалось отправить уведомление админу {admin_id}: {e}")
            
            # Если была ошибка, отправляем сообщение об ошибке
            if result.get("error"):
                error_text = (
                    f"🤖 <b>Синхронизация кабинета \"{cabinet_name_display}\"</b>\n\n"
                    f"❌ Ошибка синхронизации\n"
                    f"⚠️ Ошибка: {result.get('error')}\n"
                    f"💡 Проверьте подключение к API Ozon\n\n"
                    f"⏰ Время попытки: {period_end_str}"
                )
                for admin_id in ADMIN_IDS:
                    try:
                        await bot.send_message(admin_id, error_text, parse_mode="HTML")
                    except Exception as e:
                        print(f"⚠️ Не удалось отправить уведомление об ошибке админу {admin_id}: {e}")
    except Exception as e:
        print(f"⚠️ Ошибка при отправке уведомлений админам: {e}")

async def notify_admins_about_sync_error(error_msg: str):
    """Отправляет уведомление админам об общей ошибке синхронизации."""
    global bot
    try:
        error_time = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        text = (
            f"❌ <b>Ошибка автоматической синхронизации</b>\n\n"
            f"<code>{error_msg}</code>\n\n"
            f"⏰ Время ошибки: {error_time}\n\n"
            f"💡 Попробуйте проверить подключение к интернету или выполнить синхронизацию вручную командой /sync_orders"
        )
        
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(admin_id, text, parse_mode="HTML")
            except Exception as e:
                print(f"⚠️ Не удалось отправить уведомление об ошибке админу {admin_id}: {e}")
    except Exception as e:
        print(f"⚠️ Ошибка при отправке уведомлений об ошибке админам: {e}")

async def notify_referrer_about_new_registration(
    referrer_telegram_id: int,
    new_participant_name: str,
    new_participant_ozon_id: str,
    new_participant_username: str | None = None
) -> bool:
    """
    Отправляет уведомление рефереру о регистрации нового участника.
    
    Args:
        referrer_telegram_id: Telegram ID реферера
        new_participant_name: Имя нового участника
        new_participant_ozon_id: Ozon ID нового участника
        new_participant_username: Username нового участника (опционально)
    
    Returns:
        True если уведомление отправлено успешно, False в случае ошибки
    """
    global bot
    try:
        # Формируем имя для отображения
        display_name = new_participant_name
        if new_participant_username:
            display_name = f"{new_participant_name} (@{new_participant_username})"
        
        text = (
            f"✨ <b>Случилось чудо!</b>\n\n"
            f"Твой друг <b>{display_name}</b> присоединился к программе по твоей реферальной ссылке!\n\n"
            f"🎯 Теперь ты будешь получать бонусы с каждой его покупки и покупок его друзей!\n"
            f"Приглашай больше друзей и увеличивай свой доход! 💰"
        )
        
        await bot.send_message(referrer_telegram_id, text, parse_mode="HTML")
        return True
    except Exception as e:
        print(f"⚠️ Не удалось отправить уведомление рефереру {referrer_telegram_id}: {e}")
        return False

async def notify_admin_about_chat_request(admin_id: int, user: types.User, participant: dict):
    """Уведомляет админа о новом запросе на чат."""
    global bot
    try:
        ozon_id = participant.get("Ozon ID", "Не указан")
        user_name = user.first_name or "Пользователь"
        username = f"@{user.username}" if user.username else "нет username"
        
        text = (
            f"💬 <b>Новый запрос на чат</b>\n\n"
            f"👤 <b>Пользователь:</b> {user_name} ({username})\n"
            f"🆔 <b>Ozon ID:</b> {ozon_id}\n"
            f"🆔 <b>Telegram ID:</b> {user.id}\n\n"
            f"Пользователь запросил возможность связаться с тобой. Ожидай сообщения от него."
        )
        
        await bot.send_message(admin_id, text, parse_mode="HTML")
    except Exception as e:
        print(f"⚠️ Не удалось отправить уведомление админу: {e}")

def format_number(num):
    """Форматирует число с пробелами."""
    try:
        return f"{float(num):,.2f}".replace(',', ' ').replace('.', ',')
    except (ValueError, TypeError):
        return "0,00"

def format_int(num):
    """Форматирует целое число с пробелами."""
    try:
        return f"{int(num):,}".replace(',', ' ')
    except (ValueError, TypeError):
        return "0"

async def generate_participant_analytics(ozon_id: str) -> list[str]:
    """Генерирует подробную аналитику по участнику. Возвращает список строк для отправки."""
    
    def split_text(text: str, max_length: int = 4000) -> list[str]:
        """Разбивает текст на части по max_length символов."""
        if len(text) <= max_length:
            return [text]
        
        parts = []
        current_part = ""
        
        for line in text.split('\n'):
            if len(current_part) + len(line) + 1 <= max_length:
                current_part += line + '\n'
            else:
                if current_part:
                    parts.append(current_part.strip())
                current_part = line + '\n'
        
        if current_part:
            parts.append(current_part.strip())
        
        return parts
    
    try:
        # Получаем базовую информацию
        participant = await asyncio.to_thread(find_participant_by_ozon_id, ozon_id)
        if not participant:
            return ["❌ Участник не найден"]
        
        # Получаем статистику
        user_stats = await asyncio.to_thread(get_user_orders_stats, ozon_id)
        summary = await asyncio.to_thread(get_user_orders_summary, ozon_id)
        total_bonuses = await asyncio.to_thread(get_user_bonuses, ozon_id)
        settings = await asyncio.to_thread(get_bonus_settings)
        max_levels = settings.max_levels if settings else 3
        referrals_by_level = await asyncio.to_thread(get_referrals_by_level, ozon_id, max_level=max_levels)
        
        # Формируем аналитику
        analytics_text = ""
        
        # 1. Базовая информация
        analytics_text += "=" * 50 + "\n"
        analytics_text += f"📊 ПОДРОБНАЯ АНАЛИТИКА ПО УЧАСТНИКУ\n"
        analytics_text += "=" * 50 + "\n\n"
        
        analytics_text += "👤 <b>БАЗОВАЯ ИНФОРМАЦИЯ</b>\n\n"
        analytics_text += f"Ozon ID: <code>{participant.get('Ozon ID', 'Не указан')}</code>\n"
        analytics_text += f"Имя / ник: {participant.get('Имя / ник', 'Не указано')}\n"
        analytics_text += f"Telegram @: {participant.get('Телеграм @', 'Не указан')}\n"
        analytics_text += f"Telegram ID: <code>{participant.get('Telegram ID', 'Не указан')}</code>\n"
        analytics_text += f"Дата регистрации: {participant.get('Дата регистрации', 'Не указана')}\n"
        
        referrer_id = participant.get('ID пригласившего')
        if referrer_id:
            referrer = await asyncio.to_thread(find_participant_by_ozon_id, referrer_id)
            if referrer:
                analytics_text += f"Реферер: {referrer.get('Имя / ник', 'Не указано')} (Ozon ID: {referrer_id})\n"
            else:
                analytics_text += f"Реферер: Ozon ID {referrer_id} (не найден в базе)\n"
        else:
            analytics_text += "Реферер: Нет реферера\n"
        
        analytics_text += "\n"
        
        # 2. Статистика по заказам
        analytics_text += "📦 <b>СТАТИСТИКА ПО ТОВАРАМ</b>\n\n"
        analytics_text += f"Всего доставлено товаров: <b>{user_stats['delivered_count']}</b>\n"
        analytics_text += f"Общая сумма доставленных: <b>{format_number(user_stats['total_sum'])}</b> ₽\n"
        analytics_text += f"Всего товаров (с даты регистрации): <b>{summary['total_orders']}</b>\n"
        analytics_text += f"Общая сумма всех товаров: <b>{format_number(summary['total_sum'])}</b> ₽\n\n"
        
        # Словарь для перевода статусов
        status_names = {
            "delivered": "✅ Доставлено",
            "delivering": "🚚 В доставке",
            "awaiting_packaging": "📦 Ожидает упаковки",
            "awaiting_deliver": "⏳ Ожидает доставки",
            "cancelled": "❌ Отменено",
        }
        
        if summary.get('by_status'):
            analytics_text += "Распределение по статусам:\n"
            
            sorted_statuses = sorted(
                summary['by_status'].items(),
                key=lambda x: x[1]['count'],
                reverse=True
            )
            
            for status, data in sorted_statuses:
                status_name = status_names.get(status, f"❓ {status}")
                count = data.get('count', 0)
                sum_amount = data.get('sum', 0.0)
                analytics_text += f"  {status_name}: {count} заказ(ов) — {format_number(sum_amount)} ₽\n"
        
        analytics_text += "\n"
        
        # Получаем последние 10 заказов
        def get_last_orders(ozon_id: str, limit: int = 10):
            """Получает последние заказы участника (без фильтрации по дате регистрации для админа)."""
            from db_manager import get_orders_db_session
            db = get_orders_db_session("wistery")
            try:
                # Убираем фильтрацию по дате регистрации, чтобы показывать все заказы админу
                query = db.query(Posting).filter(Posting.buyer_id == str(ozon_id))
                
                postings = query.order_by(Posting.created_at.desc()).limit(limit).all()
                return postings
            finally:
                db.close()
        
        last_orders = await asyncio.to_thread(get_last_orders, ozon_id, 10)
        
        if last_orders:
            analytics_text += "📋 <b>ПОСЛЕДНИЕ 10 ЗАКАЗОВ</b>\n\n"
            
            from db_manager import get_orders_db_session, OrderItem
            order_db = get_orders_db_session("wistery")
            try:
                for i, posting in enumerate(last_orders, 1):
                    order_date = posting.created_at.strftime("%d.%m.%Y %H:%M") if posting.created_at else "Не указана"
                    status = posting.status or "unknown"
                    status_name = status_names.get(status, f"❓ {status}")
                    
                    # Вычисляем сумму заказа: товары + доставка
                    items = order_db.query(OrderItem).filter(OrderItem.posting_number == posting.posting_number).all()
                    total_items_price = sum(
                        float(item.price * (item.quantity - (item.returned_quantity or 0))) 
                        for item in items
                    )
                    delivery_price = float(posting.delivery_price or 0)
                    total_price = total_items_price + delivery_price
                    price = format_number(str(total_price)) if total_price > 0 else "0,00"
                    
                    order_id = posting.order_id or posting.order_number or "Не указан"
                    
                    analytics_text += f"{i}. <b>{order_date}</b>\n"
                    analytics_text += f"   Статус: {status_name}\n"
                    analytics_text += f"   Сумма: {price} ₽\n"
                    analytics_text += f"   Номер заказа: <code>{order_id}</code>\n\n"
            finally:
                order_db.close()
        else:
            analytics_text += "📋 <b>ПОСЛЕДНИЕ 10 ЗАКАЗОВ</b>\n\n"
            analytics_text += "Заказы не найдены\n\n"
        
        # 3. Бонусы
        analytics_text += "💰 <b>БОНУСЫ</b>\n\n"
        analytics_text += f"Всего начислено бонусов: <b>{format_number(total_bonuses)}</b> ₽\n\n"
        
        analytics_text += "Бонусы по уровням:\n"
        for level in range(1, max_levels + 1):
            level_bonuses = await asyncio.to_thread(get_user_bonuses, ozon_id, level=level)
            if level_bonuses > 0:
                analytics_text += f"  Уровень {level}: {format_number(level_bonuses)} ₽\n"
        
        analytics_text += "\n"
        
        # 4. Реферальная программа
        analytics_text += "👥 <b>РЕФЕРАЛЬНАЯ ПРОГРАММА</b>\n\n"
        
        total_referrals = 0
        total_referral_orders = 0
        total_referral_sum = 0.0
        total_referral_bonuses = 0.0
        
        level_names = {
            1: "Уровень 1 (прямые друзья)",
            2: "Уровень 2 (друзья друзей)",
            3: "Уровень 3 (друзья друзей друзей)",
        }
        
        for level in range(1, max_levels + 1):
            referral_ids = referrals_by_level.get(level, [])
            
            if referral_ids:
                referrals_stats = await asyncio.to_thread(get_referrals_orders_stats, referral_ids)
                referrals_bonuses = await asyncio.to_thread(get_referrals_bonuses_stats, referral_ids, level)
                
                total_referrals += len(referral_ids)
                total_referral_orders += referrals_stats['orders_count']
                total_referral_sum += referrals_stats['total_sum']
                total_referral_bonuses += referrals_bonuses
                
                level_name = level_names.get(level, f"Уровень {level}")
                analytics_text += f"{level_name}:\n"
                analytics_text += f"  Участников: <b>{len(referral_ids)}</b>\n"
                analytics_text += f"  Кол-во товаров: <b>{referrals_stats['orders_count']}</b>\n"
                analytics_text += f"  Их сумма: <b>{format_number(referrals_stats['total_sum'])}</b> ₽\n"
                analytics_text += f"  Начислено бонусов: <b>{format_number(referrals_bonuses)}</b> ₽\n\n"
            else:
                level_name = level_names.get(level, f"Уровень {level}")
                analytics_text += f"{level_name}:\n"
                analytics_text += f"  Участников: 0\n"
                analytics_text += f"  Кол-во товаров: 0\n"
                analytics_text += f"  Их сумма: 0 ₽\n"
                analytics_text += f"  Начислено бонусов: 0 ₽\n\n"
        
        analytics_text += "─" * 50 + "\n"
        analytics_text += "<b>ИТОГО ПО РЕФЕРАЛЬНОЙ ПРОГРАММЕ:</b>\n"
        analytics_text += f"Всего рефералов: <b>{total_referrals}</b>\n"
        analytics_text += f"Всего товаров рефералов: <b>{total_referral_orders}</b>\n"
        analytics_text += f"Общая сумма товаров рефералов: <b>{format_number(total_referral_sum)}</b> ₽\n"
        analytics_text += f"Всего бонусов от программы: <b>{format_number(total_referral_bonuses)}</b> ₽\n"
        
        # Разбиваем на части
        return split_text(analytics_text, max_length=4000)
        
    except Exception as e:
        return [f"❌ Ошибка при генерации аналитики: {str(e)}"]

async def notify_user_about_daily_bonuses(
    referrer_telegram_id: int,
    bonus_summary: dict
) -> bool:
    """
    Отправляет уведомление пользователю о начисленных бонусах за день.
    
    Args:
        referrer_telegram_id: Telegram ID пользователя (реферера)
        bonus_summary: Словарь со сводкой бонусов (результат get_daily_bonus_summary)
    
    Returns:
        True если уведомление отправлено успешно, False в случае ошибки
    """
    global bot
    
    # Функция для форматирования чисел с пробелами
    def format_number(num):
        try:
            return f"{float(num):,.2f}".replace(',', ' ').replace('.', ',')
        except (ValueError, TypeError):
            return "0,00"
    
    try:
        if not bonus_summary or bonus_summary.get("total_amount", 0) == 0:
            # Нет начислений - не отправляем уведомление
            return False
        
        # Форматируем дату
        date = bonus_summary.get("date")
        if isinstance(date, str):
            try:
                date_obj = datetime.strptime(date, "%Y-%m-%d")
                date_str = date_obj.strftime("%d.%m.%Y")
            except:
                date_str = date
        else:
            date_str = date.strftime("%d.%m.%Y") if date else "сегодня"
        
        # Начинаем формировать текст сообщения
        text = f"💰 <b>Начисления за {date_str}</b>\n\n"
        
        # Группируем по уровням
        levels = bonus_summary.get("levels", {})
        total_amount = bonus_summary.get("total_amount", 0)
        
        # Сортируем уровни по возрастанию
        sorted_levels = sorted(levels.keys())
        
        for level in sorted_levels:
            level_data = levels[level]
            level_count = level_data.get("count", 0)
            level_amount = level_data.get("total_amount", 0)
            
            if level_count > 0 and level_amount > 0:
                text += f"🎯 <b>Уровень {level}:</b>\n"
                text += f"• Бонусов начислено: {format_number(level_amount)} ₽ ({level_count} заказ"
                
                # Правильное склонение слова "заказ"
                if level_count == 1:
                    text += ")\n\n"
                elif level_count < 5:
                    text += "а)\n\n"
                else:
                    text += "ов)\n\n"
        
        # Итого
        text += f"💵 <b>Итого:</b> {format_number(total_amount)} ₽"
        
        await bot.send_message(referrer_telegram_id, text, parse_mode="HTML")
        return True
    except Exception as e:
        print(f"⚠️ Не удалось отправить уведомление о бонусах пользователю {referrer_telegram_id}: {e}")
        return False

async def send_daily_bonus_notifications(target_date: datetime = None):
    """
    Отправляет ежедневные уведомления о начисленных бонусах всем пользователям.
    
    Args:
        target_date: Дата, за которую отправлять уведомления (по умолчанию - вчерашний день)
    """
    if target_date is None:
        # Используем вчерашний день
        target_date = datetime.now() - timedelta(days=1)
    
    print(f"🔄 Начало отправки ежедневных уведомлений о бонусах за {target_date.strftime('%d.%m.%Y')}")
    
    # Получаем всех участников программы
    participants = await asyncio.to_thread(get_all_participants)
    
    if not participants:
        print("ℹ️ Нет участников программы для отправки уведомлений")
        return
    
    # Счетчики для статистики
    sent_count = 0
    skipped_count = 0
    error_count = 0
    
    # Отправляем уведомления параллельно с ограничением через Semaphore
    semaphore = asyncio.Semaphore(10)  # Максимум 10 одновременных отправок
    
    async def send_notification_to_user(participant: dict):
        nonlocal sent_count, skipped_count, error_count
        
        async with semaphore:
            try:
                ozon_id = participant.get("Ozon ID")
                telegram_id_str = participant.get("Telegram ID")
                
                if not ozon_id or not telegram_id_str:
                    skipped_count += 1
                    return
                
                # Преобразуем Telegram ID в int
                try:
                    telegram_id = int(telegram_id_str)
                except (ValueError, TypeError):
                    print(f"⚠️ Неверный Telegram ID для участника {ozon_id}: {telegram_id_str}")
                    skipped_count += 1
                    return
                
                # Получаем сводку бонусов за день
                bonus_summary = await asyncio.to_thread(get_daily_bonus_summary, ozon_id, target_date)
                
                # Проверяем наличие начислений
                if not bonus_summary or bonus_summary.get("total_amount", 0) == 0:
                    # Нет начислений - пропускаем (не отправляем уведомление)
                    skipped_count += 1
                    return
                
                # Отправляем уведомление
                success = await notify_user_about_daily_bonuses(telegram_id, bonus_summary)
                
                if success:
                    sent_count += 1
                else:
                    error_count += 1
                    
            except Exception as e:
                print(f"⚠️ Ошибка при обработке участника {participant.get('Ozon ID', 'unknown')}: {e}")
                error_count += 1
    
    # Запускаем отправку уведомлений параллельно
    tasks = [send_notification_to_user(p) for p in participants]
    await asyncio.gather(*tasks, return_exceptions=True)
    
    print(f"✅ Отправка уведомлений завершена:")
    print(f"   📨 Отправлено: {sent_count}")
    print(f"   ⏭️  Пропущено (нет начислений): {skipped_count}")
    print(f"   ❌ Ошибок: {error_count}")

def get_moscow_time() -> datetime:
    """Получить текущее время в московском часовом поясе (UTC+3).
    
    Returns:
        datetime: Текущее время с учетом московского часового пояса
    """
    # Простое решение: добавляем 3 часа к UTC
    # Для более точной работы можно использовать pytz или zoneinfo, но это требует дополнительных зависимостей
    utc_now = datetime.now(timezone.utc)
    moscow_offset = timedelta(hours=3)
    return utc_now + moscow_offset

async def daily_notification_task():
    """
    Фоновая задача для ежедневной отправки уведомлений о бонусах.
    Запускается в 20:00 по московскому времени каждый день.
    """
    print(f"🔄 Запущена фоновая задача ежедневных уведомлений о бонусах (время отправки: 20:00 МСК)")
    
    while True:
        try:
            # Получаем текущее московское время
            moscow_time = get_moscow_time()
            current_hour = moscow_time.hour
            current_minute = moscow_time.minute
            
            # Целевое время отправки: 20:00 МСК
            target_hour = 20
            target_minute = 0
            
            # Вычисляем время до следующего запуска
            if current_hour < target_hour or (current_hour == target_hour and current_minute < target_minute):
                # Еще не наступило время отправки сегодня - ждем до 20:00
                target_datetime = moscow_time.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
            else:
                # Время уже прошло - отправляем за сегодня, следующий запуск будет завтра
                target_datetime = (moscow_time + timedelta(days=1)).replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
            
            # Вычисляем количество секунд до следующего запуска
            wait_seconds = (target_datetime - moscow_time).total_seconds()
            
            if wait_seconds > 0:
                wait_hours = wait_seconds / 3600
                print(f"⏰ Следующая отправка уведомлений через {wait_hours:.1f} часов (в {target_datetime.strftime('%d.%m.%Y %H:%M')} МСК)")
                await asyncio.sleep(wait_seconds)
                # После ожидания пересчитываем московское время
                moscow_time = get_moscow_time()
            
            # Отправляем уведомления (за вчерашний день от текущего московского времени)
            yesterday = moscow_time - timedelta(days=1)
            print(f"📨 Начало отправки ежедневных уведомлений о бонусах за {yesterday.strftime('%d.%m.%Y')}")
            await send_daily_bonus_notifications(yesterday)
            
        except asyncio.CancelledError:
            print("🛑 Фоновая задача ежедневных уведомлений отменена")
            break
        except Exception as e:
            print(f"❌ Ошибка в фоновой задаче ежедневных уведомлений: {e}")
            import traceback
            traceback.print_exc()
            # Продолжаем работу, даже если произошла ошибка
            # Ждем 1 час перед следующей попыткой
            await asyncio.sleep(3600)

def should_sync_on_startup() -> bool:
    """
    Проверяет, нужно ли выполнить синхронизацию при старте бота.
    Возвращает True, если:
    - Синхронизации еще не было, ИЛИ
    - Сейчас уже после первого времени синхронизации (12:00) МСК, а последняя синхронизация была вчера или раньше
    """
    last_sync_time = get_last_sync_timestamp()
    
    if last_sync_time is None:
        # Первый запуск - нужно синхронизировать
        return True
    
    # Получаем текущее московское время
    moscow_time = get_moscow_time()
    first_sync_hour, first_sync_minute = SYNC_TIMES[0]  # Первое время синхронизации (13:00)
    current_time = moscow_time.replace(second=0, microsecond=0)
    first_sync_time_today = moscow_time.replace(hour=first_sync_hour, minute=first_sync_minute, second=0, microsecond=0)
    
    # Если сейчас уже после первого времени синхронизации, проверяем, была ли сегодня синхронизация
    if current_time >= first_sync_time_today:
        # Проверяем, была ли синхронизация сегодня
        last_sync_date = last_sync_time.date()
        today = moscow_time.date()
        
        # Если последняя синхронизация была не сегодня, нужно синхронизировать
        return last_sync_date < today
    
    # Если сейчас до первого времени синхронизации, проверяем, была ли синхронизация вчера
    yesterday = moscow_time.date() - timedelta(days=1)
    last_sync_date = last_sync_time.date()
    
    # Если последняя синхронизация была вчера или раньше, и сейчас уже после полуночи, нужно синхронизировать
    return last_sync_date < yesterday

async def periodic_sync_task():
    """
    Фоновая задача для ежедневной синхронизации заказов.
    Запускается в 12:00 и 19:30 по московскому времени каждый день.
    """
    sync_times_str = ", ".join([f"{h:02d}:{m:02d}" for h, m in SYNC_TIMES])
    print(f"🔄 Запущена фоновая задача ежедневной синхронизации заказов (время синхронизации: {sync_times_str} МСК)")
    
    while True:
        try:
            # Получаем текущее московское время
            moscow_time = get_moscow_time()
            current_time = moscow_time.replace(second=0, microsecond=0)
            
            # Находим ближайшее время синхронизации
            target_datetime = None
            min_seconds = float('inf')
            
            for sync_hour, sync_minute in SYNC_TIMES:
                # Создаем время синхронизации на сегодня
                sync_time_today = moscow_time.replace(hour=sync_hour, minute=sync_minute, second=0, microsecond=0)
                
                # Если время уже прошло сегодня, берем на завтра
                if sync_time_today <= current_time:
                    sync_time_today = (moscow_time + timedelta(days=1)).replace(hour=sync_hour, minute=sync_minute, second=0, microsecond=0)
                
                # Вычисляем разницу в секундах
                seconds_until_sync = (sync_time_today - current_time).total_seconds()
                
                # Если это ближайшее время, сохраняем его
                if seconds_until_sync < min_seconds:
                    min_seconds = seconds_until_sync
                    target_datetime = sync_time_today
            
            # Вычисляем количество секунд до следующего запуска
            wait_seconds = (target_datetime - current_time).total_seconds()
            
            if wait_seconds > 0:
                wait_hours = wait_seconds / 3600
                print(f"⏰ Следующая синхронизация заказов через {wait_hours:.1f} часов (в {target_datetime.strftime('%d.%m.%Y %H:%M')} МСК)")
                await asyncio.sleep(wait_seconds)
                # После ожидания пересчитываем московское время
                moscow_time = get_moscow_time()
            
            # Выполняем синхронизацию
            print(f"🔄 Начало ежедневной синхронизации заказов в {moscow_time.strftime('%d.%m.%Y %H:%M')} МСК")
            await perform_auto_sync(notify_admins=True)
            
        except asyncio.CancelledError:
            print("🛑 Фоновая задача синхронизации отменена")
            break
        except Exception as e:
            print(f"❌ Критическая ошибка в фоновой задаче синхронизации: {e}")
            import traceback
            traceback.print_exc()
            # Продолжаем работу, даже если произошла ошибка
            # Ждем еще немного перед следующей попыткой
            await asyncio.sleep(60)  # 1 минута перед повтором

async def main():
    global _sync_task
    
    # Настраиваем Bot с кастомным connector для принудительного использования IPv4
    # Делаем это внутри async функции, чтобы event loop был запущен
    try:
        import aiohttp
        from aiohttp import TCPConnector
        from aiogram.client.session.aiohttp import AiohttpSession
        
        # Создаем AiohttpSession
        aiogram_session = AiohttpSession(limit=100)
        
        # Модифицируем _connector_init для использования IPv4
        # Это должно работать, если connector создается лениво при первом использовании
        aiogram_session._connector_init['family'] = socket.AF_INET
        # Устанавливаем флаг для пересоздания connector
        aiogram_session._should_reset_connector = True
        
        # Если сессия уже создана, нужно пересоздать connector
        # Проверяем, есть ли уже созданная сессия
        
        if hasattr(aiogram_session, '_session') and aiogram_session._session is not None:
            # Проверяем, есть ли connector в сессии
            connector_exists = hasattr(aiogram_session._session, '_connector') and aiogram_session._session._connector is not None
            
            if connector_exists:
                # Закрываем старый connector
                await aiogram_session._session._connector.close()
            # Закрываем старую сессию
            await aiogram_session._session.close()
            # Удаляем старую сессию, чтобы она пересоздалась с новым connector
            aiogram_session._session = None
        
        # Пересоздаем сессию с новым connector (если она была создана)
        # Это гарантирует, что сессия будет использовать модифицированный _connector_init
        if hasattr(aiogram_session, 'create_session'):
            try:
                # Вызываем create_session для пересоздания с новым connector
                await aiogram_session.create_session()
            except Exception as recreate_err:
                pass
        
        # Пересоздаем bot с кастомной сессией
        global bot
        bot = Bot(token=API_TOKEN, session=aiogram_session)
    except Exception as session_err:
        # Если не удалось создать кастомную сессию, используем стандартную
        pass
    
    # Инициализируем базу данных (создаем все таблицы, включая новые)
    try:
        await asyncio.to_thread(create_database)
    except Exception as e:
        raise
    
    # Проверяем подключение к интернету перед запуском polling
    
    # Проверяем, нужно ли выполнить синхронизацию при старте
    if should_sync_on_startup():
        print("🔄 Выполняем синхронизацию при старте (прошло достаточно времени или еще не было синхронизации)...")
        await perform_auto_sync(notify_admins=False)  # Не уведомляем при старте, чтобы не спамить
    else:
            moscow_time = get_moscow_time()
            last_sync_time = get_last_sync_timestamp()
            current_time = moscow_time.replace(second=0, microsecond=0)
            
            # Находим ближайшее время синхронизации
            next_sync_time = None
            min_seconds = float('inf')
            
            for sync_hour, sync_minute in SYNC_TIMES:
                sync_time_today = moscow_time.replace(hour=sync_hour, minute=sync_minute, second=0, microsecond=0)
                if sync_time_today <= current_time:
                    sync_time_today = (moscow_time + timedelta(days=1)).replace(hour=sync_hour, minute=sync_minute, second=0, microsecond=0)
                
                seconds_until_sync = (sync_time_today - current_time).total_seconds()
                if seconds_until_sync < min_seconds:
                    min_seconds = seconds_until_sync
                    next_sync_time = sync_time_today
            
            if last_sync_time:
                last_sync_date = last_sync_time.date()
                today = moscow_time.date()
                if last_sync_date == today:
                    sync_times_str = ", ".join([f"{h:02d}:{m:02d}" for h, m in SYNC_TIMES])
                    print(f"⏰ Синхронизация уже была выполнена сегодня ({last_sync_time.strftime('%d.%m.%Y %H:%M')}), следующая будет в {next_sync_time.strftime('%H:%M')} МСК")
                else:
                    print(f"⏰ Последняя синхронизация была {last_sync_date.strftime('%d.%m.%Y')}, следующая будет в {next_sync_time.strftime('%H:%M')} МСК")
            else:
                wait_hours = (next_sync_time - moscow_time).total_seconds() / 3600
                print(f"ℹ️ Первая синхронизация будет выполнена в {next_sync_time.strftime('%d.%m.%Y %H:%M')} МСК (через {wait_hours:.1f} часов)")
    
    # Запускаем фоновую задачу для периодической синхронизации
    _sync_task = asyncio.create_task(periodic_sync_task())
    print("✅ Фоновая задача периодической синхронизации запущена")
    
    # Запускаем фоновую задачу для ежедневных уведомлений о бонусах
    global _notification_task
    _notification_task = asyncio.create_task(daily_notification_task())
    print("✅ Фоновая задача ежедневных уведомлений о бонусах запущена")
    
    try:
        try:
            await dp.start_polling(bot)
        except Exception as polling_err:
            raise
    except Exception as e:
        print(f"Критическая ошибка в боте: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        # Отменяем фоновую задачу синхронизации
        if _sync_task and not _sync_task.done():
            print("🛑 Останавливаем фоновую задачу синхронизации...")
            _sync_task.cancel()
            try:
                await _sync_task
            except asyncio.CancelledError:
                pass
            print("✅ Фоновая задача синхронизации остановлена")
        
        # Отменяем фоновую задачу ежедневных уведомлений
        if _notification_task and not _notification_task.done():
            print("🛑 Останавливаем фоновую задачу ежедневных уведомлений...")
            _notification_task.cancel()
            try:
                await _notification_task
            except asyncio.CancelledError:
                pass
            print("✅ Фоновая задача ежедневных уведомлений остановлена")
        
        # Закрываем кастомную сессию при завершении (если она была создана)
        try:
            # Проверяем, есть ли кастомная aiogram сессия в локальной области видимости
            if 'aiogram_session' in locals():
                await aiogram_session.close()
        except Exception as close_err:
            pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Пользователь остановил бота (Ctrl+C) - не показываем traceback
        pass
    except Exception as e:
        print(f"Ошибка при запуске бота: {e}")
        import traceback
        traceback.print_exc()
        raise