import asyncio
import logging
import os
import socket

from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv

from db_manager import (
    find_participant_by_telegram_id,
    find_participant_by_ozon_id,
    create_participant,
    create_database,
    get_user_orders_stats,
    get_referrals_by_level,
    get_referrals_orders_stats,
    get_user_bonuses,
    get_referrals_bonuses_stats,
    get_bonus_settings,
    update_bonus_settings,
    clear_bonus_settings_cache,
    get_last_sync_timestamp,
)

from states import Registration, BonusSettings
# ИМПОРТ ДЛЯ СИНХРОНИЗАЦИИ ЗАКАЗОВ
from orders_updater import update_orders_sheet, fill_customers_from_existing_orders 

# грузим переменные из .env
from datetime import datetime, timedelta
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
# СОЗДАНИЕ КЛАВИАТУР С КНОПКАМИ
# =========================================================
async def get_referral_link(bot: Bot, telegram_id: int) -> str:
    """Генерирует реферальную ссылку для пользователя."""
    me = await bot.get_me()
    bot_username = me.username
    return f"https://t.me/{bot_username}?start={telegram_id}"

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
                KeyboardButton(text="🔄 Синхронизировать заказы"),
                KeyboardButton(text="📊 Моя статистика"),
            ],
            [
                KeyboardButton(text="📦 Мои заказы"),
                KeyboardButton(text="👥 Управление"),
            ],
            [
                KeyboardButton(text="📈 Аналитика"),
                KeyboardButton(text="⚙️ Настройки"),
            ],
            [
                KeyboardButton(text="👥 Пригласить друга"),
                KeyboardButton(text="🔄 Заполнить старые клиенты"),
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
    # Сохраняем Ozon ID реферера, если он был найден
    await state.update_data(referrer_id=referrer_ozon_id)

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
        result = await asyncio.to_thread(update_orders_sheet)
        
        # Проверка структуры результата
        if not isinstance(result, dict):
            await message.answer(
                "❌ Ошибка: неверный формат результата синхронизации.",
                reply_markup=get_keyboard(message.from_user.id)
            )
            return
        
        # Форматируем период для отображения
        
        period_start = result.get("period_start")
        period_end = result.get("period_end")
        
        if period_start is None or period_end is None:
            await message.answer(
                "❌ Ошибка: отсутствует информация о периоде синхронизации.",
                reply_markup=get_keyboard(message.from_user.id)
            )
            return
        
        # Форматируем даты в читаемый вид (DD.MM.YYYY HH:MM)
        period_start_str = period_start.strftime("%d.%m.%Y %H:%M")
        period_end_str = period_end.strftime("%d.%m.%Y %H:%M")
        
        # Получаем статистику по статусам за первый день периода
        first_day_stats = result.get("first_day_stats", {})
        
        # Формируем строку со статистикой по статусам
        status_stats_text = ""
        if first_day_stats and first_day_stats.get("total", 0) > 0:
            first_day_date = period_start_str.split()[0]  # Берем только дату без времени
            status_stats_text = f"\n\n📊 <b>Статистика за {first_day_date}:</b>\n"
            status_stats_text += f"Всего заказов: <b>{first_day_stats['total']}</b>\n"
            
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
                status_stats_text += f"\n⚠️ Активных заказов: <b>{first_day_stats['active_count']}</b>"
        
        if result["count"] > 0:
            text = (
                f"🎉 Синхронизация завершена! 🎉\n\n"
                f"✅ Добавлено <b>{result['count']}</b> новых заказов\n"
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
                f"Новых заказов не найдено.\n\n"
                f"📅 <b>Период проверки:</b>\n"
                f"С: {period_start_str}\n"
                f"По: {period_end_str}"
                f"{status_stats_text}"
            )

        await message.answer(text, parse_mode="HTML", reply_markup=get_keyboard(message.from_user.id))
        
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
@dp.message(lambda message: message.text == "🔄 Синхронизировать заказы")
async def sync_orders_button_handler(message: types.Message):
    """Обработчик кнопки 'Синхронизировать заказы'."""
    
    # Проверка прав администратора
    if not is_admin(message.from_user.id):
        await message.answer(
            "❌ У тебя нет прав для выполнения этой команды.",
            reply_markup=get_keyboard(message.from_user.id)
        )
        return
    await sync_orders_handler(message)

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
        
        # Формируем текст
        text = (
            f"📊 Моя статистика\n\n"
            f"👤 Информация:\n"
            f"• Ozon ID: {ozon_id}\n"
            f"• Дата регистрации: {reg_date}\n\n"
            f"📦 Мои заказы:\n"
            f"• Всего доставлено заказов: {user_stats['delivered_count']}\n"
            f"• Общая сумма: {format_number(user_stats['total_sum'])} ₽\n"
            f"• Начислено бонусов: {format_number(user_bonuses)} ₽\n\n"
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
                    f"• Кол-во заказов: {referrals_stats['orders_count']}\n"
                    f"• Их сумма: {format_number(referrals_stats['total_sum'])} ₽\n"
                    f"• Начислено бонусов: {format_number(referrals_bonuses)} ₽\n\n"
                )
            else:
                text += (
                    f"{level_name}:\n"
                    f"• Участников: 0\n"
                    f"• Кол-во заказов: 0\n"
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
    
    # Здесь можно добавить логику получения заказов из БД
    ozon_id = participant.get('Ozon ID')
    text = (
        f"📦 <b>Твои заказы</b>\n\n"
        f"Ozon ID: <code>{ozon_id}</code>\n\n"
        f"Функция просмотра заказов будет доступна в ближайшее время."
    )
    await message.answer(text, parse_mode="HTML", reply_markup=get_keyboard(user.id))

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

@dp.message(lambda message: message.text == "❓ Помощь")
async def help_handler(message: types.Message):
    """Обработчик кнопки 'Помощь'."""
    user_id = message.from_user.id
    is_admin_user = is_admin(user_id)
    
    text = (
        "❓ <b>Помощь</b>\n\n"
        "📝 <b>Как найти свой Ozon ID?</b>\n\n"
        "Твой Ozon ID — это первые цифры номера любого твоего заказа до тире.\n\n"
        "<b>Пример:</b>\n"
        "• Номер заказа: 10054917-1093-1\n"
        "• Твой Ozon ID: 10054917\n\n"
        "💡 <b>Совет:</b> Можешь отправить полный номер заказа, я сам выделю нужные цифры.\n\n"
        "💡 <b>Доступные команды:</b>\n"
        "/start - Начать регистрацию\n"
    )
    
    if is_admin_user:
        text += "/sync_orders - Синхронизировать заказы (только для админов)\n"
    
    text += "/test_db - Проверить подключение к БД\n\n"
    text += "Или используй кнопки ниже для быстрого доступа к функциям."
    
    await message.answer(text, parse_mode="HTML", reply_markup=get_keyboard(user_id))

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

@dp.message(lambda message: message.text == "📈 Аналитика")
async def analytics_handler(message: types.Message):
    """Обработчик кнопки 'Аналитика' (только для админов)."""
    user_id = message.from_user.id
    
    if not is_admin(user_id):
        await message.answer(
            "❌ У тебя нет прав для выполнения этой команды.",
            reply_markup=get_keyboard(user_id)
        )
        return
    
    text = (
        "📈 <b>Аналитика</b>\n\n"
        "Функция аналитики будет доступна в ближайшее время.\n\n"
        "Здесь можно будет:\n"
        "• Просматривать общую статистику по заказам\n"
        "• Анализировать продажи\n"
        "• Получать отчеты по периодам"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=get_keyboard(user_id))

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
    settings = await asyncio.to_thread(get_bonus_settings)
    
    text = (
        "⚙️ <b>Настройки</b>\n\n"
        "💰 <b>Настройки бонусной программы:</b>\n\n"
        f"Количество уровней: <b>{settings.max_levels}</b>\n\n"
    )
    
    for level in range(1, min(settings.max_levels + 1, 6)):  # Ограничиваем до 5 уровней
        percent = getattr(settings, f'level_{level}_percent', 0.0)
        if percent is not None:
            text += f"Уровень {level}: <b>{percent}%</b>\n"
    
    # Создаем inline-клавиатуру
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Изменить количество уровней", callback_data="bonus_edit_levels")],
        [InlineKeyboardButton(text="📝 Изменить проценты бонусов", callback_data="bonus_edit_percents")],
        [InlineKeyboardButton(text="❌ Закрыть", callback_data="bonus_settings_close")]
    ])
    
    await message.answer(text, parse_mode="HTML", reply_markup=keyboard)

@dp.callback_query(lambda c: c.data == "bonus_settings_close")
async def bonus_settings_close_handler(callback: types.CallbackQuery):
    """Закрыть настройки бонусов."""
    await callback.answer()
    await callback.message.delete()

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
    
    # Создаем кнопки для каждого уровня (ограничиваем до 5 уровней)
    keyboard_buttons = []
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
    
    level = int(callback.data.split("_")[-1])
    await callback.answer()
    await state.set_state(BonusSettings.editing_percent)
    await state.update_data(editing_level=level)
    
    settings = await asyncio.to_thread(get_bonus_settings)
    current_percent = getattr(settings, f'level_{level}_percent', 0.0)
    
    text = (
        f"📝 <b>Редактирование процента для уровня {level}</b>\n\n"
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
        if levels < 1 or levels > 5:
            await message.answer("❌ Количество уровней должно быть от 1 до 5. Попробуй еще раз:")
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
        if percent < 0 or percent > 100:
            await message.answer("❌ Процент должен быть от 0 до 100. Попробуй еще раз:")
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

@dp.message(lambda message: message.text == "🔄 Заполнить старые клиенты")
async def fill_old_customers_handler(message: types.Message):
    """Обработчик для заполнения таблицы customers из существующих заказов."""
    user_id = message.from_user.id
    
    if not is_admin(user_id):
        await message.answer(
            "❌ У тебя нет прав для выполнения этой команды",
            reply_markup=get_keyboard(user_id)
        )
        return
    
    await message.answer("⏳ Начинаю обработку старых заказов...", reply_markup=get_keyboard(user_id))
    
    try:
        result = await asyncio.to_thread(fill_customers_from_existing_orders)
        
        text = (
            f"✅ Обработка завершена!\n\n"
            f"📦 Обработано заказов: <b>{result['processed_orders']}</b>\n"
            f"👥 Создано клиентов: <b>{result['customers_created']}</b>\n"
            f"🔄 Обновлено клиентов: <b>{result['customers_updated']}</b>\n"
            f"📊 Всего уникальных клиентов: <b>{result['total_customers']}</b>"
        )
        
        await message.answer(text, parse_mode="HTML", reply_markup=get_keyboard(user_id))
    except Exception as e:
        await message.answer(
            f"❌ Ошибка при обработке: {str(e)}",
            reply_markup=get_keyboard(user_id)
        )

# =========================================================
# 4. ОБРАБОТЧИК СОСТОЯНИЯ (Получение Ozon ID)
# =========================================================
@dp.message(Registration.waiting_for_ozon_id)
async def process_ozon_id(message: types.Message, state: FSMContext):
    # Проверяем, не нажата ли кнопка вместо ввода ID
    button_texts = ["🔄 Синхронизировать заказы", "📊 Моя статистика", "📦 Мои заказы", 
                     "❓ Помощь", "👥 Управление", "📈 Аналитика", "⚙️ Настройки", 
                     "👥 Пригласить друга", "🔄 Заполнить старые клиенты"]
    if message.text in button_texts:
        # Если нажата кнопка, обрабатываем её соответствующим обработчиком
        return
    
    user_input = message.text.strip()
    user = message.from_user
    
    # Извлекаем Ozon ID из ввода:
    # - Если есть тире, берем первые цифры до тире (например, "10054917-1093-1" -> "10054917")
    # - Если только цифры, используем как есть
    if "-" in user_input:
        ozon_id = user_input.split("-")[0].strip()
    else:
        ozon_id = user_input
    
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

# Интервал синхронизации (12 часов)
SYNC_INTERVAL_HOURS = 12
SYNC_INTERVAL_SECONDS = SYNC_INTERVAL_HOURS * 3600

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
        result = await asyncio.to_thread(update_orders_sheet)
        
        if isinstance(result, dict) and result.get("count", 0) >= 0:
            print(f"✅ Автоматическая синхронизация завершена успешно. Добавлено заказов: {result.get('count', 0)}")
            
            # Уведомляем админов, если запрошено
            if notify_admins and result.get("count", 0) > 0:
                await notify_admins_about_sync(result)
            
            return True
        else:
            print(f"⚠️ Автоматическая синхронизация завершена, но результат неожиданный: {result}")
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

async def notify_admins_about_sync(result: dict):
    """Отправляет уведомление админам об успешной синхронизации."""
    global bot
    try:
        period_start = result.get("period_start")
        period_end = result.get("period_end")
        
        if period_start and period_end:
            period_start_str = period_start.strftime("%d.%m.%Y %H:%M")
            period_end_str = period_end.strftime("%d.%m.%Y %H:%M")
        else:
            period_start_str = "не указано"
            period_end_str = "не указано"
        
        text = (
            f"🤖 <b>Автоматическая синхронизация завершена</b>\n\n"
            f"✅ Добавлено <b>{result.get('count', 0)}</b> новых заказов\n"
            f"👥 Обработано <b>{result.get('customers_count', 0)}</b> клиентов\n"
            f"📅 Период: {period_start_str} - {period_end_str}"
        )
        
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(admin_id, text, parse_mode="HTML")
            except Exception as e:
                print(f"⚠️ Не удалось отправить уведомление админу {admin_id}: {e}")
    except Exception as e:
        print(f"⚠️ Ошибка при отправке уведомлений админам: {e}")

async def notify_admins_about_sync_error(error_msg: str):
    """Отправляет уведомление админам об ошибке синхронизации."""
    global bot
    try:
        text = (
            f"❌ <b>Ошибка автоматической синхронизации</b>\n\n"
            f"<code>{error_msg}</code>"
        )
        
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(admin_id, text, parse_mode="HTML")
            except Exception as e:
                print(f"⚠️ Не удалось отправить уведомление об ошибке админу {admin_id}: {e}")
    except Exception as e:
        print(f"⚠️ Ошибка при отправке уведомлений об ошибке админам: {e}")

def should_sync_on_startup() -> bool:
    """
    Проверяет, нужно ли выполнить синхронизацию при старте бота.
    Возвращает True, если прошло более 12 часов с последней синхронизации.
    """
    last_sync_time = get_last_sync_timestamp()
    
    if last_sync_time is None:
        # Первый запуск - нужно синхронизировать
        return True
    
    time_since_last_sync = datetime.now() - last_sync_time
    return time_since_last_sync >= timedelta(hours=SYNC_INTERVAL_HOURS)

async def periodic_sync_task():
    """
    Фоновая задача для периодической синхронизации каждые 12 часов.
    """
    global _sync_task
    
    print(f"🔄 Запущена фоновая задача периодической синхронизации (интервал: {SYNC_INTERVAL_HOURS} часов)")
    
    while True:
        try:
            # Ждем 12 часов
            await asyncio.sleep(SYNC_INTERVAL_SECONDS)
            
            # Выполняем синхронизацию
            await perform_auto_sync(notify_admins=True)
            
        except asyncio.CancelledError:
            print("🛑 Фоновая задача синхронизации отменена")
            break
        except Exception as e:
            print(f"❌ Критическая ошибка в фоновой задаче синхронизации: {e}")
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
        print("🔄 Прошло более 12 часов с последней синхронизации, выполняем синхронизацию при старте...")
        await perform_auto_sync(notify_admins=False)  # Не уведомляем при старте, чтобы не спамить
    else:
        last_sync_time = get_last_sync_timestamp()
        if last_sync_time:
            time_since = datetime.now() - last_sync_time
            print(f"⏰ Последняя синхронизация была {time_since.total_seconds() / 3600:.1f} часов назад, пропускаем при старте")
        else:
            print("ℹ️ Первая синхронизация будет выполнена через 12 часов")
    
    # Запускаем фоновую задачу для периодической синхронизации
    _sync_task = asyncio.create_task(periodic_sync_task())
    print("✅ Фоновая задача периодической синхронизации запущена")
    
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
    except Exception as e:
        print(f"Ошибка при запуске бота: {e}")
        import traceback
        traceback.print_exc()
        raise