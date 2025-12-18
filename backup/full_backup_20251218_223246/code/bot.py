"""
Главный файл бота - инициализация и запуск.
"""
import asyncio
import logging
import os
import socket
import sys
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from config import API_TOKEN
from db_manager import create_database
from tasks.background_tasks import (
    perform_auto_sync,
    daily_notification_task,
    should_sync_on_startup,
    periodic_sync_task
)
from utils.helpers import get_moscow_time
from config import SYNC_TIMES
from datetime import timedelta
from db_manager import get_last_sync_timestamp

logging.basicConfig(level=logging.INFO)

# Создаем Bot и Dispatcher на уровне модуля (handlers с декораторами требуют этого)
# Проверяем, были ли уже созданы bot и dp в любом из модулей (__main__ или bot)
_module_main = sys.modules.get('__main__')
_module_bot = sys.modules.get('bot')
_existing_bot = None
_existing_dp = None

# Ищем существующие bot и dp в любом из модулей
if _module_main and hasattr(_module_main, 'bot') and hasattr(_module_main, 'dp'):
    _existing_bot = _module_main.bot
    _existing_dp = _module_main.dp
elif _module_bot and hasattr(_module_bot, 'bot') and hasattr(_module_bot, 'dp'):
    _existing_bot = _module_bot.bot
    _existing_dp = _module_bot.dp

if _existing_bot is None or _existing_dp is None:
    # bot или dp еще не созданы, создаем их
    bot = Bot(token=API_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    # Сохраняем bot и dp во все возможные модули
    if _module_main:
        _module_main.bot = bot
        _module_main.dp = dp
    if _module_bot:
        _module_bot.bot = bot
        _module_bot.dp = dp
else:
    # Используем существующие bot и dp
    bot = _existing_bot
    dp = _existing_dp

# Глобальные переменные для фоновых задач
_sync_task: asyncio.Task = None
_notification_task: asyncio.Task = None


async def main():
    global _sync_task, _notification_task
    
    # Настраиваем Bot с кастомным connector для принудительного использования IPv4
    # Делаем это внутри async функции, чтобы event loop был запущен
    try:
        import aiohttp
        from aiohttp import TCPConnector
        from aiogram.client.session.aiohttp import AiohttpSession
        
        # Создаем AiohttpSession
        aiogram_session = AiohttpSession(limit=100)
        
        # Модифицируем _connector_init для использования IPv4
        aiogram_session._connector_init['family'] = socket.AF_INET
        aiogram_session._should_reset_connector = True
        
        # Если сессия уже создана, нужно пересоздать connector
        if hasattr(aiogram_session, '_session') and aiogram_session._session is not None:
            connector_exists = hasattr(aiogram_session._session, '_connector') and aiogram_session._session._connector is not None
            
            if connector_exists:
                await aiogram_session._session._connector.close()
            await aiogram_session._session.close()
            aiogram_session._session = None
        
        # Пересоздаем сессию с новым connector
        if hasattr(aiogram_session, 'create_session'):
            try:
                await aiogram_session.create_session()
            except Exception:
                pass
        
        # Пересоздаем bot с кастомной сессией
        global bot
        bot = Bot(token=API_TOKEN, session=aiogram_session)
    except Exception:
        # Если не удалось создать кастомную сессию, используем стандартную
        pass
    
    # Инициализируем базу данных
    try:
        await asyncio.to_thread(create_database)
    except Exception as e:
        raise
    
    # Импортируем handlers (те, что используют декораторы, регистрируются автоматически)
    # Те, что используют register_handlers, регистрируем вручную
    try:
        # Импортируем handlers (те, что используют декораторы, регистрируются автоматически при импорте)
        import handlers  # Импорт handlers/__init__.py регистрирует handlers с декораторами
        # Регистрируем handlers, передавая dp (те, которые используют register_handlers)
        from handlers import common_handlers, registration_handlers, user_handlers, withdrawal_handlers, leaving_handlers
        common_handlers.register_handlers(dp)
        registration_handlers.register_handlers(dp)
        user_handlers.register_handlers(dp)
        withdrawal_handlers.register_handlers(dp)
        leaving_handlers.register_handlers(dp)
    except Exception as e:
        raise
    
    # Проверяем, нужно ли выполнить синхронизацию при старте
    if should_sync_on_startup():
        print("🔄 Выполняем синхронизацию при старте (прошло достаточно времени или еще не было синхронизации)...")
        await perform_auto_sync(bot, notify_admins=False)  # Не уведомляем при старте
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
    _sync_task = asyncio.create_task(periodic_sync_task(bot))
    print("✅ Фоновая задача периодической синхронизации запущена")
    
    # Запускаем фоновую задачу для ежедневных уведомлений о бонусах
    _notification_task = asyncio.create_task(daily_notification_task(bot))
    print("✅ Фоновая задача ежедневных уведомлений о бонусах запущена")
    
    try:
        await dp.start_polling(bot)
    except Exception as e:
        print(f"Критическая ошибка в боте: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        # Отменяем фоновые задачи
        if _sync_task and not _sync_task.done():
            print("🛑 Останавливаем фоновую задачу синхронизации...")
            _sync_task.cancel()
            try:
                await _sync_task
            except asyncio.CancelledError:
                pass
            print("✅ Фоновая задача синхронизации остановлена")
        
        if _notification_task and not _notification_task.done():
            print("🛑 Останавливаем фоновую задачу ежедневных уведомлений...")
            _notification_task.cancel()
            try:
                await _notification_task
            except asyncio.CancelledError:
                pass
            print("✅ Фоновая задача ежедневных уведомлений остановлена")
        
        # Закрываем кастомную сессию при завершении
        try:
            if hasattr(bot, 'session') and bot.session:
                await bot.session.close()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())

