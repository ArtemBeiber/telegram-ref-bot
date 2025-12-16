# db_manager.py

import os
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from datetime import datetime

# >>> НАЧАЛО БЛОКА: КОНФИГУРАЦИЯ БАЗЫ ДАННЫХ <<<
DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "referral_orders.db")
DATABASE_URL = f"sqlite:///{DB_FILE}"

engine = create_engine(DATABASE_URL)

Base = declarative_base()  # SQLAlchemy 2.0+

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# >>> КОНЕЦ БЛОКА: КОНФИГУРАЦИЯ БАЗЫ ДАННЫХ <<<

# >>> НАЧАЛО БЛОКА: ОПРЕДЕЛЕНИЕ МОДЕЛИ ТАБЛИЦЫ "orders" <<<
class Order(Base):
    """Модель для хранения заказов Ozon."""
    
    __tablename__ = "orders"
    
    # 1. Основные поля
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    order_id = Column(String, unique=False, index=True) # Номер заказа (не unique, так как может быть несколько товаров)
    posting_number = Column(String, unique=True, index=True) # Номер отправления (Должен быть уникальным для строки заказа/товара)
    status = Column(String) 
    created_at = Column(DateTime, default=datetime.utcnow) # ИСПРАВЛЕНО
    
    # 2. Поля для аналитики и пользователя
    buyer_id = Column(String, index=True) # ID покупателя (ключ для рефералов)
    price_amount = Column(String) 
    item_name = Column(String) 
    item_sku = Column(String) 
    quantity = Column(String) 
    
    # 3. Дополнительные поля (Исправлены типы)
    delivering_date = Column(String)
    in_process_at = Column(String)
    cluster_from = Column(String)
    cluster_to = Column(String)
    address = Column(String)
    
    sync_time = Column(DateTime, default=datetime.utcnow) # ИСПРАВЛЕНО
    
    currency_code = Column(String)
    articul = Column(String)
    buyer_paid = Column(String)
    shipping_cost = Column(String)
    is_redeemed = Column(String)
    price_before_discount = Column(String)
    discount_percent = Column(String) # ИСПРАВЛЕНО
    discount_rub = Column(String)
    promotion = Column(String)
    weight_kg = Column(String)
    norm_delivery_time = Column(String)
    shipping_evaluation = Column(String)
    shipping_warehouse = Column(String)
    delivery_region = Column(String)
    delivery_city = Column(String)
    delivery_method = Column(String)
    client_segment = Column(String)
    is_legal_entity = Column(String)
    payment_method = Column(String)
    
# >>> КОНЕЦ БЛОКА: ОПРЕДЕЛЕНИЕ МОДЕЛИ ТАБЛИЦЫ "orders" <<<

# >>> НАЧАЛО БЛОКА: ОПРЕДЕЛЕНИЕ МОДЕЛИ ТАБЛИЦЫ "customers" <<<
class Customer(Base):
    """Модель для хранения информации о клиентах Ozon."""
    
    __tablename__ = "customers"
    
    # Основные поля
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    buyer_id = Column(String, unique=True, index=True)  # ID покупателя (уникальный ключ)
    
    # Контактная информация
    name = Column(String)  # Имя клиента
    phone = Column(String)  # Телефон
    email = Column(String)  # Email
    
    # Адресная информация
    address = Column(String)  # Полный адрес
    delivery_region = Column(String)  # Регион доставки
    delivery_city = Column(String)  # Город доставки
    cluster_to = Column(String)  # Кластер доставки
    
    # Дополнительная информация
    client_segment = Column(String)  # Сегмент клиента
    is_legal_entity = Column(String)  # Юридическое лицо (да/нет)
    payment_method = Column(String)  # Способ оплаты
    
    # Статистика
    total_orders = Column(Integer, default=0)  # Общее количество заказов
    total_spent = Column(String, default="0")  # Общая сумма покупок
    
    # Временные метки
    first_order_date = Column(DateTime)  # Дата первого заказа
    last_order_date = Column(DateTime)  # Дата последнего заказа
    created_at = Column(DateTime, default=datetime.utcnow)  # Дата создания записи
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)  # Дата обновления
    
# >>> КОНЕЦ БЛОКА: ОПРЕДЕЛЕНИЕ МОДЕЛИ ТАБЛИЦЫ "customers" <<<

# >>> НАЧАЛО БЛОКА: ОПРЕДЕЛЕНИЕ МОДЕЛИ ТАБЛИЦЫ "participants" <<<
class Participant(Base):
    """Модель для хранения участников реферальной программы."""
    
    __tablename__ = "participants"
    
    # Основные поля
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    ozon_id = Column(String, unique=True, index=True)  # Ozon ID (уникальный ключ)
    telegram_id = Column(String, unique=True, index=True)  # Telegram ID (уникальный ключ)
    
    # Информация о пользователе
    name = Column(String)  # Имя / ник
    username = Column(String)  # Telegram username (с @)
    
    # Реферальная информация
    referrer_id = Column(String, index=True)  # ID пригласившего (ozon_id реферера)
    
    # Дополнительная информация
    language = Column(String)  # Язык пользователя
    is_active = Column(Integer, default=1)  # Флаг активности (1 = активен, 0 = неактивен)
    
    # Временные метки
    registration_date = Column(DateTime, default=datetime.utcnow)  # Дата регистрации
    deactivated_at = Column(DateTime, nullable=True)  # Дата деактивации (если участник вышел)
    created_at = Column(DateTime, default=datetime.utcnow)  # Дата создания записи
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)  # Дата обновления
    
# >>> КОНЕЦ БЛОКА: ОПРЕДЕЛЕНИЕ МОДЕЛИ ТАБЛИЦЫ "participants" <<<

# >>> НАЧАЛО БЛОКА: ОПРЕДЕЛЕНИЕ МОДЕЛИ ТАБЛИЦЫ "sync_settings" <<<
class SyncSettings(Base):
    """Модель для хранения настроек синхронизации."""
    
    __tablename__ = "sync_settings"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    key = Column(String, unique=True, index=True)  # Ключ настройки (например, "last_sync_time")
    value = Column(String)  # Значение настройки (храним как строку, парсим при использовании)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)  # Дата обновления
    
# >>> КОНЕЦ БЛОКА: ОПРЕДЕЛЕНИЕ МОДЕЛИ ТАБЛИЦЫ "sync_settings" <<<

# >>> НАЧАЛО БЛОКА: ОПРЕДЕЛЕНИЕ МОДЕЛИ ТАБЛИЦЫ "bonus_settings" <<<
class BonusSettings(Base):
    """Модель для хранения настроек бонусной программы."""
    
    __tablename__ = "bonus_settings"
    
    id = Column(Integer, primary_key=True, index=True, default=1)  # Всегда одна запись с id=1
    max_levels = Column(Integer, default=3)  # Максимальное количество уровней (1-5)
    level_0_percent = Column(Float, default=0.0)  # Процент бонуса для уровня 0 (покупки самого участника)
    level_1_percent = Column(Float, default=5.0)  # Процент бонуса для уровня 1
    level_2_percent = Column(Float, default=3.0)  # Процент бонуса для уровня 2
    level_3_percent = Column(Float, default=1.0)  # Процент бонуса для уровня 3
    level_4_percent = Column(Float, nullable=True)  # Процент бонуса для уровня 4
    level_5_percent = Column(Float, nullable=True)  # Процент бонуса для уровня 5
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)  # Дата обновления
    
# >>> КОНЕЦ БЛОКА: ОПРЕДЕЛЕНИЕ МОДЕЛИ ТАБЛИЦЫ "bonus_settings" <<<

# >>> НАЧАЛО БЛОКА: ОПРЕДЕЛЕНИЕ МОДЕЛИ ТАБЛИЦЫ "bonus_transactions" <<<
class BonusTransaction(Base):
    """Модель для хранения начислений бонусов."""
    
    __tablename__ = "bonus_transactions"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    referrer_ozon_id = Column(String, index=True)  # Ozon ID реферера (кому начислили)
    referral_ozon_id = Column(String, index=True)  # Ozon ID реферала (чья покупка)
    posting_number = Column(String, index=True)  # ID заказа (чтобы избежать двойного начисления)
    order_sum = Column(Float)  # Сумма заказа
    bonus_percentage = Column(Float)  # Процент бонуса
    bonus_amount = Column(Float)  # Сумма бонуса
    level = Column(Integer)  # Уровень реферала (1, 2 или 3)
    created_at = Column(DateTime, default=datetime.utcnow)  # Дата начисления
    
    # Поля для управления доступностью к выводу
    is_available_for_withdrawal = Column(Integer, default=0)  # Флаг доступности к выводу (0=заблокирован, 1=доступен)
    available_at = Column(DateTime, nullable=True)  # Дата, когда бонус станет доступным (created_at + 14 дней)
    is_returned = Column(Integer, default=0)  # Флаг возврата товара (0=не возвращен, 1=возвращен)
    returned_amount = Column(Float, nullable=True)  # Сумма возврата (если был частичный возврат)
    returned_at = Column(DateTime, nullable=True)  # Дата возврата
    
# >>> КОНЕЦ БЛОКА: ОПРЕДЕЛЕНИЕ МОДЕЛИ ТАБЛИЦЫ "bonus_transactions" <<<

# >>> НАЧАЛО БЛОКА: ФУНКЦИИ ВЗАИМОДЕЙСТВИЯ С БД <<<
def migrate_bonus_settings():
    """Миграция: добавляет колонку level_0_percent в таблицу bonus_settings если её нет."""
    import sqlite3
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Проверяем, существует ли колонка level_0_percent
        cursor.execute("PRAGMA table_info(bonus_settings)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'level_0_percent' not in columns:
            # Добавляем колонку level_0_percent
            cursor.execute("ALTER TABLE bonus_settings ADD COLUMN level_0_percent REAL DEFAULT 0.0")
            # Обновляем существующие записи, устанавливая значение по умолчанию
            cursor.execute("UPDATE bonus_settings SET level_0_percent = 0.0 WHERE level_0_percent IS NULL")
            conn.commit()
            print("✅ Миграция: колонка level_0_percent добавлена в bonus_settings")
        else:
            print("ℹ️ Миграция: колонка level_0_percent уже существует")
        
        conn.close()
    except Exception as e:
        print(f"❌ Ошибка миграции: {e}")
        raise

def migrate_participants():
    """Миграция: добавляет колонки is_active и deactivated_at в таблицу participants если их нет."""
    import sqlite3
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Проверяем существующие колонки
        cursor.execute("PRAGMA table_info(participants)")
        columns = [row[1] for row in cursor.fetchall()]
        
        # Добавляем новые поля, если их нет
        new_fields = [
            ("is_active", "INTEGER DEFAULT 1"),
            ("deactivated_at", "DATETIME")
        ]
        
        for field_name, field_type in new_fields:
            if field_name not in columns:
                cursor.execute(f"ALTER TABLE participants ADD COLUMN {field_name} {field_type}")
                print(f"✅ Миграция: колонка {field_name} добавлена в participants")
                
                # Для существующих записей устанавливаем значения по умолчанию
                if field_name == "is_active":
                    cursor.execute("UPDATE participants SET is_active = 1 WHERE is_active IS NULL")
            else:
                print(f"ℹ️ Миграция: колонка {field_name} уже существует")
        
        conn.commit()
        print("✅ Миграция participants завершена")
        
        conn.close()
    except Exception as e:
        print(f"❌ Ошибка миграции participants: {e}")
        raise

def migrate_bonus_transactions():
    """Миграция: добавляет новые поля в таблицу bonus_transactions для управления доступностью к выводу."""
    import sqlite3
    from datetime import timedelta
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Проверяем существующие колонки
        cursor.execute("PRAGMA table_info(bonus_transactions)")
        columns = [row[1] for row in cursor.fetchall()]
        
        # Добавляем новые поля, если их нет
        new_fields = [
            ("is_available_for_withdrawal", "INTEGER DEFAULT 0"),
            ("available_at", "DATETIME"),
            ("is_returned", "INTEGER DEFAULT 0"),
            ("returned_amount", "REAL"),
            ("returned_at", "DATETIME")
        ]
        
        for field_name, field_type in new_fields:
            if field_name not in columns:
                cursor.execute(f"ALTER TABLE bonus_transactions ADD COLUMN {field_name} {field_type}")
                print(f"✅ Миграция: колонка {field_name} добавлена в bonus_transactions")
            else:
                print(f"ℹ️ Миграция: колонка {field_name} уже существует")
        
        # Для существующих записей устанавливаем available_at = created_at + 14 дней
        # и is_available_for_withdrawal = 0 (заблокирован)
        cursor.execute("""
            UPDATE bonus_transactions 
            SET available_at = datetime(created_at, '+14 days'),
                is_available_for_withdrawal = 0,
                is_returned = 0
            WHERE available_at IS NULL
        """)
        
        conn.commit()
        print("✅ Миграция bonus_transactions завершена")
        
        conn.close()
    except Exception as e:
        print(f"❌ Ошибка миграции bonus_transactions: {e}")
        raise

def create_database():
    """Создает базу данных и все определенные таблицы."""
    Base.metadata.create_all(bind=engine)
    print(f"База данных успешно создана или обновлена: {DB_FILE}")
    # Выполняем миграцию для добавления level_0_percent
    migrate_bonus_settings()
    # Выполняем миграцию для добавления полей is_active и deactivated_at в participants
    migrate_participants()
    # Выполняем миграцию для добавления полей доступности к выводу в bonus_transactions
    migrate_bonus_transactions()
    # Сбрасываем кэш настроек после миграции
    clear_bonus_settings_cache()
    # Инициализируем дефолтные настройки бонусов
    init_bonus_settings()

def get_db():
    """Генерирует сессию для взаимодействия с БД."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def order_exists(db: Session, posting_number: str) -> bool:
    """Проверяет, существует ли заказ в базе данных по номеру отправления."""
    return db.query(Order).filter(Order.posting_number == posting_number).first() is not None

def customer_exists(db: Session, buyer_id: str) -> bool:
    """Проверяет, существует ли клиент в базе данных по buyer_id."""
    return db.query(Customer).filter(Customer.buyer_id == buyer_id).first() is not None

def get_customer(db: Session, buyer_id: str) -> Customer | None:
    """Получает клиента по buyer_id."""
    return db.query(Customer).filter(Customer.buyer_id == buyer_id).first()

def create_or_update_customer(db: Session, customer_data: dict) -> Customer:
    """Создает нового клиента или обновляет существующего."""
    buyer_id = customer_data.get("buyer_id")
    if not buyer_id:
        raise ValueError("buyer_id обязателен для создания/обновления клиента")
    
    customer = get_customer(db, buyer_id)
    
    if customer:
        # Обновляем существующего клиента
        for key, value in customer_data.items():
            if hasattr(customer, key) and value is not None:
                setattr(customer, key, value)
        customer.updated_at = datetime.utcnow()
    else:
        # Создаем нового клиента
        customer = Customer(**customer_data)
        db.add(customer)
    
    return customer

# >>> ФУНКЦИИ ДЛЯ РАБОТЫ С УЧАСТНИКАМИ РЕФЕРАЛЬНОЙ ПРОГРАММЫ <<<
def find_participant_by_ozon_id(ozon_id: str) -> dict | None:
    """Ищет участника по его Ozon ID. Возвращает словарь в формате совместимом с Google Sheets."""
    db = SessionLocal()
    try:
        participant = db.query(Participant).filter(Participant.ozon_id == str(ozon_id)).first()
        if participant:
            return {
                "ID участника": participant.ozon_id,
                "Имя / ник": participant.name or "",
                "Телеграм @": participant.username or "",
                "Ozon ID": participant.ozon_id,
                "ID пригласившего": participant.referrer_id or "",
                "Дата регистрации": participant.registration_date.strftime("%Y-%m-%d") if participant.registration_date else "",
                "Telegram ID": participant.telegram_id,
            }
        return None
    finally:
        db.close()

def find_participant_by_telegram_id(tg_id: int) -> dict | None:
    """Ищет участника по его Telegram ID. Возвращает словарь в формате совместимом с Google Sheets."""
    db = SessionLocal()
    try:
        participant = db.query(Participant).filter(Participant.telegram_id == str(tg_id)).first()
        if participant:
            return {
                "ID участника": participant.ozon_id,
                "Имя / ник": participant.name or "",
                "Телеграм @": participant.username or "",
                "Ozon ID": participant.ozon_id,
                "ID пригласившего": participant.referrer_id or "",
                "Дата регистрации": participant.registration_date.strftime("%Y-%m-%d") if participant.registration_date else "",
                "Telegram ID": participant.telegram_id,
            }
        return None
    finally:
        db.close()

def find_participant_by_username(username: str) -> dict | None:
    """Ищет участника по его Telegram username. Возвращает словарь в формате совместимом с Google Sheets."""
    db = SessionLocal()
    try:
        # Убираем @ если есть
        username_clean = username.lstrip('@')
        username_with_at = f"@{username_clean}"
        
        # Ищем по обоим вариантам
        participant = db.query(Participant).filter(
            (Participant.username == username_clean) | 
            (Participant.username == username_with_at)
        ).first()
        
        if participant:
            return {
                "ID участника": participant.ozon_id,
                "Имя / ник": participant.name or "",
                "Телеграм @": participant.username or "",
                "Ozon ID": participant.ozon_id,
                "ID пригласившего": participant.referrer_id or "",
                "Дата регистрации": participant.registration_date.strftime("%Y-%m-%d") if participant.registration_date else "",
                "Telegram ID": participant.telegram_id,
            }
        return None
    finally:
        db.close()

def create_participant(
    tg_id: int,
    username: str | None,
    first_name: str | None,
    ozon_id: str,
    referrer_id: str | None = None,
    language: str | None = None,
) -> dict:
    """Создает нового участника в базе данных или активирует существующего неактивного.
    Возвращает словарь в формате совместимом с Google Sheets."""
    db = SessionLocal()
    try:
        # Проверяем, не существует ли уже участник
        existing = db.query(Participant).filter(
            (Participant.ozon_id == str(ozon_id)) | (Participant.telegram_id == str(tg_id))
        ).first()
        
        if existing:
            # Если участник уже существует и активен, возвращаем его данные
            if existing.is_active == 1:
                return {
                    "ID участника": existing.ozon_id,
                    "Имя / ник": existing.name or "",
                    "Телеграм @": existing.username or "",
                    "Ozon ID": existing.ozon_id,
                    "ID пригласившего": existing.referrer_id or "",
                    "Дата регистрации": existing.registration_date.strftime("%Y-%m-%d") if existing.registration_date else "",
                    "Telegram ID": existing.telegram_id,
                }
            else:
                # Участник существует, но неактивен - активируем его (возврат)
                tg_username = f"@{username}" if username else ""
                name = first_name or ""
                
                # Обновляем данные участника при возврате
                existing.is_active = 1
                existing.deactivated_at = None
                existing.registration_date = datetime.utcnow()  # Новая дата регистрации
                existing.name = name
                existing.username = tg_username
                existing.language = language
                existing.updated_at = datetime.utcnow()
                
                # Если указан новый referrer_id, обновляем (но обычно сохраняем старый)
                if referrer_id:
                    existing.referrer_id = str(referrer_id)
                
                db.commit()
                
                return {
                    "ID участника": existing.ozon_id,
                    "Имя / ник": existing.name,
                    "Телеграм @": existing.username,
                    "Ozon ID": existing.ozon_id,
                    "ID пригласившего": existing.referrer_id or "",
                    "Дата регистрации": existing.registration_date.strftime("%Y-%m-%d"),
                    "Telegram ID": existing.telegram_id,
                }
        
        # Создаем нового участника
        tg_username = f"@{username}" if username else ""
        name = first_name or ""
        
        participant = Participant(
            ozon_id=str(ozon_id),
            telegram_id=str(tg_id),
            name=name,
            username=tg_username,
            referrer_id=str(referrer_id) if referrer_id else None,
            language=language,
            registration_date=datetime.utcnow(),
            is_active=1,  # Новый участник всегда активен
        )
        
        db.add(participant)
        db.commit()
        
        return {
            "ID участника": participant.ozon_id,
            "Имя / ник": participant.name,
            "Телеграм @": participant.username,
            "Ozon ID": participant.ozon_id,
            "ID пригласившего": participant.referrer_id or "",
            "Дата регистрации": participant.registration_date.strftime("%Y-%m-%d"),
            "Telegram ID": participant.telegram_id,
        }
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def get_all_participants() -> list:
    """Получить всех активных участников программы.
    
    Returns:
        list: Список словарей с данными участников в формате, совместимом с Google Sheets
              Только активные участники
    """
    db = SessionLocal()
    try:
        participants = db.query(Participant).filter(
            Participant.is_active == 1
        ).all()
        result = []
        for participant in participants:
            result.append({
                "ID участника": participant.ozon_id,
                "Имя / ник": participant.name or "",
                "Телеграм @": participant.username or "",
                "Ozon ID": participant.ozon_id,
                "ID пригласившего": participant.referrer_id or "",
                "Дата регистрации": participant.registration_date.strftime("%Y-%m-%d") if participant.registration_date else "",
                "Telegram ID": participant.telegram_id,
            })
        return result
    finally:
        db.close()

def deactivate_participant(telegram_id: int) -> dict:
    """
    Деактивирует участника программы (выход из программы).
    Участник не удаляется, а помечается как неактивный.
    Рефералы остаются связанными, но неактивный участник не получает бонусы.
    
    Args:
        telegram_id: Telegram ID участника для деактивации
        
    Returns:
        dict: {
            "success": bool,
            "referrals_count": int,  # Количество рефералов участника
            "ozon_id": str | None,  # Ozon ID деактивированного участника
            "was_already_inactive": bool  # Был ли участник уже неактивным
        }
    """
    db = SessionLocal()
    try:
        # Находим участника по Telegram ID
        participant = db.query(Participant).filter(
            Participant.telegram_id == str(telegram_id)
        ).first()
        
        if not participant:
            return {
                "success": False,
                "referrals_count": 0,
                "ozon_id": None,
                "was_already_inactive": False
            }
        
        ozon_id = participant.ozon_id
        
        # Проверяем, был ли участник уже неактивным
        was_already_inactive = (participant.is_active == 0)
        
        # Находим всех рефералов участника (для информации)
        referrals = db.query(Participant).filter(
            Participant.referrer_id == str(ozon_id)
        ).all()
        referrals_count = len(referrals)
        
        # Деактивируем участника (не удаляем!)
        participant.is_active = 0
        participant.deactivated_at = datetime.utcnow()
        
        # Сохраняем изменения
        db.commit()
        
        return {
            "success": True,
            "referrals_count": referrals_count,
            "ozon_id": ozon_id,
            "was_already_inactive": was_already_inactive
        }
    except Exception as e:
        db.rollback()
        print(f"Ошибка при деактивации участника: {e}")
        raise e
    finally:
        db.close()

def get_user_orders_stats(ozon_id: str) -> dict:
    """Получает статистику по заказам пользователя (только после даты регистрации).
    
    Args:
        ozon_id: Ozon ID пользователя
        
    Returns:
        dict: {"delivered_count": int, "total_sum": float}
    """
    db = SessionLocal()
    try:
        # Находим участника и получаем дату регистрации
        participant = db.query(Participant).filter(
            Participant.ozon_id == str(ozon_id)
        ).first()
        
        # Подсчитываем доставленные заказы и их сумму
        query = db.query(Order).filter(
            Order.buyer_id == str(ozon_id),
            Order.status == "delivered"
        )
        
        # Фильтруем по дате регистрации, если она есть
        if participant and participant.registration_date:
            query = query.filter(Order.created_at >= participant.registration_date)
        
        orders = query.all()
        
        delivered_count = len(orders)
        total_sum = 0.0
        
        for order in orders:
            try:
                if order.price_amount:
                    price = float(order.price_amount)
                    total_sum += price
            except (ValueError, TypeError):
                continue
        
        return {
            "delivered_count": delivered_count,
            "total_sum": total_sum
        }
    finally:
        db.close()

def get_user_orders_summary(ozon_id: str) -> dict:
    """Получает сводку по заказам пользователя с даты регистрации.
    
    Args:
        ozon_id: Ozon ID пользователя
        
    Returns:
        dict: Сводка по заказам с группировкой по статусам в формате:
            {
                "total_orders": int,
                "total_sum": float,
                "registration_date": str | None,
                "by_status": {
                    "delivered": {"count": int, "sum": float},
                    "delivering": {"count": int, "sum": float},
                    ...
                }
            }
    """
    db = SessionLocal()
    try:
        # Находим участника и получаем дату регистрации
        participant = db.query(Participant).filter(
            Participant.ozon_id == str(ozon_id)
        ).first()
        
        if not participant:
            return {
                "total_orders": 0,
                "total_sum": 0.0,
                "registration_date": None,
                "by_status": {}
            }
        
        registration_date = participant.registration_date
        
        # Если нет даты регистрации, используем все заказы
        query = db.query(Order).filter(Order.buyer_id == str(ozon_id))
        if registration_date:
            query = query.filter(Order.created_at >= registration_date)
        
        orders = query.all()
        
        # Группируем по статусам и считаем суммы
        by_status = {}
        total_sum = 0.0
        
        for order in orders:
            status = order.status or "unknown"
            
            if status not in by_status:
                by_status[status] = {"count": 0, "sum": 0.0}
            
            by_status[status]["count"] += 1
            
            try:
                if order.price_amount:
                    price = float(order.price_amount)
                    by_status[status]["sum"] += price
                    total_sum += price
            except (ValueError, TypeError):
                continue
        
        return {
            "total_orders": len(orders),
            "total_sum": total_sum,
            "registration_date": registration_date.strftime("%Y-%m-%d") if registration_date else None,
            "by_status": by_status
        }
    finally:
        db.close()

def get_referrals_by_level(ozon_id: str, max_level: int = None) -> dict:
    """Получает рефералов пользователя по уровням.
    Возвращает только активных участников.
    
    Args:
        ozon_id: Ozon ID пользователя
        max_level: Максимальный уровень вложенности (если None, берется из настроек)
        
    Returns:
        dict: {1: [ozon_id, ...], 2: [ozon_id, ...], 3: [ozon_id, ...]}
              Только активные участники
    """
    # Если max_level не указан, получаем из настроек
    if max_level is None:
        settings = get_bonus_settings()
        max_level = settings.max_levels if settings else 3
    
    db = SessionLocal()
    try:
        referrals_by_level = {}
        
        # Уровень 1: прямые рефералы (только активные)
        level_1 = db.query(Participant).filter(
            Participant.referrer_id == str(ozon_id),
            Participant.is_active == 1
        ).all()
        referrals_by_level[1] = [p.ozon_id for p in level_1]
        
        # Если нужны следующие уровни
        if max_level > 1:
            # Уровень 2: рефералы рефералов уровня 1 (только активные)
            level_2_ids = []
            for level_1_id in referrals_by_level[1]:
                level_2_refs = db.query(Participant).filter(
                    Participant.referrer_id == str(level_1_id),
                    Participant.is_active == 1
                ).all()
                level_2_ids.extend([p.ozon_id for p in level_2_refs])
            referrals_by_level[2] = level_2_ids
            
            if max_level > 2:
                # Уровень 3: рефералы рефералов уровня 2 (только активные)
                level_3_ids = []
                for level_2_id in referrals_by_level[2]:
                    level_3_refs = db.query(Participant).filter(
                        Participant.referrer_id == str(level_2_id),
                        Participant.is_active == 1
                    ).all()
                    level_3_ids.extend([p.ozon_id for p in level_3_refs])
                referrals_by_level[3] = level_3_ids
                
                # Если нужны уровни 4 и выше, продолжаем рекурсивно
                if max_level > 3:
                    for level in range(4, max_level + 1):
                        level_ids = []
                        prev_level_ids = referrals_by_level.get(level - 1, [])
                        for prev_id in prev_level_ids:
                            refs = db.query(Participant).filter(
                                Participant.referrer_id == str(prev_id),
                                Participant.is_active == 1
                            ).all()
                            level_ids.extend([p.ozon_id for p in refs])
                        referrals_by_level[level] = level_ids
        
        return referrals_by_level
    finally:
        db.close()

def get_referrals_orders_stats(referral_ozon_ids: list) -> dict:
    """Получает статистику по заказам рефералов.
    
    Args:
        referral_ozon_ids: Список Ozon ID рефералов
        
    Returns:
        dict: {"orders_count": int, "total_sum": float}
    """
    if not referral_ozon_ids:
        return {"orders_count": 0, "total_sum": 0.0}
    
    db = SessionLocal()
    try:
        # Подсчитываем доставленные заказы рефералов и их сумму
        orders = db.query(Order).filter(
            Order.buyer_id.in_([str(oid) for oid in referral_ozon_ids]),
            Order.status == "delivered"
        ).all()
        
        orders_count = len(orders)
        total_sum = 0.0
        
        for order in orders:
            try:
                if order.price_amount:
                    price = float(order.price_amount)
                    total_sum += price
            except (ValueError, TypeError):
                continue
        
        return {
            "orders_count": orders_count,
            "total_sum": total_sum
        }
    finally:
        db.close()

# >>> КОНЕЦ БЛОКА: ФУНКЦИИ ДЛЯ РАБОТЫ С УЧАСТНИКАМИ <<<

# >>> ФУНКЦИИ ДЛЯ РАБОТЫ С НАСТРОЙКАМИ БОНУСОВ <<<
_bonus_settings_cache = None

def clear_bonus_settings_cache():
    """Сбросить кэш настроек бонусов (использовать после обновления)."""
    global _bonus_settings_cache
    _bonus_settings_cache = None

def init_bonus_settings():
    """Создает дефолтные настройки бонусов при первом запуске."""
    db = SessionLocal()
    try:
        existing = db.query(BonusSettings).filter(BonusSettings.id == 1).first()
        if not existing:
            default_settings = BonusSettings(
                id=1,
                max_levels=3,
                level_0_percent=0.0,
                level_1_percent=5.0,
                level_2_percent=3.0,
                level_3_percent=1.0
            )
            db.add(default_settings)
            db.commit()
            
            # Отсоединяем объект от сессии перед кэшированием
            db.expunge(default_settings)
            
            global _bonus_settings_cache
            _bonus_settings_cache = default_settings
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def get_bonus_settings():
    """Получить текущие настройки бонусов (с кэшированием для производительности)."""
    global _bonus_settings_cache
    
    # Если есть кэш, возвращаем его
    if _bonus_settings_cache is not None:
        return _bonus_settings_cache
    
    db = SessionLocal()
    try:
        settings = db.query(BonusSettings).filter(BonusSettings.id == 1).first()
        if not settings:
            # Если настроек нет, создаем дефолтные
            init_bonus_settings()
            settings = db.query(BonusSettings).filter(BonusSettings.id == 1).first()
        
        # Отсоединяем объект от сессии перед кэшированием
        # Это позволяет использовать объект после закрытия сессии
        if settings:
            db.expunge(settings)
        
        _bonus_settings_cache = settings
        return settings
    except Exception as e:
        raise
    finally:
        db.close()

def update_bonus_settings(settings: dict):
    """Обновить настройки бонусов."""
    db = SessionLocal()
    try:
        existing = db.query(BonusSettings).filter(BonusSettings.id == 1).first()
        if not existing:
            existing = BonusSettings(id=1)
            db.add(existing)
        
        # Обновляем поля
        if 'max_levels' in settings:
            existing.max_levels = settings['max_levels']
        
        # Динамически обновляем проценты для любого уровня
        for key, value in settings.items():
            if key.startswith('level_') and key.endswith('_percent'):
                if hasattr(existing, key):
                    setattr(existing, key, value)
        
        existing.updated_at = datetime.utcnow()
        db.commit()
        
        # Отсоединяем объект от сессии перед кэшированием
        db.expunge(existing)
        
        # Сбрасываем кэш
        global _bonus_settings_cache
        _bonus_settings_cache = existing
        
        return existing
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

# >>> КОНЕЦ БЛОКА: ФУНКЦИИ ДЛЯ РАБОТЫ С НАСТРОЙКАМИ БОНУСОВ <<<

# >>> ФУНКЦИИ ДЛЯ РАБОТЫ С НАЧИСЛЕНИЕМ БОНУСОВ <<<
def get_referral_chain(referral_ozon_id: str, max_levels: int, order_date: datetime = None, db: Session = None) -> list:
    """Получить реферальную цепочку для указанного реферала (найти всех реферов до max_levels уровня).
    Неактивные участники пропускаются, но уровень сохраняется (не уменьшается).
    
    Args:
        referral_ozon_id: Ozon ID реферала (того, кто сделал покупку)
        max_levels: Максимальная глубина цепочки
        order_date: Дата создания заказа (для проверки, что реферер зарегистрирован до этого)
        db: Сессия БД (опционально, если None, создается новая)
        
    Returns:
        list: Список словарей [{"ozon_id": ..., "level": 1}, ...] с рефералами по уровням
              level=1 - прямой реферер, level=2 - реферер реферера и т.д.
              Неактивные участники НЕ включаются в список (пропускаются)
    """
    should_close_db = False
    if db is None:
        db = SessionLocal()
        should_close_db = True
    
    try:
        chain = []
        current_ozon_id = str(referral_ozon_id)
        real_level = 0  # Реальный уровень в цепочке (включая неактивных), начинаем с 0
        
        while real_level < max_levels:
            # Ищем участника (того, кто сделал покупку или является реферером)
            participant = db.query(Participant).filter(
                Participant.ozon_id == current_ozon_id
            ).first()
            
            if not participant or not participant.referrer_id:
                break
            
            # Переходим к рефереру
            referrer_ozon_id = participant.referrer_id
            
            # Проверяем реферера
            referrer_participant = db.query(Participant).filter(
                Participant.ozon_id == referrer_ozon_id
            ).first()
            
            if not referrer_participant:
                break  # Реферер не зарегистрирован
            
            # Проверяем дату регистрации реферера (если указана дата заказа)
            if order_date and referrer_participant.registration_date:
                if order_date < referrer_participant.registration_date:
                    break  # Заказ создан до регистрации реферера
            
            # Увеличиваем реальный уровень (включая неактивных)
            real_level += 1
            
            # Если реферер неактивен - пропускаем его, но продолжаем искать дальше
            # Уровень уже увеличен, поэтому следующий активный участник получит правильный уровень
            if referrer_participant.is_active == 0:
                # Пропускаем неактивного участника, но продолжаем поиск
                current_ozon_id = referrer_ozon_id
                continue
            
            # Добавляем активного реферера в цепочку (кому начислим бонус)
            # Используем real_level, чтобы сохранить исходный уровень
            chain.append({
                "ozon_id": referrer_ozon_id,
                "level": real_level
            })
            
            # Переходим к следующему уровню (реферер становится текущим для поиска его реферера)
            current_ozon_id = referrer_ozon_id
        
        return chain
    finally:
        if should_close_db:
            db.close()

def calculate_bonuses_for_order(order: Order, db: Session = None) -> list:
    """Рассчитать бонусы для заказа.
    
    Args:
        order: Объект заказа
        db: Сессия БД (опционально, если None, создается новая)
        
    Returns:
        list: Список словарей с данными для начисления бонусов
    """
    if not order.buyer_id or order.status != "delivered":
        return []
    
    should_close_db = False
    if db is None:
        db = SessionLocal()
        should_close_db = True
    
    try:
        # Проверяем, что покупатель зарегистрирован и активен
        buyer_participant = db.query(Participant).filter(
            Participant.ozon_id == order.buyer_id
        ).first()
        
        if not buyer_participant:
            return []  # Покупатель не зарегистрирован
        
        # Неактивные участники не получают бонусы за свои покупки
        if buyer_participant.is_active == 0:
            return []  # Покупатель неактивен
        
        # Проверяем, что заказ создан после регистрации покупателя
        if order.created_at and buyer_participant.registration_date:
            if order.created_at < buyer_participant.registration_date:
                return []  # Заказ создан до регистрации покупателя
        
        # Получаем настройки
        settings = get_bonus_settings()
        if not settings:
            return []
        
        # Получаем сумму заказа
        try:
            order_sum = float(order.price_amount) if order.price_amount else 0.0
        except (ValueError, TypeError):
            return []
        
        if order_sum <= 0:
            return []
        
        bonuses = []
        
        # Начисляем бонус самому покупателю (уровень 0)
        level_0_percent = getattr(settings, 'level_0_percent', 0.0)
        if level_0_percent is not None and level_0_percent > 0:
            bonus_amount = (order_sum * level_0_percent) / 100.0
            bonuses.append({
                "referrer_ozon_id": order.buyer_id,  # Сам покупатель получает бонус
                "referral_ozon_id": order.buyer_id,
                "posting_number": order.posting_number,
                "order_sum": order_sum,
                "bonus_percentage": level_0_percent,
                "bonus_amount": bonus_amount,
                "level": 0
            })
        
        # Получаем реферальную цепочку (передаем дату заказа для проверки)
        chain = get_referral_chain(order.buyer_id, settings.max_levels, order.created_at, db)
        
        for item in chain:
            level = item["level"]
            
            # Получаем процент для уровня динамически
            percent_attr = f'level_{level}_percent'
            percent = getattr(settings, percent_attr, None)
            
            if percent is not None and percent > 0:
                bonus_amount = (order_sum * percent) / 100.0
                
                bonuses.append({
                    "referrer_ozon_id": item["ozon_id"],
                    "referral_ozon_id": order.buyer_id,
                    "posting_number": order.posting_number,
                    "order_sum": order_sum,
                    "bonus_percentage": percent,
                    "bonus_amount": bonus_amount,
                    "level": level
                })
        
        return bonuses
    finally:
        if should_close_db:
            db.close()

def accrue_bonuses_for_order(posting_number: str, db: Session = None) -> bool:
    """Начислить бонусы за заказ.
    
    Args:
        posting_number: Номер отправления заказа
        db: Сессия БД (опционально, если None, создается новая)
        
    Returns:
        bool: True если бонусы начислены, False если уже были начислены или ошибка
    """
    should_close_db = False
    if db is None:
        db = SessionLocal()
        should_close_db = True
    
    try:
        # Проверяем, не начислялись ли уже бонусы за этот заказ
        existing = db.query(BonusTransaction).filter(
            BonusTransaction.posting_number == posting_number
        ).first()
        
        if existing:
            return False  # Бонусы уже начислены
        
        # Находим заказ
        order = db.query(Order).filter(Order.posting_number == posting_number).first()
        if not order:
            return False
        
        # Рассчитываем бонусы (передаем сессию БД для оптимизации)
        bonuses = calculate_bonuses_for_order(order, db)
        
        if not bonuses:
            return False
        
        # Сохраняем транзакции
        from datetime import timedelta
        current_time = datetime.utcnow()
        available_at = current_time + timedelta(days=14)
        
        for bonus_data in bonuses:
            # Устанавливаем поля доступности к выводу
            bonus_data["is_available_for_withdrawal"] = 0  # Заблокирован на 14 дней
            bonus_data["available_at"] = available_at
            bonus_data["is_returned"] = 0  # Не возвращен
            bonus_data["returned_amount"] = None
            bonus_data["returned_at"] = None
            
            transaction = BonusTransaction(**bonus_data)
            db.add(transaction)
        
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        print(f"Ошибка при начислении бонусов за заказ {posting_number}: {e}")
        return False
    finally:
        if should_close_db:
            db.close()

def get_user_bonuses(ozon_id: str, level: int = None) -> float:
    """Получить сумму начисленных бонусов пользователя.
    
    Args:
        ozon_id: Ozon ID пользователя
        level: Уровень реферала (опционально, если None - все уровни)
        
    Returns:
        float: Сумма бонусов
    """
    db = SessionLocal()
    try:
        query = db.query(BonusTransaction).filter(
            BonusTransaction.referrer_ozon_id == str(ozon_id)
        )
        
        if level is not None:
            query = query.filter(BonusTransaction.level == level)
        
        transactions = query.all()
        total = sum(t.bonus_amount for t in transactions if t.bonus_amount)
        return total
    finally:
        db.close()

def get_referrals_bonuses_stats(referral_ozon_ids: list, level: int) -> float:
    """Получить сумму бонусов от конкретных рефералов определенного уровня.
    
    Args:
        referral_ozon_ids: Список Ozon ID рефералов
        level: Уровень рефералов
        
    Returns:
        float: Сумма бонусов
    """
    if not referral_ozon_ids:
        return 0.0
    
    db = SessionLocal()
    try:
        transactions = db.query(BonusTransaction).filter(
            BonusTransaction.referral_ozon_id.in_([str(oid) for oid in referral_ozon_ids]),
            BonusTransaction.level == level
        ).all()
        
        total = sum(t.bonus_amount for t in transactions if t.bonus_amount)
        return total
    finally:
        db.close()

def get_daily_bonus_transactions(referrer_ozon_id: str, date: datetime) -> list:
    """Получить все транзакции бонусов за указанную дату для конкретного реферера.
    
    Args:
        referrer_ozon_id: Ozon ID реферера (кому начислены бонусы)
        date: Дата для выборки (используется только дата, без времени)
        
    Returns:
        list: Список словарей с данными транзакций, включая информацию о заказах
    """
    db = SessionLocal()
    try:
        # Определяем начало и конец дня
        date_start = datetime.combine(date.date(), datetime.min.time())
        date_end = datetime.combine(date.date(), datetime.max.time())
        
        # Получаем все транзакции за указанную дату для реферера
        transactions = db.query(BonusTransaction).filter(
            BonusTransaction.referrer_ozon_id == str(referrer_ozon_id),
            BonusTransaction.created_at >= date_start,
            BonusTransaction.created_at <= date_end
        ).all()
        
        # Формируем список с данными о транзакциях и связанных заказах
        result = []
        for trans in transactions:
            # Получаем информацию о заказе
            order = db.query(Order).filter(
                Order.posting_number == trans.posting_number
            ).first()
            
            # Получаем информацию о реферале (участнике, который сделал покупку)
            referral_participant = db.query(Participant).filter(
                Participant.ozon_id == trans.referral_ozon_id
            ).first()
            
            result.append({
                "transaction_id": trans.id,
                "referrer_ozon_id": trans.referrer_ozon_id,
                "referral_ozon_id": trans.referral_ozon_id,
                "posting_number": trans.posting_number,
                "order_sum": trans.order_sum,
                "bonus_percentage": trans.bonus_percentage,
                "bonus_amount": trans.bonus_amount,
                "level": trans.level,
                "created_at": trans.created_at,
                # Информация о заказе (если доступна)
                "order_item_name": order.item_name if order else None,
                "order_price_amount": order.price_amount if order else None,
                # Информация о реферале (если доступна)
                "referral_name": referral_participant.name if referral_participant else None,
                "referral_username": referral_participant.username if referral_participant else None,
            })
        
        return result
    finally:
        db.close()

def get_daily_bonus_summary(referrer_ozon_id: str, date: datetime) -> dict:
    """Получить сводку бонусов за день с группировкой по уровням.
    
    Args:
        referrer_ozon_id: Ozon ID реферера (кому начислены бонусы)
        date: Дата для выборки (используется только дата, без времени)
        
    Returns:
        dict: Словарь со сводкой бонусов в формате:
            {
                "referrer_ozon_id": str,
                "date": datetime.date,
                "total_amount": float,
                "levels": {
                    1: {"count": int, "total_amount": float, "transactions": list},
                    2: {"count": int, "total_amount": float, "transactions": list},
                    ...
                }
            }
            Если начислений нет, возвращает None или словарь с total_amount = 0
    """
    transactions = get_daily_bonus_transactions(referrer_ozon_id, date)
    
    if not transactions:
        return None
    
    # Группируем по уровням
    levels = {}
    total_amount = 0.0
    
    for trans in transactions:
        level = trans["level"]
        bonus_amount = trans["bonus_amount"] or 0.0
        
        if level not in levels:
            levels[level] = {
                "count": 0,
                "total_amount": 0.0,
                "transactions": []
            }
        
        levels[level]["count"] += 1
        levels[level]["total_amount"] += bonus_amount
        levels[level]["transactions"].append(trans)
        total_amount += bonus_amount
    
    return {
        "referrer_ozon_id": referrer_ozon_id,
        "date": date.date(),
        "total_amount": total_amount,
        "levels": levels
    }

def process_order_return(posting_number: str, return_amount: float = None, db: Session = None) -> bool:
    """Обработать возврат заказа и списать соответствующие бонусы.
    
    Args:
        posting_number: Номер отправления заказа
        return_amount: Сумма возврата (если None, считается полный возврат)
        db: Сессия БД (опционально, если None, создается новая)
        
    Returns:
        bool: True если возврат обработан, False если ошибка
    """
    should_close_db = False
    if db is None:
        db = SessionLocal()
        should_close_db = True
    
    try:
        # Находим все бонусы, связанные с этим заказом
        transactions = db.query(BonusTransaction).filter(
            BonusTransaction.posting_number == posting_number,
            BonusTransaction.is_returned == 0  # Только не возвращенные бонусы
        ).all()
        
        if not transactions:
            return False  # Нет бонусов для списания
        
        # Получаем информацию о заказе для расчета пропорции
        order = db.query(Order).filter(Order.posting_number == posting_number).first()
        if not order:
            return False
        
        try:
            order_sum = float(order.price_amount) if order.price_amount else 0.0
        except (ValueError, TypeError):
            order_sum = 0.0
        
        # Если сумма возврата не указана, считаем полный возврат
        if return_amount is None:
            return_amount = order_sum
        
        # Рассчитываем коэффициент возврата (0.0 - полный возврат, 1.0 - нет возврата)
        if order_sum > 0:
            return_ratio = return_amount / order_sum
            # Если возврат больше суммы заказа, ограничиваем до 1.0
            if return_ratio > 1.0:
                return_ratio = 1.0
        else:
            return_ratio = 1.0  # Если сумма заказа 0, не списываем
        
        current_time = datetime.utcnow()
        
        # Обрабатываем каждый бонус
        for transaction in transactions:
            # Рассчитываем сумму списания пропорционально возврату
            if return_ratio >= 1.0:
                # Полный возврат - списываем весь бонус
                transaction.is_returned = 1
                transaction.returned_amount = transaction.bonus_amount
                transaction.returned_at = current_time
                # Сбрасываем доступность к выводу
                transaction.is_available_for_withdrawal = 0
            else:
                # Частичный возврат - списываем пропорционально
                returned_bonus_amount = transaction.bonus_amount * return_ratio
                transaction.is_returned = 1
                transaction.returned_amount = returned_bonus_amount
                transaction.returned_at = current_time
                # Уменьшаем доступный бонус
                transaction.bonus_amount = transaction.bonus_amount - returned_bonus_amount
                # Если бонус был доступен, он остается доступным, но с уменьшенной суммой
        
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        print(f"Ошибка при обработке возврата заказа {posting_number}: {e}")
        return False
    finally:
        if should_close_db:
            db.close()

def check_and_update_bonus_availability(db: Session = None) -> int:
    """Проверить и обновить статус доступности бонусов.
    
    Проверяет все бонусы, у которых прошло 14 дней с момента начисления,
    и обновляет их статус доступности, если заказ не был возвращен.
    
    Args:
        db: Сессия БД (опционально, если None, создается новая)
        
    Returns:
        int: Количество обновленных бонусов
    """
    should_close_db = False
    if db is None:
        db = SessionLocal()
        should_close_db = True
    
    try:
        current_time = datetime.utcnow()
        
        # Находим все бонусы, которые должны стать доступными
        # (прошло 14 дней, еще не доступны, не возвращены)
        transactions = db.query(BonusTransaction).filter(
            BonusTransaction.is_available_for_withdrawal == 0,
            BonusTransaction.is_returned == 0,
            BonusTransaction.available_at <= current_time
        ).all()
        
        updated_count = 0
        
        for transaction in transactions:
            # Проверяем статус связанного заказа
            order = db.query(Order).filter(
                Order.posting_number == transaction.posting_number
            ).first()
            
            if order:
                # Если заказ не возвращен (статус не "cancelled" после доставки)
                # или статус все еще "delivered", разблокируем бонус
                if order.status == "delivered":
                    transaction.is_available_for_withdrawal = 1
                    updated_count += 1
                # Если заказ отменен после доставки - это возврат
                elif order.status == "cancelled":
                    # Помечаем как возвращенный
                    transaction.is_returned = 1
                    transaction.returned_amount = transaction.bonus_amount
                    transaction.returned_at = current_time
                    updated_count += 1
            else:
                # Заказ не найден - считаем, что он доставлен (разблокируем)
                transaction.is_available_for_withdrawal = 1
                updated_count += 1
        
        db.commit()
        return updated_count
    except Exception as e:
        db.rollback()
        print(f"Ошибка при проверке доступности бонусов: {e}")
        return 0
    finally:
        if should_close_db:
            db.close()

def get_available_bonuses_for_withdrawal(ozon_id: str) -> float:
    """Получить сумму доступных к выводу бонусов для пользователя.
    
    Args:
        ozon_id: Ozon ID пользователя
        
    Returns:
        float: Сумма доступных к выводу бонусов
    """
    db = SessionLocal()
    try:
        # Сначала обновляем доступность бонусов
        check_and_update_bonus_availability(db)
        
        # Получаем сумму доступных бонусов
        transactions = db.query(BonusTransaction).filter(
            BonusTransaction.referrer_ozon_id == str(ozon_id),
            BonusTransaction.is_available_for_withdrawal == 1,
            BonusTransaction.is_returned == 0
        ).all()
        
        total = sum(t.bonus_amount for t in transactions if t.bonus_amount)
        return total
    finally:
        db.close()

# >>> КОНЕЦ БЛОКА: ФУНКЦИИ ДЛЯ РАБОТЫ С НАЧИСЛЕНИЕМ БОНУСОВ <<<

# >>> ФУНКЦИИ ДЛЯ РАБОТЫ С НАСТРОЙКАМИ СИНХРОНИЗАЦИИ <<<
def get_last_sync_timestamp() -> datetime | None:
    """Возвращает время последней успешной синхронизации из базы данных (для проверки интервала 12 часов)."""
    db = SessionLocal()
    try:
        setting = db.query(SyncSettings).filter(SyncSettings.key == "last_sync_time").first()
        if setting and setting.value:
            try:
                return datetime.strptime(setting.value, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
        return None
    finally:
        db.close()

def set_last_sync_timestamp(timestamp: datetime):
    """Записывает время последней успешной синхронизации в базу данных (для проверки интервала 12 часов)."""
    db = SessionLocal()
    try:
        setting = db.query(SyncSettings).filter(SyncSettings.key == "last_sync_time").first()
        timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        
        if setting:
            setting.value = timestamp_str
            setting.updated_at = datetime.utcnow()
        else:
            setting = SyncSettings(key="last_sync_time", value=timestamp_str)
            db.add(setting)
        
        db.commit()
        print(f"Время синхронизации обновлено до: {timestamp_str}")
    except Exception as e:
        db.rollback()
        print(f"Ошибка записи времени синхронизации: {e}")
        raise e
    finally:
        db.close()

def get_last_order_date() -> datetime | None:
    """Возвращает дату последнего заказа из базы данных (для алгоритма скользящей даты и определения стартовой даты запроса)."""
    db = SessionLocal()
    try:
        setting = db.query(SyncSettings).filter(SyncSettings.key == "last_order_date").first()
        if setting and setting.value:
            try:
                return datetime.strptime(setting.value, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
        return None
    finally:
        db.close()

def set_last_order_date(order_date: datetime):
    """Записывает дату последнего заказа в базу данных (для алгоритма скользящей даты и определения стартовой даты запроса)."""
    db = SessionLocal()
    try:
        setting = db.query(SyncSettings).filter(SyncSettings.key == "last_order_date").first()
        date_str = order_date.strftime("%Y-%m-%d %H:%M:%S")
        
        if setting:
            setting.value = date_str
            setting.updated_at = datetime.utcnow()
        else:
            setting = SyncSettings(key="last_order_date", value=date_str)
            db.add(setting)
        
        db.commit()
        print(f"Дата последнего заказа обновлена до: {date_str}")
    except Exception as e:
        db.rollback()
        print(f"Ошибка записи даты последнего заказа: {e}")
        raise e
    finally:
        db.close()
# >>> КОНЕЦ БЛОКА: ФУНКЦИИ ДЛЯ РАБОТЫ С НАСТРОЙКАМИ СИНХРОНИЗАЦИИ <<<

# >>> КОНЕЦ БЛОКА: ФУНКЦИИ ВЗАИМОДЕЙСТВИЯ С БД <<<

if __name__ == "__main__":
    create_database()