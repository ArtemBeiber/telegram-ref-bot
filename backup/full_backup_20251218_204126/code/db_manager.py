# db_manager.py

import os
import time
import sqlite3
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Boolean, UniqueConstraint, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

# >>> НАЧАЛО БЛОКА: КОНФИГУРАЦИЯ БАЗЫ ДАННЫХ <<<
DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "referral_orders.db")
DATABASE_URL = f"sqlite:///{DB_FILE}"

# Включаем WAL режим для SQLite (позволяет читать и писать одновременно)
# и настраиваем таймауты для обработки блокировок
engine = create_engine(
    DATABASE_URL,
    connect_args={
        "check_same_thread": False,
        "timeout": 30  # Таймаут 30 секунд для операций
    },
    pool_pre_ping=True  # Проверка соединения перед использованием
)

Base = declarative_base()  # SQLAlchemy 2.0+

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# >>> КОНЕЦ БЛОКА: КОНФИГУРАЦИЯ БАЗЫ ДАННЫХ <<<


# >>> НАЧАЛО БЛОКА: ОПРЕДЕЛЕНИЕ МОДЕЛИ ТАБЛИЦЫ "postings" <<<
class Posting(Base):
    """Модель для хранения отправлений Ozon (как в API)."""
    
    __tablename__ = "postings"
    
    # Идентификаторы
    posting_number = Column(String, primary_key=True, index=True)
    order_id = Column(String, index=True)
    order_number = Column(String, index=True)
    
    # Статусы и даты
    status = Column(String, index=True)
    created_at = Column(DateTime, index=True)
    in_process_at = Column(String)
    delivering_date = Column(String)
    estimated_delivery_date = Column(String)
    shipment_date = Column(String)
    
    # География
    cluster_from = Column(String)
    cluster_to = Column(String)
    region_from = Column(String)
    region_to = Column(String)
    
    # Доставка
    delivery_price = Column(Float)  # Стоимость доставки
    delivery_method_name = Column(String)
    delivery_method_warehouse_name = Column(String)
    delivery_method_warehouse_id = Column(String)
    delivery_method_tpl_provider_id = Column(String)
    
    # Клиент (из addressee)
    addressee_name = Column(String)
    addressee_phone = Column(String)
    addressee_address = Column(String)
    addressee_address_tail = Column(String)
    addressee_postal_code = Column(String)
    
    # Клиент (из customer)
    customer_name = Column(String)
    customer_phone = Column(String)
    
    # Дополнительно
    client_segment = Column(String)
    is_legal = Column(Boolean)
    is_express = Column(Boolean)
    is_multibox = Column(Boolean)
    multi_box_qty = Column(Integer)
    tracking_number = Column(String)
    tpl_integration_type = Column(String)
    
    # Оплата
    payment_method_name = Column(String)
    payment_method_id = Column(String)
    
    # Финансовые данные (общие для заказа)
    currency_code = Column(String)
    total_discount_value = Column(Float)  # Общая скидка по заказу
    total_discount_percent = Column(Float)
    commission_amount = Column(Float)  # Комиссия Ozon
    payout = Column(Float)  # Выплата продавцу
    
    # Для реферальной программы (из posting_number)
    buyer_id = Column(String, index=True)  # Извлекается из posting_number
    
    # Метаданные
    sync_time = Column(DateTime, default=datetime.utcnow)

# >>> КОНЕЦ БЛОКА: ОПРЕДЕЛЕНИЕ МОДЕЛИ ТАБЛИЦЫ "postings" <<<

# >>> НАЧАЛО БЛОКА: ОПРЕДЕЛЕНИЕ МОДЕЛИ ТАБЛИЦЫ "order_items" <<<
class OrderItem(Base):
    """Модель для хранения товаров в отправлении (как в API financial_data.products[])."""
    
    __tablename__ = "order_items"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    posting_number = Column(String, ForeignKey('postings.posting_number'), index=True)
    
    # Данные товара
    sku = Column(String, index=True)
    product_id = Column(String)
    offer_id = Column(String)  # Артикул
    name = Column(String)
    
    # Цены и количество
    price = Column(Float)  # Цена после скидок
    old_price = Column(Float)  # Цена до скидки
    quantity = Column(Integer)
    returned_quantity = Column(Integer, default=0)  # Количество возвращенных единиц
    
    # Скидки
    total_discount_value = Column(Float)  # Скидка на товар
    total_discount_percent = Column(Float)
    discount_percent = Column(Float)  # Из item
    discount_rub = Column(Float)  # Вычисляемое: old_price - price
    
    # Комиссии
    commission_amount = Column(Float)
    commission_percent = Column(Float)
    payout = Column(Float)  # Выплата за товар
    
    # Дополнительно
    weight = Column(Float)
    promos = Column(String)  # JSON массив промокодов или строка
    
    # Составной уникальный индекс
    __table_args__ = (
        UniqueConstraint('posting_number', 'sku', name='uq_posting_sku'),
    )

# >>> КОНЕЦ БЛОКА: ОПРЕДЕЛЕНИЕ МОДЕЛИ ТАБЛИЦЫ "order_items" <<<

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
    
    # Статус активности
    is_active = Column(Integer, default=1)  # 1 = активен, 0 = неактивен
    
    # Временные метки
    registration_date = Column(DateTime, default=datetime.utcnow)  # Дата регистрации
    created_at = Column(DateTime, default=datetime.utcnow)  # Дата создания записи
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)  # Дата обновления
    deactivated_at = Column(DateTime, nullable=True)  # Дата деактивации
    
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
    status = Column(String, default="pending")  # Статус: "pending", "frozen", "available", "withdrawn", "returned"
    available_at = Column(DateTime, nullable=True)  # Дата, когда бонус станет доступным (через 14 дней)
    returned_amount = Column(Float, nullable=True)  # Сумма возвращенного бонуса
    returned_at = Column(DateTime, nullable=True)  # Дата возврата бонуса
    created_at = Column(DateTime, default=datetime.utcnow)  # Дата начисления
    
# >>> КОНЕЦ БЛОКА: ОПРЕДЕЛЕНИЕ МОДЕЛИ ТАБЛИЦЫ "bonus_transactions" <<<

# >>> НАЧАЛО БЛОКА: ОПРЕДЕЛЕНИЕ МОДЕЛИ ТАБЛИЦЫ "withdrawal_settings" <<<
class WithdrawalSettings(Base):
    """Модель для хранения настроек вывода бонусов."""
    
    __tablename__ = "withdrawal_settings"
    
    id = Column(Integer, primary_key=True, index=True, default=1)  # Всегда одна запись с id=1
    min_withdrawal_amount = Column(Float, default=100.0)  # Минимальная сумма вывода
    days_between_withdrawals = Column(Integer, nullable=True)  # Через сколько дней можно подать новую заявку (null = без ограничений)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)  # Дата обновления
    
# >>> КОНЕЦ БЛОКА: ОПРЕДЕЛЕНИЕ МОДЕЛИ ТАБЛИЦЫ "withdrawal_settings" <<<

# >>> НАЧАЛО БЛОКА: ОПРЕДЕЛЕНИЕ МОДЕЛИ ТАБЛИЦЫ "withdrawal_requests" <<<
class WithdrawalRequest(Base):
    """Модель для хранения заявок на вывод бонусов."""
    
    __tablename__ = "withdrawal_requests"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_ozon_id = Column(String, index=True)  # Ozon ID пользователя
    user_telegram_id = Column(String, index=True)  # Telegram ID пользователя
    amount = Column(Float)  # Сумма вывода
    status = Column(String)  # "processing", "approved", "rejected", "completed"
    admin_comment = Column(String, nullable=True)  # Причина отклонения/комментарий
    processed_by = Column(String, nullable=True)  # Telegram ID админа, обработавшего заявку
    created_at = Column(DateTime, default=datetime.utcnow)  # Дата создания заявки
    processed_at = Column(DateTime, nullable=True)  # Дата одобрения/отклонения
    completed_at = Column(DateTime, nullable=True)  # Дата завершения выплаты (статус "completed")
    
# >>> КОНЕЦ БЛОКА: ОПРЕДЕЛЕНИЕ МОДЕЛИ ТАБЛИЦЫ "withdrawal_requests" <<<

# >>> НАЧАЛО БЛОКА: ОПРЕДЕЛЕНИЕ МОДЕЛИ ТАБЛИЦЫ "withdrawal_transactions" <<<
class WithdrawalTransaction(Base):
    """Модель для связи заявок на вывод с транзакциями бонусов."""
    
    __tablename__ = "withdrawal_transactions"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    withdrawal_request_id = Column(Integer, index=True)  # ID заявки на вывод
    bonus_transaction_id = Column(Integer, index=True)  # ID транзакции бонуса
    amount = Column(Float)  # Сумма списанного бонуса
    created_at = Column(DateTime, default=datetime.utcnow)  # Дата списания
    
# >>> КОНЕЦ БЛОКА: ОПРЕДЕЛЕНИЕ МОДЕЛИ ТАБЛИЦЫ "withdrawal_transactions" <<<

# >>> НАЧАЛО БЛОКА: ФУНКЦИИ ВЗАИМОДЕЙСТВИЯ С БД <<<
def create_database():
    """Создает базу данных и все определенные таблицы."""
    # Создаем все таблицы, включая новые postings и order_items
    Base.metadata.create_all(bind=engine)
    
    # Включаем WAL режим для SQLite (позволяет читать и писать одновременно)
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            conn.execute(text("PRAGMA journal_mode=WAL"))
            conn.commit()
    except Exception as e:
        # Если не удалось включить WAL, продолжаем без него
        print(f"Предупреждение: не удалось включить WAL режим: {e}")
    
    # Миграция: добавляем колонку level_0_percent, если её нет
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            # Проверяем существование колонки через PRAGMA table_info
            result = conn.execute(text("PRAGMA table_info(bonus_settings)"))
            columns = [row[1] for row in result.fetchall()]  # row[1] - это имя колонки
            if 'level_0_percent' not in columns:
                conn.execute(text("ALTER TABLE bonus_settings ADD COLUMN level_0_percent REAL DEFAULT 0.0"))
                conn.commit()
                print("Миграция: добавлена колонка level_0_percent в таблицу bonus_settings")
            else:
                print("Миграция: колонка level_0_percent уже существует")
    except Exception as e:
        # Игнорируем ошибку, если таблица не существует (будет создана через create_all)
        if "no such table" not in str(e).lower():
            print(f"Предупреждение: не удалось выполнить миграцию для level_0_percent: {e}")
    
    print(f"База данных успешно создана или обновлена: {DB_FILE}")
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
    """Проверяет, существует ли отправление в базе данных по номеру отправления."""
    return db.query(Posting).filter(Posting.posting_number == posting_number).first() is not None

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
        customer.updated_at = datetime.now(timezone.utc)
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
    """Создает нового участника в базе данных. Возвращает словарь в формате совместимом с Google Sheets."""
    db = SessionLocal()
    try:
        # Проверяем, не существует ли уже участник
        existing = db.query(Participant).filter(
            (Participant.ozon_id == str(ozon_id)) | (Participant.telegram_id == str(tg_id))
        ).first()
        
        if existing:
            # Если участник уже существует, возвращаем его данные
            return {
                "ID участника": existing.ozon_id,
                "Имя / ник": existing.name or "",
                "Телеграм @": existing.username or "",
                "Ozon ID": existing.ozon_id,
                "ID пригласившего": existing.referrer_id or "",
                "Дата регистрации": existing.registration_date.strftime("%Y-%m-%d") if existing.registration_date else "",
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
            registration_date=datetime.now(timezone.utc),
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
        was_already_inactive = (participant.is_active == 0) if participant.is_active is not None else False
        
        # Находим всех рефералов участника (для информации)
        referrals = db.query(Participant).filter(
            Participant.referrer_id == str(ozon_id)
        ).all()
        referrals_count = len(referrals)
        
        # Деактивируем участника (не удаляем!)
        participant.is_active = 0
        participant.deactivated_at = datetime.now(timezone.utc)
        
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
    """Получает статистику по заказам пользователя из всех БД кабинетов.
    
    Args:
        ozon_id: Ozon ID пользователя
        
    Returns:
        dict: {"delivered_count": int, "total_sum": float}
    """
    delivered_count = 0
    total_sum = 0.0
    
    # Получаем список всех кабинетов
    cabinets = get_all_cabinets()
    
    # Объединяем данные из всех БД кабинетов
    for cabinet_name in cabinets:
        db = get_orders_db_session(cabinet_name)
        try:
            # Получаем участника из общей БД для проверки даты регистрации
            common_db = SessionLocal()
            try:
                participant = common_db.query(Participant).filter(
                    Participant.ozon_id == str(ozon_id)
                ).first()
                
                # Подсчитываем доставленные заказы и их сумму
                # Используем новую структуру (postings + order_items)
                query = db.query(Posting).filter(
                    Posting.buyer_id == str(ozon_id),
                    Posting.status == "delivered"
                )
                
                # Фильтруем по дате регистрации, если она есть
                if participant and participant.registration_date:
                    query = query.filter(Posting.created_at >= participant.registration_date)
                
                postings = query.all()
                processed_postings = set()  # Чтобы не дублировать суммы
                
                for posting in postings:
                    if posting.posting_number in processed_postings:
                        continue
                    
                    # Получить товар отправления (в одном posting всегда только 1 товар)
                    item = db.query(OrderItem).filter(
                        OrderItem.posting_number == posting.posting_number
                    ).first()
                    
                    if not item:
                        continue
                    
                    # Используем только price товара (без умножения на quantity, без доставки)
                    order_total = float(item.price or 0)
                    
                    delivered_count += 1
                    total_sum += order_total
                    processed_postings.add(posting.posting_number)
            finally:
                common_db.close()
        finally:
            db.close()
    
    return {
        "delivered_count": delivered_count,
        "total_sum": total_sum
    }

def get_user_orders_summary(ozon_id: str) -> dict:
    """Получает сводку по заказам пользователя с даты регистрации из всех БД кабинетов.
    
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
    # Получаем участника из общей БД
    common_db = SessionLocal()
    try:
        participant = common_db.query(Participant).filter(
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
    finally:
        common_db.close()
    
    # Получаем список всех кабинетов
    cabinets = get_all_cabinets()
    
    # Объединяем данные из всех БД кабинетов
    all_postings_with_cabinet = []  # Список кортежей (posting, cabinet_name)
    for cabinet_name in cabinets:
        db = get_orders_db_session(cabinet_name)
        try:
            query = db.query(Posting).filter(Posting.buyer_id == str(ozon_id))
            if registration_date:
                query = query.filter(Posting.created_at >= registration_date)
            
            postings = query.all()
            for posting in postings:
                all_postings_with_cabinet.append((posting, cabinet_name))
        finally:
            db.close()
    
    # Группируем по статусам и считаем суммы
    by_status = {}
    total_sum = 0.0
    processed_postings = set()  # Чтобы не дублировать суммы
    
    for posting, cabinet_name in all_postings_with_cabinet:
        if posting.posting_number in processed_postings:
            continue
        
        status = posting.status or "unknown"
        
        if status not in by_status:
            by_status[status] = {"count": 0, "sum": 0.0}
        
        # Получить товар отправления из правильного кабинета (в одном posting всегда только 1 товар)
        db = get_orders_db_session(cabinet_name)
        try:
            item = db.query(OrderItem).filter(
                OrderItem.posting_number == posting.posting_number
            ).first()
            
            if not item:
                continue
            
            # Используем только price товара (без умножения на quantity, без доставки)
            order_total = float(item.price or 0)
            
            by_status[status]["count"] += 1
            by_status[status]["sum"] += order_total
            total_sum += order_total
            processed_postings.add(posting.posting_number)
        finally:
            db.close()
    
    return {
        "total_orders": len(processed_postings),
        "total_sum": total_sum,
        "registration_date": registration_date.strftime("%Y-%m-%d") if registration_date else None,
        "by_status": by_status
    }

def get_referrals_by_level(ozon_id: str, max_level: int = None) -> dict:
    """Получает рефералов пользователя по уровням.
    
    Args:
        ozon_id: Ozon ID пользователя
        max_level: Максимальный уровень вложенности (если None, берется из настроек)
        
    Returns:
        dict: {1: [ozon_id, ...], 2: [ozon_id, ...], 3: [ozon_id, ...]}
    """
    # Если max_level не указан, получаем из настроек
    if max_level is None:
        settings = get_bonus_settings()
        max_level = settings.max_levels if settings else 3
    
    db = SessionLocal()
    try:
        referrals_by_level = {}
        
        # Уровень 1: прямые рефералы
        level_1 = db.query(Participant).filter(
            Participant.referrer_id == str(ozon_id)
        ).all()
        referrals_by_level[1] = [p.ozon_id for p in level_1]
        
        # Если нужны следующие уровни
        if max_level > 1:
            # Уровень 2: рефералы рефералов уровня 1
            level_2_ids = []
            for level_1_id in referrals_by_level[1]:
                level_2_refs = db.query(Participant).filter(
                    Participant.referrer_id == str(level_1_id)
                ).all()
                level_2_ids.extend([p.ozon_id for p in level_2_refs])
            referrals_by_level[2] = level_2_ids
            
            if max_level > 2:
                # Уровень 3: рефералы рефералов уровня 2
                level_3_ids = []
                for level_2_id in referrals_by_level[2]:
                    level_3_refs = db.query(Participant).filter(
                        Participant.referrer_id == str(level_2_id)
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
                                Participant.referrer_id == str(prev_id)
                            ).all()
                            level_ids.extend([p.ozon_id for p in refs])
                        referrals_by_level[level] = level_ids
        
        return referrals_by_level
    finally:
        db.close()

def get_referrals_orders_stats(referral_ozon_ids: list) -> dict:
    """Получает статистику по заказам рефералов из всех БД кабинетов.
    
    Args:
        referral_ozon_ids: Список Ozon ID рефералов
        
    Returns:
        dict: {"orders_count": int, "total_sum": float}
    """
    if not referral_ozon_ids:
        return {"orders_count": 0, "total_sum": 0.0}
    
    orders_count = 0
    total_sum = 0.0
    
    # Получаем список всех кабинетов
    cabinets = get_all_cabinets()
    
    # Получаем участников из общей БД для проверки дат регистрации
    common_db = SessionLocal()
    try:
        participants = {}
        for ozon_id in referral_ozon_ids:
            participant = common_db.query(Participant).filter(
                Participant.ozon_id == str(ozon_id)
            ).first()
            if participant:
                participants[str(ozon_id)] = participant
    finally:
        common_db.close()
    
    # Объединяем данные из всех БД кабинетов
    for cabinet_name in cabinets:
        db = get_orders_db_session(cabinet_name)
        try:
            # Подсчитываем доставленные заказы рефералов и их сумму
            # Используем новую структуру (postings + order_items)
            query = db.query(Posting).filter(
                Posting.buyer_id.in_([str(oid) for oid in referral_ozon_ids]),
                Posting.status == "delivered"
            )
            
            # Фильтруем по дате регистрации для каждого реферала
            all_postings = []
            
            for ozon_id in referral_ozon_ids:
                participant = participants.get(str(ozon_id))
                if participant and participant.registration_date:
                    # Заказы после регистрации
                    participant_postings = query.filter(
                        Posting.buyer_id == str(ozon_id),
                        Posting.created_at >= participant.registration_date
                    ).all()
                else:
                    # Все заказы, если участник не найден или нет даты регистрации
                    participant_postings = query.filter(
                        Posting.buyer_id == str(ozon_id)
                    ).all()
                all_postings.extend(participant_postings)
            
            processed_postings = set()
            for posting in all_postings:
                if posting.posting_number in processed_postings:
                    continue
                
                # Получить товар отправления (в одном posting всегда только 1 товар)
                item = db.query(OrderItem).filter(
                    OrderItem.posting_number == posting.posting_number
                ).first()
                
                if not item:
                    continue
                
                # Используем только price товара (без умножения на quantity, без доставки)
                order_total = float(item.price or 0)
                
                orders_count += 1
                total_sum += order_total
                processed_postings.add(posting.posting_number)
        finally:
            db.close()
    
    return {
        "orders_count": orders_count,
        "total_sum": total_sum
    }

# >>> КОНЕЦ БЛОКА: ФУНКЦИИ ДЛЯ РАБОТЫ С УЧАСТНИКАМИ <<<

# >>> ФУНКЦИИ ДЛЯ РАБОТЫ С НАСТРОЙКАМИ БОНУСОВ <<<
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
        
        existing.updated_at = datetime.now(timezone.utc)
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

def clear_bonus_settings_cache():
    """Сбросить кэш настроек бонусов (использовать после обновления)."""
    global _bonus_settings_cache
    _bonus_settings_cache = None

# >>> КОНЕЦ БЛОКА: ФУНКЦИИ ДЛЯ РАБОТЫ С НАСТРОЙКАМИ БОНУСОВ <<<

# >>> ФУНКЦИИ ДЛЯ РАБОТЫ С НАЧИСЛЕНИЕМ БОНУСОВ <<<
def get_referral_chain(referral_ozon_id: str, max_levels: int, order_date: datetime = None, db: Session = None) -> list:
    """Получить реферальную цепочку для указанного реферала (найти всех реферов до max_levels уровня).
    
    Args:
        referral_ozon_id: Ozon ID реферала (того, кто сделал покупку)
        max_levels: Максимальная глубина цепочки
        order_date: Дата создания заказа (для проверки, что реферер зарегистрирован до этого)
        db: Сессия БД (опционально, если None, создается новая)
        
    Returns:
        list: Список словарей [{"ozon_id": ..., "level": 1}, ...] с рефералами по уровням
              level=1 - прямой реферер, level=2 - реферер реферера и т.д.
    """
    should_close_db = False
    if db is None:
        db = SessionLocal()
        should_close_db = True
    
    try:
        chain = []
        current_ozon_id = str(referral_ozon_id)
        level = 1
        
        while level <= max_levels:
            # Ищем участника (того, кто сделал покупку или является реферером)
            participant = db.query(Participant).filter(
                Participant.ozon_id == current_ozon_id
            ).first()
            
            if not participant or not participant.referrer_id:
                break
            
            # Проверяем, что реферер зарегистрирован и был зарегистрирован до создания заказа
            referrer_participant = db.query(Participant).filter(
                Participant.ozon_id == participant.referrer_id
            ).first()
            
            if not referrer_participant:
                break  # Реферер не зарегистрирован
            
            # Проверяем дату регистрации реферера (если указана дата заказа)
            if order_date and referrer_participant.registration_date:
                if order_date < referrer_participant.registration_date:
                    break  # Заказ создан до регистрации реферера
            
            # Добавляем реферера в цепочку (кому начислим бонус)
            chain.append({
                "ozon_id": participant.referrer_id,
                "level": level
            })
            
            # Переходим к следующему уровню (реферер становится текущим для поиска его реферера)
            current_ozon_id = participant.referrer_id
            level += 1
        
        return chain
    finally:
        if should_close_db:
            db.close()

def calculate_bonuses_for_posting(
    posting_number: str, 
    common_db: Session = None,
    order_db: Session = None
) -> list:
    """Рассчитать бонусы для отправления (posting).
    
    Использует новую структуру: postings + order_items.
    В одном posting всегда только 1 товар, поэтому берем только price товара
    без доставки и без умножения на quantity.
    
    Args:
        posting_number: Номер отправления
        common_db: Сессия общей БД для рефералов (опционально)
        order_db: Сессия БД заказов кабинета (опционально)
        
    Returns:
        list: Список словарей с данными для начисления бонусов
    """
    if order_db is None:
        return []
    
    # Получить posting
    posting = order_db.query(Posting).filter(
        Posting.posting_number == posting_number
    ).first()
    
    if not posting or posting.status != "delivered":
        return []
    
    # Получить товар отправления (в одном posting всегда только 1 товар)
    item = order_db.query(OrderItem).filter(
        OrderItem.posting_number == posting_number
    ).first()
    
    if not item:
        return []
    
    # Используем только price товара (без доставки, без умножения на quantity)
    total_order_amount = float(item.price or 0)
    
    if total_order_amount <= 0:
        return []
    
    # Проверить, что покупатель зарегистрирован
    if not posting.buyer_id:
        return []
    
    should_close_db = False
    if common_db is None:
        common_db = SessionLocal()
        should_close_db = True
    
    try:
        buyer_participant = common_db.query(Participant).filter(
            Participant.ozon_id == posting.buyer_id
        ).first()
        
        if not buyer_participant:
            return []
        
        # Проверить дату регистрации
        if posting.created_at and buyer_participant.registration_date:
            if posting.created_at < buyer_participant.registration_date:
                return []
        
        # Получить настройки бонусов
        settings = get_bonus_settings()
        if not settings:
            return []
        
        # Получить реферальную цепочку
        chain = get_referral_chain(
            posting.buyer_id, 
            settings.max_levels, 
            posting.created_at, 
            common_db
        )
        
        bonuses = []
        for item in chain:
            level = item["level"]
            percent_attr = f'level_{level}_percent'
            percent = getattr(settings, percent_attr, None)
            
            if percent is not None and percent > 0:
                bonus_amount = (total_order_amount * percent) / 100.0
                
                bonuses.append({
                    "referrer_ozon_id": item["ozon_id"],
                    "referral_ozon_id": posting.buyer_id,
                    "posting_number": posting_number,
                    "order_sum": total_order_amount,
                    "bonus_percentage": percent,
                    "bonus_amount": bonus_amount,
                    "level": level
                })
        
        return bonuses
    finally:
        if should_close_db:
            common_db.close()

def accrue_bonuses_for_posting(
    posting_number: str, 
    common_db: Session = None, 
    order_db: Session = None, 
    cabinet_name: str = "wistery"
) -> int:
    """Начислить бонусы за отправление (posting).
    
    Использует новую структуру: postings + order_items.
    
    Args:
        posting_number: Номер отправления
        common_db: Сессия общей БД для рефералов и бонусов (опционально)
        order_db: Сессия БД заказов кабинета (опционально)
        cabinet_name: Название кабинета (для поиска заказа в правильной БД)
        
    Returns:
        int: Количество начисленных транзакций (0 если уже были начислены или ошибка)
    """
    should_close_common_db = False
    should_close_order_db = False
    
    if common_db is None:
        common_db = SessionLocal()
        should_close_common_db = True
    
    if order_db is None:
        if cabinet_name == "wistery" or not cabinet_name:
            order_db = SessionLocal()
        else:
            order_db = get_orders_db_session(cabinet_name)
        should_close_order_db = True
    
    # Retry логика для обработки блокировок базы данных
    max_retries = 5
    retry_delay = 0.1
    
    try:
        for attempt in range(1, max_retries + 1):
            try:
                if attempt == max_retries:
                    print(f"  Последняя попытка начисления бонусов за {posting_number}: {attempt}/{max_retries}")
                
                # Проверяем, не начислялись ли уже бонусы за этот заказ
                existing = common_db.query(BonusTransaction).filter(
                    BonusTransaction.posting_number == posting_number
                ).first()
                
                if existing:
                    return 0
                
                # Рассчитываем бонусы для новой структуры
                bonuses = calculate_bonuses_for_posting(posting_number, common_db, order_db)
                
                if not bonuses:
                    return 0
                
                # Сохраняем транзакции в общей БД
                transactions_count = len(bonuses)
                for bonus_data in bonuses:
                    transaction = BonusTransaction(**bonus_data)
                    common_db.add(transaction)
                
                common_db.commit()
                return transactions_count
                
            except (sqlite3.OperationalError, Exception) as e:
                error_str = str(e)
                is_locked = "database is locked" in error_str.lower() or "locked" in error_str.lower()
                
                if is_locked and attempt < max_retries:
                    delay = retry_delay * (2 ** (attempt - 1))
                    time.sleep(delay)
                    try:
                        common_db.rollback()
                    except:
                        pass
                    continue
                else:
                    common_db.rollback()
                    if is_locked:
                        print(f"Ошибка при начислении бонусов за заказ {posting_number}: база данных заблокирована (попытка {attempt}/{max_retries})")
                    else:
                        print(f"Ошибка при начислении бонусов за заказ {posting_number}: {e}")
                    print(f"  Пропускаем начисление бонусов за {posting_number} после {attempt} попыток")
                    break
        return 0
    finally:
        if should_close_common_db:
            try:
                common_db.close()
            except:
                pass
        if should_close_order_db:
            try:
                order_db.close()
            except:
                pass

def accrue_bonuses_for_order(posting_number: str, common_db: Session = None, order_db: Session = None, cabinet_name: str = "wistery") -> bool:
    """Начислить бонусы за заказ.
    
    Args:
        posting_number: Номер отправления заказа
        common_db: Сессия общей БД для рефералов и бонусов (опционально, если None, создается новая)
        order_db: Сессия БД заказов кабинета (опционально, если None, используется БД первого кабинета)
        cabinet_name: Название кабинета (для поиска заказа в правильной БД)
        
    Returns:
        int: Количество начисленных транзакций (0 если уже были начислены или ошибка)
    """
    should_close_common_db = False
    should_close_order_db = False
    
    if common_db is None:
        common_db = SessionLocal()
        should_close_common_db = True
    
    if order_db is None:
        # Используем БД первого кабинета для поиска заказа (если не указана другая)
        if cabinet_name == "wistery" or not cabinet_name:
            order_db = SessionLocal()
        else:
            order_db = get_orders_db_session(cabinet_name)
        should_close_order_db = True
    
    # Retry логика для обработки блокировок базы данных
    max_retries = 5
    retry_delay = 0.1  # Начальная задержка 100мс
    
    try:
        for attempt in range(1, max_retries + 1):
            try:
                # Логируем только при последней попытке, чтобы не засорять логи
                if attempt == max_retries:
                    print(f"  Последняя попытка начисления бонусов за {posting_number}: {attempt}/{max_retries}")
                
                # Проверяем, не начислялись ли уже бонусы за этот заказ (в общей БД)
                existing = common_db.query(BonusTransaction).filter(
                    BonusTransaction.posting_number == posting_number
                ).first()
                
                if existing:
                    return 0  # Бонусы уже начислены
                
                # Получаем posting из новой структуры
                posting = order_db.query(Posting).filter(Posting.posting_number == posting_number).first()
                if not posting:
                    return 0
                
                # Используем новую функцию для новой структуры
                bonuses = calculate_bonuses_for_posting(posting_number, common_db, order_db)
                
                if not bonuses:
                    return 0
                
                # Сохраняем транзакции в общей БД
                transactions_count = len(bonuses)
                for bonus_data in bonuses:
                    transaction = BonusTransaction(**bonus_data)
                    common_db.add(transaction)
                
                common_db.commit()
                return transactions_count
                
            except (sqlite3.OperationalError, Exception) as e:
                error_str = str(e)
                # Проверяем, является ли это ошибкой блокировки
                is_locked = "database is locked" in error_str.lower() or "locked" in error_str.lower()
                
                if is_locked and attempt < max_retries:
                    # Экспоненциальная задержка: 0.1, 0.2, 0.4, 0.8, 1.6 секунд
                    delay = retry_delay * (2 ** (attempt - 1))
                    time.sleep(delay)
                    # Откатываем транзакцию перед повтором
                    try:
                        common_db.rollback()
                    except:
                        pass
                    continue
                else:
                    # Если это не блокировка или попытки исчерпаны
                    common_db.rollback()
                    if is_locked:
                        print(f"Ошибка при начислении бонусов за заказ {posting_number}: база данных заблокирована (попытка {attempt}/{max_retries})")
                    else:
                        print(f"Ошибка при начислении бонусов за заказ {posting_number}: {e}")
                    # Выходим из цикла попыток и возвращаем 0
                    print(f"  Пропускаем начисление бонусов за {posting_number} после {attempt} попыток")
                    break
        # Если дошли сюда, значит все попытки исчерпаны или была ошибка
        print(f"  Завершение accrue_bonuses_for_order для {posting_number}, возвращаем 0")
        return 0
    finally:
        if should_close_common_db:
            try:
                common_db.close()
            except:
                pass
        if should_close_order_db:
            try:
                order_db.close()
            except:
                pass

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

# >>> КОНЕЦ БЛОКА: ФУНКЦИИ ДЛЯ РАБОТЫ С НАЧИСЛЕНИЕМ БОНУСОВ <<<

# >>> ФУНКЦИИ ДЛЯ РАБОТЫ С БД ЗАКАЗОВ КОНКРЕТНОГО КАБИНЕТА <<<
# Словарь для хранения engines и sessions для разных БД кабинетов
_orders_db_engines = {}
_orders_db_sessions = {}

def get_orders_db_path(cabinet_name: str) -> str:
    """Получить путь к БД заказов конкретного кабинета.
    
    Args:
        cabinet_name: Название кабинета (wistery, beiber и т.д.)
        
    Returns:
        str: Путь к файлу БД
    """
    if cabinet_name == "wistery" or not cabinet_name:
        # Первый кабинет использует существующую БД
        return DB_FILE
    else:
        # Другие кабинеты используют отдельные БД
        db_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(db_dir, f"referral_orders_{cabinet_name}.db")

def get_orders_db_session(cabinet_name: str) -> Session:
    """Получить сессию БД заказов конкретного кабинета.
    
    Args:
        cabinet_name: Название кабинета (wistery, beiber и т.д.)
        
    Returns:
        Session: Сессия БД для работы с заказами кабинета
    """
    # Для первого кабинета используем существующую сессию
    if cabinet_name == "wistery" or not cabinet_name:
        return SessionLocal()
    
    # Для других кабинетов создаем отдельную сессию
    if cabinet_name not in _orders_db_sessions:
        db_path = get_orders_db_path(cabinet_name)
        database_url = f"sqlite:///{db_path}"
        
        # Создаем engine для этого кабинета
        if cabinet_name not in _orders_db_engines:
            # Включаем WAL режим для SQLite и настраиваем таймауты
            _orders_db_engines[cabinet_name] = create_engine(
                database_url,
                connect_args={
                    "check_same_thread": False,
                    "timeout": 30  # Таймаут 30 секунд для операций
                },
                pool_pre_ping=True  # Проверка соединения перед использованием
            )
        
        # Создаем sessionmaker для этого кабинета
        session_maker = sessionmaker(autocommit=False, autoflush=False, bind=_orders_db_engines[cabinet_name])
        _orders_db_sessions[cabinet_name] = session_maker
    
    return _orders_db_sessions[cabinet_name]()

def create_orders_database(cabinet_name: str):
    """Создать БД заказов для конкретного кабинета (если не существует).
    
    Args:
        cabinet_name: Название кабинета (wistery, beiber и т.д.)
    """
    # Для первого кабинета БД уже существует
    if cabinet_name == "wistery" or not cabinet_name:
        return
    
    db_path = get_orders_db_path(cabinet_name)
    
    # Создаем engine для проверки и создания таблиц
    database_url = f"sqlite:///{db_path}"
    cabinet_engine = create_engine(
        database_url,
        connect_args={
            "check_same_thread": False,
            "timeout": 30
        },
        pool_pre_ping=True
    )
    
    # Проверяем, существуют ли таблицы
    need_create_tables = False
    if os.path.exists(db_path):
        # БД существует - проверяем наличие всех необходимых таблиц
        try:
            import sqlite3
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            # Проверяем наличие обеих таблиц: postings и order_items
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='postings'")
            postings_exists = cursor.fetchone() is not None
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='order_items'")
            order_items_exists = cursor.fetchone() is not None
            conn.close()
            
            if not postings_exists or not order_items_exists:
                need_create_tables = True
                missing_tables = []
                if not postings_exists:
                    missing_tables.append("postings")
                if not order_items_exists:
                    missing_tables.append("order_items")
                print(f"  Кабинет '{cabinet_name}': БД существует, но отсутствуют таблицы: {', '.join(missing_tables)}. Создаю таблицы...")
        except Exception as e:
            print(f"  Кабинет '{cabinet_name}': ошибка при проверке таблиц: {e}. Создаю таблицы...")
            need_create_tables = True
    else:
        # БД не существует - нужно создать
        need_create_tables = True
    
    if need_create_tables:
        # Создаем таблицы для заказов (Posting, OrderItem и Customer)
        # Используем Base.metadata, но создаем только нужные таблицы
        Base.metadata.create_all(bind=cabinet_engine, tables=[
            Posting.__table__,
            OrderItem.__table__,
            Customer.__table__
        ])
    
    # Включаем WAL режим для новой БД
    try:
        from sqlalchemy import text
        with cabinet_engine.connect() as conn:
            conn.execute(text("PRAGMA journal_mode=WAL"))
            conn.commit()
    except Exception as e:
        print(f"Предупреждение: не удалось включить WAL режим для {db_path}: {e}")

def get_all_cabinets() -> list:
    """Получить список всех настроенных кабинетов из .env.
    
    Returns:
        list: Список названий кабинетов ["wistery", "beiber", ...]
    """
    cabinets = []
    
    # Первый кабинет
    cabinet_name = os.getenv("OZON_CABINET_NAME", "wistery")
    if os.getenv("OZON_API_KEY") and os.getenv("OZON_CLIENT_ID"):
        cabinets.append(cabinet_name)
    
    # Дополнительные кабинеты
    cabinet_num = 2
    while True:
        api_key = os.getenv(f"OZON_API_KEY_{cabinet_num}")
        client_id = os.getenv(f"OZON_CLIENT_ID_{cabinet_num}")
        cabinet_name = os.getenv(f"OZON_CABINET_NAME_{cabinet_num}")
        
        if not api_key or not client_id:
            break
        
        if not cabinet_name:
            cabinet_name = f"cabinet_{cabinet_num}"
        
        cabinets.append(cabinet_name)
        cabinet_num += 1
    
    return cabinets

def get_all_orders_db_sessions() -> dict:
    """Получить сессии всех БД кабинетов.
    
    Returns:
        dict: Словарь {cabinet_name: Session, ...}
    """
    sessions = {}
    cabinets = get_all_cabinets()
    
    for cabinet_name in cabinets:
        sessions[cabinet_name] = get_orders_db_session(cabinet_name)
    
    return sessions

# >>> КОНЕЦ БЛОКА: ФУНКЦИИ ДЛЯ РАБОТЫ С БД ЗАКАЗОВ КОНКРЕТНОГО КАБИНЕТА <<<

# >>> ФУНКЦИИ ДЛЯ РАБОТЫ С НАСТРОЙКАМИ СИНХРОНИЗАЦИИ <<<
def get_last_sync_timestamp() -> datetime | None:
    """Возвращает время последней успешной синхронизации из базы данных."""
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
    """Записывает время последней успешной синхронизации в базу данных."""
    db = SessionLocal()
    try:
        setting = db.query(SyncSettings).filter(SyncSettings.key == "last_sync_time").first()
        timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        
        if setting:
            setting.value = timestamp_str
            setting.updated_at = datetime.now(timezone.utc)
        else:
            setting = SyncSettings(key="last_sync_time", value=timestamp_str)
            db.add(setting)
        
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"Ошибка записи времени синхронизации: {e}")
        raise e
    finally:
        db.close()

def get_last_order_date(cabinet_name: str = "wistery") -> datetime | None:
    """Возвращает скользящую дату последнего заказа для конкретного кабинета.
    
    Args:
        cabinet_name: Название кабинета (wistery, beiber и т.д.)
        
    Returns:
        datetime | None: Дата последнего заказа или None, если не установлена
    """
    db = SessionLocal()
    try:
        key = f"last_order_date_{cabinet_name}"
        setting = db.query(SyncSettings).filter(SyncSettings.key == key).first()
        if setting and setting.value:
            try:
                return datetime.strptime(setting.value, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
        
        # Если дата не установлена (новый кабинет), возвращаем начальную дату 01.12.2025
        return datetime(2025, 12, 1)
    finally:
        db.close()

def set_last_order_date(cabinet_name: str, order_date: datetime):
    """Записывает скользящую дату последнего заказа для конкретного кабинета.
    
    Args:
        cabinet_name: Название кабинета (wistery, beiber и т.д.)
        order_date: Дата для сохранения
    """
    db = SessionLocal()
    try:
        key = f"last_order_date_{cabinet_name}"
        setting = db.query(SyncSettings).filter(SyncSettings.key == key).first()
        date_str = order_date.strftime("%Y-%m-%d %H:%M:%S")
        
        if setting:
            setting.value = date_str
            setting.updated_at = datetime.now(timezone.utc)
        else:
            setting = SyncSettings(key=key, value=date_str)
            db.add(setting)
        
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"Ошибка записи даты последнего заказа для кабинета '{cabinet_name}': {e}")
        raise e
    finally:
        db.close()
# >>> КОНЕЦ БЛОКА: ФУНКЦИИ ДЛЯ РАБОТЫ С НАСТРОЙКАМИ СИНХРОНИЗАЦИИ <<<

# >>> ФУНКЦИИ ДЛЯ РАБОТЫ С УЧАСТНИКАМИ <<<
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
# >>> КОНЕЦ БЛОКА: ФУНКЦИИ ДЛЯ РАБОТЫ С УЧАСТНИКАМИ <<<

# >>> ФУНКЦИИ ДЛЯ РАБОТЫ С БОНУСАМИ <<<
def process_order_return(posting_number: str, return_amount: float = None, db: Session = None, order_db: Session = None) -> bool:
    """Обработать возврат заказа и списать соответствующие бонусы.
    
    Использует новую структуру: postings + order_items.
    Для статуса cancelled это всегда полный возврат (в одном posting всегда только 1 товар),
    поэтому списываем все бонусы за этот posting_number.
    """
    should_close_db = False
    should_close_order_db = False
    
    if db is None:
        db = SessionLocal()
        should_close_db = True
    
    if order_db is None:
        order_db = get_orders_db_session("wistery")
        should_close_order_db = True
    
    try:
        transactions = db.query(BonusTransaction).filter(
            BonusTransaction.posting_number == posting_number,
            BonusTransaction.status.in_(["frozen", "available"])
        ).all()
        
        if not transactions:
            return False
        
        # Проверяем, что posting существует (для информации)
        posting = order_db.query(Posting).filter(Posting.posting_number == posting_number).first()
        if not posting:
            return False
        
        # Для статуса cancelled это всегда полный возврат (в одном posting всегда только 1 товар)
        # Списываем все бонусы за этот posting_number
        current_time = datetime.now(timezone.utc)
        
        for transaction in transactions:
            # Полный возврат - списываем все бонусы
            transaction.status = "returned"
            if hasattr(transaction, 'returned_amount'):
                transaction.returned_amount = transaction.bonus_amount
            if hasattr(transaction, 'returned_at'):
                transaction.returned_at = current_time
        
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        print(f"Ошибка при обработке возврата заказа {posting_number}: {e}")
        return False
    finally:
        if should_close_db:
            db.close()
        if should_close_order_db:
            order_db.close()

def process_partial_return(
    posting_number: str,
    item_sku: str,
    returned_quantity: int,
    item_price: float,
    common_db: Session = None,
    order_db: Session = None
) -> bool:
    """Обработать частичный возврат товара.
    
    Args:
        posting_number: Номер отправления
        item_sku: SKU или product_id возвращенного товара (если sku пустой, используется product_id)
        returned_quantity: Количество возвращенных единиц
        item_price: Цена одной единицы товара
        common_db: Сессия общей БД для бонусов (опционально)
        order_db: Сессия БД заказов кабинета (опционально)
        
    Returns:
        bool: True если возврат обработан успешно
    """
    should_close_common_db = False
    should_close_order_db = False
    
    if common_db is None:
        common_db = SessionLocal()
        should_close_common_db = True
    
    if order_db is None:
        order_db = SessionLocal()
        should_close_order_db = True
    
    try:
        # Получить товар по sku
        # Если sku начинается с PROD_, это означает, что использовался product_id
        item = order_db.query(OrderItem).filter(
            OrderItem.posting_number == posting_number,
            OrderItem.sku == item_sku
        ).first()
        
        # Если не найдено и item_sku начинается с PROD_, пробуем извлечь product_id и найти по нему
        if not item and item_sku.startswith("PROD_"):
            product_id = item_sku.replace("PROD_", "")
            item = order_db.query(OrderItem).filter(
                OrderItem.posting_number == posting_number,
                OrderItem.product_id == product_id
            ).first()
        
        if not item:
            return False
        
        # Получить posting для итоговой суммы
        posting = order_db.query(Posting).filter(
            Posting.posting_number == posting_number
        ).first()
        
        if not posting:
            return False
        
        # Вычислить сумму возврата
        returned_amount = returned_quantity * item_price
        
        # Вычислить итоговую сумму заказа
        all_items = order_db.query(OrderItem).filter(
            OrderItem.posting_number == posting_number
        ).all()
        total_items_price = sum(
            float(i.price * (i.quantity - (i.returned_quantity or 0)))
            for i in all_items
        )
        delivery_price = float(posting.delivery_price or 0)
        total_order_amount = total_items_price + delivery_price
        
        if total_order_amount > 0:
            return_ratio = returned_amount / total_order_amount
            if return_ratio > 1.0:
                return_ratio = 1.0
        else:
            return_ratio = 1.0
        
        # Найти все бонусы по этому posting_number
        transactions = common_db.query(BonusTransaction).filter(
            BonusTransaction.posting_number == posting_number,
            BonusTransaction.status.in_(["frozen", "available"])
        ).all()
        
        if not transactions:
            return False
        
        current_time = datetime.now(timezone.utc)
        
        # Пересчитать бонусы пропорционально
        for transaction in transactions:
            returned_bonus = transaction.bonus_amount * return_ratio
            transaction.bonus_amount -= returned_bonus
            if transaction.bonus_amount <= 0:
                transaction.status = "returned"
            # Сохранить информацию о возврате
            transaction.returned_amount = (transaction.returned_amount or 0) + returned_bonus
            transaction.returned_at = current_time
        
        common_db.commit()
        return True
    except Exception as e:
        common_db.rollback()
        print(f"Ошибка при обработке частичного возврата товара {item_sku} в заказе {posting_number}: {e}")
        return False
    finally:
        if should_close_common_db:
            try:
                common_db.close()
            except:
                pass
        if should_close_order_db:
            try:
                order_db.close()
            except:
                pass

def check_and_update_bonus_availability(db: Session = None) -> int:
    """Проверить и обновить статус доступности бонусов."""
    should_close_db = False
    if db is None:
        db = SessionLocal()
        should_close_db = True
    
    try:
        current_time = datetime.now(timezone.utc)
        
        # Упрощенная версия - проверяем бонусы со статусом "frozen"
        # Если есть поле available_at, используем его, иначе проверяем по дате создания + 14 дней
        transactions = db.query(BonusTransaction).filter(
            BonusTransaction.status == "frozen"
        ).all()
        
        updated_count = 0
        
        for transaction in transactions:
            # Проверяем, прошло ли 14 дней
            days_passed = (current_time - transaction.created_at).days
            if hasattr(transaction, 'available_at') and transaction.available_at:
                should_be_available = transaction.available_at <= current_time
            else:
                should_be_available = days_passed >= 14
            
            if should_be_available:
                # Получаем posting из БД заказов
                order_db = get_orders_db_session("wistery")
                try:
                    posting = order_db.query(Posting).filter(Posting.posting_number == transaction.posting_number).first()
                    
                    if posting:
                        if posting.status == "delivered":
                            transaction.status = "available"
                            updated_count += 1
                        elif posting.status == "cancelled":
                            transaction.status = "returned"
                            if hasattr(transaction, 'returned_amount'):
                                transaction.returned_amount = transaction.bonus_amount
                            if hasattr(transaction, 'returned_at'):
                                transaction.returned_at = current_time
                            updated_count += 1
                    else:
                        transaction.status = "available"
                        updated_count += 1
                finally:
                    order_db.close()
        
        db.commit()
        return updated_count
    except Exception as e:
        db.rollback()
        print(f"Ошибка при проверке доступности бонусов: {e}")
        return 0
    finally:
        if should_close_db:
            db.close()

_withdrawal_settings_cache = None

class WithdrawalSettingsData:
    """Простой класс для хранения настроек вывода без привязки к сессии SQLAlchemy."""
    def __init__(self, min_withdrawal_amount: float, days_between_withdrawals: int | None, updated_at: datetime):
        self.min_withdrawal_amount = min_withdrawal_amount
        self.days_between_withdrawals = days_between_withdrawals
        self.updated_at = updated_at

def init_withdrawal_settings():
    """Создает дефолтные настройки вывода бонусов при первом запуске."""
    db = SessionLocal()
    try:
        existing = db.query(WithdrawalSettings).filter(WithdrawalSettings.id == 1).first()
        if not existing:
            default_settings = WithdrawalSettings(
                id=1,
                min_withdrawal_amount=100.0,
                days_between_withdrawals=None  # Без ограничений по умолчанию
            )
            db.add(default_settings)
            db.commit()
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def get_withdrawal_settings():
    """Получить текущие настройки вывода (с кэшированием для производительности)."""
    global _withdrawal_settings_cache
    
    if _withdrawal_settings_cache is not None:
        return _withdrawal_settings_cache
    
    db = SessionLocal()
    try:
        settings = db.query(WithdrawalSettings).filter(WithdrawalSettings.id == 1).first()
        if not settings:
            init_withdrawal_settings()
            settings = db.query(WithdrawalSettings).filter(WithdrawalSettings.id == 1).first()
        
        if settings:
            min_amount = settings.min_withdrawal_amount
            days_between = settings.days_between_withdrawals
            updated = settings.updated_at
            settings_data = WithdrawalSettingsData(min_amount, days_between, updated)
            _withdrawal_settings_cache = settings_data
            return settings_data
        else:
            return None
    finally:
        db.close()

def update_withdrawal_settings(settings: dict):
    """Обновить настройки вывода."""
    db = SessionLocal()
    try:
        existing = db.query(WithdrawalSettings).filter(WithdrawalSettings.id == 1).first()
        if not existing:
            existing = WithdrawalSettings(id=1)
            db.add(existing)
        
        if 'min_withdrawal_amount' in settings:
            existing.min_withdrawal_amount = settings['min_withdrawal_amount']
        if 'days_between_withdrawals' in settings:
            existing.days_between_withdrawals = settings['days_between_withdrawals']
        
        existing.updated_at = datetime.now(timezone.utc)
        db.commit()
        
        min_amount = existing.min_withdrawal_amount
        days_between = existing.days_between_withdrawals
        updated = existing.updated_at
        settings_data = WithdrawalSettingsData(min_amount, days_between, updated)
        
        global _withdrawal_settings_cache
        _withdrawal_settings_cache = settings_data
        return settings_data
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def clear_withdrawal_settings_cache():
    """Сбросить кэш настроек вывода (использовать после обновления)."""
    global _withdrawal_settings_cache
    _withdrawal_settings_cache = None

def get_daily_bonus_transactions(referrer_ozon_id: str, date: datetime) -> list:
    """Получить все транзакции бонусов за указанную дату для конкретного реферера."""
    db = SessionLocal()
    try:
        date_start = datetime.combine(date.date(), datetime.min.time())
        date_end = datetime.combine(date.date(), datetime.max.time())
        
        transactions = db.query(BonusTransaction).filter(
            BonusTransaction.referrer_ozon_id == str(referrer_ozon_id),
            BonusTransaction.created_at >= date_start,
            BonusTransaction.created_at <= date_end
        ).all()
        
        result = []
        # Получаем сессию БД заказов для получения данных о posting
        order_db = get_orders_db_session("wistery")
        try:
            for trans in transactions:
                posting = order_db.query(Posting).filter(Posting.posting_number == trans.posting_number).first()
                referral_participant = db.query(Participant).filter(Participant.ozon_id == trans.referral_ozon_id).first()
                
                # Получаем первый товар из заказа для отображения
                first_item = None
                if posting:
                    items = order_db.query(OrderItem).filter(OrderItem.posting_number == trans.posting_number).limit(1).all()
                    if items:
                        first_item = items[0]
                
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
                    "order_item_name": first_item.name if first_item else None,
                    "order_price_amount": str(first_item.price * first_item.quantity) if first_item else None,
                    "referral_name": referral_participant.name if referral_participant else None,
                    "referral_username": referral_participant.username if referral_participant else None,
                })
        finally:
            order_db.close()
        
        return result
    finally:
        db.close()

def get_daily_bonus_summary(referrer_ozon_id: str, date: datetime) -> dict:
    """Получить сводку бонусов за день с группировкой по уровням."""
    transactions = get_daily_bonus_transactions(referrer_ozon_id, date)
    
    if not transactions:
        return None
    
    levels = {}
    total_amount = 0.0
    
    for trans in transactions:
        level = trans["level"]
        bonus_amount = trans["bonus_amount"] or 0.0
        
        if level not in levels:
            levels[level] = {"count": 0, "total_amount": 0.0, "transactions": []}
        
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

def get_available_bonuses_for_withdrawal(ozon_id: str) -> float:
    """Получить сумму доступных к выводу бонусов для пользователя."""
    db = SessionLocal()
    try:
        check_and_update_bonus_availability(db)
        transactions = db.query(BonusTransaction).filter(
            BonusTransaction.referrer_ozon_id == str(ozon_id),
            BonusTransaction.status == "available"
        ).all()
        total = sum(t.bonus_amount for t in transactions if t.bonus_amount)
        return total
    finally:
        db.close()

def get_user_available_balance(ozon_id: str) -> float:
    """Получить доступный баланс пользователя (только бонусы со статусом 'available')."""
    db = SessionLocal()
    try:
        transactions = db.query(BonusTransaction).filter(
            BonusTransaction.referrer_ozon_id == str(ozon_id),
            BonusTransaction.status == "available"
        ).all()
        total = sum(t.bonus_amount for t in transactions if t.bonus_amount)
        return total
    finally:
        db.close()

def get_user_total_balance(ozon_id: str) -> float:
    """Получить общий баланс пользователя (все статусы)."""
    db = SessionLocal()
    try:
        transactions = db.query(BonusTransaction).filter(
            BonusTransaction.referrer_ozon_id == str(ozon_id)
        ).all()
        total = sum(t.bonus_amount for t in transactions if t.bonus_amount)
        return total
    finally:
        db.close()

def has_active_withdrawal_request(user_ozon_id: str) -> bool:
    """Проверить, есть ли у пользователя активная заявка на вывод."""
    db = SessionLocal()
    try:
        active_request = db.query(WithdrawalRequest).filter(
            WithdrawalRequest.user_ozon_id == str(user_ozon_id),
            WithdrawalRequest.status.in_(["processing", "approved"])
        ).first()
        return active_request is not None
    finally:
        db.close()

def get_active_withdrawal_request(user_ozon_id: str) -> dict | None:
    """Получить активную заявку пользователя."""
    db = SessionLocal()
    try:
        request = db.query(WithdrawalRequest).filter(
            WithdrawalRequest.user_ozon_id == str(user_ozon_id),
            WithdrawalRequest.status.in_(["processing", "approved"])
        ).first()
        
        if request:
            return {
                "id": request.id,
                "user_ozon_id": request.user_ozon_id,
                "user_telegram_id": request.user_telegram_id,
                "amount": request.amount,
                "status": request.status,
                "admin_comment": request.admin_comment,
                "created_at": request.created_at,
                "processed_at": request.processed_at
            }
        return None
    finally:
        db.close()

def check_withdrawal_period(user_ozon_id: str) -> tuple[bool, str | None]:
    """Проверить периодичность вывода (через сколько дней можно подать новую заявку)."""
    settings = get_withdrawal_settings()
    
    if settings.days_between_withdrawals is None:
        return True, None
    
    db = SessionLocal()
    try:
        last_request = db.query(WithdrawalRequest).filter(
            WithdrawalRequest.user_ozon_id == str(user_ozon_id),
            WithdrawalRequest.status.in_(["completed", "rejected"])
        ).order_by(WithdrawalRequest.processed_at.desc()).first()
        
        if not last_request:
            return True, None
        
        days_passed = (datetime.now(timezone.utc) - last_request.processed_at).days
        
        if days_passed < settings.days_between_withdrawals:
            days_left = settings.days_between_withdrawals - days_passed
            error_msg = f"Ты можешь подать новую заявку через {days_left} дней (после {last_request.processed_at.strftime('%d.%m.%Y')})"
            return False, error_msg
        
        return True, None
    finally:
        db.close()

def create_withdrawal_request(user_ozon_id: str, user_telegram_id: str, amount: float) -> dict:
    """Создать заявку на вывод бонусов."""
    db = SessionLocal()
    try:
        if has_active_withdrawal_request(user_ozon_id):
            raise ValueError("У тебя уже есть активная заявка на вывод. Дождись её обработки.")
        
        settings = get_withdrawal_settings()
        if amount < settings.min_withdrawal_amount:
            raise ValueError(f"Минимальная сумма вывода: {settings.min_withdrawal_amount} ₽")
        
        available_balance = get_user_available_balance(user_ozon_id)
        if amount > available_balance:
            raise ValueError(f"Недостаточно средств. Доступный баланс: {available_balance} ₽")
        
        allowed, error_msg = check_withdrawal_period(user_ozon_id)
        if not allowed:
            raise ValueError(error_msg)
        
        request = WithdrawalRequest(
            user_ozon_id=str(user_ozon_id),
            user_telegram_id=str(user_telegram_id),
            amount=amount,
            status="processing"
        )
        
        db.add(request)
        db.commit()
        db.refresh(request)
        
        return {
            "id": request.id,
            "user_ozon_id": request.user_ozon_id,
            "user_telegram_id": request.user_telegram_id,
            "amount": request.amount,
            "status": request.status,
            "created_at": request.created_at
        }
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def get_user_withdrawal_requests(user_ozon_id: str) -> list:
    """Получить список всех заявок пользователя."""
    db = SessionLocal()
    try:
        requests = db.query(WithdrawalRequest).filter(
            WithdrawalRequest.user_ozon_id == str(user_ozon_id)
        ).order_by(WithdrawalRequest.created_at.desc()).all()
        
        result = []
        for req in requests:
            result.append({
                "id": req.id,
                "amount": req.amount,
                "status": req.status,
                "admin_comment": req.admin_comment,
                "created_at": req.created_at,
                "processed_at": req.processed_at,
                "completed_at": req.completed_at
            })
        
        return result
    finally:
        db.close()

def get_pending_withdrawal_requests() -> list:
    """Получить список заявок со статусом 'processing' (для админов)."""
    db = SessionLocal()
    try:
        requests = db.query(WithdrawalRequest).filter(
            WithdrawalRequest.status == "processing"
        ).order_by(WithdrawalRequest.created_at.asc()).all()
        
        result = []
        for req in requests:
            participant = db.query(Participant).filter(Participant.ozon_id == req.user_ozon_id).first()
            
            result.append({
                "id": req.id,
                "user_ozon_id": req.user_ozon_id,
                "user_telegram_id": req.user_telegram_id,
                "user_name": participant.name if participant else None,
                "user_username": participant.username if participant else None,
                "amount": req.amount,
                "status": req.status,
                "created_at": req.created_at
            })
        
        return result
    finally:
        db.close()

def get_withdrawal_request_by_id(request_id: int) -> dict | None:
    """Получить заявку по ID."""
    db = SessionLocal()
    try:
        request = db.query(WithdrawalRequest).filter(WithdrawalRequest.id == request_id).first()
        
        if request:
            participant = db.query(Participant).filter(Participant.ozon_id == request.user_ozon_id).first()
            
            return {
                "id": request.id,
                "user_ozon_id": request.user_ozon_id,
                "user_telegram_id": request.user_telegram_id,
                "user_name": participant.name if participant else None,
                "user_username": participant.username if participant else None,
                "amount": request.amount,
                "status": request.status,
                "admin_comment": request.admin_comment,
                "processed_by": request.processed_by,
                "created_at": request.created_at,
                "processed_at": request.processed_at,
                "completed_at": request.completed_at
            }
        return None
    finally:
        db.close()

def cancel_withdrawal_request(request_id: int, user_ozon_id: str) -> bool:
    """Отменить заявку на вывод (только для статуса 'processing')."""
    db = SessionLocal()
    try:
        request = db.query(WithdrawalRequest).filter(
            WithdrawalRequest.id == request_id,
            WithdrawalRequest.user_ozon_id == str(user_ozon_id),
            WithdrawalRequest.status == "processing"
        ).first()
        
        if not request:
            return False
        
        db.delete(request)
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def reserve_and_withdraw_bonuses(user_ozon_id: str, amount: float, withdrawal_request_id: int) -> bool:
    """Резервировать и списать бонусы по FIFO при одобрении заявки."""
    db = SessionLocal()
    try:
        transactions = db.query(BonusTransaction).filter(
            BonusTransaction.referrer_ozon_id == str(user_ozon_id),
            BonusTransaction.status == "available"
        ).order_by(BonusTransaction.created_at.asc()).all()
        
        remaining_amount = amount
        used_transactions = []
        
        for transaction in transactions:
            if remaining_amount <= 0:
                break
            
            if transaction.bonus_amount:
                if transaction.bonus_amount <= remaining_amount:
                    used_amount = transaction.bonus_amount
                    remaining_amount -= used_amount
                else:
                    used_amount = remaining_amount
                    remaining_amount = 0
                
                transaction.status = "withdrawn"
                
                withdrawal_transaction = WithdrawalTransaction(
                    withdrawal_request_id=withdrawal_request_id,
                    bonus_transaction_id=transaction.id,
                    amount=used_amount
                )
                db.add(withdrawal_transaction)
                used_transactions.append(transaction)
        
        if remaining_amount > 0:
            db.rollback()
            return False
        
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def approve_withdrawal_request(request_id: int, admin_telegram_id: str) -> bool:
    """Одобрить заявку на вывод."""
    db = SessionLocal()
    try:
        request = db.query(WithdrawalRequest).filter(
            WithdrawalRequest.id == request_id,
            WithdrawalRequest.status == "processing"
        ).first()
        
        if not request:
            return False
        
        success = reserve_and_withdraw_bonuses(request.user_ozon_id, request.amount, request_id)
        if not success:
            return False
        
        request.status = "approved"
        request.processed_by = str(admin_telegram_id)
        request.processed_at = datetime.now(timezone.utc)
        
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def reject_withdrawal_request(request_id: int, admin_telegram_id: str, reason: str) -> bool:
    """Отклонить заявку на вывод."""
    db = SessionLocal()
    try:
        request = db.query(WithdrawalRequest).filter(
            WithdrawalRequest.id == request_id,
            WithdrawalRequest.status == "processing"
        ).first()
        
        if not request:
            return False
        
        request.status = "rejected"
        request.processed_by = str(admin_telegram_id)
        request.processed_at = datetime.now(timezone.utc)
        request.admin_comment = reason
        
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def complete_withdrawal_request(request_id: int) -> bool:
    """Завершить выплату (изменить статус на 'completed')."""
    db = SessionLocal()
    try:
        request = db.query(WithdrawalRequest).filter(
            WithdrawalRequest.id == request_id,
            WithdrawalRequest.status == "approved"
        ).first()
        
        if not request:
            return False
        
        request.status = "completed"
        request.completed_at = datetime.now(timezone.utc)
        
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()
# >>> КОНЕЦ БЛОКА: ФУНКЦИИ ДЛЯ РАБОТЫ С БОНУСАМИ <<<

# >>> КОНЕЦ БЛОКА: ФУНКЦИИ ВЗАИМОДЕЙСТВИЯ С БД <<<

if __name__ == "__main__":
    create_database()