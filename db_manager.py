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
    
    # Временные метки
    registration_date = Column(DateTime, default=datetime.utcnow)  # Дата регистрации
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
    
# >>> КОНЕЦ БЛОКА: ОПРЕДЕЛЕНИЕ МОДЕЛИ ТАБЛИЦЫ "bonus_transactions" <<<

# >>> НАЧАЛО БЛОКА: ФУНКЦИИ ВЗАИМОДЕЙСТВИЯ С БД <<<
def create_database():
    """Создает базу данных и все определенные таблицы."""
    Base.metadata.create_all(bind=engine)
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
            registration_date=datetime.utcnow(),
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
def get_user_orders_stats(ozon_id: str) -> dict:
    """Получает статистику по заказам пользователя.
    
    Args:
        ozon_id: Ozon ID пользователя
        
    Returns:
        dict: {"delivered_count": int, "total_sum": float}
    """
    db = SessionLocal()
    try:
        # Подсчитываем доставленные заказы и их сумму
        orders = db.query(Order).filter(
            Order.buyer_id == str(ozon_id),
            Order.status == "delivered"
        ).all()
        
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

def init_bonus_settings():
    """Создает дефолтные настройки бонусов при первом запуске."""
    db = SessionLocal()
    try:
        existing = db.query(BonusSettings).filter(BonusSettings.id == 1).first()
        if not existing:
            default_settings = BonusSettings(
                id=1,
                max_levels=3,
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
        # Проверяем, что покупатель зарегистрирован
        buyer_participant = db.query(Participant).filter(
            Participant.ozon_id == order.buyer_id
        ).first()
        
        if not buyer_participant:
            return []  # Покупатель не зарегистрирован
        
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
        
        # Получаем реферальную цепочку (передаем дату заказа для проверки)
        chain = get_referral_chain(order.buyer_id, settings.max_levels, order.created_at, db)
        
        bonuses = []
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
        for bonus_data in bonuses:
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

# >>> КОНЕЦ БЛОКА: ФУНКЦИИ ДЛЯ РАБОТЫ С НАЧИСЛЕНИЕМ БОНУСОВ <<<

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
# >>> КОНЕЦ БЛОКА: ФУНКЦИИ ДЛЯ РАБОТЫ С НАСТРОЙКАМИ СИНХРОНИЗАЦИИ <<<

# >>> КОНЕЦ БЛОКА: ФУНКЦИИ ВЗАИМОДЕЙСТВИЯ С БД <<<

if __name__ == "__main__":
    create_database()