import os 
from datetime import datetime, timedelta
from typing import List, Any, Dict
import json
import time

import requests 
from sqlalchemy.orm import Session # Для работы с сессией DB
from sqlalchemy import func  # Для работы с датами в SQL запросах
# Импортируем из db_manager новые функции и модель
from db_manager import (
    get_db, Posting, OrderItem, Customer, Participant, order_exists, 
    create_or_update_customer, get_customer,
    accrue_bonuses_for_posting, process_order_return, process_partial_return,
    check_and_update_bonus_availability, get_orders_db_path
) 
# Используем БД для хранения времени синхронизации
from db_manager import get_last_sync_timestamp, set_last_sync_timestamp, get_last_order_date, set_last_order_date 
from dotenv import load_dotenv

load_dotenv()
OZON_API_KEY = os.getenv("OZON_API_KEY")
OZON_CLIENT_ID = os.getenv("OZON_CLIENT_ID")

def get_ozon_cabinets() -> List[Dict[str, str]]:
    """Получает список всех настроенных кабинетов Ozon из .env.
    
    Returns:
        List[Dict]: Список словарей с настройками кабинетов:
        [
            {
                "api_key": "...",
                "client_id": "...",
                "cabinet_name": "wistery"
            },
            {
                "api_key": "...",
                "client_id": "...",
                "cabinet_name": "beiber"
            },
            ...
        ]
    """
    cabinets = []
    
    # Первый кабинет (без суффикса)
    api_key = os.getenv("OZON_API_KEY")
    client_id = os.getenv("OZON_CLIENT_ID")
    cabinet_name = os.getenv("OZON_CABINET_NAME", "wistery")  # По умолчанию wistery
    
    if api_key and client_id:
        cabinets.append({
            "api_key": api_key,
            "client_id": client_id,
            "cabinet_name": cabinet_name
        })
    
    # Дополнительные кабинеты (с суффиксами _2, _3, и т.д.)
    cabinet_num = 2
    while True:
        api_key = os.getenv(f"OZON_API_KEY_{cabinet_num}")
        client_id = os.getenv(f"OZON_CLIENT_ID_{cabinet_num}")
        cabinet_name = os.getenv(f"OZON_CABINET_NAME_{cabinet_num}")
        
        if not api_key or not client_id:
            break  # Больше кабинетов нет
        
        if not cabinet_name:
            cabinet_name = f"cabinet_{cabinet_num}"  # Дефолтное имя, если не указано
        
        cabinets.append({
            "api_key": api_key,
            "client_id": client_id,
            "cabinet_name": cabinet_name
        })
        
        cabinet_num += 1
    
    return cabinets

def transform_ozon_customer_data(posting: Dict) -> Dict:
    """Преобразует данные клиента из Ozon API в словарь для записи в DB.
    
    ВАЖНО: buyer_id извлекается из posting_number:
    - Если есть тире: первые цифры до первого тире (например: "10054917-1093-1" -> "10054917")
    - Если тире нет: весь posting_number и есть buyer_id
    """
    
    posting_number = posting.get("posting_number", "")
    
    # Извлекаем buyer_id из posting_number (первые цифры до первого тире)
    # Если тире нет, то весь posting_number и есть buyer_id
    buyer_id = ""
    if posting_number:
        if "-" in posting_number:
            buyer_id = posting_number.split("-")[0]
        else:
            buyer_id = posting_number
    
    if not buyer_id:
        return None
    
    # Пробуем получить данные из addressee или customer (если они есть)
    addressee = posting.get("addressee", {})
    customer = posting.get("customer", {})
    
    # Извлекаем данные о клиенте
    # Пробуем получить данные из addressee или customer
    address_full = ""
    name = ""
    phone = ""
    
    if isinstance(addressee, dict):
        address_full = addressee.get("address", "")
        name = addressee.get("name", "")
        phone = addressee.get("phone", "")
    
    # Если данных нет в addressee, пробуем customer
    if not name and isinstance(customer, dict):
        name = customer.get("name", "")
        phone = customer.get("phone", "")
    
    # Парсим адрес для извлечения региона и города
    delivery_region = posting.get("delivery_method", {}).get("warehouse_name", "")
    delivery_city = ""
    if address_full:
        # Пытаемся извлечь город из адреса (обычно формат: "Город, улица...")
        parts = address_full.split(",")
        if len(parts) > 0:
            delivery_city = parts[0].strip()
    
    # Извлекаем дату создания заказа
    created_at = posting.get("created_at", "")
    created_date_obj = None
    if created_at and 'T' in created_at:
        try:
            created_date_obj = datetime.strptime(created_at.split('.')[0], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            pass
    
    # Финансовые данные
    financial_data = posting.get("financial_data", {})
    products = financial_data.get("products", [])
    total_price = sum(float(item.get("price", 0)) for item in products)
    
    result = {
        "buyer_id": buyer_id,
        "name": name,
        "phone": phone,
        "email": "",  # Ozon API обычно не предоставляет email напрямую
        "address": address_full,
        "delivery_region": delivery_region or posting.get("cluster_to", ""),
        "delivery_city": delivery_city,
        "cluster_to": posting.get("cluster_to", ""),
        "client_segment": posting.get("client_segment", ""),
        "is_legal_entity": "да" if posting.get("is_legal", False) else "нет",
        "payment_method": posting.get("payment_method", {}).get("name", ""),
        "first_order_date": created_date_obj,
        "last_order_date": created_date_obj,
    }
    return result

def parse_datetime(date_str: str) -> datetime | None:
    """Парсит дату из ISO формата в объект datetime."""
    if not date_str:
        return None
    if 'T' in date_str:
        try:
            return datetime.strptime(date_str.split('.')[0], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            return None
    return None

def transform_posting_data(posting: Dict) -> Dict:
    """Преобразует данные posting из API в словарь для таблицы postings.
    
    Сохраняет все поля с теми же названиями, что в API Ozon.
    
    ВАЖНО: Некоторые поля могут быть пустыми, если Ozon API их не предоставляет.
    Это нормальное поведение - код обрабатывает пустые значения безопасно.
    """
    posting_number = posting.get("posting_number", "")
    
    # Извлекаем buyer_id из posting_number (как раньше)
    buyer_id = ""
    if posting_number:
        if "-" in posting_number:
            buyer_id = posting_number.split("-")[0]
        else:
            buyer_id = posting_number
    
    # Разворачиваем вложенные объекты
    addressee = posting.get("addressee", {})
    customer = posting.get("customer", {})
    delivery_method = posting.get("delivery_method", {})
    payment_method = posting.get("payment_method", {})
    financial_data = posting.get("financial_data", {})
    
    return {
        "posting_number": posting_number,
        "order_id": posting.get("order_id", ""),
        "order_number": posting.get("order_number", ""),
        "status": posting.get("status", ""),
        "created_at": parse_datetime(posting.get("created_at")),
        "in_process_at": posting.get("in_process_at", ""),
        "delivering_date": posting.get("delivering_date", ""),
        "estimated_delivery_date": posting.get("estimated_delivery_date", ""),
        "shipment_date": posting.get("shipment_date", ""),
        "cluster_from": posting.get("cluster_from", ""),
        "cluster_to": posting.get("cluster_to", ""),
        "region_from": posting.get("region_from", ""),
        "region_to": posting.get("region_to", ""),
        "delivery_price": float(posting.get("delivery_price", 0) or 0),
        "delivery_method_name": delivery_method.get("name", "") if isinstance(delivery_method, dict) else "",
        "delivery_method_warehouse_name": delivery_method.get("warehouse_name", "") if isinstance(delivery_method, dict) else "",
        "delivery_method_warehouse_id": str(delivery_method.get("warehouse_id", "")) if isinstance(delivery_method, dict) else "",
        "delivery_method_tpl_provider_id": str(delivery_method.get("tpl_provider_id", "")) if isinstance(delivery_method, dict) else "",
        "addressee_name": addressee.get("name", "") if isinstance(addressee, dict) else "",
        "addressee_phone": addressee.get("phone", "") if isinstance(addressee, dict) else "",
        "addressee_address": addressee.get("address", "") if isinstance(addressee, dict) else "",
        "addressee_address_tail": addressee.get("address_tail", "") if isinstance(addressee, dict) else "",
        "addressee_postal_code": addressee.get("postal_code", "") if isinstance(addressee, dict) else "",
        "customer_name": customer.get("name", "") if isinstance(customer, dict) else "",
        "customer_phone": customer.get("phone", "") if isinstance(customer, dict) else "",
        "client_segment": posting.get("client_segment", ""),
        "is_legal": posting.get("is_legal", False),
        "is_express": posting.get("is_express", False),
        "is_multibox": posting.get("is_multibox", False),
        "multi_box_qty": posting.get("multi_box_qty"),
        "tracking_number": posting.get("tracking_number", ""),
        "tpl_integration_type": posting.get("tpl_integration_type", ""),
        "payment_method_name": payment_method.get("name", "") if isinstance(payment_method, dict) else "",
        "payment_method_id": str(payment_method.get("id", "")) if isinstance(payment_method, dict) else "",
        "currency_code": financial_data.get("currency_code", "RUB") if isinstance(financial_data, dict) else "RUB",
        "total_discount_value": float(financial_data.get("total_discount_value", 0) or 0) if isinstance(financial_data, dict) else 0.0,
        "total_discount_percent": float(financial_data.get("total_discount_percent", 0) or 0) if isinstance(financial_data, dict) else 0.0,
        "commission_amount": float(financial_data.get("commission_amount", 0) or 0) if isinstance(financial_data, dict) else 0.0,
        "payout": float(financial_data.get("payout", 0) or 0) if isinstance(financial_data, dict) else 0.0,
        "buyer_id": buyer_id,
    }

def transform_order_item_data(posting_number: str, item: Dict) -> Dict:
    """Преобразует данные товара из API в словарь для таблицы order_items.
    
    Сохраняет все поля с теми же названиями, что в API Ozon.
    Если sku пустой, использует product_id для уникальности.
    """
    promos = item.get("promos", [])
    promos_str = ", ".join([p.get("name", "") for p in promos]) if promos and isinstance(promos, list) else ""
    
    old_price = float(item.get("old_price", 0)) if item.get("old_price") else None
    price = float(item.get("price", 0) or 0)
    discount_rub = (old_price - price) if old_price else None
    
    # Если sku пустой, используем product_id для уникальности
    sku = item.get("sku", "")
    if not sku or not sku.strip():
        product_id = item.get("product_id")
        if product_id:
            sku = f"PROD_{product_id}"  # Префикс, чтобы отличить от реального sku
        else:
            sku = ""  # Оставляем пустым, если нет ни sku, ни product_id
    
    return {
        "posting_number": posting_number,
        "sku": sku,
        "product_id": str(item.get("product_id", "")) if item.get("product_id") else None,
        "offer_id": item.get("offer_id", ""),
        "name": item.get("name", ""),
        "price": price,
        "old_price": old_price,
        "quantity": int(item.get("quantity", 0) or 0),
        "returned_quantity": 0,  # По умолчанию 0
        "total_discount_value": float(item.get("total_discount_value", 0) or 0),
        "total_discount_percent": float(item.get("total_discount_percent", 0) or 0),
        "discount_percent": float(item.get("discount_percent", 0) or 0),
        "discount_rub": discount_rub,
        "commission_amount": float(item.get("commission_amount", 0) or 0),
        "commission_percent": float(item.get("commission_percent", 0) or 0),
        "payout": float(item.get("payout", 0) or 0),
        "weight": float(item.get("weight", 0)) if item.get("weight") else None,
        "promos": promos_str,
    }

def transform_ozon_data_for_sheets(posting: Dict, item: Dict) -> Dict:
    """Преобразует данные одного товара (item) из Ozon API в словарь для записи в DB."""
    
    # Общие данные
    order_id = posting.get("order_id", "")
    posting_number = posting.get("posting_number", "")
    status = posting.get("status", "")
    created_at = posting.get("created_at", "")
    
    # Извлекаем buyer_id из posting_number (первые цифры до первого тире)
    # Если тире нет, то весь posting_number и есть buyer_id
    buyer_id = ""
    if posting_number:
        if "-" in posting_number:
            buyer_id = posting_number.split("-")[0]
        else:
            buyer_id = posting_number

    # Данные товара
    item_name = item.get("name", "")
    item_sku = item.get("sku", "")
    quantity = str(item.get("quantity", 0))
    price_amount = str(item.get("price", 0))

    # Форматируем даты
    created_date_obj = None
    if created_at and 'T' in created_at:
        try:
            # Преобразуем ISO строку в объект datetime
            created_date_obj = datetime.strptime(created_at.split('.')[0], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            pass
    
    # Извлекаем дополнительные данные из posting
    addressee = posting.get("addressee", {})
    delivery_method = posting.get("delivery_method", {})
    financial_data = posting.get("financial_data", {})
    
    # Мы возвращаем словарь, где ключи соответствуют полям в модели Posting (db_manager.py)
    return {
        "order_id": order_id,
        "posting_number": posting_number,
        "status": status,
        "created_at": created_date_obj if created_date_obj else datetime.now(),
        "buyer_id": buyer_id,
        "price_amount": price_amount,
        "item_name": item_name,
        "item_sku": item_sku,
        "quantity": quantity,
        
        # Заполняем остальные поля из данных posting
        "delivering_date": posting.get("delivering_date", ""),
        "in_process_at": posting.get("in_process_at", ""),
        "cluster_from": posting.get("cluster_from", ""),
        "cluster_to": posting.get("cluster_to", ""),
        "address": addressee.get("address", ""),
        "currency_code": financial_data.get("currency_code", "RUB"),
        "articul": item.get("offer_id", ""), 
        "buyer_paid": str(financial_data.get("products", [{}])[0].get("price", "") if financial_data.get("products") else ""),
        "shipping_cost": str(posting.get("delivery_price", "0")),
        "is_redeemed": "да" if posting.get("status") == "delivered" else "нет",
        "price_before_discount": str(item.get("old_price", price_amount)), 
        "discount_percent": str(item.get("discount_percent", "")),
        "discount_rub": str(float(item.get("old_price", 0)) - float(price_amount)) if item.get("old_price") else "",
        "promotion": ", ".join([p.get("name", "") for p in item.get("promos", [])]),
        "weight_kg": str(item.get("weight", "")),
        "norm_delivery_time": str(posting.get("estimated_delivery_date", "")),
        "shipping_evaluation": "",
        "shipping_warehouse": delivery_method.get("warehouse_name", ""),
        "delivery_region": delivery_method.get("warehouse_name", ""),
        "delivery_city": addressee.get("address", "").split(",")[0] if addressee.get("address") else "",
        "delivery_method": delivery_method.get("name", ""),
        "client_segment": posting.get("client_segment", ""),
        "is_legal_entity": "да" if posting.get("is_legal", False) else "нет",
        "payment_method": posting.get("payment_method", {}).get("name", "") if posting.get("payment_method") else "",
    }

def fetch_new_orders_from_api(
    date_since: datetime, 
    date_to: datetime = None,
    exclude_statuses: List[str] = None,
    api_key: str = None,
    client_id: str = None
) -> tuple[List[Dict], str | None]:
    """Получает новые заказы из API Ozon и возвращает список словарей (сырые данные).
    
    Args:
        date_since: Начальная дата периода
        date_to: Конечная дата периода (если None, используется текущая дата)
        exclude_statuses: Список статусов для исключения (например, ["delivered", "cancelled"])
                         Фильтрация происходит ПОСЛЕ получения данных из API
        api_key: API ключ кабинета (если None, используется глобальный OZON_API_KEY)
        client_id: Client ID кабинета (если None, используется глобальный OZON_CLIENT_ID)
        
    Returns:
        tuple: (список заказов, ошибка или None)
    """
    
    # Используем переданные параметры или глобальные переменные (для обратной совместимости)
    used_api_key = api_key if api_key else OZON_API_KEY
    used_client_id = client_id if client_id else OZON_CLIENT_ID
    
    if not used_api_key or not used_client_id:
        error_msg = "OZON_API_KEY или OZON_CLIENT_ID не заданы"
        print(f"Внимание: {error_msg}. Возвращаем пустой список.")
        return [], error_msg
    
    if date_to is None:
        date_to = datetime.now()
        
    # Форматируем даты для API
    date_since_str = date_since.isoformat(timespec='seconds') + "Z"
    date_to_str = date_to.isoformat(timespec='seconds') + "Z"

    headers = {
        "Client-Id": used_client_id,
        "Api-Key": used_api_key,
        "Content-Type": "application/json",
    }
    
    # Используем FBO. Если нужно FBS, замени на /v2/posting/fbs/list
    url = "https://api-seller.ozon.ru/v2/posting/fbo/list" 

    all_postings = []
    offset = 0
    limit = 100
    
    # Повторные попытки при ошибках
    max_retries = 3
    retry_delay = 5  # секунд
    
    for attempt in range(1, max_retries + 1):
        try:
            # Обрабатываем пагинацию - запрашиваем все страницы заказов
            while True:
                payload = {
                    "filter": {
                        "since": date_since_str,
                        "to": date_to_str,
                        # Убрали фильтр по статусу - запрашиваем ВСЕ заказы
                    },
                    "limit": limit, 
                    "offset": offset,
                    # Запрашиваем ВСЕ доступные данные из API Ozon
                    # Некоторые поля могут оставаться пустыми, если Ozon их не предоставляет
                    "with": {
                        "barcodes": True,  # Штрихкоды
                        "financial_data": True,  # Финансовые данные (товары, цены, комиссии)
                        "translit": True,  # Транслитерация
                        "delivery_method": True,  # Способ доставки
                        "addressee": True,  # Данные адресата (получателя)
                        "customer": True,  # Данные покупателя
                        "analytics_data": True  # Аналитические данные
                    }
                }
                
                try:
                    response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=60)
                    response.raise_for_status() 
                    data = response.json()
                except requests.exceptions.Timeout:
                    print(f"    ⚠️ Таймаут запроса к API Ozon (60 сек)")
                    raise
                except requests.exceptions.RequestException as e:
                    print(f"    ⚠️ Ошибка запроса: {e}")
                    raise
                
                # Проверяем структуру ответа API Ozon
                if not data or 'result' not in data:
                    print("    Ошибка: API Ozon вернул неверный формат ответа или нет заказов.")
                    break
                
                result = data['result']
                page_postings = []
                
                # API Ozon может вернуть result как список напрямую или как словарь с ключом postings
                if isinstance(result, list):
                    # result - это уже список заказов
                    page_postings = result
                elif isinstance(result, dict) and 'postings' in result:
                    # result - это словарь с ключом postings
                    page_postings = result['postings']
                else:
                    # Неожиданная структура
                    print("    Ошибка: API Ozon вернул неверный формат ответа.")
                    break
                
                # Фильтруем по статусу, если указан exclude_statuses
                if exclude_statuses and page_postings:
                    page_postings = [
                        p for p in page_postings 
                        if p.get("status") not in exclude_statuses
                    ]
                
                # Добавляем заказы со странице к общему списку
                if page_postings:
                    all_postings.extend(page_postings)
                    
                    # Если получили меньше заказов, чем limit, значит это последняя страница
                    # Но учитываем, что после фильтрации может быть меньше
                    # Проверяем исходный результат, а не отфильтрованный
                    original_result = result if isinstance(result, list) else result.get('postings', [])
                    if len(original_result) < limit:
                        break
                    
                    # Переходим к следующей странице
                    offset += limit
                else:
                    # Нет заказов на этой странице - выходим
                    break
            
            # Успешно получили все данные
            if len(all_postings) > 0:
                print(f"    ✅ Успешно получено {len(all_postings)} заказов за период")
            return all_postings, None

        except requests.exceptions.RequestException as e:
            error_msg = f"Ошибка при запросе к API Ozon: {e}"
            if attempt < max_retries:
                print(f"{error_msg} (попытка {attempt}/{max_retries}). Повтор через {retry_delay} сек...")
                time.sleep(retry_delay)
            else:
                print(f"{error_msg} (после {max_retries} попыток)")
                return [], f"{error_msg} (после {max_retries} попыток)"
        except json.JSONDecodeError as e:
            error_msg = f"Ошибка декодирования JSON ответа: {e}"
            if attempt < max_retries:
                print(f"{error_msg} (попытка {attempt}/{max_retries}). Повтор через {retry_delay} сек...")
                time.sleep(retry_delay)
            else:
                print(f"{error_msg} (после {max_retries} попыток)")
                return [], f"{error_msg} (после {max_retries} попыток)"
        except Exception as e:
            error_msg = f"Неожиданная ошибка: {e}"
            if attempt < max_retries:
                print(f"{error_msg} (попытка {attempt}/{max_retries}). Повтор через {retry_delay} сек...")
                time.sleep(retry_delay)
            else:
                print(f"{error_msg} (после {max_retries} попыток)")
                return [], f"{error_msg} (после {max_retries} попыток)"
    
    # Если дошли сюда, все попытки исчерпаны
    return [], f"Не удалось получить заказы после {max_retries} попыток"

def get_last_synced_time() -> datetime:
    """
    Возвращает дату последнего заказа из базы данных (для определения стартовой даты запроса к API).
    Если это первый запуск, возвращает 01.12.2025.
    
    ВАЖНО: Эта функция используется для алгоритма скользящей даты и определения стартовой даты запроса.
    Она возвращает дату последнего заказа, а не время синхронизации.
    """
    last_order_date = get_last_order_date()
    
    if last_order_date:
        return last_order_date
    else:
        # Если в БД пусто (первый запуск), начинаем скачивать данные с 01.12.2025
        default_sync_time = datetime(2025, 12, 1)
        return default_sync_time

def update_final_orders_status(
    db: Session,
    final_posting_numbers: set,
    date_start: datetime,
    date_to: datetime
) -> Dict[str, int]:
    """
    Определяет финальный статус заказов, которые исчезли из списка активных.
    Загружает эти заказы из API и обновляет их статус в БД.
    
    Args:
        db: Сессия базы данных
        final_posting_numbers: Множество posting_number заказов, которые стали финальными
        date_start: Начало периода для запроса
        date_to: Конец периода для запроса
    
    Returns:
        dict: Статистика обновлений {"delivered": X, "cancelled": Y}
    """
    if not final_posting_numbers:
        return {"delivered": 0, "cancelled": 0}
    
    # Загружаем ВСЕ заказы за период (чтобы найти исчезнувшие)
    all_orders = fetch_new_orders_from_api(date_start, date_to, exclude_statuses=None)
    
    # Создаем словарь posting_number -> posting для быстрого поиска
    api_orders_map = {
        posting.get("posting_number"): posting
        for posting in all_orders
        if posting.get("posting_number") in final_posting_numbers
    }
    
    stats = {"delivered": 0, "cancelled": 0}
    
    # Обновляем статусы в БД
    for posting_number in final_posting_numbers:
        posting = db.query(Posting).filter(Posting.posting_number == posting_number).first()
        if not posting:
            continue
        
        # Получаем posting из API (если нашли)
        api_posting = api_orders_map.get(posting_number)
        
        if api_posting:
            new_status = api_posting.get("status", "")
            if new_status == "delivered":
                posting.status = "delivered"
                stats["delivered"] += 1
            elif new_status == "cancelled":
                posting.status = "cancelled"
                stats["cancelled"] += 1
            # Обновляем другие поля из posting
            if api_posting.get("delivering_date"):
                posting.delivering_date = api_posting.get("delivering_date")
            if api_posting.get("in_process_at"):
                posting.in_process_at = api_posting.get("in_process_at")
        else:
            # Заказ не найден в API - возможно, был удален или имеет другой статус
            # Предполагаем, что он доставлен (наиболее вероятный исход)
            print(f"Предупреждение: Заказ {posting_number} не найден в API. Устанавливаем статус 'delivered'.")
            if posting.status not in ["delivered", "cancelled"]:
                posting.status = "delivered"
                stats["delivered"] += 1
    
    return stats

def sync_single_cabinet(
    cabinet_name: str,
    api_key: str,
    client_id: str
) -> Dict:
    """Синхронизирует заказы для одного кабинета.
    
    Args:
        cabinet_name: Название кабинета
        api_key: API ключ кабинета
        client_id: Client ID кабинета
        
    Returns:
        dict: Результат синхронизации кабинета
    """
    from db_manager import (
        get_orders_db_session, create_orders_database, get_orders_db_path,
        get_last_order_date, set_last_order_date,
        Posting, OrderItem, Customer, Participant,
        process_order_return, check_and_update_bonus_availability,
        create_or_update_customer, get_customer
    )
    
    try:
        print(f"  Кабинет '{cabinet_name}': начало синхронизации...")
        
        # Создаем БД для кабинета, если не существует (или создаем таблицы, если их нет)
        create_orders_database(cabinet_name)
        
        # Убеждаемся, что таблицы созданы (повторная проверка)
        db_path = get_orders_db_path(cabinet_name)
        if os.path.exists(db_path):
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
                    print(f"  Кабинет '{cabinet_name}': таблицы отсутствуют, создаю...")
                    create_orders_database(cabinet_name)  # Повторная попытка создания
            except Exception as e:
                print(f"  Кабинет '{cabinet_name}': ошибка при проверке таблиц: {e}")
        
        # Получаем скользящую дату для кабинета
        last_order_date = get_last_order_date(cabinet_name)
        if not last_order_date:
            # Если дата не установлена, используем начальную дату 01.12.2025
            last_order_date = datetime(2025, 12, 1)
        
        date_since = last_order_date
        date_to = datetime.now()
        print(f"  Кабинет '{cabinet_name}': получена дата начала синхронизации: {date_since.strftime('%d.%m.%Y %H:%M')}")
        
        # Разбиваем период на дни и обрабатываем по одному дню за раз
        current_date = date_since.date()
        end_date = date_to.date()
        
        # Подсчитываем общее количество дней для прогресса
        total_days = (end_date - current_date).days + 1
        processed_days = 0
        
        all_raw_postings = []
        last_error = None
        
        print(f"  Кабинет '{cabinet_name}': период синхронизации {total_days} дней (с {current_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')})")
        print(f"  Кабинет '{cabinet_name}': начинаю обработку дней...")
        
        # Обрабатываем каждый день отдельно
        while current_date <= end_date:
            day_start = datetime.combine(current_date, datetime.min.time())
            day_end = datetime.combine(current_date, datetime.max.time())
            
            # Если это последний день, используем текущее время
            if current_date == end_date:
                day_end = date_to
            
            processed_days += 1
            
            # Запрашиваем заказы за один день
            try:
                day_postings, error = fetch_new_orders_from_api(
                    day_start, day_end,
                    exclude_statuses=None,
                    api_key=api_key,
                    client_id=client_id
                )
            except Exception as e:
                print(f"  Кабинет '{cabinet_name}': исключение при запросе за {current_date.strftime('%d.%m.%Y')}: {e}")
                import traceback
                traceback.print_exc()
                error = str(e)
                day_postings = []
            
            if error:
                print(f"  Кабинет '{cabinet_name}': день {processed_days}/{total_days} ({current_date.strftime('%d.%m.%Y')}) - ошибка: {error}")
                last_error = error
                # Продолжаем с другими днями, но запомним ошибку
                current_date += timedelta(days=1)
                continue
            elif day_postings:
                all_raw_postings.extend(day_postings)
                print(f"  Кабинет '{cabinet_name}': день {processed_days}/{total_days} ({current_date.strftime('%d.%m.%Y')}) - получено {len(day_postings)} заказов (всего: {len(all_raw_postings)})")
            # Если нет заказов за день и нет ошибки - просто пропускаем вывод
            
            # Переходим к следующему дню
            current_date += timedelta(days=1)
        
        # Если была ошибка при получении заказов и нет заказов, возвращаем ошибку
        if last_error and not all_raw_postings:
            return {
                "success": False,
                "count": 0,
                "period_start": date_since,
                "period_end": datetime.now(),
                "customers_count": 0,
                "new_customers_count": 0,
                "participants_with_orders_count": 0,
                "cabinet_name": cabinet_name,
                "client_id": client_id,
                "error": last_error
            }
        
        if not all_raw_postings:
            print(f"  Кабинет '{cabinet_name}': нет новых заказов для обновления.")
            print(f"  Кабинет '{cabinet_name}': завершение синхронизации (нет заказов)")
            sync_end_time = datetime.now()
            return {
                "success": True,
                "count": 0,
                "period_start": date_since,
                "period_end": sync_end_time,
                "customers_count": 0,
                "new_customers_count": 0,
                "participants_with_orders_count": 0,
                "first_day_stats": {},
                "cabinet_name": cabinet_name,
                "client_id": client_id,
                "error": None
            }
        
        # Получаем сессию БД для этого кабинета
        db = get_orders_db_session(cabinet_name)
        
        # Получаем сессию общей БД для рефералов (первый кабинет)
        from db_manager import SessionLocal as CommonSessionLocal
        common_db = CommonSessionLocal()
        
        try:
            new_records_count = 0
            new_customers_count = 0
            accrued_bonuses_count = 0  # Счетчик начисленных бонусов
            
            # Словарь для отслеживания клиентов и их статистики
            customers_data = {}
            
            # Словарь для анализа дат создания заказов
            orders_by_date = {}
            
            # Множество для отслеживания уже обработанных posting_number
            processed_posting_numbers = set()
            
            # Перебираем отправления и товары
            total_postings = len(all_raw_postings)
            processed_count = 0
            commit_interval = 1000  # Коммитим каждые 1000 заказов
            print(f"  Кабинет '{cabinet_name}': начинаю обработку {total_postings} заказов...")
            
            for posting in all_raw_postings:
                processed_count += 1
                
                posting_status = posting.get("status", "")
                
                # Извлекаем дату создания заказа для анализа
                created_at = posting.get("created_at", "")
                order_date = None
                if created_at and 'T' in created_at:
                    try:
                        created_date_obj = datetime.strptime(created_at.split('.')[0], "%Y-%m-%dT%H:%M:%S")
                        order_date = created_date_obj.date()
                    except ValueError:
                        pass
                
                # Добавляем заказ в словарь для анализа
                if order_date:
                    if order_date not in orders_by_date:
                        orders_by_date[order_date] = []
                    orders_by_date[order_date].append({
                        "posting": posting,
                        "status": posting_status
                    })
                
                # Обрабатываем ВСЕ заказы
                financial_data = posting.get("financial_data", {})
                posting_number = posting.get("posting_number", "")
                
                if not posting_number or posting_number.strip() == "":
                    continue
                
                # Проверка на дубликаты
                if posting_number in processed_posting_numbers:
                    continue
                
                # Flush только периодически, чтобы не замедлять обработку
                if processed_count % 100 == 0:
                    db.flush()
                
                # 1. Создать/обновить запись в postings
                posting_data = transform_posting_data(posting)
                existing_posting = db.query(Posting).filter(Posting.posting_number == posting_number).first()
                
                if existing_posting:
                    # Отправление уже существует - обновляем
                    old_status = existing_posting.status
                    for key, value in posting_data.items():
                        setattr(existing_posting, key, value)
                    existing_posting.sync_time = datetime.now()
                else:
                    # Создать новое отправление
                    new_posting = Posting(**posting_data)
                    db.add(new_posting)
                    old_status = None
                
                # Flush только периодически
                if processed_count % 100 == 0:
                    db.flush()  # Чтобы posting был доступен для order_items
                
                # 2. Обработать ВСЕ товары из financial_data.products[]
                if not financial_data or not financial_data.get("products"):
                    processed_posting_numbers.add(posting_number)
                    continue
                
                for item in financial_data.get("products", []):
                    item_data = transform_order_item_data(posting_number, item)
                    
                    # Ищем существующий товар по posting_number и sku
                    # (sku теперь всегда заполнен, даже если был пустой - используется product_id)
                    existing_item = db.query(OrderItem).filter(
                        OrderItem.posting_number == posting_number,
                        OrderItem.sku == item_data["sku"]
                    ).first()
                    
                    if existing_item:
                        # Обновить существующий товар
                        # Проверить изменение quantity для определения возврата
                        old_quantity = existing_item.quantity
                        new_quantity = item_data["quantity"]
                        
                        if new_quantity < old_quantity:
                            # Частичный возврат
                            returned_qty = old_quantity - new_quantity
                            existing_item.returned_quantity = (existing_item.returned_quantity or 0) + returned_qty
                            # Обработать возврат бонусов
                            # sku теперь всегда заполнен (используется product_id, если оригинальный sku пустой)
                            process_partial_return(
                                posting_number, 
                                item_data["sku"], 
                                returned_qty, 
                                item_data["price"],
                                common_db=common_db,
                                order_db=db
                            )
                        
                        # Обновить остальные поля
                        for key, value in item_data.items():
                            if key != "posting_number":  # Не обновлять ключ
                                setattr(existing_item, key, value)
                    else:
                        # Создать новый товар
                        new_item = OrderItem(**item_data)
                        db.add(new_item)
                        new_records_count += 1
                
                # Если статус изменился на "delivered", начисляем бонусы (в общей БД)
                if posting_status == "delivered" and old_status != "delivered":
                    try:
                        # Используем новую функцию для новой структуры
                        bonuses_count = accrue_bonuses_for_posting(posting_number, common_db=common_db, order_db=db, cabinet_name=cabinet_name)
                        if bonuses_count is None:
                            bonuses_count = 0
                        accrued_bonuses_count += bonuses_count
                    except Exception as bonus_error:
                        print(f"  Кабинет '{cabinet_name}': исключение при начислении бонусов за {posting_number}: {bonus_error}")
                        import traceback
                        traceback.print_exc()
                    import time
                    time.sleep(0.01)  # 10мс задержка
                
                # Если статус изменился с "delivered" на "cancelled"
                if old_status == "delivered" and posting_status == "cancelled":
                    process_order_return(posting_number, return_amount=None, db=common_db)
                
                processed_posting_numbers.add(posting_number)
                
                # Периодический коммит для больших объемов данных (перед обработкой клиентов)
                if processed_count % commit_interval == 0:
                    try:
                        db.commit()
                        common_db.commit()
                    except Exception as commit_error:
                        print(f"  Кабинет '{cabinet_name}': ошибка при промежуточном коммите: {commit_error}")
                        db.rollback()
                        common_db.rollback()
                
                # Обрабатываем данные клиента
                customer_data = transform_ozon_customer_data(posting)
                if customer_data:
                    buyer_id = customer_data.get("buyer_id")
                    if buyer_id:
                        if buyer_id not in customers_data:
                            customers_data[buyer_id] = {
                                "data": customer_data,
                                "orders_count": 0,
                                "total_spent": 0.0,
                                "first_order_date": customer_data.get("first_order_date"),
                                "last_order_date": customer_data.get("last_order_date"),
                            }
                        
                        products = financial_data.get("products", [])
                        order_total = sum(float(item.get("price", 0)) for item in products)
                        delivery_price = float(posting.get("delivery_price", 0) or 0)
                        order_total += delivery_price
                        
                        customers_data[buyer_id]["orders_count"] += 1
                        customers_data[buyer_id]["total_spent"] += order_total
                        
                        order_date_obj = customer_data.get("last_order_date")
                        if order_date_obj:
                            if not customers_data[buyer_id]["first_order_date"] or order_date_obj < customers_data[buyer_id]["first_order_date"]:
                                customers_data[buyer_id]["first_order_date"] = order_date_obj
                            if not customers_data[buyer_id]["last_order_date"] or order_date_obj > customers_data[buyer_id]["last_order_date"]:
                                customers_data[buyer_id]["last_order_date"] = order_date_obj
                continue
                
            
            # Сохраняем/обновляем клиентов
            for buyer_id, customer_info in customers_data.items():
                try:
                    customer_data = customer_info["data"]
                    existing_customer = get_customer(db, buyer_id)
                    
                    if existing_customer:
                        customer_data["total_orders"] = existing_customer.total_orders + customer_info["orders_count"]
                        customer_data["total_spent"] = str(float(existing_customer.total_spent or 0) + customer_info["total_spent"])
                        
                        if customer_info["first_order_date"]:
                            if not existing_customer.first_order_date or customer_info["first_order_date"] < existing_customer.first_order_date:
                                customer_data["first_order_date"] = customer_info["first_order_date"]
                            else:
                                customer_data["first_order_date"] = existing_customer.first_order_date
                        
                        if customer_info["last_order_date"]:
                            if not existing_customer.last_order_date or customer_info["last_order_date"] > existing_customer.last_order_date:
                                customer_data["last_order_date"] = customer_info["last_order_date"]
                            else:
                                customer_data["last_order_date"] = existing_customer.last_order_date
                    else:
                        customer_data["total_orders"] = customer_info["orders_count"]
                        customer_data["total_spent"] = str(customer_info["total_spent"])
                        new_customers_count += 1
                    
                    create_or_update_customer(db, customer_data)
                except Exception as e:
                    print(f"Ошибка при сохранении клиента {buyer_id}: {e}")
                    continue
            
            # Подсчитываем участников программы, совершивших покупку
            participants_with_orders = set()
            for buyer_id in customers_data.keys():
                try:
                    participant = common_db.query(Participant).filter(Participant.ozon_id == str(buyer_id)).first()
                    if participant:
                        participants_with_orders.add(buyer_id)
                except Exception as e:
                    print(f"Ошибка при проверке участника {buyer_id}: {e}")
            
            participants_count = len(participants_with_orders)
            
            print(f"  Кабинет '{cabinet_name}': обработка {total_postings} заказов завершена")
            
            # Сохраняем все новые записи
            db.commit()
            common_db.commit()
            
            # Обновляем доступность бонусов (в общей БД)
            print(f"  Кабинет '{cabinet_name}': обновление доступности бонусов...")
            check_and_update_bonus_availability(common_db)
            print(f"  Кабинет '{cabinet_name}': обновление доступности бонусов завершено")
            
            sync_end_time = datetime.now()
            
            # Алгоритм скользящей даты
            new_last_synced_time = None
            
            if orders_by_date:
                sorted_dates = sorted(orders_by_date.keys())
                
                found_date_with_active_orders = None
                for order_date in sorted_dates:
                    orders_for_date = orders_by_date[order_date]
                    has_active_orders = any(
                        order["status"] != "delivered" and order["status"] != "cancelled" 
                        for order in orders_for_date
                    )
                    
                    if has_active_orders:
                        found_date_with_active_orders = order_date
                        break
                
                if found_date_with_active_orders:
                    new_last_synced_time = datetime.combine(found_date_with_active_orders, datetime.min.time())
                else:
                    if sorted_dates:
                        earliest_date = sorted_dates[0]
                        new_last_synced_time = datetime.combine(earliest_date, datetime.min.time())
                    else:
                        new_last_synced_time = datetime.now()
            else:
                new_last_synced_time = datetime.now()
            
            # Обновляем скользящую дату для кабинета
            set_last_order_date(cabinet_name, new_last_synced_time)
            
            # Получаем статистику по статусам за первый день периода
            first_day_stats = get_orders_status_stats_by_date_for_cabinet(date_since, cabinet_name)
            
            if new_records_count > 0 or new_customers_count > 0:
                print(f"  Кабинет '{cabinet_name}': успешно добавлено {new_records_count} новых заказов.")
                print(f"  Кабинет '{cabinet_name}': обработано {len(customers_data)} клиентов (новых: {new_customers_count}).")
                if accrued_bonuses_count > 0:
                    print(f"  Кабинет '{cabinet_name}': начислено бонусов: {accrued_bonuses_count} транзакций")
            print(f"  Кабинет '{cabinet_name}': синхронизация завершена успешно")
            
            return {
                "success": True,
                "count": new_records_count,
                "period_start": date_since,
                "period_end": sync_end_time,
                "customers_count": len(customers_data),
                "new_customers_count": new_customers_count,
                "participants_with_orders_count": participants_count,
                "accrued_bonuses_count": accrued_bonuses_count,
                "first_day_stats": first_day_stats,
                "cabinet_name": cabinet_name,
                "client_id": client_id,
                "error": None
            }
            
        except Exception as e:
            print(f"  Кабинет '{cabinet_name}': ПЕРЕХВАТ ИСКЛЮЧЕНИЯ в sync_single_cabinet")
            if db:
                try:
                    db.rollback()
                except:
                    pass
            if common_db:
                try:
                    common_db.rollback()
                except:
                    pass
            error_msg = str(e)
            # Ограничиваем длину сообщения об ошибке для читаемости
            if len(error_msg) > 500:
                error_msg = error_msg[:500] + "..."
            print(f"  Кабинет '{cabinet_name}': критическая ошибка при записи в базу данных: {error_msg}")
            import traceback
            traceback.print_exc()
            print(f"  Кабинет '{cabinet_name}': завершение обработки ошибки")
            return {
                "success": False,
                "count": 0,
                "period_start": date_since if 'date_since' in locals() else datetime.now(),
                "period_end": datetime.now(),
                "customers_count": 0,
                "new_customers_count": 0,
                "participants_with_orders_count": 0,
                "cabinet_name": cabinet_name,
                "client_id": client_id,
                "error": error_msg,
                "first_day_stats": {}  # Добавляем пустую статистику при ошибке
            }
        finally:
            db.close()
            common_db.close()
    except Exception as e:
        print(f"  Кабинет '{cabinet_name}': КРИТИЧЕСКАЯ ОШИБКА при синхронизации: {e}")
        import traceback
        traceback.print_exc()
        print(f"  Кабинет '{cabinet_name}': завершение обработки критической ошибки")
        return {
            "success": False,
            "count": 0,
            "period_start": datetime.now(),
            "period_end": datetime.now(),
            "customers_count": 0,
            "new_customers_count": 0,
            "participants_with_orders_count": 0,
            "cabinet_name": cabinet_name,
            "client_id": client_id,
            "error": str(e)
        }

def update_orders_sheet():
    """Главная функция для получения и записи новых заказов из всех кабинетов Ozon.
    
    Обрабатывает все кабинеты последовательно и возвращает результаты по каждому кабинету.
    """
    sync_start_time = datetime.now()
    
    # Получаем список всех кабинетов
    cabinets = get_ozon_cabinets()
    
    if not cabinets:
        print("Внимание: не найдено ни одного настроенного кабинета Ozon.")
        return {}
    
    results = {}
    
    # Обрабатываем каждый кабинет последовательно
    for cabinet in cabinets:
        cabinet_name = cabinet["cabinet_name"]
        api_key = cabinet["api_key"]
        client_id = cabinet["client_id"]
        
        print(f"Синхронизация кабинета '{cabinet_name}' (Client ID: {client_id})...")
        
        try:
            result = sync_single_cabinet(cabinet_name, api_key, client_id)
            results[cabinet_name] = result
        except Exception as e:
            print(f"Ошибка при синхронизации кабинета '{cabinet_name}': {e}")
            results[cabinet_name] = {
                "success": False,
                "count": 0,
                "period_start": None,
                "period_end": datetime.now(),
                "customers_count": 0,
                "new_customers_count": 0,
                "participants_with_orders_count": 0,
                "accrued_bonuses_count": 0,
                "cabinet_name": cabinet_name,
                "client_id": client_id,
                "error": str(e)
            }
    
    # Обновляем общее время синхронизации
    from db_manager import set_last_sync_timestamp
    set_last_sync_timestamp(sync_start_time)
    
    return results

def get_orders_status_stats_by_date(date: datetime) -> Dict:
    """Получает статистику по статусам заказов за указанную дату из базы данных (для первого кабинета).
    
    Args:
        date: Дата для проверки (используется только дата, без времени)
    
    Returns:
        dict: Статистика {"total": X, "statuses": {"delivered": Y, "delivering": Z, ...}, "active_count": W}
    """
    from db_manager import SessionLocal, Posting, get_orders_db_session
    from collections import Counter
    
    db = get_orders_db_session("wistery")
    try:
        date_start = datetime.combine(date.date(), datetime.min.time())
        date_end = datetime.combine(date.date(), datetime.max.time())
        
        postings = db.query(Posting).filter(
            Posting.created_at >= date_start,
            Posting.created_at <= date_end
        ).all()
        
        if not postings:
            return {
                "total": 0,
                "statuses": {},
                "active_count": 0
            }
        
        statuses = [posting.status for posting in postings if posting.status]
        status_counter = Counter(statuses)
        
        # Подсчитываем активные заказы (не delivered и не cancelled)
        active_count = len([s for s in statuses if s and s not in ["delivered", "cancelled"]])
        
        return {
            "total": len(postings),
            "statuses": dict(status_counter),
            "active_count": active_count
        }
    finally:
        db.close()

def get_orders_status_stats_by_date_for_cabinet(date: datetime, cabinet_name: str) -> Dict:
    """Получает статистику по статусам заказов за указанную дату для конкретного кабинета.
    
    Args:
        date: Дата для проверки (используется только дата, без времени)
        cabinet_name: Название кабинета
    
    Returns:
        dict: Статистика {"total": X, "statuses": {"delivered": Y, "delivering": Z, ...}, "active_count": W}
    """
    from db_manager import get_orders_db_session, Posting
    from collections import Counter
    
    db = get_orders_db_session(cabinet_name)
    try:
        date_start = datetime.combine(date.date(), datetime.min.time())
        date_end = datetime.combine(date.date(), datetime.max.time())
        
        postings = db.query(Posting).filter(
            Posting.created_at >= date_start,
            Posting.created_at <= date_end
        ).all()
        
        if not postings:
            return {
                "total": 0,
                "statuses": {},
                "active_count": 0
            }
        
        statuses = [posting.status for posting in postings if posting.status]
        status_counter = Counter(statuses)
        
        # Подсчитываем активные заказы (не delivered и не cancelled)
        active_count = len([s for s in statuses if s and s not in ["delivered", "cancelled"]])
        
        return {
            "total": len(postings),
            "statuses": dict(status_counter),
            "active_count": active_count
        }
    finally:
        db.close()

if __name__ == "__main__":
    result = update_orders_sheet()
    print(f"\nРезультат синхронизации:")
    print(f"  Заказов добавлено: {result['count']}")
    print(f"  Клиентов обработано: {result['customers_count']} (новых: {result['new_customers_count']})")
    print(f"  Период: {result['period_start'].strftime('%d.%m.%Y %H:%M')} - {result['period_end'].strftime('%d.%m.%Y %H:%M')}")