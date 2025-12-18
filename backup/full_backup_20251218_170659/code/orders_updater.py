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
    get_db, Order, Customer, Participant, order_exists, 
    create_or_update_customer, get_customer, accrue_bonuses_for_order,
    process_order_return, check_and_update_bonus_availability,
    get_orders_db_path
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
    
    # #region agent log
    import json
    log_data = {
        "sessionId": "debug-session",
        "runId": "run1",
        "hypothesisId": "A",
        "location": "orders_updater.py:199",
        "message": "Financial data structure from API",
        "data": {
            "posting_number": posting_number,
            "financial_data_keys": list(financial_data.keys()) if financial_data else [],
            "financial_data": json.dumps(financial_data, default=str, ensure_ascii=False)[:1000] if financial_data else None,
            "posting_keys": list(posting.keys()) if posting else [],
            "delivery_price": posting.get("delivery_price"),
            "item_price": item.get("price"),
            "products_in_financial": len(financial_data.get("products", [])) if financial_data else 0,
        },
        "timestamp": int(datetime.now().timestamp() * 1000)
    }
    try:
        with open(r"c:\telegram-ref-bot\.cursor\debug.log", "a", encoding="utf-8") as f:
            f.write(json.dumps(log_data, ensure_ascii=False) + "\n")
    except:
        pass
    # #endregion
    
    # Мы возвращаем словарь, где ключи соответствуют полям в модели Order (db_manager.py)
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
    
    # #region agent log
    # Вычисляем итоговую сумму (товары + доставка) для проверки
    products = financial_data.get("products", [])
    total_products_price = sum(float(p.get("price", 0)) for p in products)
    delivery_price = float(posting.get("delivery_price", 0) or 0)
    calculated_total = total_products_price + delivery_price
    
    log_data2 = {
        "sessionId": "debug-session",
        "runId": "run1",
        "hypothesisId": "B",
        "location": "orders_updater.py:238",
        "message": "Calculated prices vs saved values",
        "data": {
            "posting_number": posting_number,
            "price_amount_saved": price_amount,
            "buyer_paid_saved": str(financial_data.get("products", [{}])[0].get("price", "") if financial_data.get("products") else ""),
            "shipping_cost_saved": str(posting.get("delivery_price", "0")),
            "total_products_price": total_products_price,
            "delivery_price": delivery_price,
            "calculated_total_price": calculated_total,
            "financial_data_postings": financial_data.get("postings", []),
            "financial_data_postings_keys": list(financial_data.get("postings", [{}])[0].keys()) if financial_data.get("postings") else [],
        },
        "timestamp": int(datetime.now().timestamp() * 1000)
    }
    try:
        with open(r"c:\telegram-ref-bot\.cursor\debug.log", "a", encoding="utf-8") as f:
            f.write(json.dumps(log_data2, ensure_ascii=False) + "\n")
    except:
        pass
    # #endregion

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
                    "with": {
                        "barcodes": True,
                        "financial_data": True,
                        "translit": True,
                        "delivery_method": True,
                        "addressee": True  # Явно запрашиваем данные адресата
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
        order = db.query(Order).filter(Order.posting_number == posting_number).first()
        if not order:
            continue
        
        # Получаем posting из API (если нашли)
        posting = api_orders_map.get(posting_number)
        
        if posting:
            new_status = posting.get("status", "")
            if new_status == "delivered":
                order.status = "delivered"
                order.is_redeemed = "да"
                stats["delivered"] += 1
            elif new_status == "cancelled":
                order.status = "cancelled"
                order.is_redeemed = "нет"
                stats["cancelled"] += 1
            # Обновляем другие поля из posting
            if posting.get("delivering_date"):
                order.delivering_date = posting.get("delivering_date")
            if posting.get("in_process_at"):
                order.in_process_at = posting.get("in_process_at")
        else:
            # Заказ не найден в API - возможно, был удален или имеет другой статус
            # Предполагаем, что он доставлен (наиболее вероятный исход)
            print(f"Предупреждение: Заказ {posting_number} не найден в API. Устанавливаем статус 'delivered'.")
            if order.status not in ["delivered", "cancelled"]:
                order.status = "delivered"
                order.is_redeemed = "да"
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
        Order, Customer, Participant, accrue_bonuses_for_order,
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
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'")
                table_exists = cursor.fetchone() is not None
                conn.close()
                
                if not table_exists:
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
            print(f"  Кабинет '{cabinet_name}': начинаю обработку {total_postings} заказов...")
            
            for posting in all_raw_postings:
                processed_count += 1
                # Прогресс обработки не логируем, чтобы не засорять терминал
                
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
                
                db.flush()
                existing_order = db.query(Order).filter(Order.posting_number == posting_number).first()
                
                if existing_order:
                    # Заказ уже существует - обновляем
                    old_status = existing_order.status
                    existing_order.status = posting_status
                    existing_order.is_redeemed = "да" if posting_status == "delivered" else "нет"
                    
                    # Если статус изменился на "delivered", начисляем бонусы (в общей БД)
                    if posting_status == "delivered" and old_status != "delivered":
                        try:
                            bonuses_count = accrue_bonuses_for_order(posting_number, common_db=common_db, order_db=db, cabinet_name=cabinet_name)
                            if bonuses_count is None:
                                bonuses_count = 0
                            accrued_bonuses_count += bonuses_count
                        except Exception as bonus_error:
                            print(f"  Кабинет '{cabinet_name}': исключение при начислении бонусов за {posting_number}: {bonus_error}")
                            import traceback
                            traceback.print_exc()
                            # Продолжаем обработку других заказов
                        # Добавляем небольшую задержку после начисления бонусов, чтобы избежать блокировок
                        import time
                        time.sleep(0.01)  # 10мс задержка
                    
                    # Если статус изменился с "delivered" на "cancelled"
                    if old_status == "delivered" and posting_status == "cancelled":
                        process_order_return(posting_number, return_amount=None, db=common_db)
                    
                    # Обновляем другие поля
                    if financial_data:
                        existing_order.currency_code = financial_data.get("currency_code", existing_order.currency_code or "RUB")
                        if financial_data.get("products"):
                            existing_order.buyer_paid = str(financial_data.get("products", [{}])[0].get("price", existing_order.buyer_paid or ""))
                    
                    if posting.get("delivering_date"):
                        existing_order.delivering_date = posting.get("delivering_date")
                    if posting.get("in_process_at"):
                        existing_order.in_process_at = posting.get("in_process_at")
                    if posting.get("cluster_from"):
                        existing_order.cluster_from = posting.get("cluster_from")
                    if posting.get("cluster_to"):
                        existing_order.cluster_to = posting.get("cluster_to")
                    if posting.get("delivery_price"):
                        existing_order.shipping_cost = str(posting.get("delivery_price"))
                    if posting.get("estimated_delivery_date"):
                        existing_order.norm_delivery_time = str(posting.get("estimated_delivery_date"))
                    if posting.get("client_segment"):
                        existing_order.client_segment = posting.get("client_segment")
                    if posting.get("is_legal") is not None:
                        existing_order.is_legal_entity = "да" if posting.get("is_legal") else "нет"
                    if posting.get("payment_method"):
                        existing_order.payment_method = posting.get("payment_method", {}).get("name", existing_order.payment_method or "")
                    
                    addressee = posting.get("addressee", {})
                    if isinstance(addressee, dict) and addressee.get("address"):
                        existing_order.address = addressee.get("address")
                        if addressee.get("address"):
                            existing_order.delivery_city = addressee.get("address", "").split(",")[0] if addressee.get("address") else existing_order.delivery_city
                    
                    delivery_method = posting.get("delivery_method", {})
                    if isinstance(delivery_method, dict):
                        if delivery_method.get("warehouse_name"):
                            existing_order.shipping_warehouse = delivery_method.get("warehouse_name")
                            existing_order.delivery_region = delivery_method.get("warehouse_name")
                        if delivery_method.get("name"):
                            existing_order.delivery_method = delivery_method.get("name")
                    
                    processed_posting_numbers.add(posting_number)
                    continue
                
                # Заказ не существует - добавляем новый
                if not financial_data or not financial_data.get("products"):
                    processed_posting_numbers.add(posting_number)
                    continue
                
                items_added = False
                for item in financial_data.get("products", []):
                    if items_added:
                        break
                    
                    order_data = transform_ozon_data_for_sheets(posting, item)
                    
                    if not order_data.get("posting_number") or order_data.get("posting_number").strip() == "":
                        continue
                    
                    try:
                        # #region agent log
                        log_data3 = {
                            "sessionId": "debug-session",
                            "runId": "run1",
                            "hypothesisId": "C",
                            "location": "orders_updater.py:781",
                            "message": "Saving order to database",
                            "data": {
                                "posting_number": order_data.get("posting_number"),
                                "price_amount": order_data.get("price_amount"),
                                "buyer_paid": order_data.get("buyer_paid"),
                                "shipping_cost": order_data.get("shipping_cost"),
                                "order_data_keys": list(order_data.keys()),
                            },
                            "timestamp": int(datetime.now().timestamp() * 1000)
                        }
                        try:
                            with open(r"c:\telegram-ref-bot\.cursor\debug.log", "a", encoding="utf-8") as f:
                                f.write(json.dumps(log_data3, ensure_ascii=False) + "\n")
                        except:
                            pass
                        # #endregion
                        
                        new_order = Order(**order_data)
                        db.add(new_order)
                        db.flush()
                        new_records_count += 1
                        items_added = True
                        
                        # Если заказ доставлен, начисляем бонусы (в общей БД)
                        if posting_status == "delivered":
                            try:
                                bonuses_count = accrue_bonuses_for_order(posting_number, common_db=common_db, order_db=db, cabinet_name=cabinet_name)
                                if bonuses_count is None:
                                    bonuses_count = 0
                                accrued_bonuses_count += bonuses_count
                            except Exception as bonus_error:
                                print(f"  Кабинет '{cabinet_name}': исключение при начислении бонусов за {posting_number}: {bonus_error}")
                                import traceback
                                traceback.print_exc()
                                # Продолжаем обработку других заказов
                            # Добавляем небольшую задержку после начисления бонусов, чтобы избежать блокировок
                            import time
                            time.sleep(0.01)  # 10мс задержка
                        
                        processed_posting_numbers.add(posting_number)
                        
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
                                
                                customers_data[buyer_id]["orders_count"] += 1
                                customers_data[buyer_id]["total_spent"] += order_total
                                
                                order_date_obj = customer_data.get("last_order_date")
                                if order_date_obj:
                                    if not customers_data[buyer_id]["first_order_date"] or order_date_obj < customers_data[buyer_id]["first_order_date"]:
                                        customers_data[buyer_id]["first_order_date"] = order_date_obj
                                    if not customers_data[buyer_id]["last_order_date"] or order_date_obj > customers_data[buyer_id]["last_order_date"]:
                                        customers_data[buyer_id]["last_order_date"] = order_date_obj
                    except Exception as e:
                        print(f"Ошибка при добавлении заказа {posting_number}: {e}")
                        processed_posting_numbers.add(posting_number)
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
            print(f"  Кабинет '{cabinet_name}': коммит изменений в БД...")
            db.commit()
            print(f"  Кабинет '{cabinet_name}': коммит заказов завершен")
            common_db.commit()
            print(f"  Кабинет '{cabinet_name}': коммит общей БД завершен")
            
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
    
    # Разбиваем период на дни и обрабатываем по одному дню за раз
    date_to = datetime.now()
    current_date = date_since.date()
    end_date = date_to.date()
    
    all_raw_postings = []
    
    # Обрабатываем каждый день отдельно
    while current_date <= end_date:
        day_start = datetime.combine(current_date, datetime.min.time())
        day_end = datetime.combine(current_date, datetime.max.time())
        
        # Если это последний день, используем текущее время
        if current_date == end_date:
            day_end = date_to
        
        print(f"Обрабатываю день {current_date.strftime('%d.%m.%Y')}...")
        
        # Запрашиваем заказы за один день с повторными попытками
        day_postings = []
        max_retries = 3
        retry_delay = 5  # секунд
        
        for attempt in range(1, max_retries + 1):
            try:
                day_postings = fetch_new_orders_from_api(day_start, day_end)
                if day_postings:
                    break  # Успешно получили данные
                elif attempt < max_retries:
                    print(f"  Попытка {attempt} из {max_retries}: не получено данных. Повтор через {retry_delay} сек...")
                    time.sleep(retry_delay)
            except Exception as e:
                if attempt < max_retries:
                    print(f"  Попытка {attempt} из {max_retries}: ошибка - {e}. Повтор через {retry_delay} сек...")
                    time.sleep(retry_delay)
                else:
                    print(f"  Все попытки исчерпаны для {current_date.strftime('%d.%m.%Y')}. Пропускаем день.")
        
        if day_postings:
            all_raw_postings.extend(day_postings)
            print(f"  Получено {len(day_postings)} заказов за {current_date.strftime('%d.%m.%Y')}")
        else:
            print(f"  Предупреждение: не удалось получить заказы за {current_date.strftime('%d.%m.%Y')} после {max_retries} попыток")
        
        # Переходим к следующему дню
        current_date += timedelta(days=1)
    
    raw_postings = all_raw_postings

    if not raw_postings:
        print("Нет новых заказов для обновления.")
        sync_end_time = datetime.now()
        return {
            "count": 0,
            "period_start": date_since,
            "period_end": sync_end_time,
            "customers_count": 0,
            "new_customers_count": 0,
            "participants_with_orders_count": 0
        }

    new_records_count = 0
    new_customers_count = 0
    
    # 2. Получаем сессию базы данных
    db_generator = get_db()
    db = next(db_generator) # Получаем сессию
    
    try:
        # Словарь для отслеживания клиентов и их статистики
        customers_data = {}
        
        # Словарь для анализа дат создания заказов (для алгоритма скользящей даты)
        # Ключ: дата создания (только дата, без времени), значение: список заказов с этой датой
        orders_by_date = {}
        
        # Множество для отслеживания уже обработанных posting_number в текущей синхронизации
        # Это предотвращает повторную обработку одного и того же posting в рамках одной синхронизации
        processed_posting_numbers = set()
        
        # 3. Перебираем отправления и товары
        for posting in raw_postings:
            posting_status = posting.get("status", "")
            
            # Извлекаем дату создания заказа для анализа
            created_at = posting.get("created_at", "")
            order_date = None
            if created_at and 'T' in created_at:
                try:
                    created_date_obj = datetime.strptime(created_at.split('.')[0], "%Y-%m-%dT%H:%M:%S")
                    order_date = created_date_obj.date()  # Только дата, без времени
                except ValueError:
                    pass
            
            # Добавляем заказ в словарь для анализа (все заказы, включая не доставленные)
            if order_date:
                if order_date not in orders_by_date:
                    orders_by_date[order_date] = []
                orders_by_date[order_date].append({
                    "posting": posting,
                    "status": posting_status
                })
            
            # 3.2. Обрабатываем ВСЕ заказы (независимо от статуса)
            financial_data = posting.get("financial_data", {})
            
            # Получаем posting_number один раз для всего posting
            posting_number = posting.get("posting_number", "")
            
            # Проверяем, что posting_number не пустой
            if not posting_number or posting_number.strip() == "":
                print(f"Пропущен заказ: posting_number пустой или отсутствует")
                continue
            
            # **********************************************
            # ПРОВЕРКА НА ДУБЛИКАТЫ (САМОЕ ВАЖНОЕ)
            # Проверяем ДО обработки товаров, чтобы не обрабатывать весь posting повторно
            # **********************************************
            # Сначала проверяем, не обрабатывали ли мы этот posting_number в текущей синхронизации
            if posting_number in processed_posting_numbers:
                # Уже обработали в текущей синхронизации - пропускаем
                continue
            
            # Затем проверяем в БД
            # Делаем flush, чтобы все добавленные в текущей транзакции записи были видны
            db.flush()
            existing_order = db.query(Order).filter(Order.posting_number == posting_number).first()
            
            if existing_order:
                # Заказ уже существует в БД - обновляем его статус и другие поля
                old_status = existing_order.status
                existing_order.status = posting_status
                existing_order.is_redeemed = "да" if posting_status == "delivered" else "нет"
                
                # Если статус изменился на "delivered", начисляем бонусы
                if posting_status == "delivered" and old_status != "delivered":
                    _ = accrue_bonuses_for_order(posting_number, common_db=db, order_db=db, cabinet_name="wistery")
                
                # Если статус изменился с "delivered" на "cancelled" (возврат товара)
                if old_status == "delivered" and posting_status == "cancelled":
                    # Обрабатываем возврат заказа и списываем бонусы
                    process_order_return(posting_number, return_amount=None, db=db)
                
                # Обновляем другие поля, если они доступны
                if financial_data:
                    existing_order.currency_code = financial_data.get("currency_code", existing_order.currency_code or "RUB")
                    if financial_data.get("products"):
                        existing_order.buyer_paid = str(financial_data.get("products", [{}])[0].get("price", existing_order.buyer_paid or ""))
                
                # Обновляем даты доставки и другие поля из posting
                if posting.get("delivering_date"):
                    existing_order.delivering_date = posting.get("delivering_date")
                if posting.get("in_process_at"):
                    existing_order.in_process_at = posting.get("in_process_at")
                if posting.get("cluster_from"):
                    existing_order.cluster_from = posting.get("cluster_from")
                if posting.get("cluster_to"):
                    existing_order.cluster_to = posting.get("cluster_to")
                if posting.get("delivery_price"):
                    existing_order.shipping_cost = str(posting.get("delivery_price"))
                if posting.get("estimated_delivery_date"):
                    existing_order.norm_delivery_time = str(posting.get("estimated_delivery_date"))
                if posting.get("client_segment"):
                    existing_order.client_segment = posting.get("client_segment")
                if posting.get("is_legal") is not None:
                    existing_order.is_legal_entity = "да" if posting.get("is_legal") else "нет"
                if posting.get("payment_method"):
                    existing_order.payment_method = posting.get("payment_method", {}).get("name", existing_order.payment_method or "")
                
                # Обновляем адрес из addressee, если доступен
                addressee = posting.get("addressee", {})
                if isinstance(addressee, dict) and addressee.get("address"):
                    existing_order.address = addressee.get("address")
                    if addressee.get("address"):
                        existing_order.delivery_city = addressee.get("address", "").split(",")[0] if addressee.get("address") else existing_order.delivery_city
                
                # Обновляем delivery_method
                delivery_method = posting.get("delivery_method", {})
                if isinstance(delivery_method, dict):
                    if delivery_method.get("warehouse_name"):
                        existing_order.shipping_warehouse = delivery_method.get("warehouse_name")
                        existing_order.delivery_region = delivery_method.get("warehouse_name")
                    if delivery_method.get("name"):
                        existing_order.delivery_method = delivery_method.get("name")
                
                # Помечаем как обработанный
                processed_posting_numbers.add(posting_number)
                continue
            
            # Заказ не существует - добавляем новый (только если есть financial_data для обработки товаров)
            if not financial_data or not financial_data.get("products"):
                # Нет данных о товарах - пропускаем (возможно, заказ еще не обработан)
                processed_posting_numbers.add(posting_number)
                continue
            
            # Обрабатываем товары для нового заказа
            # ВАЖНО: если в posting несколько товаров, но posting_number уникален,
            # то мы можем добавить только первый товар (или нужно изменить модель БД)
            items_added = False
            for item in financial_data.get("products", []):
                    # Если уже добавили один товар для этого posting_number, пропускаем остальные
                    # (так как posting_number уникален в БД)
                    if items_added:
                        break
                    
                    # 4. Преобразуем данные и создаем объект DB
                    order_data = transform_ozon_data_for_sheets(posting, item)
                    
                    # Дополнительная проверка перед созданием объекта
                    if not order_data.get("posting_number") or order_data.get("posting_number").strip() == "":
                        print(f"Пропущен товар: posting_number пустой в order_data")
                        continue
                    
                    try:
                        new_order = Order(**order_data)
                        # 5. Добавляем и сохраняем
                        db.add(new_order)
                        db.flush()  # Нужно для получения ID
                        new_records_count += 1
                        items_added = True
                        
                        # Если заказ доставлен, начисляем бонусы
                        if posting_status == "delivered":
                            _ = accrue_bonuses_for_order(posting_number, common_db=db, order_db=db, cabinet_name="wistery")
                        
                        # Помечаем posting_number как обработанный
                        processed_posting_numbers.add(posting_number)
                        
                        # 3.1. Обрабатываем данные клиента ТОЛЬКО для новых заказов
                        # (собираем клиентов только для реально добавленных заказов)
                        # buyer_id теперь извлекается из posting_number (первые цифры до первого тире)
                        customer_data = transform_ozon_customer_data(posting)
                        if customer_data:
                            buyer_id = customer_data.get("buyer_id")
                            if buyer_id:
                                # Собираем статистику по клиенту
                                if buyer_id not in customers_data:
                                    customers_data[buyer_id] = {
                                        "data": customer_data,
                                        "orders_count": 0,
                                        "total_spent": 0.0,
                                        "first_order_date": customer_data.get("first_order_date"),
                                        "last_order_date": customer_data.get("last_order_date"),
                                    }
                                
                                # Обновляем статистику
                                products = financial_data.get("products", [])
                                order_total = sum(float(item.get("price", 0)) for item in products)
                                
                                customers_data[buyer_id]["orders_count"] += 1
                                customers_data[buyer_id]["total_spent"] += order_total
                                
                                # Обновляем даты заказов
                                order_date_obj = customer_data.get("last_order_date")
                                if order_date_obj:
                                    if not customers_data[buyer_id]["first_order_date"] or order_date_obj < customers_data[buyer_id]["first_order_date"]:
                                        customers_data[buyer_id]["first_order_date"] = order_date_obj
                                    if not customers_data[buyer_id]["last_order_date"] or order_date_obj > customers_data[buyer_id]["last_order_date"]:
                                        customers_data[buyer_id]["last_order_date"] = order_date_obj
                    except Exception as e:
                        # Если возникла ошибка уникальности или другая ошибка при добавлении
                        print(f"Ошибка при добавлении заказа {posting_number}: {e}")
                        # Помечаем как обработанный, чтобы не пытаться добавить снова
                        processed_posting_numbers.add(posting_number)
                        # Пропускаем этот товар, продолжаем обработку остальных
                        continue
        
        # 4. Сохраняем/обновляем клиентов
        for buyer_id, customer_info in customers_data.items():
            try:
                customer_data = customer_info["data"]
                
                # Получаем существующего клиента для обновления статистики
                existing_customer = get_customer(db, buyer_id)
                
                if existing_customer:
                    # Обновляем статистику существующего клиента
                    customer_data["total_orders"] = existing_customer.total_orders + customer_info["orders_count"]
                    customer_data["total_spent"] = str(float(existing_customer.total_spent or 0) + customer_info["total_spent"])
                    
                    # Обновляем даты
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
                    # Новый клиент
                    customer_data["total_orders"] = customer_info["orders_count"]
                    customer_data["total_spent"] = str(customer_info["total_spent"])
                    new_customers_count += 1
                
                # Создаем или обновляем клиента
                create_or_update_customer(db, customer_data)
            except Exception as e:
                print(f"Ошибка при сохранении клиента {buyer_id}: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        # 4.1. Подсчитываем участников программы, совершивших покупку
        participants_with_orders = set()  # Множество для уникальных buyer_id участников
        
        # Проверяем каждого buyer_id из обработанных заказов
        for buyer_id in customers_data.keys():
            try:
                # Проверяем, является ли этот buyer_id участником программы
                participant = db.query(Participant).filter(Participant.ozon_id == str(buyer_id)).first()
                if participant:
                    participants_with_orders.add(buyer_id)
            except Exception as e:
                print(f"Ошибка при проверке участника {buyer_id}: {e}")
        
        participants_count = len(participants_with_orders)
        
        # Сохраняем все новые записи за раз
        db.commit()
        
        # Обновляем доступность бонусов (проверяем, прошло ли 14 дней)
        updated_bonuses_count = check_and_update_bonus_availability(db)
        if updated_bonuses_count > 0:
            print(f"Обновлено статусов доступности бонусов: {updated_bonuses_count}")

        sync_end_time = datetime.now()  # Время окончания синхронизации

        # 5. АЛГОРИТМ СКОЛЬЗЯЩЕЙ ДАТЫ
        # Анализируем даты создания заказов и находим оптимальную стартовую дату для следующего запроса
        # Используем самую раннюю дату, где есть заказы со статусами, которые означают,
        # что заказ еще может быть доставлен (не "delivered" и не "cancelled")
        new_last_synced_time = None
        
        if orders_by_date:
            # Сортируем даты по возрастанию (от старых к новым)
            sorted_dates = sorted(orders_by_date.keys())
            
            # Ищем самую раннюю дату, где есть заказы со статусами, которые означают,
            # что заказ еще может быть доставлен (не "delivered" и не "cancelled")
            found_date_with_active_orders = None
            for order_date in sorted_dates:
                orders_for_date = orders_by_date[order_date]
                # Проверяем, есть ли заказы со статусами, которые означают "в процессе доставки"
                has_active_orders = any(
                    order["status"] != "delivered" and order["status"] != "cancelled" 
                    for order in orders_for_date
                )
                
                if has_active_orders:
                    found_date_with_active_orders = order_date
                    break
            
            if found_date_with_active_orders:
                # Нашли дату с заказами, которые еще могут быть доставлены - используем её без смещения
                new_last_synced_time = datetime.combine(found_date_with_active_orders, datetime.min.time())
            else:
                # Все заказы доставлены или отменены - используем самую раннюю дату из всех обработанных заказов
                # Это нужно, чтобы не пропустить заказы, которые могут изменить статус
                if sorted_dates:
                    earliest_date = sorted_dates[0]
                    new_last_synced_time = datetime.combine(earliest_date, datetime.min.time())
                else:
                    # Нет заказов вообще - используем текущую дату без смещения
                    new_last_synced_time = datetime.now()
        else:
            # Нет заказов - используем текущую дату без смещения
            new_last_synced_time = datetime.now()
        
        # Сохраняем время последней синхронизации (для проверки интервала 12 часов)
        set_last_sync_timestamp(sync_start_time)
        
        # Сохраняем дату последнего заказа (для алгоритма скользящей даты и определения стартовой даты следующего запроса)
        set_last_order_date(new_last_synced_time)

        if new_records_count > 0 or new_customers_count > 0:
            print(f"Успешно добавлено {new_records_count} новых заказов в базу данных.")
            print(f"Обработано {len(customers_data)} клиентов (новых: {new_customers_count}).")
            print(f"Новая стартовая дата для следующего запроса: {new_last_synced_time.strftime('%d.%m.%Y %H:%M')}")
        else:
            print("Новых уникальных заказов для добавления не найдено.")
        
        # Получаем статистику по статусам за первый день периода
        first_day_stats = get_orders_status_stats_by_date(date_since)
        
        result_dict = {
            "count": new_records_count,
            "period_start": date_since,
            "period_end": sync_end_time,
            "customers_count": len(customers_data),
            "new_customers_count": new_customers_count,
            "participants_with_orders_count": participants_count,
            "first_day_stats": first_day_stats
        }
        return result_dict

    except Exception as e:
        db.rollback() # Откатываем изменения при ошибке
        print(f"Критическая ошибка при записи в базу данных: {e}")
        import traceback
        traceback.print_exc()
        raise # Поднимаем ошибку выше, чтобы бот мог сообщить о ней в Telegram
    finally:
        # 6. Закрываем сессию
        db_generator.close()

def get_orders_status_stats_by_date(date: datetime) -> Dict:
    """Получает статистику по статусам заказов за указанную дату из базы данных (для первого кабинета).
    
    Args:
        date: Дата для проверки (используется только дата, без времени)
    
    Returns:
        dict: Статистика {"total": X, "statuses": {"delivered": Y, "delivering": Z, ...}, "active_count": W}
    """
    from db_manager import SessionLocal, Order
    from collections import Counter
    
    db = SessionLocal()
    try:
        date_start = datetime.combine(date.date(), datetime.min.time())
        date_end = datetime.combine(date.date(), datetime.max.time())
        
        orders = db.query(Order).filter(
            Order.created_at >= date_start,
            Order.created_at <= date_end
        ).all()
        
        if not orders:
            return {
                "total": 0,
                "statuses": {},
                "active_count": 0
            }
        
        statuses = [order.status for order in orders if order.status]
        status_counter = Counter(statuses)
        
        # Подсчитываем активные заказы (не delivered и не cancelled)
        active_count = len([s for s in statuses if s and s not in ["delivered", "cancelled"]])
        
        return {
            "total": len(orders),
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
    from db_manager import get_orders_db_session, Order
    from collections import Counter
    
    db = get_orders_db_session(cabinet_name)
    try:
        date_start = datetime.combine(date.date(), datetime.min.time())
        date_end = datetime.combine(date.date(), datetime.max.time())
        
        orders = db.query(Order).filter(
            Order.created_at >= date_start,
            Order.created_at <= date_end
        ).all()
        
        if not orders:
            return {
                "total": 0,
                "statuses": {},
                "active_count": 0
            }
        
        statuses = [order.status for order in orders if order.status]
        status_counter = Counter(statuses)
        
        # Подсчитываем активные заказы (не delivered и не cancelled)
        active_count = len([s for s in statuses if s and s not in ["delivered", "cancelled"]])
        
        return {
            "total": len(orders),
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