# check_orders_from_db.py
"""Скрипт для подсчета заказов за 01.12.2025 по статусам из базы данных."""

from datetime import datetime
from db_manager import SessionLocal, Order
from collections import Counter

db = SessionLocal()

try:
    # Запрашиваем заказы за 01.12.2025
    date_start = datetime(2025, 12, 1, 0, 0, 0)
    date_end = datetime(2025, 12, 1, 23, 59, 59)
    
    orders = db.query(Order).filter(
        Order.created_at >= date_start,
        Order.created_at <= date_end
    ).all()
    
    print(f"\n=== STATISTIKA PO ZAKAZAM ZA 01.12.2025 IZ BAZY DANNYH ===\n")
    print(f"Period: {date_start.strftime('%d.%m.%Y %H:%M')} - {date_end.strftime('%d.%m.%Y %H:%M')}\n")
    print(f"Vsego zakazov v baze za 01.12.2025: {len(orders)}\n")
    
    if orders:
        # Подсчитываем статусы
        statuses = [order.status for order in orders if order.status]
        status_counter = Counter(statuses)
        
        print("Raspredelenie po statusam:")
        for status, count in status_counter.most_common():
            percentage = (count / len(orders)) * 100 if orders else 0
            print(f"  - {status}: {count} ({percentage:.1f}%)")
        
        # Дополнительная статистика
        delivered_count = status_counter.get("delivered", 0)
        delivering_count = status_counter.get("delivering", 0)
        cancelled_count = status_counter.get("cancelled", 0)
        awaiting_packaging_count = status_counter.get("awaiting_packaging", 0)
        active_count = len([s for s in statuses if s and s not in ["delivered", "cancelled"]])
        
        print(f"\n=== DOPOLNITELNAYA STATISTIKA ===\n")
        print(f"Dostavleno (delivered): {delivered_count}")
        print(f"V protsesse dostavki (delivering): {delivering_count}")
        print(f"Ozhidaet upakovki (awaiting_packaging): {awaiting_packaging_count}")
        print(f"Otmeneno (cancelled): {cancelled_count}")
        print(f"Aktivnye zakazy (ne delivered i ne cancelled): {active_count}")
        
        # Статистика по датам создания (если есть заказы с разным временем)
        if orders:
            times = [order.created_at for order in orders if order.created_at]
            if times:
                min_time = min(times)
                max_time = max(times)
                print(f"\nPervyy zakaz: {min_time.strftime('%d.%m.%Y %H:%M:%S')}")
                print(f"Posledniy zakaz: {max_time.strftime('%d.%m.%Y %H:%M:%S')}")
    else:
        print("Zakazy za 01.12.2025 ne naydeny v baze dannyh")
        print("Vozmozhno, oni eshche ne byli sinkhronizirovany")
    
    print(f"\n=== KONETS ===\n")

except Exception as e:
    print(f"Oshibka: {e}")
    import traceback
    traceback.print_exc()
finally:
    db.close()



