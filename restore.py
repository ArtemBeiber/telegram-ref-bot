#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для восстановления базы данных из бэкапа.
Позволяет выбрать бэкап из списка и восстановить его.
"""

import os
import sys
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path

# Настройка кодировки для Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')


def list_backups(backup_dir: str = "backup/database") -> list:
    """Возвращает список всех доступных бэкапов."""
    backup_path = Path(backup_dir)
    if not backup_path.exists():
        return []
    
    backups = []
    for file in backup_path.glob("referral_orders_*.db"):
        file_stat = file.stat()
        backups.append({
            'path': str(file),
            'name': file.name,
            'size': file_stat.st_size,
            'created': datetime.fromtimestamp(file_stat.st_mtime)
        })
    
    # Сортируем по дате создания (новые первыми)
    backups.sort(key=lambda x: x['created'], reverse=True)
    return backups


def check_database_integrity(db_path: str) -> bool:
    """Проверяет целостность базы данных."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # Выполняем проверку целостности
        cursor.execute("PRAGMA integrity_check")
        result = cursor.fetchone()
        conn.close()
        
        if result and result[0] == "ok":
            return True
        else:
            print(f"⚠️ Предупреждение: База данных может быть повреждена: {result}")
            return False
    except Exception as e:
        print(f"⚠️ Не удалось проверить целостность БД: {e}")
        return False


def create_backup_before_restore(source_db: str = "referral_orders.db",
                                backup_dir: str = "backup/database") -> str:
    """Создает бэкап текущей БД перед восстановлением."""
    if not os.path.exists(source_db):
        return None
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"referral_orders_before_restore_{timestamp}.db"
    backup_path = Path(backup_dir)
    backup_path.mkdir(parents=True, exist_ok=True)
    backup_filepath = backup_path / backup_filename
    
    try:
        shutil.copy2(source_db, backup_filepath)
        return str(backup_filepath)
    except Exception as e:
        print(f"⚠️ Не удалось создать бэкап перед восстановлением: {e}")
        return None


def restore_database(backup_path: str,
                    target_db: str = "referral_orders.db",
                    create_backup: bool = True,
                    check_integrity: bool = True) -> bool:
    """
    Восстанавливает базу данных из бэкапа.
    
    Args:
        backup_path: Путь к файлу бэкапа
        target_db: Путь к целевой базе данных
        create_backup: Создавать ли бэкап текущей БД перед восстановлением
        check_integrity: Проверять ли целостность БД после восстановления
    
    Returns:
        True если восстановление успешно, False в случае ошибки
    """
    # Проверяем существование бэкапа
    if not os.path.exists(backup_path):
        print(f"❌ Ошибка: Бэкап {backup_path} не найден!")
        return False
    
    # Проверяем целостность бэкапа перед восстановлением
    print(f"🔍 Проверка целостности бэкапа...")
    if not check_database_integrity(backup_path):
        response = input("⚠️ Целостность бэкапа не подтверждена. Продолжить восстановление? (y/n): ")
        if response.lower() != 'y':
            print("❌ Восстановление отменено.")
            return False
    
    # Создаем бэкап текущей БД перед восстановлением
    if create_backup and os.path.exists(target_db):
        print("📦 Создание бэкапа текущей БД перед восстановлением...")
        backup_before = create_backup_before_restore(target_db)
        if backup_before:
            print(f"✅ Бэкап создан: {backup_before}")
        else:
            response = input("⚠️ Не удалось создать бэкап. Продолжить восстановление? (y/n): ")
            if response.lower() != 'y':
                print("❌ Восстановление отменено.")
                return False
    
    try:
        # Если целевая БД существует, удаляем её
        if os.path.exists(target_db):
            print(f"🗑️ Удаление текущей БД: {target_db}")
            os.remove(target_db)
        
        # Копируем бэкап в целевое местоположение
        print(f"📥 Восстановление из бэкапа: {os.path.basename(backup_path)}")
        shutil.copy2(backup_path, target_db)
        
        # Проверяем целостность восстановленной БД
        if check_integrity:
            print("🔍 Проверка целостности восстановленной БД...")
            if check_database_integrity(target_db):
                print("✅ Целостность БД подтверждена!")
            else:
                print("⚠️ Предупреждение: Целостность БД не подтверждена после восстановления")
        
        file_size = os.path.getsize(target_db)
        file_size_mb = file_size / (1024 * 1024)
        
        print(f"\n✅ База данных успешно восстановлена!")
        print(f"   Файл: {target_db}")
        print(f"   Размер: {file_size_mb:.2f} MB")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при восстановлении: {e}")
        return False


def interactive_restore(backup_dir: str = "backup/database",
                       target_db: str = "referral_orders.db"):
    """Интерактивное восстановление с выбором бэкапа из списка."""
    backups = list_backups(backup_dir)
    
    if not backups:
        print("❌ Бэкапы не найдены!")
        return False
    
    print(f"📋 Найдено бэкапов: {len(backups)}\n")
    for i, backup in enumerate(backups, 1):
        size_mb = backup['size'] / (1024 * 1024)
        print(f"{i}. {backup['name']}")
        print(f"   Размер: {size_mb:.2f} MB")
        print(f"   Создан: {backup['created'].strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    while True:
        try:
            choice = input(f"Выберите бэкап для восстановления (1-{len(backups)}) или 'q' для отмены: ")
            
            if choice.lower() == 'q':
                print("❌ Восстановление отменено.")
                return False
            
            choice_num = int(choice)
            if 1 <= choice_num <= len(backups):
                selected_backup = backups[choice_num - 1]
                print(f"\nВыбран бэкап: {selected_backup['name']}")
                
                confirm = input("Подтвердите восстановление (y/n): ")
                if confirm.lower() == 'y':
                    return restore_database(
                        selected_backup['path'],
                        target_db=target_db,
                        create_backup=True,
                        check_integrity=True
                    )
                else:
                    print("❌ Восстановление отменено.")
                    return False
            else:
                print(f"⚠️ Неверный выбор. Введите число от 1 до {len(backups)}")
        except ValueError:
            print("⚠️ Неверный ввод. Введите число или 'q' для отмены")
        except KeyboardInterrupt:
            print("\n❌ Восстановление отменено.")
            return False


def main():
    """Главная функция."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Восстановление базы данных из бэкапа"
    )
    parser.add_argument(
        "--backup",
        help="Путь к файлу бэкапа для восстановления"
    )
    parser.add_argument(
        "--target",
        default="referral_orders.db",
        help="Путь к целевой базе данных (по умолчанию: referral_orders.db)"
    )
    parser.add_argument(
        "--backup-dir",
        default="backup/database",
        help="Директория с бэкапами (по умолчанию: backup/database)"
    )
    parser.add_argument(
        "--no-backup-before",
        action="store_true",
        help="Не создавать бэкап текущей БД перед восстановлением"
    )
    parser.add_argument(
        "--no-integrity-check",
        action="store_true",
        help="Пропустить проверку целостности БД"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Показать список всех доступных бэкапов"
    )
    
    args = parser.parse_args()
    
    # Показать список бэкапов
    if args.list:
        backups = list_backups(args.backup_dir)
        if not backups:
            print("📭 Бэкапы не найдены")
        else:
            print(f"📋 Найдено бэкапов: {len(backups)}\n")
            for i, backup in enumerate(backups, 1):
                size_mb = backup['size'] / (1024 * 1024)
                print(f"{i}. {backup['name']}")
                print(f"   Размер: {size_mb:.2f} MB")
                print(f"   Создан: {backup['created'].strftime('%Y-%m-%d %H:%M:%S')}\n")
        return
    
    # Если указан конкретный бэкап, восстанавливаем его
    if args.backup:
        restore_database(
            args.backup,
            target_db=args.target,
            create_backup=not args.no_backup_before,
            check_integrity=not args.no_integrity_check
        )
    else:
        # Интерактивный режим
        interactive_restore(
            backup_dir=args.backup_dir,
            target_db=args.target
        )


if __name__ == "__main__":
    main()

