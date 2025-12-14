#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для создания бэкапа базы данных SQLite.
Создает копию referral_orders.db с временной меткой в имени файла.
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


def check_database_integrity(db_path: str) -> bool:
    """Проверяет целостность базы данных перед бэкапом."""
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


def create_backup(source_db: str = "referral_orders.db", 
                 backup_dir: str = "backup/database",
                 check_integrity: bool = True) -> str:
    """
    Создает бэкап базы данных.
    
    Args:
        source_db: Путь к исходной базе данных
        backup_dir: Директория для сохранения бэкапов
        check_integrity: Проверять ли целостность БД перед бэкапом
    
    Returns:
        Путь к созданному бэкапу или None в случае ошибки
    """
    # Проверяем существование исходной БД
    if not os.path.exists(source_db):
        print(f"❌ Ошибка: База данных {source_db} не найдена!")
        return None
    
    # Создаем директорию для бэкапов, если её нет
    backup_path = Path(backup_dir)
    backup_path.mkdir(parents=True, exist_ok=True)
    
    # Проверяем целостность БД перед бэкапом
    if check_integrity:
        print("🔍 Проверка целостности базы данных...")
        if not check_database_integrity(source_db):
            response = input("⚠️ Целостность БД не подтверждена. Продолжить бэкап? (y/n): ")
            if response.lower() != 'y':
                print("❌ Бэкап отменен.")
                return None
    
    # Генерируем имя файла с временной меткой
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"referral_orders_{timestamp}.db"
    backup_filepath = backup_path / backup_filename
    
    try:
        # Создаем копию БД
        print(f"📦 Создание бэкапа {backup_filename}...")
        shutil.copy2(source_db, backup_filepath)
        
        # Получаем размер файла
        file_size = os.path.getsize(backup_filepath)
        file_size_mb = file_size / (1024 * 1024)
        
        print(f"✅ Бэкап успешно создан!")
        print(f"   Файл: {backup_filepath}")
        print(f"   Размер: {file_size_mb:.2f} MB")
        
        return str(backup_filepath)
        
    except Exception as e:
        print(f"❌ Ошибка при создании бэкапа: {e}")
        return None


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


def cleanup_old_backups(backup_dir: str = "backup/database", 
                       keep_count: int = 10) -> int:
    """
    Удаляет старые бэкапы, оставляя только последние N.
    
    Args:
        backup_dir: Директория с бэкапами
        keep_count: Количество бэкапов для сохранения
    
    Returns:
        Количество удаленных файлов
    """
    backups = list_backups(backup_dir)
    
    if len(backups) <= keep_count:
        return 0
    
    # Удаляем старые бэкапы
    removed_count = 0
    for backup in backups[keep_count:]:
        try:
            os.remove(backup['path'])
            removed_count += 1
            print(f"🗑️ Удален старый бэкап: {backup['name']}")
        except Exception as e:
            print(f"⚠️ Не удалось удалить {backup['name']}: {e}")
    
    if removed_count > 0:
        print(f"✅ Удалено {removed_count} старых бэкапов")
    
    return removed_count


def main():
    """Главная функция."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Создание бэкапа базы данных SQLite"
    )
    parser.add_argument(
        "--source",
        default="referral_orders.db",
        help="Путь к исходной базе данных (по умолчанию: referral_orders.db)"
    )
    parser.add_argument(
        "--backup-dir",
        default="backup/database",
        help="Директория для сохранения бэкапов (по умолчанию: backup/database)"
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
    parser.add_argument(
        "--cleanup",
        type=int,
        metavar="N",
        help="Удалить старые бэкапы, оставив только последние N"
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
    
    # Очистка старых бэкапов
    if args.cleanup is not None:
        cleanup_old_backups(args.backup_dir, args.cleanup)
        return
    
    # Создание бэкапа
    backup_path = create_backup(
        source_db=args.source,
        backup_dir=args.backup_dir,
        check_integrity=not args.no_integrity_check
    )
    
    if backup_path:
        print("\n💡 Следующие шаги:")
        print(f"   1. git add {backup_path}")
        print(f"   2. git commit -m \"Backup: database snapshot {datetime.now().strftime('%Y-%m-%d')}\"")
        print(f"   3. git push")


if __name__ == "__main__":
    main()

