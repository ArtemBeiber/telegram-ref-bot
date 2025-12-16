#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для создания полного бэкапа проекта.
Создает копию всех важных файлов проекта с пометкой о версии.
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


def create_full_backup(
    source_dir: str = ".",
    backup_dir: str = "backup",
    version_note: str = "Перед созданием приложения"
) -> str:
    """
    Создает полный бэкап проекта.
    
    Args:
        source_dir: Директория проекта
        backup_dir: Директория для сохранения бэкапов
        version_note: Пометка о версии
    
    Returns:
        Путь к созданному бэкапу или None в случае ошибки
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_folder_name = f"full_backup_{timestamp}"
    backup_path = Path(backup_dir) / backup_folder_name
    backup_path.mkdir(parents=True, exist_ok=True)
    
    # Список файлов для бэкапа
    files_to_backup = [
        "bot.py",
        "db_manager.py",
        "orders_updater.py",
        "sheets_client.py",
        "states.py",
        "backup.py",
        "restore.py",
        "requirements.txt",
        "setup_github.py",
        "setup_github.bat",
        "FINAL_SETUP.md",
        "QUICK_SETUP.md",
    ]
    
    # Директории для бэкапа
    dirs_to_backup = []
    
    # Файлы, которые нужно исключить (секретные данные)
    exclude_files = [
        ".env",
        ".git",
        "__pycache__",
        "venv",
        "*.pyc",
        "*.pyo",
        "*.db-journal",
    ]
    
    print("=" * 60)
    print("Создание полного бэкапа проекта")
    print("=" * 60)
    print(f"Версия: {version_note}")
    print(f"Время создания: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Директория бэкапа: {backup_path}")
    print()
    
    # Создаем поддиректории
    (backup_path / "code").mkdir(exist_ok=True)
    (backup_path / "database").mkdir(exist_ok=True)
    (backup_path / "docs").mkdir(exist_ok=True)
    (backup_path / "scripts").mkdir(exist_ok=True)
    
    copied_files = []
    skipped_files = []
    
    # Копируем файлы кода
    print("📦 Копирование файлов проекта...")
    for filename in files_to_backup:
        source_file = Path(source_dir) / filename
        if source_file.exists():
            if filename.endswith(('.md',)):
                dest_file = backup_path / "docs" / filename
            elif filename.endswith(('.py',)):
                dest_file = backup_path / "code" / filename
            elif filename.endswith(('.bat',)):
                dest_file = backup_path / "scripts" / filename
            else:
                dest_file = backup_path / "code" / filename
            
            try:
                shutil.copy2(source_file, dest_file)
                copied_files.append(filename)
                print(f"  ✅ {filename}")
            except Exception as e:
                print(f"  ❌ Ошибка при копировании {filename}: {e}")
                skipped_files.append(filename)
        else:
            print(f"  ⚠️ Файл не найден: {filename}")
            skipped_files.append(filename)
    
    # Копируем базу данных
    print()
    print("💾 Копирование базы данных...")
    db_file = Path(source_dir) / "referral_orders.db"
    if db_file.exists():
        # Проверяем целостность БД
        print("  🔍 Проверка целостности базы данных...")
        if check_database_integrity(str(db_file)):
            print("  ✅ Целостность БД подтверждена")
        else:
            print("  ⚠️ Предупреждение: Целостность БД не подтверждена")
        
        try:
            db_backup_file = backup_path / "database" / f"referral_orders_{timestamp}.db"
            shutil.copy2(db_file, db_backup_file)
            file_size = os.path.getsize(db_backup_file)
            file_size_mb = file_size / (1024 * 1024)
            print(f"  ✅ База данных скопирована: {file_size_mb:.2f} MB")
            copied_files.append("referral_orders.db")
        except Exception as e:
            print(f"  ❌ Ошибка при копировании БД: {e}")
            skipped_files.append("referral_orders.db")
    else:
        print("  ⚠️ База данных не найдена")
        skipped_files.append("referral_orders.db")
    
    # Создаем README с информацией о бэкапе
    print()
    print("📝 Создание README...")
    readme_content = f"""# Полный бэкап проекта telegram-ref-bot

## Информация о бэкапе

- **Дата создания:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- **Версия:** {version_note}
- **Тип бэкапа:** Полный бэкап проекта

## Структура бэкапа

```
{backup_folder_name}/
├── code/              # Исходный код проекта
│   ├── bot.py
│   ├── db_manager.py
│   ├── orders_updater.py
│   ├── sheets_client.py
│   ├── states.py
│   ├── backup.py
│   ├── restore.py
│   └── requirements.txt
├── database/          # База данных
│   └── referral_orders_{timestamp}.db
├── docs/              # Документация
│   ├── FINAL_SETUP.md
│   └── QUICK_SETUP.md
├── scripts/           # Скрипты
│   └── setup_github.bat
└── README.md          # Этот файл
```

## Статистика

- **Скопировано файлов:** {len(copied_files)}
- **Пропущено файлов:** {len(skipped_files)}

## Скопированные файлы

{chr(10).join(f"- {f}" for f in copied_files)}

## Пропущенные файлы

{chr(10).join(f"- {f}" for f in skipped_files) if skipped_files else "- Нет"}

## Восстановление

Для восстановления проекта из этого бэкапа:

1. Скопируйте файлы из `code/` в корневую директорию проекта
2. Скопируйте файлы из `docs/` в корневую директорию проекта
3. Скопируйте файлы из `scripts/` в корневую директорию проекта
4. Восстановите базу данных из `database/` используя скрипт `restore.py`:
   ```bash
   python restore.py --backup database/referral_orders_{timestamp}.db
   ```

## Примечания

⚠️ **ВАЖНО:** Этот бэкап создан **{version_note}**.

⚠️ **Секретные файлы не включены в бэкап:**
- `.env` (переменные окружения)
- `google-credentials.json` (учетные данные Google)
- Другие файлы с секретными данными

Убедитесь, что у вас есть отдельные копии этих файлов!

## Системные требования

- Python 3.8+
- Зависимости из `requirements.txt`
- SQLite 3.x

## Контакты

При возникновении проблем с восстановлением проверьте:
1. Версию Python
2. Установленные зависимости
3. Целостность базы данных
"""
    
    readme_file = backup_path / "README.md"
    try:
        with open(readme_file, 'w', encoding='utf-8') as f:
            f.write(readme_content)
        print(f"  ✅ README создан")
    except Exception as e:
        print(f"  ❌ Ошибка при создании README: {e}")
    
    # Создаем файл со списком всех файлов
    print()
    print("📋 Создание списка файлов...")
    file_list_content = f"""Список файлов в бэкапе
Дата создания: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Версия: {version_note}

Скопированные файлы:
{chr(10).join(f"  {f}" for f in copied_files)}

Пропущенные файлы:
{chr(10).join(f"  {f}" for f in skipped_files) if skipped_files else "  Нет"}
"""
    
    file_list_file = backup_path / "file_list.txt"
    try:
        with open(file_list_file, 'w', encoding='utf-8') as f:
            f.write(file_list_content)
        print(f"  ✅ Список файлов создан")
    except Exception as e:
        print(f"  ❌ Ошибка при создании списка файлов: {e}")
    
    # Подсчитываем общий размер бэкапа
    total_size = 0
    for root, dirs, files in os.walk(backup_path):
        for file in files:
            file_path = Path(root) / file
            total_size += file_path.stat().st_size
    
    total_size_mb = total_size / (1024 * 1024)
    
    print()
    print("=" * 60)
    print("✅ Полный бэкап успешно создан!")
    print("=" * 60)
    print(f"Директория: {backup_path}")
    print(f"Размер: {total_size_mb:.2f} MB")
    print(f"Скопировано файлов: {len(copied_files)}")
    if skipped_files:
        print(f"Пропущено файлов: {len(skipped_files)}")
    print()
    print("💡 Следующие шаги:")
    print(f"   1. Проверьте содержимое: {backup_path}")
    print(f"   2. Убедитесь, что все важные файлы скопированы")
    print(f"   3. Сохраните бэкап в безопасном месте")
    
    return str(backup_path)


def main():
    """Главная функция."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Создание полного бэкапа проекта telegram-ref-bot"
    )
    parser.add_argument(
        "--source",
        default=".",
        help="Директория проекта (по умолчанию: текущая)"
    )
    parser.add_argument(
        "--backup-dir",
        default="backup",
        help="Директория для сохранения бэкапов (по умолчанию: backup)"
    )
    parser.add_argument(
        "--version-note",
        default="Перед созданием приложения",
        help="Пометка о версии (по умолчанию: 'Перед созданием приложения')"
    )
    
    args = parser.parse_args()
    
    backup_path = create_full_backup(
        source_dir=args.source,
        backup_dir=args.backup_dir,
        version_note=args.version_note
    )
    
    if backup_path:
        print()
        print("🎉 Бэкап готов к использованию!")


if __name__ == "__main__":
    main()

