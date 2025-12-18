# Полный бэкап проекта telegram-ref-bot

## Информация о бэкапе

- **Дата создания:** 2025-12-18 22:32:48
- **Версия:** Перед созданием приложения
- **Тип бэкапа:** Полный бэкап проекта

## Структура бэкапа

```
full_backup_20251218_223246/
├── code/              # Исходный код проекта
│   ├── bot.py
│   ├── db_manager.py
│   ├── orders_updater.py
│   ├── sheets_client.py
│   ├── states.py
│   ├── backup.py
│   ├── restore.py
│   └── requirements.txt
├── database/          # Базы данных кабинетов
│   ├── referral_orders_20251218_223246.db
│   └── referral_orders_<cabinet>_20251218_223246.db
├── docs/              # Документация
│   ├── FINAL_SETUP.md
│   └── QUICK_SETUP.md
├── scripts/           # Скрипты
│   └── setup_github.bat
└── README.md          # Этот файл
```

## Статистика

- **Скопировано файлов:** 12
- **Пропущено файлов:** 2

## Скопированные файлы

- bot.py
- db_manager.py
- orders_updater.py
- sheets_client.py
- states.py
- backup.py
- restore.py
- requirements.txt
- setup_github.py
- setup_github.bat
- referral_orders.db
- referral_orders_beiber.db

## Пропущенные файлы

- FINAL_SETUP.md
- QUICK_SETUP.md

## Git репозиторий

После создания бэкапа рекомендуется добавить его в git репозиторий:

```bash
# Добавить бэкап в git
git add backup/full_backup_20251218_223246/

# Создать коммит
git commit -m "Backup: полный бэкап проекта от 2025-12-18"

# Отправить в удаленный репозиторий
git push origin main
```

⚠️ **Примечание:** Базы данных (файлы `.db`) обычно не включаются в git из-за их размера. Они сохранены локально в папке `database/`.

## Восстановление

Для восстановления проекта из этого бэкапа:

1. Скопируйте файлы из `code/` в корневую директорию проекта
2. Скопируйте файлы из `docs/` в корневую директорию проекта
3. Скопируйте файлы из `scripts/` в корневую директорию проекта
4. Восстановите базы данных из `database/` используя скрипт `restore.py`:
   ```bash
   python restore.py --backup database/referral_orders_20251218_223246.db
   python restore.py --backup database/referral_orders_<cabinet>_20251218_223246.db
   ```

## Примечания

⚠️ **ВАЖНО:** Этот бэкап создан **Перед созданием приложения**.

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
