# Скрипт для создания полного бэкапа проекта
$ErrorActionPreference = "Stop"

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$backupDir = "backup\full_backup_$timestamp"
$versionNote = "Перед созданием приложения"

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "Создание полного бэкапа проекта" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "Версия: $versionNote" -ForegroundColor Yellow
Write-Host "Время создания: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Yellow
Write-Host "Директория бэкапа: $backupDir" -ForegroundColor Yellow
Write-Host ""

# Создаем структуру директорий
New-Item -ItemType Directory -Path "$backupDir\code" -Force | Out-Null
New-Item -ItemType Directory -Path "$backupDir\database" -Force | Out-Null
New-Item -ItemType Directory -Path "$backupDir\docs" -Force | Out-Null
New-Item -ItemType Directory -Path "$backupDir\scripts" -Force | Out-Null

Write-Host "📦 Копирование файлов проекта..." -ForegroundColor Green

# Файлы для копирования
$filesToBackup = @{
    "bot.py" = "code"
    "db_manager.py" = "code"
    "orders_updater.py" = "code"
    "sheets_client.py" = "code"
    "states.py" = "code"
    "backup.py" = "code"
    "restore.py" = "code"
    "requirements.txt" = "code"
    "FINAL_SETUP.md" = "docs"
    "QUICK_SETUP.md" = "docs"
    "setup_github.py" = "scripts"
    "setup_github.bat" = "scripts"
}

$copiedFiles = @()
$skippedFiles = @()

foreach ($file in $filesToBackup.Keys) {
    $destDir = $filesToBackup[$file]
    $sourcePath = $file
    $destPath = "$backupDir\$destDir\$file"
    
    if (Test-Path $sourcePath) {
        try {
            Copy-Item -Path $sourcePath -Destination $destPath -Force
            Write-Host "  ✅ $file" -ForegroundColor Green
            $copiedFiles += $file
        } catch {
            Write-Host "  ❌ Ошибка при копировании $file : $_" -ForegroundColor Red
            $skippedFiles += $file
        }
    } else {
        Write-Host "  ⚠️ Файл не найден: $file" -ForegroundColor Yellow
        $skippedFiles += $file
    }
}

# Копируем базу данных
Write-Host ""
Write-Host "💾 Копирование базы данных..." -ForegroundColor Green

$dbFile = "referral_orders.db"
if (Test-Path $dbFile) {
    try {
        $dbBackupFile = "$backupDir\database\referral_orders_$timestamp.db"
        Copy-Item -Path $dbFile -Destination $dbBackupFile -Force
        $dbSize = (Get-Item $dbBackupFile).Length / 1MB
        Write-Host "  ✅ База данных скопирована: $([math]::Round($dbSize, 2)) MB" -ForegroundColor Green
        $copiedFiles += "referral_orders.db"
    } catch {
        Write-Host "  ❌ Ошибка при копировании БД: $_" -ForegroundColor Red
        $skippedFiles += "referral_orders.db"
    }
} else {
    Write-Host "  ⚠️ База данных не найдена" -ForegroundColor Yellow
    $skippedFiles += "referral_orders.db"
}

# Создаем README
Write-Host ""
Write-Host "📝 Создание README..." -ForegroundColor Green

$readmePath = "$backupDir\README.md"
$dateStr = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
$copiedFilesList = $copiedFiles -join "`n- "
$skippedFilesList = if ($skippedFiles.Count -gt 0) { $skippedFiles -join "`n- " } else { "Нет" }

$readmeContent = @"
# Полный бэкап проекта telegram-ref-bot

## Информация о бэкапе

- Дата создания: $dateStr
- Версия: $versionNote
- Тип бэкапа: Полный бэкап проекта

## Структура бэкапа

full_backup_$timestamp/
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
│   └── referral_orders_$timestamp.db
├── docs/              # Документация
│   ├── FINAL_SETUP.md
│   └── QUICK_SETUP.md
├── scripts/           # Скрипты
│   └── setup_github.bat
└── README.md          # Этот файл

## Статистика

- Скопировано файлов: $($copiedFiles.Count)
- Пропущено файлов: $($skippedFiles.Count)

## Скопированные файлы

- $copiedFilesList

## Пропущенные файлы

- $skippedFilesList

## Восстановление

Для восстановления проекта из этого бэкапа:

1. Скопируйте файлы из code/ в корневую директорию проекта
2. Скопируйте файлы из docs/ в корневую директорию проекта
3. Скопируйте файлы из scripts/ в корневую директорию проекта
4. Восстановите базу данных из database/ используя скрипт restore.py:
   python restore.py --backup database/referral_orders_$timestamp.db

## Примечания

ВАЖНО: Этот бэкап создан $versionNote.

Секретные файлы не включены в бэкап:
- .env (переменные окружения)
- google-credentials.json (учетные данные Google)
- Другие файлы с секретными данными

Убедитесь, что у вас есть отдельные копии этих файлов!

## Системные требования

- Python 3.8+
- Зависимости из requirements.txt
- SQLite 3.x

## Контакты

При возникновении проблем с восстановлением проверьте:
1. Версию Python
2. Установленные зависимости
3. Целостность базы данных
"@

try {
    $readmeContent | Out-File -FilePath $readmePath -Encoding UTF8
    Write-Host "  ✅ README создан" -ForegroundColor Green
} catch {
    Write-Host "  ❌ Ошибка при создании README: $_" -ForegroundColor Red
}

# Подсчитываем размер
$totalSize = (Get-ChildItem -Path $backupDir -Recurse -File | Measure-Object -Property Length -Sum).Sum / 1MB

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "✅ Полный бэкап успешно создан!" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "Директория: $backupDir" -ForegroundColor Yellow
Write-Host "Размер: $([math]::Round($totalSize, 2)) MB" -ForegroundColor Yellow
Write-Host "Скопировано файлов: $($copiedFiles.Count)" -ForegroundColor Yellow
if ($skippedFiles.Count -gt 0) {
    Write-Host "Пропущено файлов: $($skippedFiles.Count)" -ForegroundColor Yellow
}
Write-Host ""
Write-Host "💡 Следующие шаги:" -ForegroundColor Cyan
Write-Host "   1. Проверьте содержимое: $backupDir" -ForegroundColor White
Write-Host "   2. Убедитесь, что все важные файлы скопированы" -ForegroundColor White
Write-Host "   3. Сохраните бэкап в безопасном месте" -ForegroundColor White

