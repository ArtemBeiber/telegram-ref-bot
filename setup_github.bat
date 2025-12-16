@echo off
chcp 65001 >nul
echo ============================================================
echo Настройка GitHub Remote для telegram-ref-bot
echo ============================================================
echo.

set /p GITHUB_USERNAME="Введите ваш GitHub username: "
if "%GITHUB_USERNAME%"=="" (
    echo Ошибка: Username не может быть пустым!
    pause
    exit /b 1
)

set /p REPO_NAME="Введите имя репозитория [telegram-ref-bot]: "
if "%REPO_NAME%"=="" set REPO_NAME=telegram-ref-bot

set /p USE_SSH="Использовать SSH? (y/N): "
if /i "%USE_SSH%"=="y" (
    set REMOTE_URL=git@github.com:%GITHUB_USERNAME%/%REPO_NAME%.git
) else (
    set REMOTE_URL=https://github.com/%GITHUB_USERNAME%/%REPO_NAME%.git
)

echo.
echo Добавляю remote: %REMOTE_URL%
git remote add origin %REMOTE_URL%

if errorlevel 1 (
    echo Ошибка при добавлении remote!
    pause
    exit /b 1
)

echo.
echo Проверка remote:
git remote -v

echo.
set /p PUSH_NOW="Отправить изменения на GitHub сейчас? (Y/n): "
if /i not "%PUSH_NOW%"=="n" (
    echo.
    echo Отправляю изменения...
    git push -u origin main
    if errorlevel 1 (
        echo.
        echo ВНИМАНИЕ: Ошибка при отправке!
        echo.
        echo Возможные причины:
        echo 1. Репозиторий не существует на GitHub
        echo    Создайте его на https://github.com/new
        echo 2. Нет прав доступа
        echo 3. Проблемы с аутентификацией
        echo.
        echo После создания репозитория выполните:
        echo   git push -u origin main
    ) else (
        echo.
        echo Успешно! Изменения отправлены на GitHub.
    )
) else (
    echo.
    echo Для отправки изменений выполните:
    echo   git push -u origin main
)

echo.
pause

