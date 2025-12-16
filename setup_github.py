#!/usr/bin/env python3
"""
Скрипт для настройки GitHub remote репозитория.
"""
import subprocess
import sys
import os

def run_command(cmd, check=True):
    """Выполнить команду и вернуть результат."""
    try:
        result = subprocess.run(
            cmd, 
            shell=True, 
            capture_output=True, 
            text=True,
            check=check
        )
        return result.stdout.strip(), result.stderr.strip(), result.returncode
    except subprocess.CalledProcessError as e:
        return e.stdout.strip(), e.stderr.strip(), e.returncode

def check_remote_exists():
    """Проверить, существует ли уже remote origin."""
    stdout, stderr, code = run_command("git remote -v", check=False)
    return "origin" in stdout

def get_current_branch():
    """Получить текущую ветку."""
    stdout, _, _ = run_command("git branch --show-current", check=False)
    return stdout.strip()

def main():
    print("=" * 60)
    print("Настройка GitHub Remote для telegram-ref-bot")
    print("=" * 60)
    print()
    
    # Проверяем, есть ли уже remote
    if check_remote_exists():
        print("⚠️  Remote 'origin' уже настроен!")
        stdout, _, _ = run_command("git remote -v", check=False)
        print(stdout)
        response = input("\nПерезаписать существующий remote? (y/N): ").strip().lower()
        if response != 'y':
            print("Отмена операции.")
            return
        
        # Удаляем существующий remote
        run_command("git remote remove origin", check=False)
        print("✅ Старый remote удален.")
        print()
    
    # Получаем информацию о репозитории
    print("Введите данные для настройки GitHub remote:")
    print()
    
    username = input("GitHub username: ").strip()
    if not username:
        print("❌ Username не может быть пустым!")
        return
    
    repo_name = input(f"Repository name [telegram-ref-bot]: ").strip()
    if not repo_name:
        repo_name = "telegram-ref-bot"
    
    use_ssh = input("Использовать SSH вместо HTTPS? (y/N): ").strip().lower() == 'y'
    
    if use_ssh:
        remote_url = f"git@github.com:{username}/{repo_name}.git"
    else:
        remote_url = f"https://github.com/{username}/{repo_name}.git"
    
    print()
    print(f"Добавляю remote: {remote_url}")
    
    # Добавляем remote
    stdout, stderr, code = run_command(f'git remote add origin "{remote_url}"', check=False)
    
    if code != 0:
        print(f"❌ Ошибка при добавлении remote: {stderr}")
        return
    
    print("✅ Remote успешно добавлен!")
    print()
    
    # Проверяем remote
    stdout, _, _ = run_command("git remote -v", check=False)
    print("Текущие remote репозитории:")
    print(stdout)
    print()
    
    # Получаем текущую ветку
    current_branch = get_current_branch()
    if not current_branch:
        current_branch = "main"
    
    print(f"Текущая ветка: {current_branch}")
    print()
    
    # Предлагаем отправить изменения
    push_now = input("Отправить изменения на GitHub сейчас? (Y/n): ").strip().lower()
    if push_now != 'n':
        print()
        print(f"Отправляю изменения в {current_branch}...")
        stdout, stderr, code = run_command(f"git push -u origin {current_branch}", check=False)
        
        if code == 0:
            print("✅ Изменения успешно отправлены на GitHub!")
        else:
            print(f"⚠️  Ошибка при отправке: {stderr}")
            print()
            print("Возможные причины:")
            print("1. Репозиторий не существует на GitHub - создайте его на https://github.com/new")
            print("2. Нет прав доступа - проверьте настройки GitHub")
            print("3. Проблемы с аутентификацией - настройте SSH ключи или Personal Access Token")
            print()
            print(f"После создания репозитория выполните:")
            print(f"  git push -u origin {current_branch}")
    else:
        print()
        print("Для отправки изменений выполните:")
        print(f"  git push -u origin {current_branch}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nОперация отменена пользователем.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        sys.exit(1)

