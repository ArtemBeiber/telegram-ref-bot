import os
from datetime import datetime

import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

# Загружаем переменные окружения из .env
load_dotenv()

GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")

if not GOOGLE_CREDENTIALS_FILE:
    raise RuntimeError("Не задан GOOGLE_CREDENTIALS_FILE в .env")

if not GOOGLE_SHEET_ID:
    raise RuntimeError("Не задан GOOGLE_SHEET_ID в .env")


def get_gspread_client():
    """Создаёт авторизованный клиент gspread."""
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_FILE,
        scopes=scopes,
    )
    client = gspread.authorize(creds)
    return client


def get_spreadsheet():
    """Возвращает объект таблицы по ID."""
    client = get_gspread_client()
    return client.open_by_key(GOOGLE_SHEET_ID)


def find_participant_by_ozon_id(ozon_id: str):
    """Ищет участника по его Ozon ID."""
    spreadsheet = get_spreadsheet()
    ws = spreadsheet.worksheet("Участники")

    data = ws.get_all_records()

    for row in data:
        if str(row.get("Ozon ID")) == str(ozon_id):
            return row

    return None

def create_participant(
    tg_id: int,
    username: str | None,
    first_name: str | None,
    ozon_id: str,
    referrer_id: str | None = None,
    language: str | None = None,
):

    """
    Создаёт нового участника в листе 'Участники'.
    """
    spreadsheet = get_spreadsheet()
    ws = spreadsheet.worksheet("Участники")

    all_values = ws.get_all_values()
    next_row = len(all_values) + 1 

    tg_username = f"@{username}" if username else ""
    name = first_name or ""

    today = datetime.today().strftime("%Y-%m-%d")

    row = [
        int(ozon_id),                             # A: ID участника (Ozon ID)
        name,                                     # B: Имя / ник
        tg_username,                              # C: Телеграм @
        int(ozon_id),                             # D: Ozon ID
        str(referrer_id) if referrer_id else "",  # E: ID пригласившего
        today,                                    # F: Дата регистрации
        str(tg_id),                               # G: Telegram ID
    ]

    ws.update(f"A{next_row}:g{next_row}", [row])

    return {
        "ID участника": str(ozon_id),
        "Имя / ник": name,
        "Телеграм @": tg_username,
        "Ozon ID": str(ozon_id),
        "ID пригласившего": str(referrer_id) if referrer_id else "",
        "Дата регистрации": today,
        "row_index": next_row,
    }

def find_participant_by_telegram_id(tg_id: int):
    """Ищет участника по его Telegram ID."""
    spreadsheet = get_spreadsheet()
    ws = spreadsheet.worksheet("Участники")

    data = ws.get_all_records()

    for row in data:
        if str(row.get("Telegram ID")) == str(tg_id):
            return row

    return None


# >>> НАЧАЛО БЛОКА: ЛОГИКА СИНХРОНИЗАЦИИ <<<
def get_last_sync_timestamp() -> datetime | None:
    """Возвращает время последней успешной синхронизации из листа Настройки (B10)."""
    try:
        spreadsheet = get_spreadsheet()
        ws = spreadsheet.worksheet("Настройки")
        
        # Читаем значение из ячейки B10
        cell = ws.acell('B10').value 
        
        if cell:
            # Преобразуем строку в объект datetime
            return datetime.strptime(cell, "%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(f"Ошибка чтения времени синхронизации: {e}")
        return None
    return None

def set_last_sync_timestamp(timestamp: datetime):
    """Записывает время последней успешной синхронизации в лист Настройки (B10)."""
    try:
        spreadsheet = get_spreadsheet()
        ws = spreadsheet.worksheet("Настройки")
        
        # Записываем текущее время в ячейку B10
        timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        ws.update_acell('B10', timestamp_str) 
        print(f"Время синхронизации обновлено до: {timestamp_str}")
    except Exception as e:
        print(f"Ошибка записи времени синхронизации: {e}")
# >>> КОНЕЦ БЛОКА: ЛОГИКА СИНХРОНИЗАЦИИ <<<


if __name__ == "__main__":
    # Простой тест: выводим название таблицы и имена листов
    try:
        spreadsheet = get_spreadsheet()
        print("Подключение к таблице успешно ✅")
        print("Название таблицы:", spreadsheet.title)

        worksheets = spreadsheet.worksheets()
        print("Листы в файле:")
        for ws in worksheets:
            print(" -", ws.title)

    except Exception as e:
        print("Ошибка при подключении к таблице ❌")
        print(e)