from aiogram.fsm.state import State, StatesGroup

class Registration(StatesGroup):
    waiting_for_ozon_id = State()

class BonusSettings(StatesGroup):
    editing_levels = State()
    editing_percent = State()