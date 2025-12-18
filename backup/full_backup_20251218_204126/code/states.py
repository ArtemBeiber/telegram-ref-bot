from aiogram.fsm.state import State, StatesGroup

class Registration(StatesGroup):
    waiting_for_ozon_id = State()

class BonusSettings(StatesGroup):
    editing_levels = State()
    editing_percent = State()

class LeavingProgram(StatesGroup):
    confirming_leave = State()

class ParticipantAnalytics(StatesGroup):
    waiting_for_participant_data = State()

class Withdrawal(StatesGroup):
    entering_amount = State()
    confirming = State()

class WithdrawalRejection(StatesGroup):
    entering_reason = State()

class WithdrawalSettings(StatesGroup):
    editing_min_amount = State()