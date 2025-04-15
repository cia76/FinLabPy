from datetime import time

from FinLabPy.Schedule.MarketSchedule import Schedule, Session


class Stocks(Schedule):
    """Расписание торгов Московской Биржи: Фондовый рынок - Акции https://www.moex.com/s1167"""
    def __init__(self):
        super(Stocks, self).__init__([
            Session(time(7, 0, 0), time(9, 49, 59)),  # Утренняя сессия
            Session(time(9, 50, 0), time(18, 39, 59)),  # Основная сессия
            Session(time(19, 5, 0), time(23, 49, 59))])  # Вечерняя сессия


class Bonds(Schedule):
    """Расписание торгов Московской Биржи: Фондовый рынок - Облигации https://www.moex.com/s1167"""
    def __init__(self):
        super(Bonds, self).__init__([
            Session(time(9, 0, 0), time(9, 49, 59)),  # Утренняя сессия
            Session(time(10, 0, 0), time(18, 39, 59)),  # Основная сессия
            Session(time(19, 5, 0), time(23, 49, 59))])  # Вечерняя сессия


class Futures(Schedule):
    """Расписание торгов Московской Биржи: Срочный рынок https://www.moex.com/ru/derivatives/"""
    def __init__(self):
        super(Futures, self).__init__([
            Session(time(9, 0, 0), time(9, 59, 59)),  # Утренняя дополнительная торговая сессия
            Session(time(10, 0, 0), time(13, 59, 59)),  # Основная торговая сессия (Дневной расчетный период)
            Session(time(14, 5, 0), time(18, 49, 59)),  # Основная торговая сессия (Вечерний расчетный период)
            Session(time(19, 5, 0), time(23, 49, 59))])  # Вечерняя дополнительная торговая сессия
