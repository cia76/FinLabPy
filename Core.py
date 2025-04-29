# Курс Мультиброкер: Контроль https://finlab.vip/wpm-category/mbcontrol/

from datetime import datetime  # Работа с датой и временем
from math import copysign  # Знак числа
from typing import Union  # Объединение типов

import pandas as pd


class Symbol:
    """Тикер"""
    def __init__(self, board: str, symbol: str, dataname: str, description: str, decimals: int, min_step: float, lot_size: int, broker_info=None):
        self.board = board  # Код режима торгов
        self.symbol = symbol  # Тикер
        self.dataname = dataname  # Название тикера
        self.description = description  # Описание тикера
        self.decimals = decimals  # Кол-во десятичных знаков в цене
        self.min_step = min_step  # Минимальный шаг цены
        self.lot_size = lot_size  # Кол-во штук в лоте
        self.broker_info = broker_info  # Информация от брокера, позволяющая идентифицировать тикер. Доп. информация о тикере

    def __repr__(self):
        return f'{self.dataname} ({self.description}) Лот: {self.lot_size}, Шаг цены: {self.min_step}, Кол-во десятичных знаков: {self.decimals}'


class Bar:
    """Бар"""
    def __init__(self, board: str, symbol: str, dataname: str, time_frame: str, datetime: datetime, open: float, high: float, low: float, close: float, volume: int):
        self.board = board  # Код режима торгов
        self.symbol = symbol  # Тикер
        self.dataname = dataname  # Название тикера
        self.time_frame = time_frame  # Временной интервал
        self.datetime = datetime
        self.open = open  # Цена открытия
        self.high = high  # Максимальная цена
        self.low = low  # Минимальная цена
        self.close = close  # Цена закрытия
        self.volume = volume  # Объем

    def to_dict(self) -> dict:
        """Перевод в словарь, чтобы легче было импортировать в pandas DataFrame"""
        return {'datetime': self.datetime, 'open': self.open, 'high': self.high, 'low': self.low, 'close': self.close, 'volume': self.volume}

    def __repr__(self):
        return f'{self.dataname} ({self.time_frame}) {self.datetime} Open: {self.open}, High: {self.high}, Low: {self.low}, Close: {self.close}, Volume: {self.volume}'


class Order:
    """Заявка"""
    (Market, Limit, Stop, StopLimit) = range(4)  # Тип заявки. По рынку/лимит/стоп/стоп-лимит
    ExecTypes = ['Market', 'Limit', 'Stop', 'StopLimit']  # Отображение типа заявки

    def __init__(self, broker, order_id: str, buy: bool, exec_type, dataname: str, decimals: int, quantity: int, price: float=0, stop_price: float=0):
        self.broker = broker  # Брокер
        self.id = order_id  # Уникальный код заявки
        self.buy = buy  # Покупка
        self.exec_type = exec_type  # Тип заявки
        self.dataname = dataname  # Название тикера
        self.decimals = decimals  # Кол-во десятичных знаков в цене
        self.quantity = quantity  # Кол-во в штуках
        self.price = price  # Цена
        self.stop_price = stop_price  # Стоп цена

    def __repr__(self):
        if self.decimals == 0:  # Если цена в рублях без копеек
            format_price = str(int(self.price))  # То указываем цену как целое число без десятичных знаков
        elif self.decimals <= 2:  # Если цена в рублях с копейками
            format_price = f'{self.price:.2f}'  # То указываем цену как целое число с 2-мя десятичными знаками
        else:  # В остальных случаях
            format_price = '{p:.{d}f}'.format(p=self.price, d=self.decimals)  # Указываем цену какая есть
        return f'[{self.broker.code}] {Order.ExecTypes[self.exec_type]} {"Buy" if self.buy else "Sell"} {self.dataname} {self.quantity} @ {format_price}'


class Position:
    """Позиция"""
    def __init__(self, broker, dataname: str, description: str, decimals: int, quantity: int, average_price: Union[int, float], current_price: Union[int, float]):
        self.broker = broker  # Брокер
        self.dataname = dataname  # Название тикера
        self.description = description  # Описание тикера
        self.decimals = decimals  # Кол-во десятичных знаков в цене
        self.quantity = quantity  # Кол-во в штуках. Положительное - длинная позиция, отрицательное - короткая позиция
        self.average_price = average_price  # Средняя цена входа в рублях
        self.current_price = current_price  # Последняя цена в рублях
        self.change_pct = copysign(1, quantity) * (current_price / average_price - 1) * 100 if average_price else 0  # Процент изменения цены в зависимости от направления позиции (кол-ва)

    def __repr__(self):
        if self.decimals == 0:  # Если цены в рублях без копеек
            format_average_price = str(int(self.average_price))  # То указываем цены
            format_current_price = str(int(self.current_price))  # как целое число без десятичных знаков
        elif self.decimals <= 2:  # Если цены в рублях с копейками
            format_average_price = f'{self.average_price:.2f}'  # То указываем цены
            format_current_price = f'{self.current_price:.2f}'  # как целое число с 2-мя десятичными знаками
        else:  # В остальных случаях
            format_average_price = '{p:.{d}f}'.format(p=self.average_price, d=self.decimals)  # Указываем цены
            format_current_price = '{p:.{d}f}'.format(p=self.current_price, d=self.decimals)  # какие есть
        return f'[{self.broker.code}] {self.dataname} ({self.description})\n      {self.quantity} @ {format_average_price} / {format_current_price} {self.change_pct:.2f}% '


# noinspection PyShadowingBuiltins
class Broker:
    """Брокер"""
    def __init__(self, code: str, name: str, provider, account_id: int = 0, storage: str = 'file'):
        self.code = code  # Код брокера
        self.name = name  # Название провайдера
        self.provider = provider  # Провайдер
        self.account_id = account_id  # Порядковый номер счета
        if storage == 'file':  # Если файловое хранилище
            from FinLabPy.Storage.FileStorage import FileStorage as storage  # то ипортируем библиотеку файлового хранилища
            self.storage = storage(self.__class__.__name__)  # Инициализируем хранилище
        elif storage == 'db':  # Если хранилище в БД
            try:
                from FinLabPy.Storage.SQLiteStorage import SQLiteStorage as storage  # Пытаемся импортировать библиотеку Курса Базы данных для трейдеров https://finlab.vip/wpm-category/databases/
                self.storage = storage()  # Инициализируем хранилище
            except ModuleNotFoundError:  # Если библиотека не найдена
                from FinLabPy.Storage.FileStorage import FileStorage as storage  # то ипортируем библиотеку файлового хранилища
                self.storage = storage(self.__class__.__name__)  # Инициализируем хранилище
        else:  # В остальных случаях
            from FinLabPy.Storage.FileStorage import FileStorage as storage  # то ипортируем библиотеку файлового хранилища
        self.positions: list[Position] = []  # Текущие позиции
        self.orders: list[Order] = []  # Активные заявки

    def get_symbol_by_dataname(self, dataname: str) -> Union[Symbol, None]:
        """Тикер по названию"""
        raise NotImplementedError

    @staticmethod
    def board_symbol_to_dataname(board, symbol) -> str:
        """Название тикера из кода режима торгов и тикера"""
        return f'{board}.{symbol}'

    def get_history(self, symbol: Symbol, time_frame: str, dt_from: datetime = None, dt_to: datetime = None) -> Union[list[Bar], None]:
        """История тикера"""
        return self.storage.get_bars(symbol, time_frame, dt_from, dt_to)

    def subscribe_history(self, dataname: str, time_frame: str) -> None:
        """Подписка на историю тикера"""
        raise NotImplementedError

    def on_new_bar(self, bar: Bar) -> None:
        """Получение нового бара по подписке"""
        raise NotImplementedError

    def get_last_price(self, dataname: str) -> Union[float, None]:
        """Последняя цена тикера"""
        raise NotImplementedError

    def get_value(self) -> float:
        """Стоимость позиций"""
        raise NotImplementedError

    def get_cash(self) -> float:
        """Свободные средства"""
        raise NotImplementedError

    def get_positions(self) -> list[Position]:
        """Открытые позиции"""
        raise NotImplementedError

    def get_orders(self) -> list[Order]:
        """Активные заявки"""
        raise NotImplementedError

    def new_order(self, order: Order) -> None:
        """Создание и отправка заявки брокеру"""
        raise NotImplementedError

    def cancel_order(self, order: Order) -> None:
        """Отмена активной заявки"""
        raise NotImplementedError

    def close(self) -> None:
        """Закрытие провайдера"""
        raise NotImplementedError


class Storage:
    """Хранилище бар и спецификации тикеров брокера"""
    def __init__(self, source: str):
        self.source = source  # Источник хранилища
        self.symbols: dict[str, Symbol] = {}  # Словать тикеров

    def get_symbol(self, dataname: str) -> Union[Symbol, None]:
        """Получение тикера"""
        return self.symbols[dataname] if dataname in self.symbols else None  # Пробуем получить тикер по названию из словаря

    def set_symbol(self, symbol: Symbol) -> None:
        """Сохранение тикера"""
        self.symbols[symbol.dataname] = symbol  # Добавляем/изменяем тикер в словаре

    def get_bars(self, symbol: Symbol, time_frame: str, dt_from: datetime = None, dt_to: datetime = None) -> Union[list[Bar], None]:
        """Получение бар"""
        raise NotImplementedError

    def set_bars(self, bars: list[Bar]) -> None:
        """Сохранение бар"""
        raise NotImplementedError


def bars_to_df(bars: list[Bar]) -> pd.DataFrame:
    """Перевод списка бар в pandas DataFrame с индексом по дате/времени бара"""
    pd_bars = pd.DataFrame.from_records([bar.to_dict() for bar in bars], index='datetime')  # Переводим в pandas DataFrame
    pd_bars['volume'] = pd_bars['volume'].astype(int)  # Объемы могут быть только целыми
    return pd_bars
