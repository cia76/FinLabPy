import backtrader as bt  # Библиотека BackTrader

from FinLabPy.Config import brokers, default_broker  # Все брокеры и брокер по умолчанию
from FinLabPy.BackTrader import Store  # Хранилище BackTrader


def get_cash_value():
    cash = cerebro.broker.getcash()  # Свободные средства
    value = cerebro.broker.getvalue()  # Стоимость позиций
    print(f'Свободные средства : {cash}')
    print(f'Стоимость позиций  : {value - cash}')
    print(f'Стоимость портфеля : {value}')


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    cerebro = bt.Cerebro()  # Инициируем "движок" BackTrader (Cerebro = Мозг на испанском)
    store = Store(broker=default_broker)  # Хранилище брокера по умолчанию
    # store = Store(broker=brokers['<Ключ словаря brokers из Config.py>'])  # Хранилище выбранного брокера
    broker = store.getbroker()  # Брокер
    cerebro.setbroker(broker)  # Устанавливаем брокера

    print('\nСтартовый капитал')
    get_cash_value()  # Отображаем статистику портфеля до запуска ТС
    cerebro.run()  # Запуск ТС. Пока ее у нас нет
    print('\nКонечный капитал')
    get_cash_value()  # Отображаем статистику портфеля после запуска ТС
