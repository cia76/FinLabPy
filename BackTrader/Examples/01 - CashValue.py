import backtrader as bt

# noinspection PyUnusedImports
from FinLabPy.Config import brokers, default_broker  # Все брокеры и брокер по умолчанию
from FinLabPy.BackTrader import Store  # Хранилище BackTrader


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    # noinspection PyArgumentList
    cerebro = bt.Cerebro()  # Инициируем "движок" BackTrader
    store = Store(broker=default_broker)  # Хранилище брокера по умолчанию
    # store = Store(broker=brokers['<Ключ словаря brokers из Config.py>'])  # Хранилище выбранного брокера
    broker = store.getbroker()  # Брокер
    # noinspection PyArgumentList
    cerebro.setbroker(broker)  # Устанавливаем брокера
    cash = cerebro.broker.getcash()  # Свободные средства
    value = cerebro.broker.getvalue()  # Стоимость позиций
    print(f'Свободные средства : {cash}')
    print(f'Стоимость позиций  : {value}')
    print(f'Стоимость портфеля : {cash + value}')
