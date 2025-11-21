import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import backtrader as bt

# noinspection PyUnusedImports
from FinLabPy.Config import brokers, default_broker  # Все брокеры и брокер по умолчанию
from FinLabPy.BackTrader import Store  # Хранилище BackTrader


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.INFO,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Logging.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=ZoneInfo('Europe/Moscow')).timetuple()  # В логе время указываем по МСК
    logger = logging.getLogger('Logging')  # Будем вести лог

    # noinspection PyArgumentList
    cerebro = bt.Cerebro()  # Инициируем "движок" BackTrader
    store = Store(broker=default_broker)  # Хранилище брокера по умолчанию
    # store = Store(broker=brokers['<Ключ словаря brokers из Config.py>'])  # Хранилище выбранного брокера
    broker = store.getbroker()  # Брокер
    # noinspection PyArgumentList
    cerebro.setbroker(broker)  # Устанавливаем брокера
    cash = cerebro.broker.getcash()  # Свободные средства
    value = cerebro.broker.getvalue()  # Стоимость позиций
    logger.info(f'Свободные средства : {cash}')
    logger.info(f'Стоимость позиций  : {value}')
    logger.info(f'Стоимость портфеля : {cash + value}')
