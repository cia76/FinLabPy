import logging
from datetime import datetime

from pytz import timezone

from FinLabPy.Config import brokers, default_broker  # Все брокеры и брокер по умолчанию
from FinLabPy.Core import bars_to_df  # Перевод бар в pandas DataFrame


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    dataname = 'TQBR.SBER'
    time_frame = 'D1'

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('BarsHistory.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=timezone('Europe/Moscow')).timetuple()  # В логе время указываем по МСК
    logging.getLogger('urllib3').setLevel(logging.CRITICAL + 1)  # Пропускаем события запросов

    broker = default_broker  # Брокер по умолчанию
    # broker = brokers['Т']  # Брокер по ключу из Config.py словаря brokers
    symbol = broker.get_symbol_by_dataname(dataname)  # Тикер по названию
    bars = broker.get_history(symbol, time_frame)  # Получаем всю историю тикера
    print(bars[0])  # Первый бар
    print(bars[-1])  # Последний бар
    print(bars_to_df(bars))  # Все бары в pandas DataFrame
    broker.close()  # Закрываем брокера
