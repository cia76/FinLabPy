import logging
from datetime import datetime
from zoneinfo import ZoneInfo  # ВременнАя зона

import backtrader as bt


logger = logging.getLogger('Logging')  # Будем вести лог


def get_cash_value():
    cash = cerebro.broker.getcash()  # Свободные средства
    value = cerebro.broker.getvalue()  # Стоимость позиций
    logger.info(f'Свободные средства : {cash}')
    logger.info(f'Стоимость позиций  : {value - cash}')
    logger.info(f'Стоимость портфеля : {value}')


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Logging.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=ZoneInfo('Europe/Moscow')).timetuple()  # В логе время указываем по МСК

    cerebro = bt.Cerebro()  # Инициируем "движок" BackTrader

    logger.debug('Стартовый капитал')
    get_cash_value()  # Отображаем статистику портфеля до запуска ТС
    cerebro.run()  # Запуск ТС. Пока ее у нас нет
    logger.debug('Конечный капитал')
    get_cash_value()  # Отображаем статистику портфеля после запуска ТС
