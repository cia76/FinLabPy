import logging
from datetime import date, datetime
from zoneinfo import ZoneInfo  # ВременнАя зона

import backtrader as bt

from FinLabPy.Config import brokers, default_broker  # Все брокеры и брокер по умолчанию
from FinLabPy.BackTrader import Store  # Хранилище BackTrader
from MOEXPy import MOEXPy  # Работа с Algopack API Московской Биржи


class LogAlgopack(bt.Strategy):
    """Простейшая система без выставления заявок и совершения сделок. Получает бары и выводит их в лог"""

    def __init__(self):
        """Инициализация торговой системы"""
        self.logger = logging.getLogger('AlgopackData')  # Будем вести лог
        self.mp_provider = MOEXPy()  # Работа с Algopack API Московской Биржи
        self.ticker = self.data._dataname.split('.')[1]  # Тикер Московской Биржи

    def next(self):
        """Получение следующего исторического/нового бара"""
        dt = bt.num2date(self.data.datetime[0])  # Дата и время пришедшего бара
        self.logger.info(f'{dt:%d.%m.%Y %H:%M:%S} O:{self.data.open[0]} H:{self.data.high[0]} L:{self.data.low[0]} C:{self.data.close[0]} V:{int(self.data.volume[0])}')
        futoi = self.mp_provider.get_futoi(self.ticker, dt, dt)  # Получаем открытый интерес фьючерса за пришедший бар
        futoi_data = [dict(zip(futoi['futoi']['columns'], row)) for row in futoi['futoi']['data']]  # Переводим в список словаря
        [self.logger.info(futoi) for futoi in futoi_data]  # Разбиваем на строки для FIZ/YUR


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    dataname = 'SPBFUT.IMOEXF'  # Тикер

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.INFO,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('AlgopackData.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=ZoneInfo('Europe/Moscow')).timetuple()  # В логе время указываем по МСК

    cerebro = bt.Cerebro(stdstats=False)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна
    store = Store(broker=default_broker)  # Хранилище брокера по умолчанию
    # store = Store(broker=brokers['<Ключ словаря brokers из Config.py>'])  # Хранилище выбранного брокера
    broker = store.getbroker()  # Брокер
    cerebro.setbroker(broker)  # Устанавливаем брокера

    data = store.getdata(dataname=dataname, timeframe=bt.TimeFrame.Minutes, compression=5, fromdate=date.today(), four_price_doji=True)  # 5-и минутки за сегодня
    cerebro.adddata(data)  # Добавляем данные
    cerebro.addstrategy(LogAlgopack)  # Добавляем торговую систему
    cerebro.run()  # Запуск торговой системы
