import logging
from datetime import date, datetime, timedelta

from pytz import timezone
from backtrader.backtrader import Strategy, num2date, Cerebro, TimeFrame

from FinLabPy.Config import brokers, default_broker  # Все брокеры и брокер по умолчанию
from FinLabPy.Schedule.MOEX import Stocks  # Расписание торгов фондового рынка Московской Биржи
from FinLabPy.BackTrader import Store  # Хранилище BackTrader


class LogBars(Strategy):
    """Торговая система, которая получает бары и выводит их в лог"""
    logger = logging.getLogger('Data')  # Будем вести лог

    def next(self):
        """Получение следующего исторического/нового бара"""
        self.logger.info(f'{num2date(self.data.datetime[0]):%d.%m.%Y %H:%M:%S} O:{self.data.open[0]} H:{self.data.high[0]} L:{self.data.low[0]} C:{self.data.close[0]} V:{int(self.data.volume[0])}')

    def notify_data(self, data, status, *args, **kwargs):
        """Изменение статуса приходящих баров"""
        self.logger.info(data._getstatusname(status))  # Получаем статус только при live_bars=True

    # def notify_cashvalue(self, cash, value):
    #     """Текущие свободные средства и стоимость позиций"""
    #     self.logger.info(f'Свободные средства : {cash}')
    #     self.logger.info(f'Стоимость позиций  : {value}')
    #     self.logger.info(f'Стоимость портфеля : {cash + value}')


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    dataname = 'TQBR.SBER'  # Тикер
    week_ago = date.today() - timedelta(days=7)  # Дата неделю назад без времени
    schedule = Stocks()  # Расписание биржи

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Data.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=timezone('Europe/Moscow')).timetuple()  # В логе время указываем по МСК

    # noinspection PyArgumentList
    cerebro = Cerebro(stdstats=False, quicknotify=True)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна. События принимаем без задержек, не дожидаясь нового бара
    store = Store(broker=default_broker)  # Хранилище брокера по умолчанию
    # store = Store(broker=brokers['Ф'])  # Хранилище выбранного брокера
    broker = store.getbroker()  # Брокер
    # noinspection PyArgumentList
    cerebro.setbroker(broker)  # Устанавливаем брокера

    # data = store.getdata(dataname=dataname)  # 1. Все исторические дневные бары
    # data = store.getdata(dataname=dataname, timeframe=TimeFrame.Minutes, compression=1, fromdate=week_ago, four_price_doji=True)  # 2. Исторические минутные бары за последнюю неделю с дожи 4-х цен
    data = store.getdata(dataname=dataname, timeframe=TimeFrame.Minutes, compression=1, fromdate=week_ago, live_bars=True)  # 3. Исторические и новые минутные бары за последнюю неделю по подписке
    # data = store.getdata(dataname=dataname, timeframe=TimeFrame.Minutes, compression=1, fromdate=week_ago, live_bars=True, schedule=schedule)  # 4. Исторические и новые минутные бары за последнюю неделю по расписанию

    cerebro.adddata(data)  # Добавляем данные
    cerebro.addstrategy(LogBars)  # Добавляем торговую систему
    cerebro.run()  # Запуск торговой системы
