import logging
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import backtrader as bt

from FinLabPy.Config import brokers, default_broker  # Все брокеры и брокер по умолчанию
from FinLabPy.BackTrader import Store, PlotLC  # Хранилище BackTrader


class PriceSMACross(bt.Strategy):
    """Пересечение цены и SMA"""
    logger = logging.getLogger('PriceSMACross')  # Будем вести лог
    params = (  # Параметры торговой системы
        ('sma_period', 26),  # Период SMA
    )

    def __init__(self):
        """Инициализация торговой системы"""
        self.close = self.datas[0].close  # Цены закрытия
        self.order = None  # Заявка
        self.sma = bt.indicators.MovingAverageSimple(self.close, period=self.p.sma_period)  # SMA по ценам закрытия
        self.broker_start_value = self.broker.getvalue()  # Стартовый капитал

    def next(self):
        """Получение следующего бара"""
        self.logger.info(f'{bt.num2date(self.data.datetime[0]):%d.%m.%Y %H:%M:%S} O:{self.data.open[0]} H:{self.data.high[0]} L:{self.data.low[0]} C:{self.close[0]} V:{int(self.data.volume[0])}')
        if self.order:  # Если есть неисполненная заявка
            return  # то ждем ее исполнения, выходим, дальше не продолжаем
        if not self.position:  # Если позиции нет
            signal_buy = self.close[0] > self.sma[0]  # Цена закрылась выше скользящцей
            if signal_buy:  # Если пришла заявка на покупку
                self.logger.info('Buy Market @ Price crossed up SMA')
                self.order = self.buy()  # Заявка на покупку по рыночной цене
        else:  # Если позиция есть
            signal_sell = self.close[0] < self.sma[0]  # Цена закрылась ниже скользящей
            if signal_sell:  # Если пришла заявка на продажу
                self.logger.info('Sell Market @ Price crossed down SMA')
                self.order = self.sell()  # Заявка на продажу по рыночной цене

    def notify_order(self, order):
        """Изменение статуса заявки"""
        if order.status in [order.Submitted, order.Accepted]:  # Если заявка не исполнена (отправлена брокеру или принята брокером)
            return  # то статус заявки не изменился, выходим, дальше не продолжаем
        if order.status in [order.Completed]:  # Если заявка исполнена
            if order.isbuy():  # Заявка на покупку
                self.logger.info(f'Bought @{order.executed.price:.2f}, Cost={order.executed.value:.2f}, Comm={order.executed.comm:.2f}')
            elif order.issell():  # Заявка на продажу
                self.logger.info(f'Sold @{order.executed.price:.2f}, Cost={order.executed.value:.2f}, Comm={order.executed.comm:.2f}')
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:  # Заявка отменена, нет средств, отклонена брокером
            self.logger.info('Canceled/Margin/Rejected')
        self.order = None  # Этой заявки больше нет

    def notify_trade(self, trade):
        """Изменение статуса позиции"""
        if not trade.isclosed:  # Если позиция не закрыта
            return  # то статус позиции не изменился, выходим, дальше не продолжаем
        self.logger.info(f'Trade Profit, Gross={trade.pnl:.2f}, NET={trade.pnlcomm:.2f}')

    def stop(self):
        """Окончание запуска торговой системы"""
        self.logger.info(f'SMA({self.p.sma_period}), Profit = {(self.broker.getvalue() - self.broker_start_value):.2f}')


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    dataname = 'TQBR.SBER'  # Тикер
    five_years_ago = date.today() - timedelta(days=365 * 5)  # 5 лет назад

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.INFO,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('PriceSMACross.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=ZoneInfo('Europe/Moscow')).timetuple()  # В логе время указываем по МСК

    cerebro = bt.Cerebro()  # Инициируем "движок" BackTrader
    # cerebro = bt.Cerebro(stdstats=False)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна
    store = Store(broker=default_broker)  # Хранилище брокера по умолчанию
    # store = Store(broker=brokers['<Ключ словаря brokers из Config.py>'])  # Хранилище выбранного брокера
    # broker = store.getbroker()  # Брокер
    # cerebro.setbroker(broker)  # Устанавливаем брокера
    cerebro.broker.setcash(1_000_000)  # Стартовый капитал для "бумажной" торговли
    cerebro.addsizer(bt.sizers.FixedSize, stake=1_000)  # Кол-во акций для покупки/продажи
    cerebro.broker.setcommission(commission=0.001)  # Комиссия брокера 0.1% от суммы каждой исполненной заявки
    data = store.getdata(dataname=dataname, fromdate=five_years_ago)
    cerebro.adddata(data)  # Привязываем исторические данные
    cerebro.addstrategy(PriceSMACross, sma_period=100)  # Привязываем торговую систему
    cerebro.run()  # Запуск торговой системы
    # cerebro.plot(volume=False)  # Рисуем график
    cerebro.plot(plotter=PlotLC.Plot(volume=False))  # Рисуем график Lightweight Charts
