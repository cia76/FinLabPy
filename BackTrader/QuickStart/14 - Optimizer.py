import logging
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import backtrader as bt


logger = logging.getLogger('Optimizer')  # Будем вести лог


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
    logging.getLogger('PriceSMACross').setLevel(logging.CRITICAL + 1)  # Логи ТС не пропускаем в лог

    cerebro = bt.Cerebro()  # Инициируем "движок" BackTrader
    cerebro.broker.setcash(1_000_000)  # Стартовый капитал для "бумажной" торговли
    cerebro.addsizer(bt.sizers.FixedSize, stake=1_000)  # Кол-во акций для покупки/продажи
    cerebro.broker.setcommission(commission=0.001)  # Комиссия брокера 0.1% от суммы каждой исполненной заявки
    data = bt.feeds.GenericCSVData(  # Можно принимать любые CSV файлы с разделителем десятичных знаков в виде точки https://backtrader.com/docu/datafeed-develop-csv/
        dataname='TQBR.SBER_D1.txt',  # Файл для импорта
        separator='\t',  # Колонки разделены табуляцией
        dtformat='%d.%m.%Y %H:%M',  # Формат даты/времени DD.MM.YYYY HH:MI
        openinterest=-1,  # Открытого интереса в файле нет
        fromdate=five_years_ago)  # Начальная дата приема исторических данных (Входит)
    cerebro.adddata(data)  # Привязываем исторические данные

    cerebro.optstrategy(PriceSMACross, sma_period=range(8, 100+1, 2))  # Торговая система на оптимизацию с параметрами. Первое значение входит, последнее - нет
    logger.info('Прибыль/убытки по закрытым сделкам:')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='TradeAnalyzer')  # Привязываем анализатор закрытых сделок
    results = cerebro.run()  # Запуск торговой системы. Получение статистики. Можно указать кол-во ядер процессора, которые будут загружены. Например, maxcpus=2
    stats = {}  # Статистику будем вести в виде словаря
    for result in results:  # Пробегаемся по статистике по всем параметрам
        p = result[0].p.sma_period  # Параметр
        analysis = result[0].analyzers.TradeAnalyzer.get_analysis()  # Получаем данные анализатора закрытых сделок
        v = analysis['pnl']['net']['total']  # Прибыль/убытки по закрытым сделкам
        stats[p] = v  # Заносим статистику в словарь
        logger.info(f'SMA({p}), {v:.2f}')
    bestStat = max(stats.items(), key=lambda x: x[1])  # Для получения лучшего/худшего значений в словаре переводим их
    worstStat = min(stats.items(), key=lambda x: x[1])  # в список кортежей, сравниваем 2-ой элемент (значения)
    avgStat = sum(stats.values()) / len(stats.values())  # Среднее значение как сумма значений разделенная на их кол-во
    logger.info(f'Лучшее значение: SMA({bestStat[0]}), {bestStat[1]:.2f}')
    logger.info(f'Худшее значение: SMA({worstStat[0]}), {worstStat[1]:.2f}')
    logger.info(f'Среднее значение: {avgStat:.2f}')
