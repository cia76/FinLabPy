import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import backtrader as bt


class BuyPullbackSellTime(bt.Strategy):
    """Покупка на откате, удержание заданное кол-во бар с комиссией"""
    logger = logging.getLogger('BuyPullbackSellTime')  # Будем вести лог
    params = (  # Параметры торговой системы
        ('exit_bars', 5),  # Кол-во бар удержания позиции
    )

    def __init__(self):
        """Инициализация торговой системы"""
        self.close = self.datas[0].close  # Цены закрытия
        self.order = None  # Заявка на вход/выход из позиции
        self.bar_executed = None  # Номер бара, на котором была исполнена заявка

    def next(self):
        """Получение следующего бара"""
        self.logger.info(f'{bt.num2date(self.data.datetime[0]):%d.%m.%Y %H:%M:%S} O:{self.data.open[0]} H:{self.data.high[0]} L:{self.data.low[0]} C:{self.close[0]} V:{int(self.data.volume[0])}')
        if self.order:  # Если есть неисполненная заявка
            return  # то ждем ее исполнения, выходим, дальше не продолжаем
        if not self.position:  # Если позиции нет
            signal_buy = self.close[0] < self.close[-1] < self.close[-2] < self.close[-3]  # Сигнал на покупку, когда цена падает 3 сессии подряд
            if signal_buy:  # Если пришел сигнал на покупку
                self.logger.info('Buy Market @ Pullback')
                self.order = self.buy()  # Заявка на покупку по рыночной цене
        else:  # Если позиция есть
            is_signal_sell = len(self) - self.bar_executed == self.p.exit_bars  # Прошло заданное кол-во бар с момента входа в позицию
            if is_signal_sell:  # Если пришел сигнал на продажу
                self.logger.info('Sell Market @ Time')
                self.order = self.sell()  # Заявка на продажу по рыночной цене

    def notify_order(self, order):
        """Изменение статуса заявки"""
        if order.status in (order.Submitted, order.Accepted):  # Если заявка не исполнена (отправлена брокеру или принята брокером)
            return  # то статус заявки не изменился, выходим, дальше не продолжаем
        if order.status == order.Completed:  # Если заявка исполнена
            if order.isbuy():  # Заявка на покупку
                self.logger.info(f'Bought @ {order.executed.price:.2f}')
            elif order.issell():  # Заявка на продажу
                self.logger.info(f'Sold @ {order.executed.price:.2f}')
            self.bar_executed = len(self)  # Номер бара, на котором была исполнена заявка
        elif order.status in (order.Canceled, order.Margin, order.Rejected):  # Заявка отменена, нет средств, отклонена брокером (для реальной торговли)
            self.logger.info('Canceled/Margin/Rejected')
        self.order = None  # Этой заявки больше нет

    def notify_trade(self, trade):
        """Изменение статуса позиции"""
        if not trade.isclosed:  # Если позиция не закрыта
            return  # то статус позиции не изменился, выходим, дальше не продолжаем
        self.log(f'Trade Profit, Gross={trade.pnl:.2f}, NET={trade.pnlcomm:.2f}')


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.INFO,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('BuyPullbackSellTime.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=ZoneInfo('Europe/Moscow')).timetuple()  # В логе время указываем по МСК

    cerebro = bt.Cerebro(quicknotify=True)  # Инициируем "движок" BackTrader. События принимаем без задержек, не дожидаясь нового бара
    cerebro.addstrategy(BuyPullbackSellTime, exit_bars=6)  # Привязываем торговую систему с параметрами
    data = bt.feeds.GenericCSVData(
        # Можно принимать любые CSV файлы с разделителем десятичных знаков в виде точки https://backtrader.com/docu/datafeed-develop-csv/
        dataname='TQBR.SBER_D1.txt',  # Файл для импорта
        separator='\t',  # Колонки разделены табуляцией
        dtformat='%d.%m.%Y %H:%M',  # Формат даты/времени DD.MM.YYYY HH:MI
        openinterest=-1,  # Открытого интереса в файле нет
        fromdate=datetime(2024, 1, 1),  # Начальная дата приема исторических данных (Входит)
        todate=datetime(2026, 1, 1))  # Конечная дата приема исторических данных (Не входит)
    cerebro.adddata(data)  # Привязываем исторические данные
    cerebro.broker.setcash(1_000_000)  # Стартовый капитал для "бумажной" торговли
    cerebro.addsizer(bt.sizers.FixedSize, stake=1_000)  # Кол-во акций для покупки/продажи
    cerebro.broker.setcommission(commission=0.001)  # Комиссия брокера 0.1% от суммы каждой исполненной заявки
    cerebro.run()  # Запуск торговой системы
    cerebro.plot(volume=False)  # Рисуем график
