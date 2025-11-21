import logging
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import backtrader as bt

# noinspection PyUnusedImports
from FinLabPy.Config import brokers, default_broker  # Все брокеры и брокер по умолчанию
from FinLabPy.BackTrader import Store  # Хранилище BackTrader


class LimitCancel(bt.Strategy):
    """
    Выставляем заявку на покупку на n% ниже цены закрытия
    Если за 1 бар заявка не срабатывает, то закрываем ее
    Если срабатывает, то закрываем позицию. Неважно, с прибылью или убытком
    """
    logger = logging.getLogger('LimitCancel')  # Будем вести лог
    params = (  # Параметры торговой системы
        ('limit_pct', 1),  # Заявка на покупку на n% ниже цены закрытия
    )

    def __init__(self):
        """Инициализация торговой системы"""
        self.live = False  # Сначала будут приходить исторические данные, затем перейдем в режим реальной торговли
        self.order = None  # Заявка на вход/выход из позиции

    def next(self):
        """Получение следующего исторического/нового бара"""
        # noinspection PyProtectedMember
        self.logger.info(f'{self.data._name} ({bt.TimeFrame.Names[self.data.p.timeframe]} {self.data.p.compression}) {bt.num2date(self.data.datetime[0]):%d.%m.%Y %H:%M:%S} O:{self.data.open[0]} H:{self.data.high[0]} L:{self.data.low[0]} C:{self.data.close[0]} V:{int(self.data.volume[0])}')
        if not self.live:  # Если не в режиме реальной торговли
            return  # то выходим, дальше не продолжаем
        if self.order and self.order.status == bt.Order.Submitted:  # Если заявка не исполнена (отправлена брокеру)
            return  # то ждем исполнения, выходим, дальше не продолжаем
        if self.position:  # Если позиция есть
            self.order = self.close()  # Заявка на закрытие позиции по рыночной цене
        else:  # Если позиции нет
            if self.order and self.order.status == bt.Order.Accepted:  # Если заявка не исполнена (принята брокером)
                self.cancel(self.order)  # то снимаем ее
            limit_price = self.data.close[0] * (1 - self.p.limit_pct / 100)  # На n% ниже цены закрытия
            self.order = self.buy(exectype=bt.Order.Limit, price=limit_price)  # Лимитная заявка на покупку
            # noinspection PyProtectedMember
            self.logger.info(f'Заявка {self.order.ref} - {"Покупка" if self.order.isbuy else "Продажа"} {self.order.data._name} {self.order.size} @ {self.order.price} cоздана и отправлена на биржу {self.order.data.exchange}')

    # noinspection PyShadowingNames
    def notify_data(self, data, status, *args, **kwargs):
        """Изменение статуса приходящих баров"""
        # noinspection PyProtectedMember
        data_status = data._getstatusname(status)  # Получаем статус (только при live_bars=True)
        self.live = data_status == 'LIVE'  # Режим реальной торговли
        self.logger.info(data_status)

    def notify_order(self, order):
        """Изменение статуса заявки"""
        self.logger.info(order)
        if order.status in (bt.Order.Created, bt.Order.Submitted, bt.Order.Accepted):  # Если заявка создана, отправлена брокеру, принята брокером (не исполнена)
            self.logger.info(f'Alive Status: {order.getstatusname()}')
        elif order.status in (bt.Order.Canceled, bt.Order.Margin, bt.Order.Rejected, bt.Order.Expired):  # Если заявка отменена, нет средств, заявка отклонена брокером, снята по времени (снята)
            self.logger.info(f'Cancel Status: {order.getstatusname()}')
        elif order.status == bt.Order.Partial:  # Если заявка частично исполнена
            self.logger.info(f'Part Status: {order.getstatusname()}')
        elif order.status == bt.Order.Completed:  # Если заявка полностью исполнена
            if order.isbuy():  # Заявка на покупку
                self.logger.info(f'Bought @{order.executed.price}, Cost={order.executed.value:.2f}, Comm={order.executed.comm:.2f}')
            elif order.issell():  # Заявка на продажу
                self.logger.info(f'Sold @{order.executed.price}, Cost={order.executed.value:.2f}, Comm={order.executed.comm:.2f}')
            self.order = None  # Сбрасываем заявку на вход в позицию

    def notify_trade(self, trade):
        """Изменение статуса позиции"""
        if trade.isclosed:  # Если позиция закрыта
            self.logger.info(f'Позиция закрыта. Прибыль (Gross) = {trade.pnl:.2f}, С учетом комиссий (NET) = {trade.pnlcomm:.2f}')


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    dataname = 'TQBR.SBER'  # Тикер
    week_ago = date.today() - timedelta(days=7)  # Дата неделю назад без времени

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('LimitCancel.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=ZoneInfo('Europe/Moscow')).timetuple()  # В логе время указываем по МСК

    # noinspection PyArgumentList
    cerebro = bt.Cerebro(stdstats=False, quicknotify=True)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна. События принимаем без задержек, не дожидаясь нового бара
    store = Store(broker=default_broker)  # Хранилище брокера по умолчанию
    # store = Store(broker=brokers['<Ключ словаря brokers из Config.py>'])  # Хранилище выбранного брокера
    broker = store.getbroker()  # Брокер
    # noinspection PyArgumentList
    cerebro.setbroker(broker)  # Устанавливаем брокера
    data = store.getdata(dataname=dataname, timeframe=bt.TimeFrame.Minutes, compression=1, fromdate=week_ago, live_bars=True)  # Исторические и новые минутные бары за последнюю неделю по подписке
    cerebro.adddata(data)  # Добавляем данные
    cerebro.addstrategy(LimitCancel)  # Добавляем торговую систему
    cerebro.run()  # Запуск торговой системы
