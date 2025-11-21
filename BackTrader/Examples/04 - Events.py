import logging
from datetime import date, datetime, timedelta

from pytz import timezone
import backtrader as bt

# noinspection PyUnusedImports
from FinLabPy.Config import brokers, default_broker  # Все брокеры и брокер по умолчанию
from FinLabPy.Schedule.MOEX import Stocks  # Расписание торгов фондового рынка Московской Биржи
from FinLabPy.BackTrader import Store  # Хранилище BackTrader


class Events(bt.Strategy):
    """Получение и отображение событий:
    - Получение следующего исторического/нового бара
    - Изменение статуса заявки
    - Изменение статуса позиции
    - Изменение статуса приходящих баров (DELAYED / CONNECTED / DISCONNECTED / LIVE)
    - Получение нового бара

    Не используются события:
    - notify_timer - Срабатывание таймера
    - notify_fund - Свободные средства, стоимость позиций с фондированием
    - notify_store - Уведомления хранилища

    Для проверки работы этого скрипта можно вручную открывать/закрывать позиции. Все события будут выведены на экран и записаны в лог
    """
    logger = logging.getLogger('Events')  # Будем вести лог

    def __init__(self):
        """Инициализация торговой системы"""
        self.live = False  # Сначала будут приходить исторические данные, затем перейдем в режим реальной торговли

    def next(self):
        """Получение следующего исторического/нового бара"""
        self.logger.info(f'{bt.num2date(self.data.datetime[0]):%d.%m.%Y %H:%M:%S} O:{self.data.open[0]} H:{self.data.high[0]} L:{self.data.low[0]} C:{self.data.close[0]} V:{int(self.data.volume[0])}')

    def notify_cashvalue(self, cash, value):
        """Свободные средства, стоимость позиций"""
        if self.live:  # Это событие будет приходить после получения каждого бара. Будем выводить данные только в режиме реальной торговли
            self.logger.info(f'Свободные средства : {cash}')
            self.logger.info(f'Стоимость позиций  : {value}')
            self.logger.info(f'Стоимость портфеля : {cash + value}')

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

    def notify_trade(self, trade):
        """Изменение статуса позиции"""
        self.logger.info(trade)
        if trade.isclosed:  # Если позиция закрыта
            self.logger.info(f'Trade Profit, Gross={trade.pnl:.2f}, NET={trade.pnlcomm:.2f}')

    # noinspection PyShadowingNames
    def notify_data(self, data, status, *args, **kwargs):
        """Изменение статуса приходящих баров"""
        # noinspection PyProtectedMember
        data_status = data._getstatusname(status)  # Получаем статус
        self.logger.info(data_status)
        self.live = data_status == 'LIVE'  # Режим реальной торговли


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    dataname = 'TQBR.SBER'  # Тикер
    week_ago = date.today() - timedelta(days=7)  # Дата неделю назад без времени
    schedule = Stocks()  # Расписание биржи

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Events.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=timezone('Europe/Moscow')).timetuple()  # В логе время указываем по МСК

    # noinspection PyArgumentList
    cerebro = bt.Cerebro(stdstats=False, quicknotify=True)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна. События принимаем без задержек, не дожидаясь нового бара
    store = Store(broker=default_broker)  # Хранилище брокера по умолчанию
    # store = Store(broker=brokers['<Ключ словаря brokers из Config.py>'])  # Хранилище выбранного брокера
    broker = store.getbroker()  # Брокер
    # noinspection PyArgumentList
    cerebro.setbroker(broker)  # Устанавливаем брокера
    data = store.getdata(dataname=dataname, timeframe=bt.TimeFrame.Minutes, compression=1, fromdate=week_ago, live_bars=True)  # Исторические и новые минутные бары за последнюю неделю по подписке
    cerebro.adddata(data)  # Добавляем данные
    cerebro.addstrategy(Events)  # Добавляем торговую систему
    cerebro.run()  # Запуск торговой системы
