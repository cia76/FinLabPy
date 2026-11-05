[Видео](https://vkvideo.ru/video_ext.php?oid=-215710796&id=456239072&hash=870d2517c3535d89&hd=3)

Не всегда текущая рыночная цена является достаточно низкой для входа в длинную позицию. В таком случае используется техника выставления лимитных заявок.

Построим торговую систему, которая будет выставлять лимитную заявку на покупку на заданное кол-во процентов ниже текущей цены.
При приходе следующей свечи, если заявка не исполнилась, то она снимается. Если заявка исполнилась, то ее позиция закрывается.

## Параметр торговой системы

Самый простой способ - через параметр торговой системы указать на сколько от текущей цены должна упасть цена, чтобы произошел вход в позицию.

```python
class LimitCancel(bt.Strategy):
    params = (  # Параметры торговой системы
        ('limit_pct', 1),  # Заявка на покупку на n% ниже цены закрытия
    )
```

## Реальная торговля

Чтобы вести реальную торговлю, в блоке инициализации торговой системы введем переменную **self.live**, в которой будем хранить признак реальной торговли.
Изначально считаем, что получаем исторические данные, и реальной торговли нет.

```python
    def __init__(self):
        self.live = False  # Сначала будут приходить исторические данные, затем перейдем в режим реальной торговли
        self.order = None  # Заявка на вход/выход из позиции
```

## Работа только с реальными данными

При каждой новой свече проверяем режим реальной торговли. До тех пор, пока не будет режима реальной торговли, дальнейшие действия в торговой системе проводить не будем.

```python
    def next(self):
        if not self.live:  # Если не в режиме реальной торговли
            return  # то выходим, дальше не продолжаем
```

## Проверка статуса заявки

Заявки не должны уходить в никуда. После отправки заявки брокеру необходимо получить от брокера подтверждение. Пока не получим, ничего с заявкой не делаем, ждем.

```python
        if self.order and self.order.status == bt.Order.Submitted:  # Если заявка отправлена брокеру, но им не принята
            return  # то ждем исполнения, выходим, дальше не продолжаем
```

## Закрытие позиции

Если позиция есть, то ее нужно полностью закрыть через **self.close()**.
Если заявка исполнена не полностью **self.order.alive()** (активна), то ее тоже нужно закрыть через **self.cancel(self.order)**.

```python
        if self.position:  # Если позиция есть
            if self.order.alive():  # Если заявка исполнена не полностью
                self.cancel(self.order)  # то снимаем ее
            self.order = self.close()  # Заявка на закрытие позиции по рыночной цене
```

## Снятие заявки

Аналогично, снимаем заявку, если она не исполнена.

```python
        else:  # Если позиции нет
            if self.order and self.order.alive():  # Если заявка принята брокером и не исполнена
                self.cancel(self.order)  # то снимаем ее
```

## Выставление лимитной заявки

Рассчитываем цену входа по лимитной заявке через параметр **self.p.limit_pct**
Выставляем заявку через **self.buy** Параметром **exectype=bt.Order.Limit** задаем лимитную заявку. Параметром **price=limit_price** - ее цену исполнения.

```python
            limit_price = self.data.close[0] * (1 - self.p.limit_pct / 100)  # На n% ниже цены закрытия
            self.order = self.buy(exectype=bt.Order.Limit, price=limit_price)  # Лимитная заявка на покупку
```

## Все статусы заявки

Можно при изменении статуса заявки **def notify_order(self, order):** выводить это событие в лог.

```python
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
```

## Переход в режим реальной торговли

При изменении режима торговли **notify_data** получаем статус. Проверяем его на режим **LIVE**. Выставляем статус режима реальной торговли.

```python
    def notify_data(self, data, status, *args, **kwargs):
        data_status = data._getstatusname(status)  # Получаем статус (только при live_bars=True)
        self.live = data_status == 'LIVE'  # Режим реальной торговли
```

## Полный код примера:

```python
import logging
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import backtrader as bt

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
        self.logger.info(f'{self.data._name} ({bt.TimeFrame.Names[self.data.p.timeframe]} {self.data.p.compression}) {bt.num2date(self.data.datetime[0]):%d.%m.%Y %H:%M:%S} O:{self.data.open[0]} H:{self.data.high[0]} L:{self.data.low[0]} C:{self.data.close[0]} V:{int(self.data.volume[0])}')
        if not self.live:  # Если не в режиме реальной торговли
            return  # то выходим, дальше не продолжаем
        if self.order and self.order.status == bt.Order.Submitted:  # Если заявка отправлена брокеру, но им не принята
            return  # то ждем исполнения, выходим, дальше не продолжаем
        if self.position:  # Если позиция есть
            if self.order.alive():  # Если заявка исполнена не полностью
                self.cancel(self.order)  # то снимаем ее
            self.order = self.close()  # Заявка на закрытие позиции по рыночной цене
        else:  # Если позиции нет
            if self.order and self.order.alive():  # Если заявка принята брокером и не исполнена
                self.cancel(self.order)  # то снимаем ее
            limit_price = self.data.close[0] * (1 - self.p.limit_pct / 100)  # На n% ниже цены закрытия
            self.order = self.buy(exectype=bt.Order.Limit, price=limit_price)  # Лимитная заявка на покупку
            self.logger.info(f'Order #{self.order.ref} - {"Buy" if self.order.isbuy else "Sell"} {self.order.data._name} {self.order.size} @ {self.order.price} has been created')

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
            self.logger.info(f'Position has been closed. Gross Profit = {trade.pnl:.2f}, NET Profit = {trade.pnlcomm:.2f}')

    def notify_data(self, data, status, *args, **kwargs):
        """Изменение статуса приходящих баров"""
        data_status = data._getstatusname(status)  # Получаем статус (только при live_bars=True)
        self.live = data_status == 'LIVE'  # Режим реальной торговли
        self.logger.info(data_status)


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    dataname = 'TQBR.SBER'  # Тикер
    week_ago = date.today() - timedelta(days=7)  # Дата неделю назад без времени

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('LimitCancel.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=ZoneInfo('Europe/Moscow')).timetuple()  # В логе время указываем по МСК

    cerebro = bt.Cerebro(stdstats=False, quicknotify=True)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна. События принимаем без задержек, не дожидаясь нового бара
    store = Store(broker=default_broker)  # Хранилище брокера по умолчанию
    # store = Store(broker=brokers['<Ключ словаря brokers из Config.py>'])  # Хранилище выбранного брокера
    broker = store.getbroker()  # Брокер
    cerebro.setbroker(broker)  # Устанавливаем брокера
    data = store.getdata(dataname=dataname, timeframe=bt.TimeFrame.Minutes, compression=1, fromdate=week_ago, live_bars=True)  # Исторические и новые минутные бары за последнюю неделю по подписке
    cerebro.adddata(data)  # Добавляем данные
    cerebro.addstrategy(LimitCancel)  # Добавляем торговую систему
    cerebro.addsizer(bt.sizers.FixedSize, stake=1)  # Кол-во акций для покупки/продажи
    cerebro.broker.setcommission(commission=0.001)  # Комиссия брокера 0.1% от суммы каждой исполненной заявки
    cerebro.run()  # Запуск торговой системы
```
