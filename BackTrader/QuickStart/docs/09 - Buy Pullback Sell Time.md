[Видео](https://vkvideo.ru/video_ext.php?oid=-215710796&id=456239071&hash=1ba5f113711f7e87&hd=3)

Выходы в торговых системах не менее важны чем входы. Одна из техник выхода - выход по времени.
Считается, что если был вход в нижней точке (дешевле), то через некоторое время он будет близок к верхней точке (дороже).

## Лог внутри торговой системы

Если в блоке запуска торговой системы не требуется работа с логом, то лог можно перенести в класс торговой системы.

```python
class BuyPullbackSellTime(bt.Strategy):
    """Покупка на откате, удержание заданное кол-во бар с комиссией"""
    logger = logging.getLogger('BuyPullbackSellTime')  # Будем вести лог
```

## Параметры торговой системы

Здесь же можно задать все параметры торговой системы. В специальной переменной **params** задаются параметры в виде кортежа кортежей **(<Название переменной>, <Значение по умолчанию>)**

У нас параметром будет количество свечей, которое будем ждать перед выходом.

```python
    params = (  # Параметры торговой системы
        ('exit_bars', 5),  # Кол-во бар удержания позиции
    )
```

## Инициализация торговой системы

В блоке инициализации торговой системы можно указать все необходимые переменные, которые нам потребуются.
Кроме цен закрытия, в нашей системе это заявка, чтобы знать, исполнилась она или нет, а также номер свечи входа в позицию, чтобы правильно рассчитать сввечу выхода по времени.

```python
    def __init__(self):
        """Инициализация торговой системы"""
        self.close = self.datas[0].close  # Цены закрытия
        self.order = None  # Заявка на вход/выход из позиции
        self.bar_executed = None  # Номер бара, на котором была исполнена заявка
```

## Исполнение заявки

Когда приходит новая свеча полезно проверить, исполнилась ли прошлая заявка. Если заявка есть, то она не исполнилась. Подождем ее исполнения или отмены.

```python
    def next(self):
        """Получение следующего бара"""
        if self.order:  # Если есть неисполненная заявка
            return  # то ждем ее исполнения, выходим, дальше не продолжаем
```

## Сигналы на покупку и продажу

Далее нужно разделить логику на вход в позицию, если позиции нет (**not self.position**), и на выход из позиции, если она есть.

Для выхода из позиции получаем текущее кол-во свечей через **len(self)**.
Если разница между ним и свечи, где был вход **self.bar_executed**, равна кол-ву свечей, которое удерживаем позицию **self.p.exit_bars**, то формируем сигнал на продажу.
Обратите внимание, что в BackTrader есть сокращения. Нотация **self.p.exit_bars** является сокращением от **self.params.exit_bars**.

```python
        if not self.position:  # Если позиции нет
            signal_buy = self.close[0] < self.close[-1] < self.close[-2] < self.close[-3]  # Сигнал на покупку, когда цена падает 3 сессии подряд
            if signal_buy:  # Если пришел сигнал на покупку
                self.order = self.buy()  # Заявка на покупку по рыночной цене
        else:  # Если позиция есть
            is_signal_sell = len(self) - self.bar_executed == self.p.exit_bars  # Прошло заданное кол-во бар с момента входа в позицию
            if is_signal_sell:  # Если пришел сигнал на продажу
                self.order = self.sell()  # Заявка на продажу по рыночной цене
```

## Обработка статуса заявок

В BackTrader можно отслеживать жизненный цикл заявки через **def notify_order(self, order):**
В нашей системе при исполнении заявки мы запоминаем номер бара, где это исполнение произошло через тот же самый **len(self)**
Если заявка еще не исполнилась, то выходим. Если отменена, то выдадим предупреждение в лог. После исполения или отмены заявки, сбрасываем ее в торговой системе.

```python
    def notify_order(self, order):
        """Изменение статуса заявки"""
        if order.status in (order.Submitted, order.Accepted):  # Если заявка не исполнена (отправлена брокеру или принята брокером)
            return  # то статус заявки не изменился, выходим, дальше не продолжаем
        if order.status == order.Completed:  # Если заявка исполнена
            self.bar_executed = len(self)  # Номер бара, на котором была исполнена заявка
        elif order.status in (order.Canceled, order.Margin, order.Rejected):  # Заявка отменена, нет средств, отклонена брокером (для реальной торговли)
            self.logger.info('Canceled/Margin/Rejected')
        self.order = None  # Этой заявки больше нет
```

## Обработка сделки

При частичном или полном исполнения заявки заключается сделка, которая меняет размер позиции.
Когда размер позиции становится нулевым, это означает, что позиция закрыта.
В BackTrader сделки обрабатываются через **def notify_trade(self, trade):** Свойство **trade.isclosed** определяет, закрыта позиция или нет.
В нашем примере если позиция не закрыта, то ничего не делаем. Если закрыта, то выводим ее прибыль и прибыль с вычетом комиссии.

```python
    def notify_trade(self, trade):
        """Изменение статуса позиции"""
        if not trade.isclosed:  # Если позиция не закрыта
            return  # то статус позиции не изменился, выходим, дальше не продолжаем
        self.logger.info(f'Trade Profit, Gross={trade.pnl:.2f}, NET={trade.pnlcomm:.2f}')
```

## Установка параметров торговой системы

Наша торговая система имеет параметр **exit_bars** и его значение по умолчанию 5.
Если при запуске нужно задать другое значение, то просто указываем его как параметр при добавлении торговой системы.

```python
cerebro.addstrategy(BuyPullbackSellTime, exit_bars=6)  # Привязываем торговую систему с параметрами
```

## Обработка событий

По умолчанию, все события, которые произошли до прихода новой свечи, ставятся в очередь, и приходят вместе с новой свечой.
Чтобы события приходили по мере появления, нужно при инициализации cerebro указать параметр **quicknotify=True**

```python
cerebro = bt.Cerebro(quicknotify=True)  # Инициируем "движок" BackTrader. События принимаем без задержек, не дожидаясь нового бара
```

## Размер комиссии

Размер комиссии в долях (процент / 100) мы можем задать параметром **commission** при конфигурировании размера комиссии **cerebro.broker.setcommission**

```python
cerebro.broker.setcommission(commission=0.001)  # Комиссия брокера 0.1% от суммы каждой исполненной заявки
```

## Полный код примера:

```python
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
        self.logger.info(f'Trade Profit, Gross={trade.pnl:.2f}, NET={trade.pnlcomm:.2f}')


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
```
