[Видео](https://vkvideo.ru/video_ext.php?oid=-215710796&id=456239067&hash=9007112289e2d806&hd=3)

После того, как получены все необходимые данные, можно приступать к написанию торговой системы.
Простейшая система будет перехватывать приход новой свечи, а также переход в режим, когда можно торговать.

## Создание класса торговой системы

Создадим класс торговой системы BackTrader. Он будет наследоваться (получит все функции) от класса **bt.Strategy**

```python
class LogBars(bt.Strategy):
    """Простейшая система без выставления заявок и совершения сделок. Получает бары и выводит их в лог"""
```

## Инициализация торговой системы

При создании класс инициализируется. Это хорошее место для указания лога.

```python
    def __init__(self):
        """Инициализация торговой системы"""
        self.logger = logging.getLogger('Data')  # Будем вести лог
```

## Получение новой свечи

Функция обработки события прихода новой свечи **next** из bt.Strategy будет вызываться каждый раз, когда придет новая свеча. Не важно, из истории, или только что сформированная, которую можно торговать.
Пока просто выведем данные пришедшей свечи в лог.

В BackTrader даты хранятся в виде чисел. Чтобы вывести дату, нужно ее получить через функцию **bt.num2date**. Выводить будем в формате российской даты и времени **%d.%m.%Y %H:%M:%S**
При приходе новой свечи нужно получить именно ее, т.е. последнюю свечу на данный момент. Индекс у такой свечи **[0]**. У предыдущей [-1], и т.д.

```python
    def next(self):
        """Получение следующего исторического/нового бара"""
        self.logger.info(f'{bt.num2date(self.data.datetime[0]):%d.%m.%Y %H:%M:%S} O:{self.data.open[0]} H:{self.data.high[0]} L:{self.data.low[0]} C:{self.data.close[0]} V:{int(self.data.volume[0])}')
```

## Изменение статуса свечей

BackTrader предназначен для реальной торговли, а не только для тестирования торговых систем. Поэтому, нам надо знать, когда заканчивается история и начинается режим реальных торгов.
Для изменения статуса приходящих свечей в bt.Strategy есть функция обработки события изменения статуса приходящих свечей **notify_data**. Выведем статус в лог. Перед началом реальной торговли придет статус **LIVE**.

```python
    def notify_data(self, data, status, *args, **kwargs):
        """Изменение статуса приходящих баров"""
        self.logger.info(data._getstatusname(status))
```

## Получение данных для реальной торговли

По умолчанию, в BackTrader приходят только исторические свечи. Чтобы приходили свечи для реальных торгов, нужно в функции **store.getdata** указать параметр **live_bars=True**

```python
week_ago = date.today() - timedelta(days=7)  # Дата неделю назад без времени
data = store.getdata(dataname=dataname, timeframe=bt.TimeFrame.Minutes, compression=1, fromdate=week_ago, live_bars=True)  # 3. Исторические и новые минутные бары за последнюю неделю по подписке
```

## Расписание торгов

BackTrader также может работать с расписанием биржи. Для этого нужно в функции **store.getdata** указать параметр **schedule**:

```python
from FinLabPy.Schedule.MOEX import Stocks  # Расписание торгов фондового рынка Московской Биржи

schedule = Stocks()  # Расписание биржи
data = store.getdata(dataname=dataname, timeframe=bt.TimeFrame.Minutes, compression=1, fromdate=week_ago, live_bars=True, schedule=schedule)  # 4. Исторические и новые минутные бары за последнюю неделю по расписанию
```

## Дожи 4-х цен

На небольших временнЫх интервалах бывает, что торги идут, но никто не покупает и не продает. По умолчанию, такие интервалы игнорируются, т.е. свеча за пустой интервал не приходит.
Если требуется, чтобы приходила свеча дожи 4-х цен, то нужно в функции **store.getdata** указать параметр **four_price_doji=True**

```python
data = store.getdata(dataname=dataname, timeframe=bt.TimeFrame.Minutes, compression=1, fromdate=week_ago, four_price_doji=True)  # 2. Исторические минутные бары за последнюю неделю с дожи 4-х цен
```

## Полный код примера:

```python
import logging
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo  # ВременнАя зона

import backtrader as bt

from FinLabPy.Config import brokers, default_broker  # Все брокеры и брокер по умолчанию
from FinLabPy.Schedule.MOEX import Stocks  # Расписание торгов фондового рынка Московской Биржи
from FinLabPy.BackTrader import Store  # Хранилище BackTrader


class LogBars(bt.Strategy):
    """Простейшая система без выставления заявок и совершения сделок. Получает бары и выводит их в лог"""

    def __init__(self):
        """Инициализация торговой системы"""
        self.logger = logging.getLogger('Data')  # Будем вести лог

    def next(self):
        """Получение следующего исторического/нового бара"""
        self.logger.info(f'{bt.num2date(self.data.datetime[0]):%d.%m.%Y %H:%M:%S} O:{self.data.open[0]} H:{self.data.high[0]} L:{self.data.low[0]} C:{self.data.close[0]} V:{int(self.data.volume[0])}')

    def notify_data(self, data, status, *args, **kwargs):
        """Изменение статуса приходящих баров"""
        self.logger.info(data._getstatusname(status))


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    dataname = 'TQBR.SBER'  # Тикер
    week_ago = date.today() - timedelta(days=7)  # Дата неделю назад без времени
    schedule = Stocks()  # Расписание биржи

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.INFO,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Data.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=ZoneInfo('Europe/Moscow')).timetuple()  # В логе время указываем по МСК

    cerebro = bt.Cerebro(stdstats=False)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна
    store = Store(broker=default_broker)  # Хранилище брокера по умолчанию
    # store = Store(broker=brokers['<Ключ словаря brokers из Config.py>'])  # Хранилище выбранного брокера
    broker = store.getbroker()  # Брокер
    cerebro.setbroker(broker)  # Устанавливаем брокера

    data = store.getdata(dataname=dataname, fromdate=datetime(2024, 1, 1), todate=datetime(2026, 1, 1))  # 1. Исторические дневные бары за период
    # data = store.getdata(dataname=dataname, timeframe=bt.TimeFrame.Minutes, compression=1, fromdate=week_ago, four_price_doji=True)  # 2. Исторические минутные бары за последнюю неделю с дожи 4-х цен
    # data = store.getdata(dataname=dataname, timeframe=bt.TimeFrame.Minutes, compression=1, fromdate=week_ago, live_bars=True)  # 3. Исторические и новые минутные бары за последнюю неделю по подписке
    # data = store.getdata(dataname=dataname, timeframe=bt.TimeFrame.Minutes, compression=1, fromdate=week_ago, live_bars=True, schedule=schedule)  # 4. Исторические и новые минутные бары за последнюю неделю по расписанию

    cerebro.adddata(data)  # Добавляем данные
    cerebro.addstrategy(LogBars)  # Добавляем торговую систему
    cerebro.run()  # Запуск торговой системы
    cerebro.plot()  # Рисуем график
```
