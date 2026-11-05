[Видео](https://vkvideo.ru/video_ext.php?oid=-215710796&id=456239068&hash=1ae8629dc3fa3773&hd=3)

Почему бы историю не брать напрямую с Московской Биржи?

## Импорт провайдера данных МосБиржи

Используя настроенную библиотеку [MOEXPy](https://github.com/cia76/MOEXPy) с [FinLabPy](https://github.com/cia76/FinLabPy) сделать это легко. Просто импортируем провайдер данных МосБиржи.

```python
from FinLabPy.Brokers.MOEX import MOEX  # Провайдер данных Московской Биржи (без торговых функций)
```

## Инициализация провайдера данных МосБиржи

В прошлых примерах при инициализации хранилища мы указывали брокера, с которым будем работать. С помощью параметра **data** можно указывать брокера или биржу, откуда будет браться история.

```python
store = Store(broker=default_broker, data=MOEX())  # Хранилище. Брокер по умолчанию. Данные - Московская Биржа
```

Такой подход позволяет брать данные с самого быстрого и надежного брокера, а торговать у другого.

Остальная часть кода инициализации брокера и данных остается без изменений.

```python
broker = store.getbroker()  # Брокер
cerebro.setbroker(broker)  # Устанавливаем брокера
data = store.getdata(dataname=dataname, fromdate=datetime(2024, 1, 1), todate=datetime(2026, 1, 1))  # 1. Исторические дневные бары за период
cerebro.adddata(data)  # Добавляем данные
```

## Полный код примера:

```python
import logging
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo  # ВременнАя зона

import backtrader as bt

from FinLabPy.Config import default_broker  # Брокер по умолчанию
from FinLabPy.Schedule.MOEX import Stocks  # Расписание торгов фондового рынка Московской Биржи
from FinLabPy.BackTrader import Store  # Хранилище BackTrader
from FinLabPy.Brokers.MOEX import MOEX  # Провайдер данных Московской Биржи (без торговых функций)


def get_cash_value():
    cash = cerebro.broker.getcash()  # Свободные средства
    value = cerebro.broker.getvalue()  # Стоимость портфеля
    print(f'Свободные средства : {cash}')
    print(f'Стоимость позиций  : {value - cash}')
    print(f'Стоимость портфеля : {value}')


class LogBars(bt.Strategy):
    """Простейшая система без выставления заявок и совершения сделок. Получает бары и выводит их в лог"""

    def __init__(self):
        """Инициализация торговой системы"""
        self.logger = logging.getLogger('MOEXPyData')  # Будем вести лог

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
                        handlers=[logging.FileHandler('MOEXPyData.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=ZoneInfo('Europe/Moscow')).timetuple()  # В логе время указываем по МСК

    cerebro = bt.Cerebro(stdstats=False)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна
    store = Store(broker=default_broker, data=MOEX())  # Хранилище. Брокер по умолчанию. Данные - Московская Биржа
    broker = store.getbroker()  # Брокер
    cerebro.setbroker(broker)  # Устанавливаем брокера

    data = store.getdata(dataname=dataname, fromdate=datetime(2024, 1, 1), todate=datetime(2026, 1, 1))  # 1. Исторические дневные бары за период
    # data = store.getdata(dataname=dataname, timeframe=bt.TimeFrame.Minutes, compression=1, fromdate=week_ago, four_price_doji=True)  # 2. Исторические минутные бары за последнюю неделю с дожи 4-х цен
    # data = store.getdata(dataname=dataname, timeframe=bt.TimeFrame.Minutes, compression=1, fromdate=week_ago, live_bars=True)  # 3. Исторические и новые минутные бары за последнюю неделю по подписке
    # data = store.getdata(dataname=dataname, timeframe=bt.TimeFrame.Minutes, compression=1, fromdate=week_ago, live_bars=True, schedule=schedule)  # 4. Исторические и новые минутные бары за последнюю неделю по расписанию

    cerebro.adddata(data)  # Добавляем данные
    cerebro.addstrategy(LogBars)  # Добавляем торговую систему
    cerebro.run()  # Запуск торговой системы
    get_cash_value()  # Отображаем статистику портфеля. Брокер
    cerebro.plot()  # Рисуем график. Московская Биржа
```
