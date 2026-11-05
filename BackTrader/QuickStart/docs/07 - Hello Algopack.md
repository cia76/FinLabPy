[Видео](https://vkvideo.ru/video_ext.php?oid=-215710796&id=456239069&hash=199c215e40dee507&hd=3)

Кроме данных о ценах и объемах инструмента в торговых системах могут быть другие данные. На их основании также могут приниматься решения о сделках.
Например, у Московской Биржи есть продукт [Алгопак](https://moexalgo.github.io/), который позволяет получать сотни полезных метрик по инструментам.
Например, расширенный открытый интерес для фьючерсов [FUTOI](https://moexalgo.github.io/docs/method/futoi).

Система автоторговли "Финансовой Лаборатории" выстроена так, что с уровня торговой системы можно всегда добраться до любых специализированных функций библиотек-оберток.

## Инициализация библиотеки-обертки

Для этого нужно в раздел инициализации торговой системы указать библиотеку-обертку Московской Биржи [MOEXPy](https://github.com/cia76/MOEXPy).

```python
from MOEXPy import MOEXPy  # Работа с Algopack API Московской Биржи
```

```python
    def __init__(self):
        """Инициализация торговой системы"""
        self.mp_provider = MOEXPy()  # Работа с Algopack API Московской Биржи
```

## Преобразования данных

В разделе инициализации нужно выполнить все необходимые преобразования. В FinLabPy каждый инструмент представлен в виде <Код режима торгов>.<Код инструмента>. Например, акция Сбербанка будет иметь вид TQBR.SBER
Для Алгопака требуется просто указать код инструмента без его кода режима торгов. Т.е. взять значение после точки.
Разобьем инструмент **self.data._dataname** на части по точке через **split('.')**. Возьмем последнюю, вторую часть. Т.к. все части в Python нумеруются с нуля, то берем часть с индексом **[1]**

```python
        self.ticker = self.data._dataname.split('.')[1]  # Тикер Московской Биржи
```

Для нашего примера фьючерсов такое преобразование допустимо. Но что делать, если в названии инструмента есть точка?
Тогда можно получить название режима торгов и инструмента через функцию MOEXPy **dataname_to_board_symbol**. Режим торгов нам не нужен. Поэтому, вместо переменной по синтаксису Python мы ставим _.

```python
        _, self.ticker = self.mp_provider.dataname_to_board_symbol(self.data._dataname)  # Тикер Московской Биржи
```

## Получение новой свечи

### Конвертация даты и времени в Python

При получении новой свечки нужно перевести дату и время из формата BackTrader функцией **num2date**

```python
    def next(self):
        """Получение следующего исторического/нового бара"""
        dt = bt.num2date(self.data.datetime[0])  # Дата и время пришедшего бара
```

### Запрос открытого интереса

Через библиотеку-обертку Московской Биржи MOEXPy получаем данные открытого интереса фьючерса за пришедшую свечку через функцию **get_futoi**.

```python
        futoi = self.mp_provider.get_futoi(self.ticker, dt, dt)  # Получаем открытый интерес фьючерса за пришедший бар
```

### Конвертация открытого интереса в Python

Данные приходят в формате, определенном МосБиржей. Почему бы нам не получить их в удобном для дальнейшей работы виде списка с элементами в виде справочника.

```python
        futoi_data = [dict(zip(futoi['futoi']['columns'], row)) for row in futoi['futoi']['data']]  # Переводим в список словаря
```

Этот код разбивает пришедшие данные по открытому интересу на 2 элемента списка из **futoi['futoi']['data']**. Для физических и юридических лиц.
Название сущностей берется из **futoi['futoi']['columns']**. Они сопостовляются со значениями через **zip** и переводятся в словарь Python через **dict**

### Работа с открытым интересом

Далее можно работать с открытым интересом в торговой системе. В этом примере мы просто выведем значения для физических лиц (FIZ) и юридических лиц (YUR) в лог.

```python
        [self.logger.info(futoi) for futoi in futoi_data]  # Разбиваем на строки для FIZ/YUR
```

## Полный код примера:

```python
import logging
from datetime import date, datetime
from zoneinfo import ZoneInfo  # ВременнАя зона

import backtrader as bt

from FinLabPy.Config import brokers, default_broker  # Все брокеры и брокер по умолчанию
from FinLabPy.BackTrader import Store  # Хранилище BackTrader
from MOEXPy import MOEXPy  # Работа с Algopack API Московской Биржи


class LogAlgopack(bt.Strategy):
    """Простейшая система без выставления заявок и совершения сделок. Получает бары и выводит их в лог"""

    def __init__(self):
        """Инициализация торговой системы"""
        self.logger = logging.getLogger('AlgopackData')  # Будем вести лог
        self.mp_provider = MOEXPy()  # Работа с Algopack API Московской Биржи
        self.ticker = self.data._dataname.split('.')[1]  # Тикер Московской Биржи

    def next(self):
        """Получение следующего исторического/нового бара"""
        dt = bt.num2date(self.data.datetime[0])  # Дата и время пришедшего бара
        self.logger.info(f'{dt:%d.%m.%Y %H:%M:%S} O:{self.data.open[0]} H:{self.data.high[0]} L:{self.data.low[0]} C:{self.data.close[0]} V:{int(self.data.volume[0])}')
        futoi = self.mp_provider.get_futoi(self.ticker, dt, dt)  # Получаем открытый интерес фьючерса за пришедший бар
        futoi_data = [dict(zip(futoi['futoi']['columns'], row)) for row in futoi['futoi']['data']]  # Переводим в список словаря
        [self.logger.info(futoi) for futoi in futoi_data]  # Разбиваем на строки для FIZ/YUR


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    dataname = 'SPBFUT.IMOEXF'  # Тикер

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.INFO,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('AlgopackData.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=ZoneInfo('Europe/Moscow')).timetuple()  # В логе время указываем по МСК

    cerebro = bt.Cerebro(stdstats=False)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна
    store = Store(broker=default_broker)  # Хранилище брокера по умолчанию
    # store = Store(broker=brokers['<Ключ словаря brokers из Config.py>'])  # Хранилище выбранного брокера
    broker = store.getbroker()  # Брокер
    cerebro.setbroker(broker)  # Устанавливаем брокера

    data = store.getdata(dataname=dataname, timeframe=bt.TimeFrame.Minutes, compression=5, fromdate=date.today(), four_price_doji=True)  # 5-и минутки за сегодня
    cerebro.adddata(data)  # Добавляем данные
    cerebro.addstrategy(LogAlgopack)  # Добавляем торговую систему
    cerebro.run()  # Запуск торговой системы
```
