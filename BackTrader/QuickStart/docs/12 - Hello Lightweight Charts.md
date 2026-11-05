[Видео](https://vkvideo.ru/video_ext.php?oid=-215710796&id=456239113&hash=689623df41ec987b&hd=3)

Мы уже выводили встроенные графики в BackTrader. С их помощью можно получить общую картину. Также с помощью инструмента выделения фрагмента графика, можно изучать нужные места графика.

TradingView предоставляет библиотеку [Lightweight Charts](https://tradingview.github.io/lightweight-charts/) для отрисовки графиков, которые мы привыклии видеть в торговых терминалах.
Можно ли использовать графики от TradingView в BackTrader?

Конечно, можно через библиотеку интеграции FinLabPy. Построим график Lightweight Charts с 3-мя классическими индикаторами: SMA, RSI, Momentum.

## Импорт индикаторов

В BackTrader встроены, практически, все классические индикаторы. Вам не придется их программировать. Более того, индикаторы, как и BackTrader, доступны в исходном коде.
Вы можете изучать код индикаторов, чтобы в будущем применить его при разработке собственных индикаторов.

```python
from backtrader.indicators import MovingAverageSimple, RelativeStrengthIndex, Momentum  # Классические индикаторы SMA, RSI, Momentum
```

## Индикаторы в торговой системе

Индикаторы определяются при инициализации торговой системы. Строить их будем по ценам закрытия с заданным периодом в параметре **period**

```python
class PlotIndicators(bt.Strategy):
    def __init__(self):
        self.sma = MovingAverageSimple(self.data.close, period=100)  # SMA
        self.rsi = RelativeStrengthIndex(self.data.close, period=14)  # RSI
        self.momentum = Momentum(self.data.close, period=1)  # Momentum
```

## Импорт Lightweight Charts

В библиотеке FinLabPy реализовано отображение графиков в Lightweight Charts из BackTrader.

```python
from FinLabPy.BackTrader import PlotLC  # Построение графика Lightweight Charts
```

## Результаты запуска торговой системы

Чтобы настраивать графики под свой вкус, мы запускаем торговую систему и сохраняем результаты запуска в переменную.

```python
run_strat = cerebro.runstrats[0][0]  # ТС с результатами запуска
```

## Настройка графиков

Мы можем настраивать линии графиков **lines** через **setattr**.
Например, можно настраивать номер панели **pane_id** для отображения, цвет **color**, тип линии **style** и многое другое.

```python
setattr(run_strat.momentum.plotinfo, 'lines', {'momentum': {'pane_id': 2, 'color': 'red'}})
setattr(run_strat.rsi.plotinfo, 'lines', {'rsi': {'pane_id': 1, 'color': 'green'}})
setattr(run_strat.sma.plotinfo, 'lines', {'sma': {'style': 'solid', 'color': 'blue'}})  # style='solid'/'dotted'/'dashed'/'large_dashed'/'sparse_dotted'
```

## Вывод через Lightweight Charts

Вывод графика Lightweight Charts реализован через стандартную функцию **cerebro.plot** с параметром **plotter=PlotLC.Plot(volume=False)**.
Заметьте, что инструкция не отображать объемы **volume=False** вводится не на уровне cerebro.plot, а на уровне PlotLC.Plot, т.к. BackTrader не передает параметры в плоттер.

```python
cerebro.plot(plotter=PlotLC.Plot(volume=False))  # Рисуем график Lightweight Charts
```

## Полный код примера:

```python
from datetime import date, timedelta  # Диапазон дат за последний год

import backtrader as bt  # Основная библиотека BackTrader
from backtrader.indicators import MovingAverageSimple, RelativeStrengthIndex, Momentum  # Классические индикаторы SMA, RSI, Momentum

from FinLabPy.Config import brokers, default_broker  # Все брокеры и брокер по умолчанию
from FinLabPy.BackTrader import Store, PlotLC  # Хранилище BackTrader, построение графика Lightweight Charts


class PlotIndicators(bt.Strategy):
    def __init__(self):
        self.sma = MovingAverageSimple(self.data.close, period=100)  # SMA
        self.rsi = RelativeStrengthIndex(self.data.close, period=14)  # RSI
        self.momentum = Momentum(self.data.close, period=1)  # Momentum


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    dataname = 'TQBR.SBER'  # Тикер
    year_ago = date.today() - timedelta(days=365)  # Год назад

    store = Store(broker=default_broker)  # Хранилище брокера по умолчанию
    # store = Store(broker=brokers['<Ключ словаря brokers из Config.py>'])  # Хранилище выбранного брокера
    broker = store.getbroker()  # Брокер
    data = store.getdata(dataname=dataname, fromdate=year_ago)

    # cerebro = bt.Cerebro()  # Инициируем "движок" BackTrader
    cerebro = bt.Cerebro(stdstats=False)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна
    cerebro.setbroker(broker)  # Устанавливаем брокера
    cerebro.adddata(data)  # Привязываем исторические данные
    cerebro.addstrategy(PlotIndicators)  # Привязываем индикаторы через торговую систему
    cerebro.run()  # Запуск торговой системы

    # cerebro.plot(volume=False)  # Рисуем стандартный график BackTrader

    run_strat = cerebro.runstrats[0][0]  # ТС с результатами запуска
    setattr(run_strat.momentum.plotinfo, 'lines', {'momentum': {'pane_id': 2, 'color': 'red'}})
    setattr(run_strat.rsi.plotinfo, 'lines', {'rsi': {'pane_id': 1, 'color': 'green'}})
    setattr(run_strat.sma.plotinfo, 'lines', {'sma': {'style': 'solid', 'color': 'blue'}})  # style='solid'/'dotted'/'dashed'/'large_dashed'/'sparse_dotted'
    cerebro.plot(plotter=PlotLC.Plot(volume=False))  # Рисуем график Lightweight Charts
```
