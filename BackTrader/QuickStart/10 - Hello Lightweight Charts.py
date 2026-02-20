from datetime import date, timedelta

import backtrader as bt
from backtrader.indicators import MovingAverageSimple, Momentum, RelativeStrengthIndex  # Классические индикаторы

from FinLabPy.Config import brokers, default_broker  # Все брокеры и брокер по умолчанию
from FinLabPy.BackTrader import Store, PlotLC  # Хранилище BackTrader, график Lightweight Charts


class PlotIndicators(bt.Strategy):
    def __init__(self):
        sma = MovingAverageSimple(self.data.close, period=100)  # SMA
        rsi = RelativeStrengthIndex(self.data.close, period=14)  # RSI
        momentum = Momentum(self.data.close, period=1)  # Momentum

        setattr(sma.plotinfo, 'lines', {'sma': {'style': 'solid', 'color': 'blue'}})  # style='solid'/'dotted'/'dashed'/'large_dashed'/'sparse_dotted'
        setattr(rsi.plotinfo, 'lines', {'rsi': {'pane_id': 1, 'color': 'green'}})
        setattr(momentum.plotinfo, 'lines', {'momentum': {'pane_id': 2, 'color': 'red'}})


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    dataname = 'TQBR.SBER'  # Тикер
    year_ago = date.today() - timedelta(days=365)  # Год назад

    # cerebro = bt.Cerebro()  # Инициируем "движок" BackTrader
    cerebro = bt.Cerebro(stdstats=False)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна
    store = Store(broker=default_broker)  # Хранилище брокера по умолчанию
    # store = Store(broker=brokers['<Ключ словаря brokers из Config.py>'])  # Хранилище выбранного брокера
    broker = store.getbroker()  # Брокер
    cerebro.setbroker(broker)  # Устанавливаем брокера
    data = store.getdata(dataname=dataname, fromdate=year_ago)
    cerebro.adddata(data)  # Привязываем исторические данные
    cerebro.addstrategy(PlotIndicators)  # Привязываем торговую систему
    cerebro.run()  # Запуск торговой системы
    # cerebro.plot(volume=False)  # Рисуем график
    cerebro.plot(plotter=PlotLC.Plot(volume=False))  # Рисуем график Lightweight Charts
