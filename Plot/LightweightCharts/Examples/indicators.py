import pandas as pd

from FinLabPy.Config import brokers, default_broker  # Все брокеры и брокер по умолчанию
from FinLabPy.Core import bars_to_df  # Перевод бар в pandas DataFrame
from FinLabPy.Plot.LightweightCharts import Chart  # TradingView Lightweight Charts


def calculate_sma(df, period: int = 50):
    return pd.DataFrame({'time': df.index, f'SMA({period})': df['close'].rolling(window=period).mean()}).dropna()


def calculate_macd(df, short_period=12, long_period=26, signal_period=9):
    short_ema = df['close'].ewm(span=short_period, adjust=False).mean()
    long_ema = df['close'].ewm(span=long_period, adjust=False).mean()
    macd = short_ema - long_ema
    signal = macd.ewm(span=signal_period, adjust=False).mean()
    return pd.DataFrame(
        {
            'time': df.index,
            'MACD': macd,
            'Signal': signal,
            'MACD Histogram': macd - signal,
        }
    ).dropna()


if __name__ == '__main__':
    dataname = 'TQBR.SBER'
    time_frame = 'D1'

    broker = default_broker  # Брокер по умолчанию
    # broker = brokers['Т']  # Брокер по ключу из Config.py словаря brokers
    symbol = broker.get_symbol_by_dataname(dataname)  # Тикер по названию
    bars = broker.get_history(symbol, time_frame)  # Получаем всю историю тикера
    broker.close()  # Закрываем брокера

    chart = Chart()  # График

    pd_bars = bars_to_df(bars)  # Бары в pandas DataFrame
    chart.set(pd_bars)  # Отправляем бары на график

    sma_period = 64
    pd_sma = calculate_sma(pd_bars, period=sma_period)  # SMA в виде pandas Dataframe
    line = chart.create_line(f'SMA({sma_period})', 'red')  # На графике цен создаем линию SMA
    line.set(pd_sma)  # Заполняем ее значениями SMA

    pd_macd = calculate_macd(pd_bars)  # Гистограмма MACD с исходными линиями
    histogram = chart.create_histogram('MACD Histogram', 'blue', scale_margin_top=0.05, scale_margin_bottom=0.05, pane_index=1)  # Для MACD создаем отдельную панель
    chart.resize_pane(1, 100)  # Уменьшаем вертикальный размер
    histogram.set(pd_macd[['time', 'MACD Histogram']])  # Заполняем ее значениями MACD

    chart.show(block=True)
