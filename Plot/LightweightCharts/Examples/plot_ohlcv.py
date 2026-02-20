import pandas as pd
from FinLabPy.Plot.LightweightCharts import Chart  # TradingView Lightweight Charts


if __name__ == '__main__':
    chart = Chart()
    df = pd.read_csv('ohlcv.csv')  # Колонки date, open, high, low, close, volume
    chart.set(df)
    chart.show(block=True)
