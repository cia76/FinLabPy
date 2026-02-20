from datetime import datetime

import backtrader as bt


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    # cerebro = bt.Cerebro()  # Инициируем "движок" BackTrader
    cerebro = bt.Cerebro(stdstats=False)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна
    data = bt.feeds.GenericCSVData(  # Можно принимать любые CSV файлы с разделителем десятичных знаков в виде точки https://backtrader.com/docu/datafeed-develop-csv/
        dataname='TQBR.SBER_D1.txt',  # Файл для импорта
        separator='\t',  # Колонки разделены табуляцией
        dtformat='%d.%m.%Y %H:%M',  # Формат даты/времени DD.MM.YYYY HH:MI
        openinterest=-1,  # Открытого интереса в файле нет
        # timeframe=bt.TimeFrame.Minutes,  # Для временнОго интервала отличного от дневок нужно его указать
        # compression=15,  # Для миннутного интервала, отличного от 1, его нужно указать
        fromdate=datetime(2024, 1, 1),  # Начальная дата приема исторических данных (Входит)
        todate=datetime(2026, 1, 1))  # Конечная дата приема исторических данных (Не входит)
    cerebro.adddata(data)  # Привязываем исторические данные
    cerebro.run()  # Запуск торговой системы. Пока ее у нас нет
    cerebro.plot()  # Рисуем график
