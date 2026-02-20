from datetime import timedelta

from FinLabPy.Config import default_broker  # Брокер по умолчанию
from FinLabPy.Core import bars_to_df  # Перевод бар в pandas DataFrame
from FinLabPy.Schedule.MOEX import Stocks  # Расписание биржи
from FinLabPy.Plot.LightweightCharts import Chart  # TradingView Lightweight Charts


schedule = Stocks()  # Расписание биржи. Из него будем получать текущее время на бирже


def bars_to_chart(dataname, time_frame='D1', last_bars=100) -> bytes:
    """Отрисовка свечного графика

    :param str dataname: Название тикера
    :param str time_frame: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм
    :param int last_bars: Кол-во последних бар для отображения
    """
    days = last_bars + 5  # Для дневнного интервала берем смещение с запасом, т.к. возможны выходные/праздники
    if time_frame == 'M60':  # Для часового интервала
        days = days / 24 * 15  # В торговой сессии максимум, 15 часовых баров за день
    elif time_frame == 'M30':  # Для 30-и минутного интервала
        days = days / 24 * 15 / 2  # В часе 2 30-и минутки
    elif time_frame == 'M15':  # Для 15-и минутного интервала
        days = days / 24 * 15 / 4  # В часе 4 15-и минутки
    elif time_frame == 'M5':  # Для 5-и минутного интервала
        days = days / 24 * 15 / 12  # В часе 12 5-и минуток
    elif time_frame == 'M1':  # Для минутного интервала
        days = days / 24 * 15 / 60  # В часе 60 минуток

    broker = default_broker  # Брокер по умолчанию
    # broker = brokers['Т']  # Брокер по ключу из Config.py словаря brokers
    symbol = broker.get_symbol_by_dataname(dataname)  # Тикер по названию
    days = broker.get_history(symbol, time_frame, schedule.market_datetime_now - timedelta(days=days))  # Получаем историю тикера с запасом
    broker.close()  # Закрываем брокера

    pd_bars = bars_to_df(days).tail(last_bars)  # Бары в pandas DataFrame
    width = 71 + 6 * len(pd_bars)  # Ширина графика в зависимости от кол-ва полученных бар
    height = width * 9 // 16  # формата 16 на 9
    chart = Chart(width=width, height=height)  # График
    chart.set(pd_bars)  # Отправляем бары на график
    chart.show()  # Отображаем график
    return chart.screenshot()  # Делаем снимок графика


if __name__ == '__main__':
    dataname = 'SPBFUT.CNYRUBF'
    time_frame = 'D1'

    img = bars_to_chart(dataname, last_bars=200)
    with open('screenshot.png', 'wb') as f:
        f.write(img)
    print('График успешно сохранен в файл screenshot.png')
