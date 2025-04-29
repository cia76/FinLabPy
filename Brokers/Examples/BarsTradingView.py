from lightweight_charts import Chart  # pip install lightweight-charts

from FinLabPy.Config import brokers, default_broker  # Все брокеры и брокер по умолчанию
from FinLabPy.Core import bars_to_df  # Перевод бар в pandas DataFrame


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    dataname = 'TQBR.SBER'
    time_frame = 'D1'

    broker = default_broker  # Брокер по умолчанию
    # broker = brokers['Т']  # Брокер по ключу из Config.py словаря brokers
    bars = broker.get_history(dataname, time_frame)  # Получаем всю историю тикера
    pd_bars = bars_to_df(bars)  # Бары в pandas DataFrame

    chart = Chart(toolbox=True)  # График с элементами рисования
    chart.legend(True)  # В верхнем левом углу отображаются значения по указателю мыши
    chart.set(pd_bars)  # Отправляем бары на график
    chart.show(block=True)  # Отображаем график. Блокируем дальнейшее исполнение кода, пока его не закроем

    broker.close()  # Закрываем брокера
