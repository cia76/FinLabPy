from FinLabPy.Config import brokers, default_broker  # Все брокеры и брокер по умолчанию


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    dataname = 'TQBR.SBER'
    time_frame = 'D1'

    broker = default_broker  # Брокер по умолчанию
    # broker = brokers['Т']  # Брокер по ключу из Config.py словаря brokers
    bars = broker.get_history(dataname, time_frame)  # Получаем всю историю тикера
    print(bars[0])  # Первый бар
    print(bars[-1])  # Последний бар
    print(broker.bars_to_df(bars))  # Бары в pandas DataFrame
    broker.close()  # Закрываем брокера
