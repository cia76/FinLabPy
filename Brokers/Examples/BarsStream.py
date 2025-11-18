from FinLabPy.Config import brokers, default_broker  # Все брокеры и брокер по умолчанию


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    dataname = 'TQBR.SBER'
    time_frame = 'M1'

    broker = default_broker  # Брокер по умолчанию
    # broker = brokers['Т']  # Брокер по ключу из Config.py словаря brokers
    symbol = broker.get_symbol_by_dataname(dataname)  # Тикер по названию
    broker.on_new_bar.subscribe(lambda bar: print(bar))  # Перехватываем событие получения нового бара по подписке
    broker.subscribe_history(symbol, time_frame)  # Подписка на историю тикера
    input('\nEnter - выход\n')
    broker.close()  # Закрываем брокера
