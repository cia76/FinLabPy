from FinLabPy.Config import brokers, default_broker  # Все брокеры и брокер по умолчанию


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    dataname = 'TQBR.SBER'

    broker = default_broker  # Брокер по умолчанию
    # broker = brokers['Т']  # Брокер по ключу из Config.py словаря brokers
    print(broker.get_symbol_by_dataname(dataname))  # Тикер по названию
    broker.close()  # Закрываем брокера
