from FinLabPy.Config import brokers  # Все брокеры


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    dataname = 'TQBR.SBER'

    for code, broker in brokers.items():  # Пробегаемся по всем брокерам
        symbol = broker.get_symbol_by_dataname(dataname)  # Спецификация тикера брокера. Должна совпадать у всех брокеров
        print(f'[{code}] {symbol} Информация брокера: {symbol.broker_info}')
        broker.close()  # Закрываем брокера
