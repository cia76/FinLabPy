from FinLabPy.Config import brokers  # Все брокеры


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    for code, broker in brokers.items():  # Пробегаемся по всем брокерам
        print(f'[{code}] {broker.name}')
        print(f'Стоимость позиций : {broker.get_value()}')
        print(f'Свободные средства: {broker.get_cash()}')
        for position in broker.get_positions():  # Пробегаемся по всем позициям брокера
            print(position)
        for order in broker.get_orders():  # Пробегаемся по всем заявкам брокера
            print(order)
    for broker in brokers.values():  # Пробегаемся по всем брокерам
        broker.close()  # Закрываем брокера
