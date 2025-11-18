from FinLabPy.Config import brokers  # Все брокеры


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    for code, broker in brokers.items():  # Пробегаемся по всем брокерам
        print(f'[{code}] {broker.name}')
        print('- Позиции:')
        for position in broker.get_positions():  # Пробегаемся по всем позициям брокера
            print(f'  - {position}')
        value = broker.get_value()  # Стоимость позиций
        print(f'- Стоимость позиций  : {value}')
        print('- Заявки:')
        for order in broker.get_orders():  # Пробегаемся по всем заявкам брокера
            print(f'  - {order}')
        cash = broker.get_cash()  # Свободные средств
        print(f'- Свободные средства : {cash}')
        print(f'- Итого              : {value + cash}')
    for _, broker in brokers.items():  # Пробегаемся по всем брокерам. Отдельный вызов нужен из-за того, что счета брокеров могут относится к одному брокеру
        broker.close()  # Закрываем брокера
