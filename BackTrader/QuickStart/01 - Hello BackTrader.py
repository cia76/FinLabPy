import backtrader as bt  # Библиотека BackTrader


def get_cash_value():
    cash = cerebro.broker.getcash()  # Свободные средства
    value = cerebro.broker.getvalue()  # Стоимость позиций
    print(f'Свободные средства : {cash}')
    print(f'Стоимость позиций  : {value - cash}')
    print(f'Стоимость портфеля : {value}')


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    cerebro = bt.Cerebro()  # Инициируем "движок" BackTrader (Cerebro = Мозг на испанском)
    cerebro.broker.setcash(1_000_000)  # Стартовый капитал для "бумажной" торговли (по умолчанию, 10_000)
    print('\nСтартовый капитал')
    get_cash_value()  # Отображаем статистику портфеля до запуска ТС
    cerebro.run()  # Запуск ТС. Пока ее у нас нет
    print('\nКонечный капитал')
    get_cash_value()  # Отображаем статистику портфеля после запуска ТС
