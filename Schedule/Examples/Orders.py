from datetime import datetime, timedelta  # Работа с датой и временем

from FinLabPy.Config import brokers, default_broker  # Все брокеры и брокер по умолчанию
from FinLabPy.Core import Bar, Order
from FinLabPy.Schedule.MOEX import Stocks  # Расписание торгов акций


def exec_order(bars: list[Bar], order: Order, market_dt: datetime) -> None:
    print(f'{order} - {market_dt}')
    if schedule.trade_session(market_dt) is None:  # Если биржа не работает
        print('Биржа не работает')
        return
    order_open_dt = schedule.trade_bar_open_datetime(market_dt, market_tf)  # Дата и время открытия последнего бара
    if order.exec_type == Order.Market:  # Рыночная заявка
        price = next(bar.open for bar in bars if bar.datetime == order_open_dt)  # Вход по цене открытия последнего бара, т.к. внутри бара цены неизвестны
        print(f'Рыночная заявка {"на покупку" if order.buy else "на продажу"} исполнена {order_open_dt} по цене {price}')
    elif order.exec_type == Order.Limit:  # Лимитная заявка
        print(f'Лимитная заявка {"на покупку" if order.buy else "на продажу"} выставлена {order_open_dt} по цене {order.price}')
        bar = next((bar for bar in bars if bar.datetime >= order_open_dt and bar.low <= order.price), None) if order.buy \
            else next((bar for bar in bars if bar.datetime >= order_open_dt and bar.high >= order.price), None)  # Бар исполнения заявки
        if bar is None or bar.datetime.date() > order_open_dt.date():  # Если бара исполнения заявки нет, или она за пределами торговой сессии постановки бара
            print('Лимитная заявка снята на бирже')
        else:  # Заявка исполнена
            price = min(order.price, bar.open) if order.buy else max(order.price, bar.open)  # Возможно, лучшей ценой будет цена открытия, а не лимитная цена
            print(f'Лимитная заявка исполнена {bar.datetime} по цене {price}')
    elif order.exec_type in (Order.Stop, Order.StopLimit):  # Стоп заявка
        print(f'Стоп заявка {"на покупку" if order.buy else "на продажу"} выставлена {order_open_dt} по цене {order.stop_price}')
        stop_bar = next((bar for bar in bars if bar.datetime >= order_open_dt and bar.low <= order.stop_price <= bar.high), None)  # Бар исполнения стоп заявки
        if stop_bar is None:  # Если бара исполнения заявки нет
            print('Стоп заявка не исполнилась (активна)')
        else:  # Стоп заявка исполнилась
            print(f'Стоп заявка исполнена {stop_bar.datetime} по цене {order.stop_price}')
            if order.exec_type == Order.Stop:  # Рыночная стоп заявка
                print(f'Рыночная заявка {"на покупку" if order.buy else "на продажу"} исполнена {stop_bar.datetime} по цене {order.stop_price}')
            else:  # Лимитная стоп заявка
                exec_order(bars, Order(order.broker, '2', order.buy, Order.Limit, order.dataname, order.decimals, order.quantity, order.price), stop_bar.datetime)


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    dataname = 'TQBR.SBER'
    market_tf = 'M5'  # 5-и минутный временной интервал
    # market_tf = 'M60'  # Часовой временной интервал
    # market_tf = 'D1'  # Дневной временной интервал

    broker = default_broker  # Брокер по умолчанию
    # broker = brokers['Ф']  # Брокер по ключу из Config.py словаря brokers
    schedule = Stocks()  # Расписание фондового рынка Московской Биржи
    # schedule.delta = timedelta(seconds=5)  # Для Т-Инвестиций 3 секунды задержки недостаточно для получения нового бара. Увеличиваем задержку
    bars = broker.get_history(dataname, market_tf, datetime(2025, 3, 1))  # Получаем ответ на запрос истории рынка

    print(f'Временной интервал     : {market_tf}')
    print(f'Рассинхронизация часов : {schedule.delta.seconds} с')

    market_order = Order(broker, '1', True, Order.Market, dataname, 2, 10, 0, 0)  # Рыночная заявка
    exec_order(bars, market_order, datetime(2025, 3, 30))  # Биржа не работает
    exec_order(bars, market_order, datetime(2025, 3, 31, 7, 0))  # Рыночная заявка на покупку исполнена 2025-03-31 07:00:00 по цене 300.07

    limit_order = Order(broker, '1', True, Order.Limit, dataname, 2, 10, 300.10, 0)  # Лимитная заявка
    exec_order(bars, limit_order, datetime(2025, 3, 31, 7, 0))  # Лимитная заявка исполнена 2025-03-31 07:00:00 по цене 300.07
    limit_order.price = 299
    exec_order(bars, limit_order, datetime(2025, 3, 31, 10, 5))  # Лимитная заявка снята на бирже
    limit_order.price = 305
    exec_order(bars, limit_order, datetime(2025, 3, 31, 10, 5))  # Лимитная заявка исполнена 2025-03-31 10:55:00 по цене 305

    stop_order = Order(broker, '1', True, Order.Stop, dataname, 2, 10, 0, 305)  # Стоп заявка
    exec_order(bars, stop_order, datetime(2025, 3, 31, 10, 5))  # Стоп заявка исполнена 2025-03-31 10:55:00 по цене 305, Рыночная заявка на покупку исполнена 2025-03-31 10:55:00 по цене 305
    stop_order.stop_price = 299
    exec_order(bars, stop_order, datetime(2025, 3, 31, 10, 5))  # Стоп заявка исполнена 2025-04-03 18:25:00 по цене 299, Рыночная заявка на покупку исполнена 2025-04-03 18:25:00 по цене 299
    stop_order.stop_price = 200
    exec_order(bars, stop_order, datetime(2025, 3, 31, 10, 5))  # Стоп заявка не исполнилась (активна)

    stop_order = Order(broker, '1', True, Order.StopLimit, dataname, 2, 10, 304, 305)  # Стоп заявка
    exec_order(bars, stop_order, datetime(2025, 3, 31, 10, 5))  # Стоп заявка исполнена 2025-03-31 10:55:00 по цене 305, Лимитная заявка исполнена 2025-03-31 11:45:00 по цене 304
    stop_order.price = 200
    exec_order(bars, stop_order, datetime(2025, 3, 31, 10, 5))  # Стоп заявка исполнена 2025-03-31 10:55:00 по цене 305, Лимитная заявка снята на бирже
