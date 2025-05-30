from datetime import datetime, timedelta

from FinLabPy.Schedule.MOEX import Stocks

# Результат работы скрипта

# Временной интервал     : M5
# Рассинхронизация часов : 3 с

# Перерыв на бирже (утро пн)
# Дата и время на бирже : 24.03.2025 06:59:59
# Идет торговая сессия  : Нет
# Запрос бара           : 21.03.2025 23:45:00
# Дата и время запроса  : 24.03.2025 07:00:03
# Секунд до запроса     : 4
# Действителен до       : 24.03.2025 07:05:00

# Биржа работает (открытие пн)
# Дата и время на бирже : 24.03.2025 07:00:00
# Идет торговая сессия  : 07:00:00 - 09:49:59
# Запрос бара           : 24.03.2025 07:00:00
# Дата и время запроса  : 24.03.2025 07:05:03
# Секунд до запроса     : 303
# Действителен до       : 24.03.2025 07:10:00

# Перерыв на бирже (аукцион закрытия)
# Дата и время на бирже : 24.03.2025 18:40:00
# Идет торговая сессия  : Нет
# Запрос бара           : 24.03.2025 18:35:00
# Дата и время запроса  : 24.03.2025 19:05:03
# Секунд до запроса     : 1503
# Действителен до       : 24.03.2025 19:10:00

# Перерыв на бирже (вечер пн)
# Дата и время на бирже : 24.03.2025 23:50:00
# Идет торговая сессия  : Нет
# Запрос бара           : 24.03.2025 23:45:00
# Дата и время запроса  : 25.03.2025 07:00:03
# Секунд до запроса     : 25803
# Действителен до       : 25.03.2025 07:05:00

# Перерыв на бирже (вечер пт)
# Дата и время на бирже : 28.03.2025 23:50:00
# Идет торговая сессия  : Нет
# Запрос бара           : 28.03.2025 23:45:00
# Дата и время запроса  : 31.03.2025 07:00:03
# Секунд до запроса     : 198603
# Действителен до       : 31.03.2025 07:05:00

# Выходной на бирже (сб)
# Дата и время на бирже : 29.03.2025 00:00:00
# Идет торговая сессия  : Нет
# Запрос бара           : 28.03.2025 23:45:00
# Дата и время запроса  : 31.03.2025 07:00:03
# Секунд до запроса     : 198003
# Действителен до       : 31.03.2025 07:05:00

# Выходной на бирже (вс)
# Дата и время на бирже : 30.03.2025 00:00:00
# Идет торговая сессия  : Нет
# Запрос бара           : 28.03.2025 23:45:00
# Дата и время запроса  : 31.03.2025 07:00:03
# Секунд до запроса     : 111603
# Действителен до       : 31.03.2025 07:05:00

if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    schedule = Stocks()  # Расписание торгов акций
    # schedule.delta = timedelta(seconds=5)  # Для Т-Инвестиций 3 секунды задержки недостаточно для получения нового бара. Увеличиваем задержку
    market_tf = 'M5'  # 5-и минутный временной интервал
    # market_tf = 'M60'  # Часовой временной интервал
    # market_tf = 'D1'  # Дневной временной интервал

    market_dts = {datetime(2025, 3, 24, 6, 59, 59): 'Перерыв на бирже (утро пн)',
                  datetime(2025, 3, 24, 7, 0): 'Биржа работает (открытие пн)',
                  datetime(2025, 3, 24, 18, 40): 'Перерыв на бирже (аукцион закрытия)',
                  datetime(2025, 3, 24, 23, 50): 'Перерыв на бирже (вечер пн)',
                  datetime(2025, 3, 28, 23, 50): 'Перерыв на бирже (вечер пт)',
                  datetime(2025, 3, 29): 'Выходной на бирже (сб)',
                  datetime(2025, 3, 30): 'Выходной на бирже (вс)',
                  schedule.market_datetime_now: 'Текущее время на бирже по часам локального компьютера'}

    print(f'Временной интервал     : {market_tf}')
    print(f'Рассинхронизация часов : {schedule.delta.seconds} с')
    for market_dt, v in market_dts.items():  # Пробегаемся по справочнику дат
        print()
        print(v)
        print(f'Дата и время на бирже : {market_dt:{schedule.dt_format}}')
        session = schedule.trade_session(market_dt)  # Торговая сессия
        str_session = f'{session.time_begin} - {session.time_end}' if session else 'Нет'
        print(f'Идет торговая сессия  : {str_session}')
        trade_bar_open_datetime = schedule.trade_bar_open_datetime(market_dt, market_tf)  # Дата и время открытия бара
        print(f'Запрос бара           : {trade_bar_open_datetime:{schedule.dt_format}}')
        trade_bar_request_datetime = schedule.trade_bar_request_datetime(market_dt, market_tf)  # Дата и время запроса бара
        print(f'Дата и время запроса  : {trade_bar_request_datetime:{schedule.dt_format}}')
        sleep_time_secs = int((trade_bar_request_datetime - market_dt).total_seconds())  # Время ожидания до запроса в секундах
        print(f'Секунд до запроса     : {sleep_time_secs}')
        trade_bar_valid_to_datetime = schedule.trade_bar_close_datetime(trade_bar_request_datetime, market_tf)  # Время, до которого бар действителен
        print(f'Действителен до       : {trade_bar_valid_to_datetime:{schedule.dt_format}}')
