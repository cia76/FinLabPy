import logging  # Будем вести лог
from datetime import datetime, timedelta  # Работа с датой и временем
from threading import Thread, Event  # Поток и событие выхода из потока

from FinLabPy.Config import brokers, default_broker  # Все брокеры и брокер по умолчанию
from FinLabPy.Core import Broker
from FinLabPy.Schedule.MarketSchedule import Schedule  # Расписание работы биржи
from FinLabPy.Schedule.MOEX import Stocks  # Расписание торгов акций


logger = logging.getLogger('Schedule.BarsStream')  # Будем вести лог


# noinspection PyShadowingNames
def bars_stream(broker, dataname, schedule, time_frame):
    """Поток получения новых бар по расписанию биржи

    :param Broker broker: Брокер
    :param str dataname: Название тикера
    :param Schedule schedule: Расписание торгов
    :param str time_frame: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм
    """
    dt_format = '%d.%m.%Y %H:%M:%S'  # Формат даты и времени
    while True:
        market_datetime_now = schedule.market_datetime_now  # Текущее время на бирже по часам локального компьютера
        logger.debug(f'Текущая дата и время на бирже: {market_datetime_now:{dt_format}}')
        trade_bar_open_datetime = schedule.trade_bar_open_datetime(market_datetime_now, time_frame)  # Дата и время открытия бара, который будем получать
        logger.debug(f'Нужно получить бар: {trade_bar_open_datetime:{dt_format}}')
        trade_bar_request_datetime = schedule.trade_bar_request_datetime(market_datetime_now, time_frame)  # Дата и время запроса бара на бирже
        logger.debug(f'Время запроса бара: {trade_bar_request_datetime:{dt_format}}')
        sleep_time_secs = (trade_bar_request_datetime - market_datetime_now).total_seconds()  # Время ожидания в секундах
        logger.debug(f'Ожидание в секундах: {sleep_time_secs}')
        exit_event_set = exit_event.wait(sleep_time_secs)  # Ждем нового бара или события выхода из потока
        if exit_event_set:  # Если произошло событие выхода из потока
            broker.close()  # Перед выходом закрываем брокера
            return  # Выходим из потока, дальше не продолжаем
        bars = broker.get_history(dataname, time_frame, trade_bar_open_datetime)  # Получаем ответ на запрос истории рынка
        if bars is None:  # Если ничего не получили
            logger.warning('Данные не получены')
            continue  # Будем получать следующий бар
        logger.debug(f'Получены данные {bars}')
        if len(bars) == 0:  # Если бары не получены
            logger.warning('Бар не получен')
            continue  # Будем получать следующий бар
        bar = bars[0]  # Получаем первый (завершенный) бар
        logger.info(bar)


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    dataname = 'TQBR.SBER'
    time_frame = 'M1'  # 1 минута
    # time_frame = 'M5'  # 5 минут
    # time_frame = 'M15'  # 15 минут
    # time_frame = 'M60'  # 1 час
    # time_frame = 'D'  # 1 день

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('ScheduleBarsStream.log'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=Schedule.market_timezone).timetuple()  # В логе время указываем по временнОй зоне расписания (МСК)
    logging.getLogger('urllib3').setLevel(logging.CRITICAL + 1)

    # broker = default_broker  # Брокер по умолчанию
    broker = brokers['Ф']  # Брокер по ключу из Config.py словаря brokers
    schedule = Stocks()  # Расписание фондового рынка Московской Биржи
    # schedule.delta = timedelta(seconds=5)  # Для Т-Инвестиций 3 секунды задержки недостаточно для получения нового бара. Увеличиваем задержку
    exit_event = Event()  # Определяем событие выхода из потока
    stream_bars_thread = Thread(name='bars_stream', target=bars_stream, args=(broker, dataname, schedule, time_frame))  # Создаем поток получения новых бар
    stream_bars_thread.start()  # Запускаем поток
    print('\nEnter - выход')
    input()  # Ожидаем нажатия на клавишу Ввод (Enter)
    exit_event.set()  # Устанавливаем событие выхода из потока
