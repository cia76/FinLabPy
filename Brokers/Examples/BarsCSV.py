import logging  # Выводим лог на консоль и в файл
from datetime import datetime  # Дата и время
from time import time
import os.path

from FinLabPy.Config import brokers, default_broker  # Все брокеры и брокер по умолчанию
from FinLabPy.Core import Broker, bars_to_df  # Класс брокера, перевод бар в pandas DataFrame

from pytz import timezone  # ВременнАя зона МСК
import pandas as pd


logger = logging.getLogger('BarsCSV')  # Будем вести лог. Определяем здесь, т.к. возможен внешний вызов ф-ии
delimiter = '\t'  # Разделитель значений в файле истории. По умолчанию табуляция
dt_format = '%d.%m.%Y %H:%M'  # Формат представления даты и времени в файле истории. По умолчанию русский формат
d_format = '%d.%m.%Y'  # Формат представления даты в файле истории. По умолчанию русский формат


def get_bars_from_file(datapath, dataname, time_frame) -> pd.DataFrame:
    """Получение истории бар тикера из файла

    :param str datapath: Путь файла истории
    :param str dataname: Название тикера
    :param str time_frame: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм
    """
    filename = f'{datapath}{dataname}_{time_frame}.txt'
    if os.path.isfile(filename):  # Если файл существует
        logger.info(f'Получение файла {filename}')
        file_bars = pd.read_csv(filename,  # Имя файла
                                sep=delimiter,  # Разделитель значений
                                usecols=['datetime', 'open', 'high', 'low', 'close', 'volume'],  # Для ускорения обработки задаем названия колонокки
                                parse_dates=['datetime'],  # Колонку datetime разбираем как дату/время
                                dayfirst=True,  # В дате/времени сначала идет день, затем месяц и год
                                index_col='datetime')  # Индексом будет колонка datetime  # Дневки тикера
        if len(file_bars) == 0:  # Если в файле нет записей
            logger.info(f'В файле {filename} нет записей')
            return pd.DataFrame()
        file_bars['datetime'] = file_bars.index  # Колонка datetime нужна, чтобы не удалять одинаковые OHLCV на разное время
        logger.info(f'Первый бар    : {file_bars.index[0]:{dt_format}}')
        logger.info(f'Последний бар : {file_bars.index[-1]:{dt_format}}')
        logger.info(f'Кол-во бар    : {len(file_bars)}')
        return file_bars
    else:  # Если файл не существует
        logger.warning(f'Файл {filename} не найден')
        return pd.DataFrame()


def get_bars_from_broker(broker, dataname, time_frame, dt_from=None, dt_to=None) -> pd.DataFrame:
    """Получение истории бар тикера от брокера

    :param Broker broker: Брокер
    :param str dataname: Название тикера
    :param str time_frame: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм
    :param datetime dt_from: Начало запроса по МСК
    :param datetime dt_to: Начало запроса по МСК
    """
    logger.info(f'Получение истории из брокера {broker.__class__.__name__}')
    pd_bars = bars_to_df(broker.get_history(dataname, time_frame, dt_from, dt_to))
    if len(pd_bars) == 0:  # Если новых бар нет
        logger.info('Новых бар нет')
        return pd.DataFrame()
    logger.info(f'Первый бар    : {pd_bars.index[0]:{dt_format}}')
    logger.info(f'Последний бар : {pd_bars.index[-1]:{dt_format}}')
    logger.info(f'Кол-во бар    : {len(pd_bars)}')
    return pd_bars


def save_bars_to_file(broker, datapath, dataname, time_frame, skip_first_date=False, skip_last_date=False, four_price_doji=False) -> None:
    """Получение истории бар тикера от брокера, объединение с имеющимися барами из файла (если есть), сохранение бар в файл

    :param Broker broker: Брокер
    :param str datapath: Путь файла истории
    :param str dataname: Название тикера
    :param str time_frame: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм
    :param bool skip_first_date: Убрать бары на первую полученную дату
    :param bool skip_last_date: Убрать бары на последнюю полученную дату
    :param bool four_price_doji: Оставить бары с дожи 4-х цен
    """
    logger.info(f'Получение истории {dataname} ({time_frame}) из файла и брокера {broker.__class__.__name__}')
    file_bars = get_bars_from_file(datapath, dataname, time_frame)  # Получаем бары из файла
    dt_from = None if len(file_bars) == 0 else file_bars.index[-1]  # Если файла или записей в файле нет, то запрос с начала. Иначе, запрос с последнего бара (он может быть еще не был сформирован)
    pd_bars = get_bars_from_broker(broker, dataname, time_frame, dt_from)  # Получаем бары из провайдера
    if pd_bars.empty:  # Если бары не получены
        logger.info('Новых бар нет')
        return  # то выходим, дальше не продолжаем
    if file_bars.empty and skip_first_date:  # Если файла нет, и убираем бары на первую дату
        len_with_first_date = len(pd_bars)  # Кол-во бар до удаления на первую дату
        first_date = pd_bars.index[0].date()  # Первая дата
        pd_bars.drop(pd_bars[(pd_bars.index.date == first_date)].index, inplace=True)  # Удаляем их
        logger.warning(f'Удалено бар на первую дату {first_date:{d_format}}: {len_with_first_date - len(pd_bars)}')
    if skip_last_date:  # Если убираем бары на последнюю дату
        len_with_last_date = len(pd_bars)  # Кол-во бар до удаления на последнюю дату
        last_date = pd_bars.index[-1].date()  # Дата последнего бара
        pd_bars.drop(pd_bars[pd_bars.index.date == last_date].index, inplace=True)  # Удаляем их
        logger.warning(f'Удалено бар на последнюю дату {last_date:{d_format}}: {len_with_last_date - len(pd_bars)}')
    if not four_price_doji:  # Если удаляем дожи 4-х цен
        len_with_doji = len(pd_bars)  # Кол-во бар до удаления дожи
        pd_bars.drop(pd_bars[pd_bars.high == pd_bars.low].index, inplace=True)  # Удаляем их по условия High == Low
        logger.warning(f'Удалено дожи 4-х цен: {len_with_doji - len(pd_bars)}')
    if len(pd_bars) == 0:  # Если нечего объединять
        logger.info('Новых бар нет после удаления')
        return  # то выходим, дальше не продолжаем
    if not file_bars.empty:  # Если файл существует
        pd_bars = pd.concat([file_bars, pd_bars])  # Объединяем файл с данными из Alor
        pd_bars = pd_bars[~pd_bars.index.duplicated(keep='last')]  # Убираем дубликаты самым быстрым методом
        pd_bars.sort_index(inplace=True)  # Сортируем по индексу заново
    pd_bars = pd_bars[['open', 'high', 'low', 'close', 'volume']]  # Отбираем нужные колонки. Дата и время будут экспортированы как индекс
    filename = f'{datapath}{dataname}_{time_frame}.txt'
    logger.info('Сохранение файла')
    pd_bars.to_csv(filename, sep=delimiter, date_format=dt_format)
    logger.info(f'Первый бар    : {pd_bars.index[0]:{dt_format}}')
    logger.info(f'Последний бар : {pd_bars.index[-1]:{dt_format}}')
    logger.info(f'Кол-во бар    : {len(pd_bars)}')
    logger.info(f'В файл {filename} сохранено записей: {len(pd_bars)}')


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    start_time = time()  # Время начала запуска скрипта

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('BarsCSV.log'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=timezone('Europe/Moscow')).timetuple()  # В логе время указываем по МСК
    logging.getLogger('urllib3').setLevel(logging.CRITICAL + 1)  # Пропускаем события запросов

    dataset = [{'broker': default_broker,  # Брокер по умолчанию
                'board': 'SPBFUT',  # Режим торгов
                'symbols': ('USDRUBF', 'EURRUBF', 'CNYRUBF', 'GLDRUBF', 'IMOEXF', 'SBERF', 'GAZPF'),  # Вечные фьючерсы https://www.moex.com/s3581
                'time_frames': ('M15', 'M60', 'D1')},  # ВременнЫе интервалы
               {'broker': default_broker,
                'board': 'TQBR',
                'symbols': ('SBER', 'GAZP'),  # Акции https://www.moex.com/s3122
                'time_frames': ('D1',)}]
    skip_last_date = True  # Не берем бары за дату незавершенной сессии (идут торги)
    # skip_last_date = False  # Берем все бары (торги не идут)

    for row in dataset:  # Пробегаемся по всем строкам набора данных
        broker = row['broker']  # Брокер
        broker_name = broker.__class__.__name__  # Название брокера по его классу
        datapath = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', '..', 'Data', broker_name, '')  # Путь сохранения файла истории
        board = row['board']  # Режим торгов
        symbols = row['symbols']  # Тикеры
        time_frames = row['time_frames']  # ВременнЫе интервалы
        for time_frame in time_frames:  # Пробегаемся по всем временнЫм интервалам
            four_price_doji = time_frame in ('D1', 'M1')  # Для дневных и минутных данных оставляем дожи 4-х цен (все цены одиннаковые)
            for symbol in symbols:  # Пробегаемся по всем тикерам
                save_bars_to_file(broker, datapath, f'{board}.{symbol}', time_frame, skip_last_date=skip_last_date, four_price_doji=four_price_doji)
    for broker in brokers.values():  # Пробегаемся по всем брокерам
        broker.close()  # Закрываем брокера
    logger.info(f'Скрипт выполнен за {(time() - start_time):.2f} с')
