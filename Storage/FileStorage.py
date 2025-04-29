import logging
import os.path

import pandas as pd

from FinLabPy.Core import Storage, Bar, bars_to_df  # Хранилище, бар, перевод бар в pandas DataFrame


class FileStorage(Storage):
    """Файловое хранилище"""
    logger = logging.getLogger('FileStorage')  # Будем вести лог
    delimiter = '\t'  # Разделитель значений в файле истории. По умолчанию табуляция
    dt_format = '%d.%m.%Y %H:%M'  # Формат представления даты и времени в файле истории. По умолчанию русский формат

    def __init__(self, source):
        super().__init__(source)
        self.datapath = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'Data', source, '')  # Путь сохранения файлов

    def get_bars(self, symbol, time_frame, dt_from=None, dt_to=None):
        filename = f'{self.datapath}{symbol.dataname}_{time_frame}.txt'  # Полное имя файла
        if not os.path.isfile(filename):  # Если файл не существует
            self.logger.warning(f'Файл {filename} не найден')
            return None  # то выходим, дальше не продолжаем
        self.logger.debug(f'Получение файла {filename}')
        file_bars = pd.read_csv(  # Импортируем бары из CSV файла в pandas DataFrame
            filename,  # Имя файла
            sep=self.delimiter,  # Разделитель значений
            usecols=['datetime', 'open', 'high', 'low', 'close', 'volume'],  # Для ускорения обработки задаем названия колонок
            parse_dates=['datetime'],  # Колонку datetime разбираем как дату/время
            dayfirst=True,  # В дате/времени сначала идет день, затем месяц и год
            index_col='datetime')  # Индексом будет колонка datetime
        self.logger.debug(f'Первый бар    : {file_bars.index[0]:{self.dt_format}}')
        self.logger.debug(f'Последний бар : {file_bars.index[-1]:{self.dt_format}}')
        self.logger.debug(f'Кол-во бар    : {len(file_bars)}')
        bars: list[Bar] = []
        for index, row in file_bars.iterrows():  # Пробегаемся по всем полученным барам
            if dt_from is not None and index < dt_from:  # Если задана дата/время начала выборки, и она больше даты/времени текущего бара
                continue  # то переходим к следующему бару, дальше не продолжаем
            if dt_to is not None and index > dt_to:  # Если задана дата/время окончания выборки, и она меньше даты/времени текущего бара
                continue  # то переходим к следующему бару, дальше не продолжаем
            bars.append(Bar(symbol.board, symbol.symbol, symbol.dataname, time_frame, index, row['open'], row['high'], row['low'], row['close'], row['volume']))
        if len(bars) == 0:  # Если бары не получены
            self.logger.debug(f'Бары отстутствуют')
            return None  # то выходим, дальше не продолжаем
        if dt_from is not None or dt_to is not None:  # Если задан фильтр с ... по ...
            self.logger.debug(f'Фильтр с {dt_from:{self.dt_format}} по {dt_to:{self.dt_format}}')
            self.logger.debug(f'Первый бар    : {bars[0]}')
            self.logger.debug(f'Последний бар : {bars[-1]}')
            self.logger.debug(f'Кол-во бар    : {len(bars)}')
        return bars

    def set_bars(self, bars):
        if len(bars) == 0:  # Если бар нет
            return  # то выходим, дальше не продолжаем
        pd_bars = bars_to_df(bars)  # Переводим бары в pandas DataFrame
        symbol = self.get_symbol(bars[0].symbol)  # Спецификация тикера по первому бару
        time_frame = bars[0].time_frame  # Временной интервал по первому бару
        file_bars = self.get_bars(symbol, time_frame)  # Все бары из файла
        if file_bars is not None:  # Если в файле есть бары
            pd_file_bars = bars_to_df(file_bars)  # Переводим бары в pandas DataFrame
            pd_bars = pd.concat([pd_file_bars, pd_bars])  # Объединяем бары
            pd_bars = pd_bars[~pd_bars.index.duplicated(keep='last')]  # Убираем дубликаты самым быстрым методом
            pd_bars.sort_index(inplace=True)  # Сортируем по индексу заново
        pd_bars = pd_bars[['open', 'high', 'low', 'close', 'volume']]  # Отбираем нужные колонки. Дата и время будут экспортированы как индекс
        filename = f'{self.datapath}{symbol.dataname}_{time_frame}.txt'  # Полное имя файла
        self.logger.debug(f'Сохранение файла {filename}')
        pd_bars.to_csv(filename, sep=self.delimiter, date_format=self.dt_format)  # Экспортируем бары из pandas DataFrame в CSV файл
        self.logger.debug(f'Первый бар    : {pd_bars.index[0]:{self.dt_format}}')
        self.logger.debug(f'Последний бар : {pd_bars.index[-1]:{self.dt_format}}')
        self.logger.debug(f'Кол-во бар    : {len(pd_bars)}')
