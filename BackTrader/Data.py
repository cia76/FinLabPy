import logging
from time import sleep
from threading import Thread, Event

from backtrader.feed import AbstractDataBase
from backtrader.utils.py3 import with_metaclass
from backtrader import TimeFrame, date2num

from FinLabPy.BackTrader import Store  # Хранилище для BackTrader
from FinLabPy.Schedule.MarketSchedule import Schedule  # Расписание торгов биржи


# noinspection PyMethodParameters
class MetaData(AbstractDataBase.__class__):
    def __init__(cls, name, bases, dct):
        super(MetaData, cls).__init__(name, bases, dct)  # Инициализируем класс данных
        Store.DataCls = cls  # Регистрируем класс данных в хранилище


class Data(with_metaclass(MetaData, AbstractDataBase)):
    """Данные для BackTrader"""
    params = (
        ('four_price_doji', False),  # False - не пропускать дожи 4-х цен ("пустые" бары), True - пропускать
        ('live_bars', False),  # False - только история (для тестов), True - история и новые бары (для реальной торговли)
        ('schedule', None),  # Экземпляр класса расписания. Если указано, то будем запрашивать новые бары из истории по расписанию. Иначе, подписываемся на новые бары
    )
    sleep_time_sec = 1  # Время ожидания в секундах, если не пришел новый бар. Для снижения нагрузки/энергопотребления процессора

    def __init__(self, **kwargs):
        self.store = Store(**kwargs)  # Хранилище BackTrader
        self.logger = logging.getLogger(f'BTData.{self.store.broker.code}')  # Будем вести лог
        self.schedule: Schedule = self.p.schedule  # Расписание
        self.symbol = self.store.broker.get_symbol_by_dataname(self.p.dataname)  # Тикер по названию
        self.time_frame = self._bt_timeframe_to_tf(self.p.timeframe, self.p.compression)  # Конвертируем временной интервал из BackTrader
        self.history_bars = []  # Бары из хранилища и брокера
        self.exit_event = Event()  # Событие выхода из потока подписки на новые бары по расписанию
        self.last_bar_received = False  # Получен последний бар
        self.live_mode = False  # Режим получения бар. False = История, True = Новые бары

    def islive(self) -> bool:
        """Если подаем новые бары, то Cerebro не будет запускать preload и runonce, т.к. новые бары должны идти один за другим"""
        return self.p.live_bars

    def setenvironment(self, env):
        """Добавление хранилища BackTrader в окружение"""
        super(Data, self).setenvironment(env)  # Сохраняем ссылку на окружение в базовом классе
        env.addstore(self.store)  # Добавляем хранилище BackTrader в окружение

    def start(self):
        super(Data, self).start()
        self.put_notification(self.DELAYED)  # Отправляем уведомление об отправке исторических (не новых) бар
        self.history_bars = self.store.broker.get_history(self.symbol, self.time_frame, self.p.fromdate, self.p.todate)  # Получаем исторические бары
        if len(self.history_bars) > 0:  # Если был получен хотя бы 1 бар
            self.put_notification(self.CONNECTED)  # то отправляем уведомление о подключении и начале получения исторических бар
        if not self.p.live_bars:  # Если получаеем только историю
            return  # то подписка на новые бары не нужна. Выходим, дальше не продолжаем
        if self.schedule is None:  # Если получаем новые бары по подписке
            self.logger.debug(f'Запуск получения новыех бар {self.symbol.dataname} {self.time_frame} через подписку')
            self.store.broker.subscribe_history(self.symbol, self.time_frame)
        else:  # Если получаем новые бары по расписанию
            self.logger.debug(f'Запуск получения новыех бар {self.symbol.dataname} {self.time_frame} по расписанию')
            Thread(target=self._schedule_bars_thread).start()  # Создаем и запускаем получение новых бар по расписанию в потоке

    def _load(self) -> bool | None:
        """Загрузка бара из истории или нового бара"""
        if len(self.history_bars) > 0:  # Если есть исторические данные
            bar = self.history_bars.pop(0)  # Берем и удаляем первый бар из хранилища. С ним будем работать
        elif not self.p.live_bars:  # Если получаем только историю (self.history_bars) и исторических данных нет / все исторические данные получены
            self.put_notification(self.DISCONNECTED)  # Отправляем уведомление об окончании получения исторических бар
            self.logger.debug('Бары из файла/истории отправлены в ТС. Новые бары получать не нужно. Выход')
            return False  # Больше сюда заходить не будем
        else:  # Если получаем историю и новые бары (self.store.new_bars)
            new_bars = [new_bar for new_bar in self.store.new_bars if new_bar.symbol == self.symbol and new_bar.time_frame == self.time_frame]  # Получаем новые бары из хранилища по guid
            if len(new_bars) == 0:  # Если новый бар еще не появился
                # self.logger.debug(f'Новых бар нет. Ожидание {self.sleep_time_sec} с')  # Для отладки. Грузит процессор
                sleep(self.sleep_time_sec)  # Ждем для снижения нагрузки/энергопотребления процессора
                return None  # то нового бара нет, будем заходить еще
            self.last_bar_received = len(new_bars) == 1  # Если в хранилище остался 1 бар, то мы будем получать последний возможный бар
            if self.last_bar_received:  # Получаем последний возможный бар
                self.logger.debug('Получение последнего возможного на данный момент бара')
            bar = new_bars[0]  # Берем первый бар из хранилища новых бар. С ним будем работать
            self.store.new_bars.remove(bar)  # Удаляем этот бар из хранилища новых бар
            if self.last_bar_received and not self.live_mode:  # Если получили последний бар и еще не находимся в режиме получения новых бар (LIVE)
                self.put_notification(self.LIVE)  # Отправляем уведомление о получении новых бар
                self.live_mode = True  # Переходим в режим получения новых бар (LIVE)
            elif self.live_mode and not self.last_bar_received:  # Если находимся в режиме получения новых бар (LIVE)
                self.put_notification(self.DELAYED)  # Отправляем уведомление об отправке исторических (не новых) бар
                self.live_mode = False  # Переходим в режим получения истории
        if bar.high == bar.low:  # Если пришел бар дожи 4-х цен
            self.logger.debug(f'Бар {bar} - дожи 4-х цен')
            if not self.p.four_price_doji:  # Если не пропускаем дожи 4-х цен
                return None  # то нового бара нет, будем заходить еще
        self.lines.datetime[0] = date2num(bar.datetime)  # Переводим в формат хранения даты/времени в BackTrader
        self.lines.open[0] = bar.open
        self.lines.high[0] = bar.high
        self.lines.low[0] = bar.low
        self.lines.close[0] = bar.close
        self.lines.volume[0] = bar.volume
        self.lines.openinterest[0] = 0  # Открытый интерес не учитывается
        return True  # Будем заходить сюда еще

    def stop(self):
        super(Data, self).stop()
        if self.p.live_bars:  # Если была подписка/расписание
            if self.schedule is not None:  # Если получаем новые бары по расписанию
                self.logger.info(f'Отмена подписки по расписанию на новые бары {self.symbol.dataname} {self.time_frame}')
                self.exit_event.set()  # то отменяем расписание
            else:  # Если получаем новые бары по подписке
                self.logger.info(f'Отмена подписки {self.guid} на новые бары {self.symbol.dataname} {self.time_frame}')
                self.store.broker.unsubscribe_history(self.symbol, self.time_frame)  # то отменяем подписку
            self.put_notification(self.DISCONNECTED)  # Отправляем уведомление об окончании получения новых бар
        self.store.DataCls = None  # Удаляем класс данных в хранилище

    # Внутренние функции

    def _schedule_bars_thread(self) -> None:
        """Поток получения новых бар по расписанию"""
        while True:  # Работаем пока не придет пустое значение или событие отмены
            trade_bar_open_datetime = self.schedule.trade_bar_open_datetime(self.schedule.market_datetime_now, self.time_frame)  # Дата и время открытия бара, который будем получать
            trade_bar_request_datetime = self.schedule.trade_bar_request_datetime(self.schedule.market_datetime_now, self.time_frame)  # Дата и время запроса бара
            wait_seconds = (trade_bar_request_datetime - self.schedule.market_datetime_now).total_seconds()  # Кол-во секунд до запроса последнего бара
            self.logger.debug(f'Время до запроса бара {self.symbol.dataname} {self.time_frame} {wait_seconds} с')
            exit_event_set = self.exit_event.wait(wait_seconds)  # Ждем до запроса следующего бара или до отмены
            if exit_event_set:  # Если отмена
                self.logger.debug(f'Отмена. Выход из потока очереди бар {self.symbol.dataname} {self.time_frame}')
                return  # то выходим из потока, дальше не продолжаем
            bars = self.store.broker.get_history(self.symbol, self.time_frame, trade_bar_open_datetime)  # Получаем бар когда наступит дата и время запроса
            if bars is None:  # Если бар не получен
                self.logger.warning(f'Бар {self.symbol.dataname} {self.time_frame} по расписанию на {trade_bar_open_datetime} не получен')
            else:  # Если бар получен
                self.store.new_bars.append(bars[0])  # то добавляем его в хранилище новых бар

    @staticmethod
    def _bt_timeframe_to_tf(timeframe, compression=1) -> str:
        """Перевод временнОго интервала из BackTrader для имени файла истории и расписания https://ru.wikipedia.org/wiki/Таймфрейм

        :param TimeFrame timeframe: Временной интервал
        :param int compression: Размер временнОго интервала
        :return: Временной интервал для имени файла истории и расписания
        """
        if timeframe == TimeFrame.Minutes:  # Минутный временной интервал
            return f'M{compression}'
        # Часовой график f'H{compression}' заменяем минутным. Пример: H1 = M60
        elif timeframe == TimeFrame.Days:  # Дневной временной интервал
            return 'D1'
        elif timeframe == TimeFrame.Weeks:  # Недельный временной интервал
            return 'W1'
        elif timeframe == TimeFrame.Months:  # Месячный временной интервал
            return 'MN1'
        elif timeframe == TimeFrame.Years:  # Годовой временной интервал
            return 'Y1'
        raise NotImplementedError  # С остальными временнЫми интервалами не работаем
