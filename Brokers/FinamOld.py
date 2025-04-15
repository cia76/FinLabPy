# Курс Мультиброкер: Контроль https://finlab.vip/wpm-category/mbcontrol/

from datetime import datetime, timedelta, timezone

from FinLabPy.Core import Broker, Bar, Position, Order, Symbol  # Брокер, бар, позиция, заявка, тикер
from FinamPy import FinamPyOld  # Работа с сервером TRANSAQ из Python через REST/gRPC


class Finam(Broker):
    """Брокер Финам"""
    min_bar_open_utc = datetime(1990, 1, 1, tzinfo=timezone.utc)  # Дата, когда никакой тикер еще не торговался

    def __init__(self, code: str, name: str, provider: FinamPyOld, account_id: int = 0):
        super().__init__(code, name, provider, account_id)
        self.provider = provider  # Уже инициирован в базовом классе. Выполням для того, чтобы работать с типом провайдера
        self.client_id = self.provider.client_ids[account_id]  # Номер счета по порядковому номеру
        self.symbols = self.provider.symbols  # Получаем справочник всех тикеров из провайдера

    def get_symbol_by_dataname(self, dataname: str):
        board, symbol = self.provider.dataname_to_board_symbol(dataname)  # Код режима торгов Финама и тикер из названия тикера
        if not board:  # Если код режима торгов не найден
            print(f'Код режима торгов тикера {dataname} не найден')
            return None  # То выходим, дальше не продолжаем
        si = next((item for item in self.symbols.securities if item.board == board and item.code == symbol), None)  # Поиск тикера по режиму торгов
        if not si:  # Если тикер не найден
            print(f'Информация о тикере {dataname} не найдена')
            return None  # То выходим, дальше не продолжаем
        if board == 'FUT':  # Для фьючерсов
            board = 'SPBFUT'  # Меняем код режима торгов Финам на канонический
        elif board == 'OPT':  # Для опционов
            board = 'SPBOPT'  # Меняем код режима торгов Финам на канонический
        return Symbol(board, symbol, dataname, si.short_name, si.decimals, si.min_step, si.lot_size)

    def get_history(self, dataname: str, tf: str, dt_from: datetime = None, dt_to: datetime = None):
        board, symbol = self.provider.dataname_to_board_symbol(dataname)  # Код режима торгов Финама и тикер из названия тикера
        time_frame, intraday = self.provider.timeframe_to_finam_timeframe(tf)  # Временной интервал Finam, внутридневной интервал
        td = timedelta(days=(30 if intraday else 365))  # Максимальный запрос за 30 дней для внутридневных интервалов и 1 год (365 дней) для дневных и выше
        interval = self.provider.proto_candles.IntradayCandleInterval(count=500) if intraday else self.provider.proto_candles.DayCandleInterval(count=500)  # Нужно поставить максимальное кол-во бар. Максимум, можно поставить 500
        todate_utc = datetime.utcnow().replace(tzinfo=timezone.utc)  # Будем получать бары до текущей даты и времени UTC
        from_ = getattr(interval, 'from')  # Т.к. from - ключевое слово в Python, то получаем атрибут from из атрибута интервала
        to_ = getattr(interval, 'to')  # Аналогично будем работать с атрибутом to для единообразия
        first_request = dt_from is None  # Если не задана дата начала, то первый запрос будем формировать без даты окончания. Так мы в первом запросе получим первые бары истории
        next_bar_open_utc = self.min_bar_open_utc if first_request else self.provider.msk_to_utc_datetime(dt_from, True)  # Дата и время начала интервала UTC
        bars = []  # Список полученных бар
        while True:  # Будем получать бары пока не получим все
            todate_min_utc = min(todate_utc, next_bar_open_utc + td)  # До какой даты можем делать запрос
            if intraday:  # Для интрадея datetime -> Timestamp
                from_.seconds = int(next_bar_open_utc.timestamp())  # Дата и время начала интервала UTC
                if not first_request:  # Для всех запросов, кроме первого
                    to_.seconds = int(todate_min_utc.timestamp())  # Дата и время окончания интервала UTC
                    if from_.seconds == to_.seconds:  # Если дата и время окончания интервала совпадает с датой и временем начала
                        break  # то выходим из цикла получения бар
            else:  # Для дневных интервалов и выше datetime -> Date
                date_from = self.provider.google_date(year=next_bar_open_utc.year, month=next_bar_open_utc.month, day=next_bar_open_utc.day)  # Дата начала интервала UTC
                from_.year = date_from.year
                from_.month = date_from.month
                from_.day = date_from.day
                if not first_request:  # Для всех запросов, кроме первого
                    date_to = self.provider.google_date(year=todate_min_utc.year, month=todate_min_utc.month, day=todate_min_utc.day)  # Дата окончания интервала UTC
                    if date_to == date_from:  # Если дата окончания интервала совпадает с датой начала
                        break  # то выходим из цикла получения бар
                    to_.year = date_to.year
                    to_.month = date_to.month
                    to_.day = date_to.day
            if first_request:  # Для первого запроса
                first_request = False  # далее будем ставить в запросы дату окончания интервала
            candles = (self.provider.get_intraday_candles(board, symbol, time_frame, interval) if intraday else
                       self.provider.get_day_candles(board, symbol, time_frame, interval))  # Получаем ответ на запрос бар с режимом торгов Финам
            if not candles:  # Если бары не получены
                print('Ошибка при получении истории: История не получена')
                return None  # то выходим, дальше не продолжаем
            candles_dict = self.provider.message_to_dict(candles, always_print_fields_with_no_presence=True)  # Переводим в словарь из JSON
            if 'candles' not in candles_dict:  # Если бар нет в словаре
                print(f'Ошибка при получении истории: {candles_dict}')
                return None  # то выходим, дальше не продолжаем
            new_bars_dict = candles_dict['candles']  # Получаем все бары из Finam
            if len(new_bars_dict) > 0:  # Если пришли новые бары
                # Дату/время UTC получаем в формате ISO 8601. Пример: 2023-06-16T20:01:00Z
                # В статье https://stackoverflow.com/questions/127803/how-do-i-parse-an-iso-8601-formatted-date описывается проблема, что Z на конце нужно убирать
                first_bar_open_dt = self.provider.utc_to_msk_datetime(
                    datetime.fromisoformat(new_bars_dict[0]['timestamp'][:-1])) if intraday else \
                    datetime(new_bars_dict[0]['date']['year'], new_bars_dict[0]['date']['month'], new_bars_dict[0]['date']['day'])  # Дату и время первого полученного бара переводим из UTC в МСК
                last_bar_open_dt = self.provider.utc_to_msk_datetime(
                    datetime.fromisoformat(new_bars_dict[-1]['timestamp'][:-1])) if intraday else \
                    datetime(new_bars_dict[-1]['date']['year'], new_bars_dict[-1]['date']['month'], new_bars_dict[-1]['date']['day'])  # Дату и время последнего полученного бара переводим из UTC в МСК
                print(f'Получены бары с {first_bar_open_dt} по {last_bar_open_dt}')
                for new_bar in new_bars_dict:  # Пробегаемся по всем полученным барам
                    dt = self.provider.utc_to_msk_datetime(
                        datetime.fromisoformat(new_bar['timestamp'][:-1])) if intraday else \
                        datetime(new_bar['date']['year'], new_bar['date']['month'], new_bar['date']['day'])  # Дату и время переводим из UTC в МСК
                    open_ = self.provider.dict_decimal_to_float(new_bar['open'])
                    high = self.provider.dict_decimal_to_float(new_bar['high'])
                    low = self.provider.dict_decimal_to_float(new_bar['low'])
                    close = self.provider.dict_decimal_to_float(new_bar['close'])
                    volume = int(new_bar['volume'])  # Объем в штуках
                    bars.append(Bar(board, symbol, dataname, tf, dt, open_, high, low, close, volume))  # Добавляем бар
                last_bar_open_utc = self.provider.msk_to_utc_datetime(last_bar_open_dt, True) if intraday else last_bar_open_dt.replace(tzinfo=timezone.utc)  # Дата и время открытия последнего бара UTC
                next_bar_open_utc = last_bar_open_utc + timedelta(minutes=1) if intraday else last_bar_open_utc + timedelta(days=1)  # Смещаем время на возможный следующий бар UTC
            else:  # Если новых бар нет
                next_bar_open_utc = todate_min_utc + timedelta(minutes=1) if intraday else todate_min_utc + timedelta(days=1)  # то смещаем время на возможный следующий бар UTC
            if next_bar_open_utc > todate_utc:  # Если пройден весь интервал
                break  # то выходим из цикла получения бар
        if len(bars) == 0:  # Если новых бар нет
            print('Новых записей нет')
            return None  # то выходим, дальше не продолжаем
        return bars

    def get_last_price(self, dataname: str):
        si = self.get_symbol_by_dataname(dataname)  # Тикер по названию
        tommorrow = datetime.today() + timedelta(days=1)  # Завтрашняя дата
        last_bar = self.provider.get_day_candles(
            si.board, si.symbol, self.provider.proto_candles.DAYCANDLE_TIMEFRAME_D1,
            self.provider.proto_candles.DayCandleInterval(to=self.provider.google_date(year=tommorrow.year, month=tommorrow.month, day=tommorrow.day), count=1))  # Последний бар (до завтра)
        return self.provider.decimal_to_float(last_bar.candles[0].close)  # Последняя цена сделки

    def get_value(self):
        response = self.provider.get_portfolio(self.client_id)  # Портфель по счету
        try:  # Пытаемся получить стоимость позиций
            return round(response.equity - self.get_cash(), 2)  # Стоимость позиций = Общая стоимость портфеля - Свободные средства
        except AttributeError:  # Если сервер отключен, то стоимость позиций не придет
            return 0  # Выдаем пустое значение. Получим стоимость позиций когда сервер будет работать

    def get_cash(self):
        cash = 0  # Будем набирать свободные средства по всем валютам с конвертацией в рубли
        response = self.provider.get_portfolio(self.client_id)  # Портфель по счету
        try:  # Пытаемся получить свободные средства
            for money in response.money:  # Пробегаемся по всем свободным средствам в валютах
                cross_rate = next(item.cross_rate for item in response.currencies if item.name == money.currency)  # Кол-во рублей за единицу валюты
                cash += money.balance * cross_rate  # Переводим в рубли и добавляем к свободным средствам
            return round(cash, 2)
        except AttributeError:  # Если сервер отключен, то свободные средства не придут
            return 0  # Выдаем пустое значение. Получим свободные средства когда сервер будет работать

    def get_positions(self):
        self.positions = []  # Текущие позиции
        portfolio = self.provider.get_portfolio(self.client_id, include_money=False)  # Получаем позиции без денежных позиций
        try:  # Пытаемся получить позиции
            for position in portfolio.positions:  # Пробегаемся по всем текущим позициям счета
                si = next(item for item in self.symbols.securities if item.market == position.market and item.code == position.security_code)  # Поиск тикера по рынку (не по площадке)
                cross_rate = next(item.cross_rate for item in portfolio.currencies if item.name == position.currency)  # Кол-во рублей за единицу валюты
                self.positions.append(Position(  # Добавляем текущую позицию в список
                    self,  # Брокер
                    self.provider.board_symbol_to_dataname(si.board, si.code),  # Название тикера
                    si.short_name,  # Описание тикера
                    si.decimals,  # Кол-во десятичных знаков в цене
                    int(position.balance),  # Кол-во в штуках
                    self.provider.finam_price_to_price(si.board, si.code, position.average_price * cross_rate),  # Средняя цена входа в рублях
                    self.provider.finam_price_to_price(si.board, si.code, position.current_price * cross_rate)))  # Последняя цена в рублях
        except (AttributeError, StopIteration):  # Если сервер отключен, то позиции/тикер не придут
            pass  # Игнорируем ошибку. Получим позиции когда сервер будет работать
        return self.positions

    def get_orders(self):
        self.orders = []  # Активные заявки
        orders = self.provider.get_orders(self.client_id, False, False, True)  # Получаем только активные заявки
        for order in orders.orders:  # Пробегаемся по всем заявкам
            si = next(item for item in self.symbols.securities if item.board == order.security_board and item.code == order.security_code)  # Поиск тикера по площадке
            self.orders.append(Order(  # Добавляем заявки в список
                self,  # Брокер
                order.transaction_id,  # Уникальный код заявки (номер транзакции)
                order.buy_sell == self.provider.proto_common.BUY_SELL_BUY,  # Покупка/продажа
                Order.Limit if order.price else Order.Market,  # Лимит/по рынку
                self.provider.board_symbol_to_dataname(si.board, si.code),  # Название тикера
                si.decimals,  # Кол-во десятичных знаков в цене
                order.quantity * si.lot_size,  # Кол-во в штуках
                self.provider.finam_price_to_price(si.board, si.code, order.price)))  # Цена
        stop_orders = self.provider.get_stops(self.client_id, False, False, True)  # Получаем только активные стоп заявки
        for order in stop_orders.stops:  # Пробегаемся по всем стоп заявкам
            si = next(item for item in self.symbols.securities if item.board == order.security_board and item.code == order.security_code)  # Поиск тикера по площадке
            if order.stop_loss.activation_price:  # Если выставлен Stop Loss
                self.orders.append(Order(  # Добавляем заявки в список
                    self,  # Брокер
                    order.stop_id,  # Уникальный код заявки (идентификатор стоп заявки)
                    order.buy_sell == self.provider.proto_common.BUY_SELL_BUY,  # Покупка/продажа
                    Order.StopLimit if order.stop_loss.price else Order.Stop,  # Стоп-лимит/стоп
                    self.provider.board_symbol_to_dataname(si.board, si.code),  # Название тикера
                    si.decimals,  # Кол-во десятичных знаков в цене
                    order.stop_loss.quantity.value * si.lot_size,  # Кол-во в штуках
                    self.provider.finam_price_to_price(si.board, si.code, order.stop_loss.activation_price)))  # Цена (активации)
            if order.take_profit.activation_price:  # Если выставлен Take Profit
                self.orders.append(Order(  # Добавляем заявки в список
                    self,  # Брокер
                    order.stop_id,  # Уникальный код заявки (идентификатор стоп заявки)
                    order.buy_sell == self.provider.proto_common.BUY_SELL_BUY,  # Покупка/продажа
                    Order.Stop,  # Стоп
                    self.provider.board_symbol_to_dataname(si.board, si.code),  # Название тикера
                    si.decimals,  # Кол-во десятичных знаков в цене
                    order.take_profit.quantity.value * si.lot_size,  # Кол-во в штуках
                    self.provider.finam_price_to_price(si.board, si.code, order.take_profit.activation_price)))  # Цена (активации)
        return self.orders

    def new_order(self, order: Order):
        board, symbol = self.provider.dataname_to_board_symbol(order.dataname)  # Код режима торгов Финама и тикер из названия тикера
        buy_sell = self.provider.proto_common.BUY_SELL_BUY if order.buy else self.provider.proto_common.BUY_SELL_SELL  # Покупка или продажа
        si = next(item for item in self.symbols.securities if item.board == board and item.code == symbol)  # Поиск тикера по площадке
        quantity_lots = order.quantity // si.lot_size  # Кол-во в лотах
        price = 0 if order.exec_type == Order.Market else self.provider.price_to_finam_price(si.board, si.code, order.price)  # Для рыночной заявки цену не ставим
        stop_price = self.provider.price_to_finam_price(si.board, si.code, order.stop_price)  # Стоп цена
        if order.exec_type in (Order.Market, Order.Limit):  # Рыночная или лимитная заявка
            self.provider.new_order(self.client_id, board, symbol, buy_sell, quantity_lots, price=price)  # Новая заявка
        else:  # Стоп или стоп-лимитная заявка
            market_price = order.exec_type == Order.Stop  # При срабатывании стоп заявки будет выставлена рыночная заявка
            quantity = self.provider.proto_stops.StopQuantity(value=quantity_lots, units=self.provider.proto_stops.StopQuantityUnits.STOP_QUANTITY_UNITS_LOTS)  # Кол-во в лотах
            stop_loss = self.provider.proto_stops.StopLoss(activation_price=stop_price, market_price=market_price, price=price, quantity=quantity)  # Стоп заявка
            self.provider.new_stop(self.client_id, board, symbol, buy_sell, stop_loss)  # Новая стоп заявка

    def cancel_order(self, order: Order):
        if order.exec_type in (Order.Market, Order.Limit):  # Заявка
            self.provider.cancel_order(self.client_id, int(order.id))  # Отменяем заявку по номеру транзакции
        else:  # Стоп заявка
            self.provider.cancel_stop(self.client_id, int(order.id))  # Отменяем стоп заявку по идентификатору стоп заявки

    def close(self):
        self.provider.close_channel()  # Закрываем канал перед выходом
