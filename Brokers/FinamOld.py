# Курс Мультиброкер: Контроль https://finlab.vip/wpm-category/mbcontrol/

from datetime import datetime, timedelta, timezone
from typing import Union  # Объединение типов

from FinLabPy.Core import Broker, Bar, Position, Order, Symbol  # Брокер, бар, позиция, заявка, тикер
from FinamPy import FinamPyOld  # Работа с сервером TRANSAQ из Python через REST/gRPC


class Finam(Broker):
    """Брокер Финам"""
    min_bar_open_utc = datetime(1990, 1, 1, tzinfo=timezone.utc)  # Дата, когда никакой тикер еще не торговался

    def __init__(self, code, name, provider: FinamPyOld, account_id=0, storage='file'):
        super().__init__(code, name, provider, account_id, storage)
        self.provider = provider  # Уже инициирован в базовом классе. Выполням для того, чтобы работать с типом провайдера
        self.client_id = self.provider.client_ids[account_id]  # Номер счета по порядковому номеру

    def _get_symbol_info(self, board_market: str, symbol: str) -> Union[Symbol, None]:
        si = self.provider.get_symbol_info(board_market, symbol)  # Получаем спецификацию тикера из Финам
        if si is None:  # Если информация о тикере не найдена
            print(f'Информация о тикере {board_market}.{symbol} не найдена')
            return None  # то выходим, дальше не продолжаем
        board = self.provider.finam_board_to_board(si.board)  # Канонический код режима торгов
        dataname = self.provider.finam_board_symbol_to_dataname(si.board, si.ticker)  # Название тикера
        symbol = Symbol(board, si.ticker, dataname, si.name, si.decimals, si.min_step, si.lot_size)
        self.storage.set_symbol(symbol)  # Добавляем спецификацию тикера в хранилище
        return symbol

    def get_symbol_by_dataname(self, dataname):
        symbol = self.storage.get_symbol(dataname)  # Проверяем, есть ли спецификация тикера в хранилище
        if symbol is not None:  # Если есть тикер
            return symbol  # то возвращаем его, дальше не продолжаем
        finam_board, finam_symbol = self.provider.dataname_to_finam_board_symbol(dataname)  # Режим торгов и тикер Финам
        return self._get_symbol_info(finam_board, finam_symbol)

    def get_history(self, symbol, time_frame, dt_from=None, dt_to=None):
        bars = super().get_history(symbol, time_frame, dt_from, dt_to)  # Получаем бары из хранилища
        finam_time_frame, intraday = self.provider.timeframe_to_finam_timeframe(time_frame)  # Временной интервал Финам, внутридневной интервал
        if bars is None:  # Если бары из хранилища не получены
            bars = []  # Пока список полученных бар пустой
            next_bar_open_utc = self.min_bar_open_utc if dt_from is None else self.provider.msk_to_utc_datetime(dt_from, True)  # Первый возможный бар по UTC
        else:  # Если бары из хранилища получены
            last_bar_open_msk = bars[-1].datetime  # Дата и время открытия последнего бара
            next_bar_open_utc = self.provider.msk_to_utc_datetime(last_bar_open_msk, True) if intraday else last_bar_open_msk.replace(tzinfo=timezone.utc)  # Дата и время последнего полученого бара из хранилища по UTC
            del bars[-1]  # Этот бар удалим из выборки хранилища. Возможно, он был несформированный
        finam_board, finam_symbol = self.provider.dataname_to_finam_board_symbol(symbol.dataname)  # Режим торгов и тикер Финам
        interval = self.provider.proto_candles.IntradayCandleInterval(count=500) if intraday else self.provider.proto_candles.DayCandleInterval(count=500)  # Нужно поставить максимальное кол-во бар. Максимум, можно поставить 500
        from_ = getattr(interval, 'from')  # Т.к. from - ключевое слово в Python, то получаем атрибут from из атрибута интервала
        while True:  # Будем получать бары пока не получим все
            if intraday:  # Для интрадея datetime -> Timestamp
                from_.seconds = int(next_bar_open_utc.timestamp())  # Дата и время начала интервала UTC
            else:  # Для дневных интервалов и выше datetime -> Date
                date_from = self.provider.google_date(year=next_bar_open_utc.year, month=next_bar_open_utc.month, day=next_bar_open_utc.day)  # Дата начала интервала UTC
                from_.year = date_from.year
                from_.month = date_from.month
                from_.day = date_from.day
            candles = (self.provider.get_intraday_candles(finam_board, finam_symbol, finam_time_frame, interval) if intraday else
                       self.provider.get_day_candles(finam_board, finam_symbol, finam_time_frame, interval))  # Получаем ответ на запрос бар с режимом торгов Финам
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
                first_bar_open_msk = self.provider.utc_to_msk_datetime(
                    datetime.fromisoformat(new_bars_dict[0]['timestamp'][:-1])) if intraday else \
                    datetime(new_bars_dict[0]['date']['year'], new_bars_dict[0]['date']['month'], new_bars_dict[0]['date']['day'])  # Дату и время первого полученного бара переводим из UTC в МСК
                last_bar_open_msk = self.provider.utc_to_msk_datetime(
                    datetime.fromisoformat(new_bars_dict[-1]['timestamp'][:-1])) if intraday else \
                    datetime(new_bars_dict[-1]['date']['year'], new_bars_dict[-1]['date']['month'], new_bars_dict[-1]['date']['day'])  # Дату и время последнего полученного бара переводим из UTC в МСК
                print(f'Получены бары с {first_bar_open_msk} по {last_bar_open_msk}')
                for new_bar in new_bars_dict:  # Пробегаемся по всем полученным барам
                    dt = self.provider.utc_to_msk_datetime(
                        datetime.fromisoformat(new_bar['timestamp'][:-1])) if intraday else \
                        datetime(new_bar['date']['year'], new_bar['date']['month'], new_bar['date']['day'])  # Дату и время переводим из UTC в МСК
                    open_ = self.provider.dict_decimal_to_float(new_bar['open'])
                    high = self.provider.dict_decimal_to_float(new_bar['high'])
                    low = self.provider.dict_decimal_to_float(new_bar['low'])
                    close = self.provider.dict_decimal_to_float(new_bar['close'])
                    volume = int(new_bar['volume'])  # Объем в штуках
                    bars.append(Bar(symbol.board, symbol.symbol, symbol.dataname, time_frame, dt, open_, high, low, close, volume))  # Добавляем бар
                last_bar_open_utc = self.provider.msk_to_utc_datetime(last_bar_open_msk, True) if intraday else last_bar_open_msk.replace(tzinfo=timezone.utc)  # Дата и время открытия последнего бара UTC
                next_bar_open_utc = last_bar_open_utc + timedelta(minutes=1) if intraday else last_bar_open_utc + timedelta(days=1)  # Смещаем время на возможный следующий бар UTC
            else:  # Если новых бар нет
                break  # то выходим из цикла получения бар
        if len(bars) == 0:  # Если новых бар нет
            print('Новых записей нет')
            return None  # то выходим, дальше не продолжаем
        self.storage.set_bars(bars)  # Сохраняем бары в хранилище
        return bars

    def get_last_price(self, dataname):
        symbol = self.get_symbol_by_dataname(dataname)  # Тикер по названию
        if symbol is None:  # Если тикер не получен
            return None  # то выходим, дальше не продолжаем
        tommorrow = datetime.today() + timedelta(days=1)  # Завтрашняя дата
        last_bar = self.provider.get_day_candles(
            symbol.board, symbol.symbol, self.provider.proto_candles.DAYCANDLE_TIMEFRAME_D1,
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
                symbol = self._get_symbol_info(position.market, position.security_code)  # Спецификация тикера по бирже и тикеру Финама
                cross_rate = next(item.cross_rate for item in portfolio.currencies if item.name == position.currency)  # Кол-во рублей за единицу валюты
                self.positions.append(Position(  # Добавляем текущую позицию в список
                    self,  # Брокер
                    symbol.dataname,  # Название тикера
                    symbol.description,  # Описание тикера
                    symbol.decimals,  # Кол-во десятичных знаков в цене
                    int(position.balance),  # Кол-во в штуках
                    self.provider.finam_price_to_price(symbol.board, symbol.symbol, position.average_price * cross_rate),  # Средняя цена входа в рублях
                    self.provider.finam_price_to_price(symbol.board, symbol.symbol, position.current_price * cross_rate)))  # Последняя цена в рублях
        except (AttributeError, StopIteration):  # Если сервер отключен, то позиции/тикер не придут
            pass  # Игнорируем ошибку. Получим позиции когда сервер будет работать
        return self.positions

    def get_orders(self):
        self.orders = []  # Активные заявки
        orders = self.provider.get_orders(self.client_id, False, False, True)  # Получаем только активные заявки
        for order in orders.orders:  # Пробегаемся по всем заявкам
            symbol = self._get_symbol_info(order.security_board, order.security_code)  # Спецификация тикера по бирже и тикеру Финама
            self.orders.append(Order(  # Добавляем заявки в список
                self,  # Брокер
                order.transaction_id,  # Уникальный код заявки (номер транзакции)
                order.buy_sell == self.provider.proto_common.BUY_SELL_BUY,  # Покупка/продажа
                Order.Limit if order.price else Order.Market,  # Лимит/по рынку
                symbol.dataname,  # Название тикера
                symbol.decimals,  # Кол-во десятичных знаков в цене
                order.quantity * symbol.lot_size,  # Кол-во в штуках
                self.provider.finam_price_to_price(symbol.board, symbol.symbol, order.price)))  # Цена
        stop_orders = self.provider.get_stops(self.client_id, False, False, True)  # Получаем только активные стоп заявки
        for stop_order in stop_orders.stops:  # Пробегаемся по всем стоп заявкам
            symbol = self._get_symbol_info(stop_order.security_board, stop_order.security_code)  # Спецификация тикера по бирже и тикеру Финама
            if stop_order.stop_loss.activation_price:  # Если выставлен Stop Loss
                self.orders.append(Order(  # Добавляем заявки в список
                    self,  # Брокер
                    stop_order.stop_id,  # Уникальный код заявки (идентификатор стоп заявки)
                    stop_order.buy_sell == self.provider.proto_common.BUY_SELL_BUY,  # Покупка/продажа
                    Order.StopLimit if stop_order.stop_loss.price else Order.Stop,  # Стоп-лимит/стоп
                    symbol.dataname,  # Название тикера
                    symbol.decimals,  # Кол-во десятичных знаков в цене
                    stop_order.stop_loss.quantity.value * symbol.lot_size,  # Кол-во в штуках
                    self.provider.finam_price_to_price(symbol.board, symbol.symbol, stop_order.stop_loss.activation_price)))  # Цена (активации)
            if stop_order.take_profit.activation_price:  # Если выставлен Take Profit
                self.orders.append(Order(  # Добавляем заявки в список
                    self,  # Брокер
                    stop_order.stop_id,  # Уникальный код заявки (идентификатор стоп заявки)
                    stop_order.buy_sell == self.provider.proto_common.BUY_SELL_BUY,  # Покупка/продажа
                    Order.Stop,  # Стоп
                    symbol.dataname,  # Название тикера
                    symbol.decimals,  # Кол-во десятичных знаков в цене
                    stop_order.take_profit.quantity.value * symbol.lot_size,  # Кол-во в штуках
                    self.provider.finam_price_to_price(symbol.board, symbol.symbol, stop_order.take_profit.activation_price)))  # Цена (активации)
        return self.orders

    def new_order(self, order):
        symbol = self.get_symbol_by_dataname(order.dataname)  # Тикер
        buy_sell = self.provider.proto_common.BUY_SELL_BUY if order.buy else self.provider.proto_common.BUY_SELL_SELL  # Покупка или продажа
        quantity_lots = order.quantity // symbol.lot_size  # Кол-во в лотах
        price = 0 if order.exec_type == Order.Market else self.provider.price_to_finam_price(symbol.board, symbol.symbol, order.price)  # Для рыночной заявки цену не ставим
        stop_price = self.provider.price_to_finam_price(symbol.board, symbol.symbol, order.stop_price)  # Стоп цена
        if order.exec_type in (Order.Market, Order.Limit):  # Рыночная или лимитная заявка
            self.provider.new_order(self.client_id, symbol.board, symbol.symbol, buy_sell, quantity_lots, price=price)  # Новая заявка
        else:  # Стоп или стоп-лимитная заявка
            market_price = order.exec_type == Order.Stop  # При срабатывании стоп заявки будет выставлена рыночная заявка
            quantity = self.provider.proto_stops.StopQuantity(value=quantity_lots, units=self.provider.proto_stops.StopQuantityUnits.STOP_QUANTITY_UNITS_LOTS)  # Кол-во в лотах
            stop_loss = self.provider.proto_stops.StopLoss(activation_price=stop_price, market_price=market_price, price=price, quantity=quantity)  # Стоп заявка
            self.provider.new_stop(self.client_id, symbol.board, symbol.symbol, buy_sell, stop_loss)  # Новая стоп заявка

    def cancel_order(self, order):
        if order.exec_type in (Order.Market, Order.Limit):  # Заявка
            self.provider.cancel_order(self.client_id, int(order.id))  # Отменяем заявку по номеру транзакции
        else:  # Стоп заявка
            self.provider.cancel_stop(self.client_id, int(order.id))  # Отменяем стоп заявку по идентификатору стоп заявки

    def close(self):
        self.provider.close_channel()  # Закрываем канал перед выходом
