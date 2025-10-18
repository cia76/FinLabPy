from datetime import datetime

from google.protobuf.timestamp_pb2 import Timestamp
from google.type.interval_pb2 import Interval
from google.type.decimal_pb2 import Decimal

from FinLabPy.Core import Broker, Bar, Position, Order, Symbol  # Брокер, бар, позиция, заявка, тикер
from FinamPy import FinamPy  # Работа с Finam Trade API gRPC https://tradeapi.finam.ru из Python
from FinamPy.grpc.marketdata.marketdata_service_pb2 import BarsRequest, BarsResponse, QuoteRequest, QuoteResponse  # История
from FinamPy.grpc.accounts.accounts_service_pb2 import GetAccountRequest, GetAccountResponse  # Счет
from FinamPy.grpc.orders.orders_service_pb2 import OrdersRequest, OrdersResponse, \
    ORDER_STATUS_NEW, ORDER_STATUS_PARTIALLY_FILLED, ORDER_TYPE_MARKET, ORDER_TYPE_LIMIT, ORDER_TYPE_STOP, ORDER_TYPE_STOP_LIMIT, \
    Order as FinamOrder, StopCondition, CancelOrderRequest  # Заявки
from FinamPy.grpc.side_pb2 import SIDE_BUY, SIDE_SELL  # Покупка/продажа


class Finam(Broker):
    """Брокер Финам"""
    def __init__(self, code, name, provider: FinamPy, account_id=0, storage='file'):
        super().__init__(code, name, provider, account_id, storage)
        self.provider = provider  # Уже инициирован в базовом классе. Выполням для того, чтобы работать с типом провайдера
        self.account_id = self.provider.account_ids[account_id]  # Номер счета по порядковому номеру

    def _get_symbol_info(self, ticker: str, mic: str) -> Symbol | None:
        """Спецификация тикера по коду и бирже"""
        si = self.provider.get_symbol_info(ticker, mic)  # Спецификация тикера
        if si is None:  # Если информация о тикере не найдена
            return None  # то выходим, дальше не продолжаем
        board = self.provider.finam_board_to_board(si.board)  # Канонический код режима торгов
        dataname = self.provider.finam_board_ticker_to_dataname(si.board, si.ticker)  # Название тикера
        min_step = si.min_step / (10 ** si.decimals)  # Минимальный шаг цены
        broker_info = {'mic': mic}  # Информация брокера
        symbol = Symbol(board, si.ticker, dataname, si.name, si.decimals, min_step, int(float(si.lot_size.value)), broker_info)  # Составляем спецификацию тикера
        self.storage.set_symbol(symbol)  # Добавляем спецификацию тикера в хранилище
        return symbol

    def get_symbol_by_dataname(self, dataname):
        symbol = self.storage.get_symbol(dataname)  # Проверяем, есть ли спецификация тикера в хранилище
        if symbol is not None:  # Если есть тикер
            return symbol  # то возвращаем его, дальше не продолжаем
        finam_board, ticker = self.provider.dataname_to_finam_board_ticker(dataname)  # Код режима торгов Финама и тикер
        mic = self.provider.get_mic(finam_board, ticker)  # Код биржи по ISO 10383
        return self._get_symbol_info(ticker, mic)  # то пробуем получить его спецификацию

    def get_history(self, symbol, time_frame, dt_from=None, dt_to=None):
        bars = super().get_history(symbol, time_frame, dt_from, dt_to)  # Получаем бары из хранилища
        if bars is None:  # Если бары из хранилища не получены
            bars = []  # Пока список полученных бар пустой
            start_dt = self.provider.min_history_date if dt_from is None else self.provider.msk_to_utc_datetime(dt_from, True)  # Первый возможный бар
        else:  # Если бары из хранилища получены
            start_dt = bars[-1].datetime  # Дата и время открытия последнего бара
            del bars[-1]  # Этот бар удалим из выборки хранилища. Возможно, он был несформированный
        if dt_to is None:  # Если дата и время окончания истории не заданы
            dt_to = datetime.now()  # то будем получать бары до текущего момента
        finam_tf, tf_range, intraday = self.provider.timeframe_to_finam_timeframe(time_frame)  # Временной интервал Финама, максимальный размер запроса в днях, внутридневной бар
        while start_dt <= dt_to:  # Пока нужно получать данные
            end_dt = start_dt + tf_range  # Конечную дату запроса ставим на максимальный размер от даты начала
            start_time = Timestamp(seconds=int(datetime.timestamp(start_dt)))  # Дату начала запроса переводим в Google Timestamp
            end_time = Timestamp(seconds=int(datetime.timestamp(end_dt)))  # Дату окончания запроса переводим в Google Timestamp
            bars_response: BarsResponse = self.provider.call_function(
                self.provider.marketdata_stub.Bars,
                BarsRequest(symbol=f'{symbol.symbol}@{symbol.broker_info['mic']}', timeframe=finam_tf, interval=Interval(start_time=start_time, end_time=end_time))
            )  # Получаем историю тикера за период
            if len(bars_response.bars) > 0:  # Если за период получены бары
                for bar in bars_response.bars:
                    dt_bar = datetime.fromtimestamp(bar.timestamp.seconds, self.provider.tz_msk)  # Дата/время полученного бара
                    if not intraday:  # Для дневных временнЫх интервалов и выше
                        dt_bar = dt_bar.date()  # убираем время, оставляем только дату
                    bars.append(Bar(symbol.board, symbol.symbol, symbol.dataname, time_frame,
                                    dt_bar, bar.open.value, bar.high.value, bar.low.value, bar.close.value, int(float(bar.volume.value))))  # Добавляем бар
            start_dt = end_dt  # Дату начала переносим на дату окончания
        if len(bars) == 0:  # Если новых бар нет
            return None  # то выходим, дальше не продолжаем
        self.storage.set_bars(bars)  # Сохраняем бары в хранилище
        return bars

    def get_last_price(self, symbol):
        quote_response: QuoteResponse = self.provider.call_function(self.provider.marketdata_stub.LastQuote, QuoteRequest(symbol=f'{symbol.symbol}@{symbol.broker_info['mic']}'))  # Получение последней котировки по инструменту
        return None if quote_response is None else float(quote_response.quote.last.value)  # Последняя цена сделки

    def get_value(self):
        account: GetAccountResponse = self.provider.call_function(self.provider.accounts_stub.GetAccount, GetAccountRequest(account_id=self.account_id))  # Получаем счет
        cash_rub = next((round(cash.units + cash.nanos * 10**-9, 2) for cash in account.cash if cash.currency_code == 'RUB'), 0)  # Свободные средства в рублях, если есть
        return round(float(account.equity.value), 2) - cash_rub  # Стоимость портфеля - свободные средства в рублях

    def get_cash(self):
        account: GetAccountResponse = self.provider.call_function(self.provider.accounts_stub.GetAccount, GetAccountRequest(account_id=self.account_id))  # Получаем счет
        return next((round(cash.units + cash.nanos * 10 ** -9, 2) for cash in account.cash if cash.currency_code == 'RUB'), 0)  # Свободные средства в рублях, если есть

    def get_positions(self):
        self.positions = []  # Сбрасываем текущие позиции
        account: GetAccountResponse = self.provider.call_function(self.provider.accounts_stub.GetAccount, GetAccountRequest(account_id=self.account_id))  # Получаем счет
        for position in account.positions:  # Пробегаемся по всем позициям
            ticker, mic = position.symbol.split('@')  # По разделителю разбиваем на тикер и биржу
            finam_board = self.provider.get_symbol_info(ticker, mic).board  # Режим торгов Финама
            dataname = self.provider.finam_board_ticker_to_dataname(finam_board, ticker)  # Название тикера
            symbol = self.get_symbol_by_dataname(dataname)  # Получаем тикер по названию
            self.positions.append(Position(  # Добавляем текущую позицию в список
                self,  # Брокер
                dataname,  # Название тикера
                symbol.description,  # Описание тикера
                symbol.decimals,  # Кол-во десятичных знаков в цене
                int(float(position.quantity.value)),  # Кол-во в штуках
                float(position.average_price.value),  # Средняя цена входа в рублях
                float(position.current_price.value)))  # Последняя цена в рублях
        return self.positions

    def get_orders(self):
        self.orders = []  # Сбрасываем активные заявки
        orders: OrdersResponse = self.provider.call_function(self.provider.orders_stub.GetOrders, OrdersRequest(account_id=self.account_id))  # Получаем заявки
        for order in orders.orders:  # Пробегаемся по всем заявкам
            if order.status not in (ORDER_STATUS_NEW, ORDER_STATUS_PARTIALLY_FILLED):  # Если заявка еще не активная
                continue  # то переходим к следующей заявке, дальше не продолжаем
            ticker, mic = order.order.symbol.split('@')  # По разделителю разбиваем на тикер и биржу
            finam_board = self.provider.get_symbol_info(ticker, mic).board  # Режим торгов Финама
            dataname = self.provider.finam_board_ticker_to_dataname(finam_board, ticker)  # Название тикера
            symbol = self.get_symbol_by_dataname(dataname)  # Получаем тикер по названию
            exec_type = Order.Limit if order.order.type == ORDER_TYPE_LIMIT else Order.Stop if order.order.type == ORDER_TYPE_STOP else Order.StopLimit if order.order.type == ORDER_TYPE_STOP_LIMIT else Order.Market  # Лимит/стоп/стоп-лимит/по рынку
            price = float(order.order.limit_price.value) if order.order.type == ORDER_TYPE_LIMIT else float(order.order.stop_price) if order.order.type in (ORDER_TYPE_STOP, ORDER_TYPE_STOP_LIMIT) else 0  # Цена для лимитной и стоп заявок
            self.orders.append(Order(  # Добавляем заявки в список
                self,  # Брокер
                order.order_id,  # Уникальный код заявки
                order.order.side.buy_sell == SIDE_BUY,  # Покупка/продажа
                exec_type,  # Тип
                symbol.dataname,  # писание тикера
                symbol.decimals,  # Кол-во десятичных знаков в цене
                order.order.quantity,  # Кол-во в штуках
                price))  # Цена
        return self.orders

    def new_order(self, order):
        symbol = self.get_symbol_by_dataname(order.dataname)  # Получаем тикер по названию
        finam_symbol = f'{symbol.symbol}@{symbol.broker_info['mic']}'  # Тикер Финама
        side = SIDE_BUY if order.buy else SIDE_SELL  # Заявка на покупку или продажу
        limit_price = Decimal(value=str(round(order.price, symbol.decimals)))  # Лимитная цена
        stop_price = Decimal(value=str(round(order.stop_price, symbol.decimals)))  # Стоп цена
        stop_condition = StopCondition.STOP_CONDITION_LAST_UP if order.buy else StopCondition.STOP_CONDITION_LAST_DOWN  # Условие стоп цены
        client_order_id = str(int(datetime.now().timestamp()))  # Уникальный номер заявки
        if order.exec_type == Order.Limit:  # Лимит
            finam_order = FinamOrder(account_id=self.account_id, symbol=finam_symbol, quantity=order.quantity, side=side, type=ORDER_TYPE_LIMIT, client_order_id=client_order_id,
                                     limit_price=limit_price)
        elif order.exec_type == Order.Stop:  # Стоп
            finam_order = FinamOrder(account_id=self.account_id, symbol=finam_symbol, quantity=order.quantity, side=side, type=ORDER_TYPE_STOP, client_order_id=client_order_id,
                                     stop_price=stop_price, stop_condition=stop_condition)
        elif order.exec_type == Order.StopLimit:  # Стоп-лимит
            finam_order = FinamOrder(account_id=self.account_id, symbol=finam_symbol, quantity=order.quantity, side=side, type=ORDER_TYPE_STOP_LIMIT, client_order_id=client_order_id,
                                     stop_price=stop_price, stop_condition=stop_condition,
                                     limit_price=limit_price)
        else:  # По рынку
            finam_order = FinamOrder(account_id=self.account_id, symbol=finam_symbol, quantity=order.quantity, side=side, type=ORDER_TYPE_MARKET, client_order_id=client_order_id)
        self.provider.call_function(self.provider.orders_stub.PlaceOrder, finam_order)

    def cancel_order(self, order):
        self.provider.call_function(self.provider.orders_stub.CancelOrder, CancelOrderRequest(account_id=self.account_id, order_id=order.id))  # Удаление заявки

    def close(self):
        self.provider.close_channel()  # Закрываем канал перед выходом
