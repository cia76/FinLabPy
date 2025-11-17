from datetime import datetime
from threading import Thread

from google.protobuf.timestamp_pb2 import Timestamp
from google.type.interval_pb2 import Interval
from google.type.decimal_pb2 import Decimal

from FinLabPy.Core import Broker, Bar, Position, Trade, Order, Symbol  # Брокер, бар, позиция, сделка, заявка, тикер
from FinamPy import FinamPy  # Работа с Finam Trade API gRPC https://tradeapi.finam.ru из Python
from FinamPy.grpc.marketdata.marketdata_service_pb2 import BarsRequest, BarsResponse, QuoteRequest, QuoteResponse, SubscribeBarsResponse, TimeFrame  # История
from FinamPy.grpc.accounts.accounts_service_pb2 import GetAccountRequest, GetAccountResponse  # Счет
from FinamPy.grpc.orders.orders_service_pb2 import OrdersRequest, OrdersResponse, OrderType, OrderState, OrderStatus, \
    Order as FinamOrder, StopCondition, CancelOrderRequest, OrderTradeRequest  # Заявки
from FinamPy.grpc.side_pb2 import Side  # Покупка/продажа
from FinamPy.grpc.trade_pb2 import AccountTrade  # Сделка


class Finam(Broker):
    """Брокер Финам"""
    def __init__(self, code, name, provider: FinamPy, account_id=0, storage='file'):
        super().__init__(code, name, provider, account_id, storage)
        self.provider = provider  # Уже инициирован в базовом классе. Выполням для того, чтобы работать с типом провайдера
        self.account_id = self.provider.account_ids[account_id]  # Номер счета по порядковому номеру
        self.last_bars = {}  # Последний бар. Он может быть не завершен

        self.provider.on_new_bar.subscribe(self._on_new_bar)  # Обработка нового бара
        self.provider.on_order.subscribe(self._on_order)  # Обработка заявок
        self.provider.on_trade.subscribe(self._on_trade)  # Обработка сделок

    def get_symbol_by_dataname(self, dataname):
        symbol = self.storage.get_symbol(dataname)  # Проверяем, есть ли спецификация тикера в хранилище
        if symbol is not None:  # Если есть тикер
            return symbol  # то возвращаем его, дальше не продолжаем
        finam_board, ticker = self.provider.dataname_to_finam_board_ticker(dataname)  # Код режима торгов Финама и тикер
        mic = self.provider.get_mic(finam_board, ticker)  # Код биржи по ISO 10383
        return self._get_symbol_info(f'{ticker}@{mic}')

    def get_history(self, symbol, time_frame, dt_from=None, dt_to=None):
        bars = super().get_history(symbol, time_frame, dt_from, dt_to)  # Получаем бары из хранилища
        if bars is None:  # Если бары из хранилища не получены
            bars = []  # Пока список полученных бар пустой
            seconds_from = self.provider.msk_datetime_to_timestamp(self.provider.min_history_date if dt_from is None else dt_from)  # Первый возможный бар
        else:  # Если бары из хранилища получены
            seconds_from = self.provider.msk_datetime_to_timestamp(bars[-1].datetime)  # Дата и время открытия последнего бара
        seconds_to = self.provider.msk_datetime_to_timestamp(datetime.now() if dt_to is None else dt_to)  # Последний возможный бар
        finam_tf, tf_range, intraday = self.provider.timeframe_to_finam_timeframe(time_frame)  # Временной интервал Финама, максимальный размер запроса в днях, внутридневной бар
        while seconds_from <= seconds_to:  # Пока нужно получать данные
            bars_response: BarsResponse = self.provider.call_function(  # Получаем историю тикера за период
                self.provider.marketdata_stub.Bars,  # Получение исторических данных по инструменту (агрегированные свечи)
                BarsRequest(symbol=f'{symbol.symbol}@{symbol.broker_info['mic']}',  # Тикер Финама
                            timeframe=finam_tf,  # Временной интервал Финама
                            interval=Interval(start_time=Timestamp(seconds=seconds_from),  # Дата и время начала запроса
                                              end_time=Timestamp(seconds=seconds_from + int(tf_range.total_seconds())))))  # Дата и время окончания запроса
            if len(bars_response.bars) > 0:  # Если за период получены бары
                if len(bars) > 0:  # Если список бар не пустой
                    del bars[-1]  # то удаляем последний бар. Он перепишется первым полученным баром за период
                for bar in bars_response.bars:  # Пробегаемся по всем пришедшим барам
                    dt_msk = self.provider.timestamp_to_msk_datetime(bar.timestamp.seconds)  # Дата и время полученного бара
                    if not intraday:  # Для дневных временнЫх интервалов и выше
                        dt_msk = dt_msk.replace(hour=0, minute=0)  # убираем время, оставляем только дату
                    open_ = self.provider.finam_price_to_price(symbol.board, float(bar.open.value))  # Конвертируем цены
                    high = self.provider.finam_price_to_price(symbol.board, float(bar.high.value))  # из цен Финама
                    low = self.provider.finam_price_to_price(symbol.board, float(bar.low.value))  # в зависимости от
                    close = self.provider.finam_price_to_price(symbol.board, float(bar.close.value))  # режима торгов
                    bars.append(Bar(symbol.board, symbol.symbol, symbol.dataname, time_frame, dt_msk, open_, high, low, close, int(float(bar.volume.value))))  # Добавляем бар
            seconds_from += int(tf_range.total_seconds())  # Дату и время начала запроса переносим на дату окончания
        if len(bars) == 0:  # Если новых бар нет
            return None  # то выходим, дальше не продолжаем
        self.storage.set_bars(bars)  # Сохраняем бары в хранилище
        self.last_bars[(symbol.dataname, time_frame)] = bars[-1]  # Запомним последний бар. Он может быть не завершен
        return bars

    def subscribe_history(self, symbol, time_frame):
        if (symbol, time_frame) in self.history_subscriptions.keys():  # Если подписка уже есть
            return  # то выходим, дальше не продолжаем
        finam_board, ticker = self.provider.dataname_to_finam_board_ticker(symbol.dataname)  # Код режима торгов Финама и тикер
        mic = self.provider.get_mic(finam_board, ticker)  # Код биржи по ISO 10383
        finam_tf, _, _ = self.provider.timeframe_to_finam_timeframe(time_frame)  # Временной интервал Финама
        Thread(target=self.provider.subscribe_bars_thread, name=f'BarsThread {symbol.dataname} {time_frame}', args=(f'{ticker}@{mic}', finam_tf)).start()  # Создаем и запускаем поток подписки на новые бары
        self.history_subscriptions[(symbol, time_frame)] = True  # Ставим отметку в справочнике подписок

    def unsubscribe_history(self, symbol, time_frame):
        self.history_subscriptions[(symbol, time_frame)] = False  # Реальной отмены подписки на историю тикера нет. Снимаем отметку в справочнике подписок

    def get_last_price(self, symbol):
        quote_response: QuoteResponse = self.provider.call_function(self.provider.marketdata_stub.LastQuote, QuoteRequest(symbol=f'{symbol.symbol}@{symbol.broker_info['mic']}'))  # Получение последней котировки по инструменту
        return None if quote_response is None else self.provider.finam_price_to_price(symbol.board, float(quote_response.quote.last.value))  # Последняя цена сделки

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
            symbol = self._get_symbol_info(position.symbol)  # Тикер
            self.positions.append(Position(  # Добавляем текущую позицию в список
                self,  # Брокер
                symbol.dataname,  # Название тикера
                symbol.description,  # Описание тикера
                symbol.decimals,  # Кол-во десятичных знаков в цене
                int(float(position.quantity.value)),  # Кол-во в штуках
                self.provider.finam_price_to_price(symbol.board, float(position.average_price.value)) if position.average_price.value != '' else 0,  # Средняя цена входа в рублях. Для фьючерсов не задается
                self.provider.finam_price_to_price(symbol.board, float(position.current_price.value))))  # Последняя цена в рублях
        return self.positions

    def get_orders(self):
        self.orders = []  # Сбрасываем активные заявки
        orders: OrdersResponse = self.provider.call_function(self.provider.orders_stub.GetOrders, OrdersRequest(account_id=self.account_id))  # Получаем заявки
        for order in orders.orders:  # Пробегаемся по всем заявкам
            if order.status not in (OrderStatus.ORDER_STATUS_NEW, OrderStatus.ORDER_STATUS_WAIT, OrderStatus.ORDER_STATUS_PARTIALLY_FILLED):  # Если заявка еще не активная
                continue  # то переходим к следующей заявке, дальше не продолжаем
            symbol = self._get_symbol_info(order.order.symbol)  # Тикер
            exec_type = Order.Limit if order.order.type == OrderType.ORDER_TYPE_LIMIT else Order.Stop if order.order.type == OrderType.ORDER_TYPE_STOP else Order.StopLimit if order.order.type == OrderType.ORDER_TYPE_STOP_LIMIT else Order.Market  # Лимит/стоп/стоп-лимит/по рынку
            price = 0  # Лимитная цена для лимитных и стоп лимитных заявок
            stop_price = 0  # Стоп цена срабатывания для стоп и стоп лимитных заявок
            if exec_type in (Order.Limit, Order.StopLimit):
                price = self.provider.finam_price_to_price(symbol.board, float(order.order.limit_price.value))  # Лимитная цена
            if exec_type in (Order.Stop, Order.StopLimit):
                stop_price = self.provider.finam_price_to_price(symbol.board, float(order.order.stop_price.value))  # Цена срабатывания
            self.orders.append(Order(  # Добавляем заявки в список
                self,  # Брокер
                order.order_id,  # Уникальный код заявки
                order.order.side.buy_sell == Side.SIDE_BUY,  # Покупка/продажа
                exec_type,  # Тип
                symbol.dataname,  # писание тикера
                symbol.decimals,  # Кол-во десятичных знаков в цене
                order.order.quantity,  # Кол-во в штуках
                price,  # Цена
                stop_price,  # Цена срабатывания стоп заявки
                Order.Partial if order.status == OrderStatus.ORDER_STATUS_PARTIALLY_FILLED else Order.Accepted))  # Статус
        return self.orders

    def new_order(self, order):
        symbol = self.get_symbol_by_dataname(order.dataname)  # Получаем тикер по названию
        finam_symbol = f'{symbol.symbol}@{symbol.broker_info['mic']}'  # Тикер Финама
        side = Side.SIDE_BUY if order.buy else Side.SIDE_SELL  # Заявка на покупку или продажу
        limit_price = Decimal(value=str(round(self.provider.price_to_finam_price(symbol.board, order.price), symbol.decimals)))  # Лимитная цена Финама
        stop_price = Decimal(value=str(round(self.provider.price_to_finam_price(symbol.board, order.stop_price), symbol.decimals)))  # Стоп цена Финама
        stop_condition = StopCondition.STOP_CONDITION_LAST_UP if order.buy else StopCondition.STOP_CONDITION_LAST_DOWN  # Условие стоп цены
        client_order_id = str(int(datetime.now().timestamp()))  # Уникальный номер заявки
        if order.exec_type == Order.Limit:  # Лимит
            finam_order = FinamOrder(account_id=self.account_id, symbol=finam_symbol, quantity=order.quantity, side=side, type=OrderType.ORDER_TYPE_LIMIT, client_order_id=client_order_id,
                                     limit_price=limit_price)
        elif order.exec_type == Order.Stop:  # Стоп
            finam_order = FinamOrder(account_id=self.account_id, symbol=finam_symbol, quantity=order.quantity, side=side, type=OrderType.ORDER_TYPE_STOP, client_order_id=client_order_id,
                                     stop_price=stop_price, stop_condition=stop_condition)
        elif order.exec_type == Order.StopLimit:  # Стоп-лимит
            finam_order = FinamOrder(account_id=self.account_id, symbol=finam_symbol, quantity=order.quantity, side=side, type=OrderType.ORDER_TYPE_STOP_LIMIT, client_order_id=client_order_id,
                                     stop_price=stop_price, stop_condition=stop_condition,
                                     limit_price=limit_price)
        else:  # По рынку
            finam_order = FinamOrder(account_id=self.account_id, symbol=finam_symbol, quantity=order.quantity, side=side, type=OrderType.ORDER_TYPE_MARKET, client_order_id=client_order_id)
        order_state: OrderState = self.provider.call_function(self.provider.orders_stub.PlaceOrder, finam_order)
        if order_state.status == OrderStatus.ORDER_STATUS_NEW:  # Должен вернуться статус "Новая заявка"
            order.id = order_state.order_id  # Уникальный код заявки
            order.status = Order.Submitted  # Заявка отправлена брокеру
            self.orders.append(order)  # Добавляем новую заявку в список заявок
            return True  # Операция завершилась успешно
        return False  # Операция завершилась с ошибкой

    def cancel_order(self, order):
        self.provider.call_function(self.provider.orders_stub.CancelOrder, CancelOrderRequest(account_id=self.account_id, order_id=order.id))  # Удаление заявки

    def subscribe_transactions(self):
        Thread(target=self.provider.subscriptions_order_trade_handler, name='SubscriptionsOrderTradeThread').start()  # Создаем и запускаем поток обработки своих заявок и сделок
        self.provider.order_trade_queue.put(OrderTradeRequest(  # Ставим в буфер команд/сделок
            action=OrderTradeRequest.Action.ACTION_SUBSCRIBE,  # Подписываемся
            data_type=OrderTradeRequest.DataType.DATA_TYPE_ALL,  # на свои заявки и сделки
            account_id=self.account_id))  # по торговому счету

    def unsubscribe_transactions(self):
        self.provider.order_trade_queue.put(OrderTradeRequest(  # Ставим в буфер команд/сделок
            action=OrderTradeRequest.Action.ACTION_UNSUBSCRIBE,  # Отменяем подписку
            data_type=OrderTradeRequest.DataType.DATA_TYPE_ALL,  # на свои заявки и сделки
            account_id=self.account_id))  # по торговому счету

    def close(self):
        self.provider.on_new_bar.unsubscribe(self._on_new_bar)  # Обработка нового бара
        self.provider.on_order.unsubscribe(self._on_order)  # Обработка заявок
        self.provider.on_trade.unsubscribe(self._on_trade)  # Обработка сделок

        self.provider.close_channel()  # Закрываем канал перед выходом

    # Внутренние функции

    def _get_symbol_info(self, finam_symbol: str) -> Symbol | None:
        """Спецификация тикера по тикеру Финама"""
        ticker, mic = finam_symbol.split('@')  # По разделителю разбиваем на тикер и биржу
        symbol = next((symbol for symbol in self.storage.symbols.values() if symbol.symbol == ticker and symbol.broker_info['mic'] == mic), None)  # Проверяем, есть ли спецификация тикера в хранилище по тикеру и бирже
        if symbol is not None:  # Если есть тикер
            return symbol  # то возвращаем его, выходим, дальше не продолжаем
        si = self.provider.get_symbol_info(ticker, mic)  # Спецификация тикера
        if si is None:  # Если тикер не найден
            return None  # то выходим, дальше не продолжаем
        board = self.provider.finam_board_to_board(si.board)  # Канонический код режима торгов
        dataname = self.provider.finam_board_ticker_to_dataname(si.board, ticker)  # Название тикера
        broker_info = {'mic': mic}  # Информация брокера
        symbol = Symbol(board, ticker, dataname, si.name, si.decimals, si.min_step / (10 ** si.decimals), int(float(si.lot_size.value)), broker_info)  # Составляем спецификацию тикера
        self.storage.set_symbol(symbol)  # Добавляем спецификацию тикера в хранилище
        return symbol

    def _on_new_bar(self, bars: SubscribeBarsResponse, timeframe: TimeFrame.ValueType):
        """Получение нового бара по подписке"""
        symbol = self._get_symbol_info(bars.symbol)  # Спецификация тикера
        time_frame, _, _ = self.provider.finam_timeframe_to_timeframe(timeframe)  # Временной интервал
        if not self.history_subscriptions[(symbol, time_frame)]:  # Если была отписка от тикера
            return  # Выходим, дальше не продолжаем
        last_bar: Bar = None if (symbol.dataname, time_frame) not in self.last_bars else self.last_bars[(symbol.dataname, time_frame)]  # Последний бар. Он может быть не завершен
        for bar in bars.bars:  # Пробегаемся по всем полученным барам
            dt_msk = self.provider.timestamp_to_msk_datetime(bar.timestamp.seconds)  # Дата и время полученного бара
            if last_bar is not None and last_bar.datetime < dt_msk:  # Если время бара стало больше (предыдущий бар закрыт, новый бар открыт)
                self.on_new_bar.trigger(Bar(symbol.board, symbol.symbol, symbol.dataname, time_frame, last_bar.datetime, last_bar.open, last_bar.high, last_bar.low, last_bar.close, last_bar.volume))  # Вызываем событие добавления нового бара
            open_ = self.provider.finam_price_to_price(symbol.board, float(bar.open.value))  # Конвертируем цены
            high = self.provider.finam_price_to_price(symbol.board, float(bar.high.value))  # из цен Финама
            low = self.provider.finam_price_to_price(symbol.board, float(bar.low.value))  # в зависимости от
            close = self.provider.finam_price_to_price(symbol.board, float(bar.close.value))  # режима торгов
            self.last_bars[(symbol.dataname, time_frame)] = Bar(symbol.board, symbol.symbol, symbol.dataname, time_frame, dt_msk, open_, high, low, close, int(float(bar.volume.value)))  # Запоминаем бар

    def _on_trade(self, trade: AccountTrade):
        """Получение сделки по подписке. Изменение позиции"""
        symbol = self._get_symbol_info(trade.symbol)  # Спецификация тикера
        dt_trade = self.provider.timestamp_to_msk_datetime(trade.timestamp.seconds)  # Дата и время исполнения сделки
        quantity = trade.size.value  # Кол-во в штуках. Всегда положительное
        if trade.side.ValueType == Side.SIDE_SELL:  # Если сделка на продажу
            quantity *= -1  # то кол-во ставим отрицательным
        self.on_trade.trigger(Trade(
            self,  # Брокер
            trade.order_id,  # Номер заявки из сделки
            symbol.dataname,  # Название тикера
            symbol.description,  # Описание тикера
            symbol.decimals,  # Кол-во десятичных знаков в цене
            dt_trade,  # Дата и время сделки по времени биржи (МСК)
            quantity,  # Кол-во в штуках
            self.provider.finam_price_to_price(symbol.board, trade.price.value)))  # Цена сделки
        self.on_position.trigger(self.get_position(symbol))  # При любой сделке позиция изменяется. Отправим текущую или пустую позицию по тикеру по подписке

    def _on_order(self, order: OrderState):
        """Получение заявки по подписке"""
        if order.order.type == OrderType.ORDER_TYPE_MARKET:  # Рыночная заявка
            order_type = Order.Market
        elif order.order.type == OrderType.ORDER_TYPE_LIMIT:  # Лимитная заявка
            order_type = Order.Limit
        elif order.order.type == OrderType.ORDER_TYPE_STOP:  # Стоп заявка
            order_type = Order.Stop
        elif order.order.type == OrderType.ORDER_TYPE_STOP_LIMIT:  # Стоп лимитная заявка
            order_type = Order.StopLimit
        else:  # Остальные типы заявок
            raise NotImplementedError  # не реализованы
        if order.status == OrderStatus.ORDER_STATUS_NEW:  # Новая заявка
            order_status = Order.Created
        elif order.status == OrderStatus.ORDER_STATUS_PARTIALLY_FILLED:  # Частично исполненная
            order_status = Order.Partial
        elif order.status in (OrderStatus.ORDER_STATUS_FILLED,  # Исполненная
                              OrderStatus.ORDER_STATUS_EXECUTED):  # Исполнена
            order_status = Order.Completed
        elif order.status == OrderStatus.ORDER_STATUS_CANCELED:  # Отменена
            order_status = Order.Canceled
        elif order.status in (OrderStatus.ORDER_STATUS_REJECTED,  # Отклонена
                              OrderStatus.ORDER_STATUS_REJECTED_BY_EXCHANGE,  # Отклонено биржей
                              OrderStatus.ORDER_STATUS_FAILED,  # Ошибка
                              OrderStatus.ORDER_STATUS_DENIED_BY_BROKER,  # Отклонено брокером
                              OrderStatus.ORDER_STATUS_REJECTED_BY_EXCHANGE):  # Отклонено биржей
            order_status = Order.Rejected
        elif order.status == OrderStatus.ORDER_STATUS_EXPIRED:  # Истекла
            order_status = Order.Expired
        elif order.status == OrderStatus.ORDER_STATUS_WAIT:  # Ожидает
            order_status = Order.Accepted
        else:  # Остальные статусы заявок
            raise NotImplementedError  # не реализованы
        symbol = self._get_symbol_info(order.order.symbol)  # Спецификация тикера
        self.on_order.trigger(Order(
            self,  # Брокер
            order.order_id,  # Уникальный код заявки
            order.order.side.ValueType == Side.SIDE_BUY,  # Покупка/продажа
            order_type,  # Тип заявки
            symbol.dataname,  # Название тикера
            symbol.decimals,  # Кол-во десятичных знаков в цене
            order.order.quantity.value,  # Кол-во в штуках
            self.provider.finam_price_to_price(symbol.board, order.order.limit_price.value) if order.order.limit_price else 0,  # Цена
            self.provider.finam_price_to_price(symbol.board, order.order.stop_price.value) if order.order.stop_price else 0,  # Цена срабатывания стоп заявки
            order_status))  # Статус заявки (без Submitted/Margin)
