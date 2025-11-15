from datetime import datetime
from threading import Thread  # Запускаем поток подписки
from math import log10  # Кол-во десятичных знаков будем получать из шага цены через десятичный логарифм
from uuid import uuid4  # Номера заявок должны быть уникальными во времени и пространстве

from FinLabPy.Core import Broker, Bar, Position, Trade, Order, Symbol  # Брокер, бар, позиция, сделка, заявка, тикер
from TinvestPy import TinvestPy  # Работа с T-Invest API из Python
from TinvestPy.grpc.instruments_pb2 import InstrumentRequest, InstrumentIdType, InstrumentResponse  # Тикер
from TinvestPy.grpc.operations_pb2 import PortfolioRequest, PortfolioResponse  # Портфель
from TinvestPy.grpc.marketdata_pb2 import (
    MarketDataRequest, SubscribeCandlesRequest, SubscriptionAction, CandleInstrument,  # Подписка на свечи
    GetCandlesRequest, GetCandlesResponse, Candle, GetLastPricesRequest, GetLastPricesResponse)  # Свечи, стакан
from TinvestPy.grpc.orders_pb2 import (
    GetOrdersRequest, GetOrdersResponse, PostOrderRequest, PostOrderResponse, CancelOrderRequest,
    OrderExecutionReportStatus, OrderType, OrderTrades, OrderState, OrderDirection)  # Заявки
from TinvestPy.grpc.stoporders_pb2 import (
    GetStopOrdersRequest, GetStopOrdersResponse, PostStopOrderRequest, PostStopOrderResponse, CancelStopOrderRequest, StopOrderExpirationType, StopOrderType, StopOrderDirection)  # Стоп заявки


class Tinvest(Broker):
    """Брокер Т-Инвестиции"""
    def __init__(self, code, name, provider: TinvestPy, account_id=0, storage='file'):
        super().__init__(code, name, provider, account_id, storage)
        self.provider = provider  # Уже инициирован в базовом классе. Выполням для того, чтобы работать с типом провайдера
        self.account_id = self.provider.accounts[account_id].id  # Номер счета по порядковому номеру
        self.history_thread = None  # Поток подписок на историю тикера

        self.provider.on_candle.subscribe(self._on_new_bar)  # Обработка нового бара
        self.provider.on_order_state.subscribe(self._on_order)  # Обработка заявок
        self.provider.on_order_trades.subscribe(self._on_trade)  # Обработка сделок

    def get_symbol_by_dataname(self, dataname):
        symbol = self.storage.get_symbol(dataname)  # Проверяем, есть ли спецификация тикера в хранилище
        if symbol is not None:  # Если есть тикер
            return symbol  # то возвращаем его, дальше не продолжаем
        class_code, sec_code = self.provider.dataname_to_class_code_symbol(dataname)  # Код режима торгов и тикер из названия тикера
        return self._get_symbol_info(class_code=class_code, sec_code=sec_code)  # Спецификация тикера по режиму торгов и тикеру

    def get_history(self, symbol, time_frame, dt_from=None, dt_to=None):
        bars = super().get_history(symbol, time_frame, dt_from, dt_to)  # Получаем бары из хранилища
        tinvest_time_frame, intraday = self.provider.timeframe_to_tinvest_timeframe(time_frame)  # Временной интервал Т-Инвестиции, внутридневной интервал
        if bars is None:  # Если бары из хранилища не получены
            bars = []  # Пока список полученных бар пустой
            seconds_from = self.provider.msk_datetime_to_timestamp(dt_from) if dt_from is not None else symbol.broker_info['first_1min_timestamp'] if intraday else symbol.broker_info['first_1day_timestamp']  # Первый возможный бар для внутридневного / дневного интервалов
        else:  # Если бары из хранилища получены
            seconds_from = self.provider.msk_datetime_to_timestamp(bars[-1].datetime)  # Дата и время открытия последнего бара
            del bars[-1]  # Этот бар удалим из выборки хранилища. Возможно, он был несформированный
        seconds_to = self.provider.msk_datetime_to_timestamp(datetime.now() if dt_to is None else dt_to)  # Последний возможный бар
        _, td = self.provider.tinvest_timeframe_to_timeframe(tinvest_time_frame)  # Временной интервал для имени файла и максимальный период запроса
        while seconds_from <= seconds_to:  # Пока нужно получать данные
            request = GetCandlesRequest(instrument_id=symbol.broker_info['figi'], interval=tinvest_time_frame)  # Запрос на получение бар
            from_ = getattr(request, 'from')  # т.к. from - ключевое слово в Python, то получаем атрибут from из атрибута интервала
            from_.seconds = seconds_from  # Дата и время начала запроса
            to_ = getattr(request, 'to')  # Аналогично будем работать с атрибутом to для единообразия
            to_.seconds = seconds_from + int(td.total_seconds())  # Дата и время окончания запроса
            candles_response: GetCandlesResponse = self.provider.call_function(self.provider.stub_marketdata.GetCandles, request)  # Получаем ответ на запрос бар
            if len(candles_response.candles) > 0:  # Если за период получены бары
                if len(bars) > 0:  # Если список бар не пустой
                    del bars[-1]  # то удаляем последний бар. Он перепишется первым полученным баром за период
                for candle in candles_response.candles:  # Пробегаемся по всем пришедшим барам
                    dt_msk = self.provider.google_timestamp_to_msk_datetime(candle.time)  # Дата и время полученного бара
                    if not intraday:  # Для дневных временнЫх интервалов и выше
                        dt_msk = dt_msk.replace(hour=0, minute=0)  # убираем время, оставляем только дату
                    open_ = self.provider.tinvest_price_to_price(symbol.board, symbol.symbol, self.provider.quotation_to_float(candle.open))  # Конвертируем цены
                    high = self.provider.tinvest_price_to_price(symbol.board, symbol.symbol, self.provider.quotation_to_float(candle.high))  # из цен Т-Инвестиции
                    low = self.provider.tinvest_price_to_price(symbol.board, symbol.symbol, self.provider.quotation_to_float(candle.low))  # в зависимости от
                    close = self.provider.tinvest_price_to_price(symbol.board, symbol.symbol, self.provider.quotation_to_float(candle.close))  # режима торгов
                    volume = candle.volume * symbol.lot_size  # Объем в шутках
                    bars.append(Bar(symbol.board, symbol.symbol, symbol.dataname, time_frame, dt_msk, open_, high, low, close, volume))  # Добавляем бар
            seconds_from += int(td.total_seconds())  # Дату и время начала запроса переносим на дату окончания
        if len(bars) == 0:  # Если новых бар нет
            return None  # то выходим, дальше не продолжаем
        self.storage.set_bars(bars)  # Сохраняем бары в хранилище
        return bars

    def subscribe_history(self, symbol, time_frame):
        if (symbol, time_frame) in self.history_subscriptions.keys():  # Если подписка уже есть
            return  # то выходим, дальше не продолжаем
        if self.history_thread is None:  # Если поток подписок на историю тикера еще не создан (первая подписка)
            self.history_thread = Thread(target=self.provider.subscriptions_marketdata_handler, name='TKSubscriptionsMarketdataThread')  # то создаем
            self.history_thread.start()  # и запускаем поток
        self.provider.subscription_marketdata_queue.put(  # Ставим в буфер команд подписки на биржевую информацию
            MarketDataRequest(subscribe_candles_request=SubscribeCandlesRequest(  # запрос на новые бары
                subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,  # подписка
                instruments=(CandleInstrument(interval=self.provider.timeframe_to_tinvest_subscription_timeframe(time_frame),  # по временнОму интервалу
                                              instrument_id=symbol.broker_info['figi']),),  # на тикер
                waiting_close=True)))  # по закрытию бара
        self.history_subscriptions[(symbol, time_frame)] = True  # Ставим отметку в справочнике подписок

    def unsubscribe_history(self, symbol, time_frame):
        self.provider.subscription_marketdata_queue.put(  # Ставим в буфер команд подписки на биржевую информацию
            MarketDataRequest(subscribe_candles_request=SubscribeCandlesRequest(  # запрос на новые бары
                subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_UNSUBSCRIBE,  # отмена подписки
                instruments=(CandleInstrument(interval=self.provider.timeframe_to_tinvest_subscription_timeframe(time_frame),  # по временнОму интервалу
                                              instrument_id=symbol.broker_info['figi']),),  # на тикер
                waiting_close=True)))  # по закрытию бара
        del self.history_subscriptions[(symbol, time_frame)]  # Удаляем из справочника подписок

    def get_last_price(self, symbol):
        request = GetLastPricesRequest(instrument_id=[symbol.broker_info['figi']])
        response: GetLastPricesResponse = self.provider.call_function(self.provider.stub_marketdata.GetLastPrices, request)  # Запрос последних цен
        return self.provider.quotation_to_float(response.last_prices[-1].price)  # Последняя цена

    def get_value(self):
        request = PortfolioRequest(account_id=self.account_id, currency=self.provider.currency)
        response: PortfolioResponse = self.provider.call_function(self.provider.stub_operations.GetPortfolio, request)  # Получаем портфель по счету
        value = self.provider.money_value_to_float(response.total_amount_portfolio)  # Оценка портфеля
        value -= self.provider.money_value_to_float(response.total_amount_currencies)  # без свободных средств по счету
        return value

    def get_cash(self):
        request = PortfolioRequest(account_id=self.account_id, currency=self.provider.currency)
        response: PortfolioResponse = self.provider.call_function(self.provider.stub_operations.GetPortfolio, request)  # Получаем портфель по счету
        cash = self.provider.money_value_to_float(response.total_amount_currencies)  # Свободные средства по счету
        return cash

    def get_positions(self):
        self.positions = []  # Текущие позиции
        request = PortfolioRequest(account_id=self.account_id, currency=self.provider.currency)
        response: PortfolioResponse = self.provider.call_function(self.provider.stub_operations.GetPortfolio, request)  # Получаем портфель по счету
        for position in response.positions:  # Пробегаемся по всем активным позициям счета
            symbol = self._get_symbol_info(figi=position.figi)  # Спецификация тикера по figi
            if symbol.board == 'CETS':  # Валюты
                continue  # за позиции не считаем
            self.positions.append(Position(  # Добавляем текущую позицию в список
                self,  # Брокер
                symbol.dataname,  # Название тикера
                symbol.description,  # Описание тикера
                symbol.decimals,  # Кол-во десятичных знаков в цене
                int(self.provider.quotation_to_float(position.quantity)),  # Кол-во в штуках
                self.provider.money_value_to_float(position.average_position_price),  # Средняя цена входа в рублях
                self.provider.money_value_to_float(position.current_price)))  # Последняя цена в рублях
        return self.positions

    def get_orders(self):
        self.orders = []  # Активные заявки
        request = GetOrdersRequest(account_id=self.account_id)
        response: GetOrdersResponse = self.provider.call_function(self.provider.stub_orders.GetOrders, request)  # Получаем активные заявки
        for order in response.orders:  # Пробегаемся по всем заявкам
            if order.execution_report_status in (OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL, OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_REJECTED, OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_CANCELLED):  # Если заявка не активная
                continue  # то переходим к следующей заявке, дальше не продолжаем
            symbol = self._get_symbol_info(figi=order.figi)  # Спецификация тикера по figi
            self.orders.append(Order(  # Добавляем заявки в список
                self,  # Брокер
                order.order_id,  # Уникальный код заявки (номер транзакции)
                order.direction == OrderDirection.ORDER_DIRECTION_BUY,  # Покупка/продажа
                Order.Limit if order.order_type == OrderType.ORDER_TYPE_LIMIT else Order.Market,  # Лимит/по рынку
                symbol.dataname,  # Название тикера
                symbol.decimals,  # Кол-во десятичных знаков в цене
                order.lots_requested * symbol.lot_size,  # Кол-во в штуках
                self.provider.money_value_to_float(order.initial_security_price),  # Цена
                status=Order.Partial if order.execution_report_status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_PARTIALLYFILL else Order.Accepted))  # Статус
        request = GetStopOrdersRequest(account_id=self.account_id)
        response: GetStopOrdersResponse = self.provider.call_function(self.provider.stub_stop_orders.GetStopOrders, request)  # Получаем активные стоп заявки
        for stop_order in response.stop_orders:  # Пробегаемся по всем стоп заявкам
            symbol = self._get_symbol_info(figi=stop_order.figi)  # Спецификация тикера по figi
            self.orders.append(Order(  # Добавляем заявки в список
                self,  # Брокер
                stop_order.stop_order_id,  # Уникальный код заявки (номер транзакции)
                stop_order.direction == StopOrderDirection.STOP_ORDER_DIRECTION_BUY,  # Покупка/продажа
                Order.StopLimit if stop_order.order_type == StopOrderType.STOP_ORDER_TYPE_STOP_LIMIT else Order.Stop,  # Лимит/по рынку
                symbol.dataname,  # Название тикера
                symbol.decimals,  # Кол-во десятичных знаков в цене
                stop_order.lots_requested * symbol.lot_size,  # Кол-во в штуках
                self.provider.money_value_to_float(stop_order.price),  # Цена
                self.provider.money_value_to_float(stop_order.stop_price),  # Цена срабатывания стоп заявки
                Order.Accepted))  # Статус
        return self.orders

    def new_order(self, order):
        symbol = self.get_symbol_by_dataname(order.dataname)  # Тикер
        quantity: int = abs(order.quantity // symbol.lot_size)  # Размер позиции в лотах. В Тинькофф всегда передается положительный размер лота
        price = 0 if order.exec_type in (Order.Market, Order.Stop) else self.provider.float_to_quotation(order.price)  # Для рыночной заявки цену не ставим
        stop_price = 0 if order.exec_type in (Order.Market, Order.Limit) else self.provider.float_to_quotation(order.stop_price)  # Стоп цена
        order_id = str(uuid4())  # Уникальный идентификатор заявки
        if order.exec_type == Order.Market:  # Рыночная заявка
            direction = OrderDirection.ORDER_DIRECTION_BUY if order.buy else OrderDirection.ORDER_DIRECTION_SELL  # Покупка/продажа
            request = PostOrderRequest(instrument_id=symbol.broker_info['figi'], quantity=quantity, direction=direction,
                                       account_id=self.account_id, order_type=OrderType.ORDER_TYPE_MARKET, order_id=order_id)
            response: PostOrderResponse = self.provider.call_function(self.provider.stub_orders.PostOrder, request)  # Отправляем рыночную заявку брокеру
            if response.execution_report_status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW:  # Должен вернуться статус "Новая"
                order.id = response.order_id  # Уникальный код заявки
                order.status = Order.Submitted  # Заявка отправлена брокеру
                self.orders.append(order)  # Добавляем новую заявку в список заявок
                return True  # Операция завершилась успешно
        elif order.exec_type == Order.Limit:  # Лимитная заявка
            direction = OrderDirection.ORDER_DIRECTION_BUY if order.buy else OrderDirection.ORDER_DIRECTION_SELL  # Покупка/продажа
            request = PostOrderRequest(instrument_id=symbol.broker_info['figi'], quantity=quantity, price=price, direction=direction,
                                       account_id=self.account_id, order_type=OrderType.ORDER_TYPE_LIMIT, order_id=order_id)
            response: PostOrderResponse = self.provider.call_function(self.provider.stub_orders.PostOrder, request)  # Отправляем лимитную заявку брокеру
            if response.execution_report_status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW:  # Должен вернуться статус "Новая"
                order.id = response.order_id  # Уникальный код заявки
                order.status = Order.Submitted  # Заявка отправлена брокеру
                self.orders.append(order)  # Добавляем новую заявку в список заявок
                return True  # Операция завершилась успешно
        elif order.exec_type == Order.Stop:  # Стоп заявка
            direction = StopOrderDirection.STOP_ORDER_DIRECTION_BUY if order.buy else StopOrderDirection.STOP_ORDER_DIRECTION_SELL  # Покупка/продажа
            request = PostStopOrderRequest(instrument_id=symbol.broker_info['figi'], quantity=quantity, stop_price=stop_price,
                                           direction=direction, account_id=self.account_id,
                                           expiration_type=StopOrderExpirationType.STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_CANCEL,
                                           stop_order_type=StopOrderType.STOP_ORDER_TYPE_STOP_LOSS)
            response: PostStopOrderResponse = self.provider.call_function(self.provider.stub_stop_orders.PostStopOrder, request)  # Отправляем стоп заявку брокеру
            order.id = response.order_request_id  # Уникальный код заявки
            order.status = Order.Submitted  # Заявка отправлена брокеру
            self.orders.append(order)  # Добавляем новую заявку в список заявок
            return True  # Операция завершилась успешно
        elif order.exec_type == Order.StopLimit:  # Стоп лимитная заявка
            direction = StopOrderDirection.STOP_ORDER_DIRECTION_BUY if order.buy else StopOrderDirection.STOP_ORDER_DIRECTION_SELL  # Покупка/продажа
            request = PostStopOrderRequest(instrument_id=symbol.broker_info['figi'], quantity=quantity, stop_price=stop_price, price=price,
                                           direction=direction, account_id=self.account_id,
                                           expiration_type=StopOrderExpirationType.STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_CANCEL,
                                           stop_order_type=StopOrderType.STOP_ORDER_TYPE_STOP_LIMIT)
            response: PostStopOrderResponse = self.provider.call_function(self.provider.stub_stop_orders.PostStopOrder, request)  # Отправляем стоп лимитную заявку брокеру
            order.id = response.order_request_id  # Уникальный код заявки
            order.status = Order.Submitted  # Заявка отправлена брокеру
            self.orders.append(order)  # Добавляем новую заявку в список заявок
            return True  # Операция завершилась успешно
        return False  # Операция завершилась с ошибкой

    def cancel_order(self, order):
        if order.exec_type in (Order.Market, Order.Limit):  # Заявка
            request = CancelOrderRequest(account_id=self.account_id, order_id=order.id)
            self.provider.call_function(self.provider.stub_orders.CancelOrder, request)  # Отменяем активную заявку
        else:  # Стоп заявка
            request = CancelStopOrderRequest(account_id=self.account_id, stop_order_id=order.id)
            self.provider.call_function(self.provider.stub_stop_orders.CancelStopOrder, request)  # Отменяем активную стоп заявку

    def subscribe_transactions(self):
        Thread(target=self.provider.subscriptions_trades_handler, name='SubscriptionsTradesThread').start()  # Создаем и запускаем поток обработки подписок на сделки
        Thread(target=self.provider.subscriptions_order_state_handler, name='SubscriptionsOrderStateThread').start()  # Создаем и запускаем поток обработки подписок на заявки

    def unsubscribe_transactions(self):
        pass  # Подписки на позиции, сделки, заявки автоматически закроются при закрытии канала в функции close

    def close(self):
        self.provider.on_candle.unsubscribe(self._on_new_bar)  # Обработка нового бара
        self.provider.on_order_state.unsubscribe(self._on_order)  # Обработка заявок
        self.provider.on_order_trades.unsubscribe(self._on_trade)  # Обработка сделок

        self.provider.close_channel()  # Закрываем канал перед выходом

    # Внутренние функции

    def _get_symbol_info(self, class_code: str = None, sec_code: str = None, figi: str = None) -> Symbol | None:
        """Спецификация тикера по режиму торгов и тикеру или figi"""
        if class_code is not None and sec_code is not None:  # Если передали режим торгов и тикер
            symbol = next((symbol for symbol in self.storage.symbols.values() if symbol.board == class_code and symbol.symbol == sec_code), None)  # Проверяем, есть ли спецификация тикера в хранилище по режиму торгов и тикеру
            if symbol is not None:  # Если есть тикер
                return symbol  # то возвращаем его, выходим, дальше не продолжаем
            request = InstrumentRequest(id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_TICKER, class_code=class_code, id=sec_code)  # Поиск тикера по коду режима торгов/названию
        elif figi is not None:  # Если передали figi
            symbol = next((symbol for symbol in self.storage.symbols.values() if symbol.broker_info['figi'] == figi), None)  # Проверяем, есть ли спецификация тикера в хранилище по figi
            if symbol is not None:  # Если есть тикер
                return symbol  # то возвращаем его, выходим, дальше не продолжаем
            request = InstrumentRequest(id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI, class_code='', id=figi)  # Поиск тикера по figi
        else:  # Если не передали режим торгов или тикер
            return None  # то выходим, дальше не продолжаем
        response: InstrumentResponse = self.provider.call_function(self.provider.stub_instruments.GetInstrumentBy, request)  # Получаем информацию о тикере
        if not response:  # Если информация о тикере не найдена
            return None  # то выходим, дальше не продолжаем
        si = response.instrument  # Информация о тикере
        min_step = self.provider.quotation_to_float(si.min_price_increment)  # Шаг цены
        decimals = 0 if min_step == 0 else int(log10(1 / min_step + 0.99))  # Из шага цены получаем кол-во десятичных знаков
        dataname = self.provider.class_code_symbol_to_dataname(si.class_code, si.ticker)  # Название тикера
        broker_info = {'figi': si.figi, 'first_1min_timestamp': si.first_1min_candle_date.seconds, 'first_1day_timestamp': si.first_1day_candle_date.seconds}  # Информация брокера
        symbol = Symbol(si.class_code, si.ticker, dataname, si.name, decimals, min_step, si.lot, broker_info)
        self.storage.set_symbol(symbol)  # Добавляем спецификацию тикера в хранилище
        return symbol

    def _on_new_bar(self, candle: Candle):
        """Получение нового бара по подписке"""
        symbol = self._get_symbol_info(figi=candle.figi)  # Спецификация тикера по figi
        time_frame = self.provider.tinvest_subscription_timeframe_to_timeframe(candle.interval)  # Временной интервал
        dt_msk = self.provider.google_timestamp_to_msk_datetime(candle.time)  # Дата и время полученного бара
        open_ = self.provider.quotation_to_float(candle.open)
        high = self.provider.quotation_to_float(candle.high)
        low = self.provider.quotation_to_float(candle.low)
        close = self.provider.quotation_to_float(candle.close)
        volume = int(candle.volume) * symbol.lot_size  # Объем в шутках
        self.on_new_bar.trigger(Bar(symbol.board, symbol.symbol, symbol.dataname, time_frame, dt_msk, open_, high, low, close, volume))  # Вызываем событие добавления нового бара

    def _on_trade(self, order_trades: OrderTrades):
        symbol = self._get_symbol_info(figi=order_trades.figi)  # Спецификация тикера
        for trade in order_trades.trades:
            self.on_trade.trigger(Trade(
                self,  # Брокер
                order_trades.order_id,  # Номер заявки из сделки
                symbol.dataname,  # Название тикера
                symbol.description,  # Описание тикера
                symbol.decimals,  # Кол-во десятичных знаков в цене
                self.provider.google_timestamp_to_msk_datetime(trade.date_time),  # Дата и время сделки по времени биржи (МСК)
                trade.quantity,  # Кол-во в штуках
                self.provider.quotation_to_float(trade.price)))  # Цена сделки
        self.on_position.trigger(self.get_position(symbol))  # При любой сделке позиция изменяется. Отправим текущую или пустую позицию по тикеру по подписке

    def _on_order(self, order_state: OrderState):
        if order_state.order_type == OrderType.ORDER_TYPE_MARKET:  # Рыночная заявка
            order_type = Order.Market
        elif order_state.order_type == OrderType.ORDER_TYPE_LIMIT:  # Лимитная заявка
            order_type = Order.Limit
        else:  # Остальные типы заявок
            raise NotImplementedError  # не реализованы
        if order_state.execution_report_status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW:  # Ожидает
            order_status = Order.Accepted
        elif order_state.execution_report_status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_PARTIALLYFILL:  # Частично исполнена
            order_status = Order.Partial
        elif order_state.execution_report_status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL:  # Исполнена
            order_status = Order.Completed
        elif order_state.execution_report_status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_CANCELLED:  # Отменена
            order_status = Order.Canceled
        elif order_state.execution_report_status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_REJECTED:  # Отклонена
            order_status = Order.Rejected
        else:  # Остальные статусы заявок
            raise NotImplementedError  # не реализованы Expired, Margin
        symbol = self._get_symbol_info(figi=order_state.figi)  # Спецификация тикера по figi
        if order_status in (Order.Partial, Order.Completed):  # Если заявка хотя бы частично исполнена
            old_stop_orders = [order for order in self.orders if order.exec_type in (Order.Stop, Order.StopLimit) and order.dataname == symbol.dataname]  # Все стоп и стоп лимитные заявки по тикеру до исполнения заявки
            new_stop_orders = [order for order in self.get_orders() if order.exec_type in (Order.Stop, Order.StopLimit) and order.dataname == symbol.dataname]  # Все стоп и стоп лимитные заявки по тикеру после исполнения заявки
            stop_order_to_fill = list(set(old_stop_orders).difference(new_stop_orders))  # Стоп и стоп лимитные заявки по тикеру, которых не стало после исполнения заявки
            for stop_order in stop_order_to_fill:  # Пробегаемся по всем стоп и стоп лимитным заявкам по тикеру, которых не стало
                if stop_order.buy and order_state.direction == OrderDirection.ORDER_DIRECTION_BUY or not stop_order.buy and order_state.direction == OrderDirection.ORDER_DIRECTION_SELL:  # Если направления заявок совпадают
                    stop_order.status = Order.Completed  # то считаем, что стоп или стоп лимитная заявка исполнена
                else:  # Если направления заявок не совпадают
                    stop_order.status = Order.Canceled  # то считаем, что стоп или стоп лимитная заявка отменена
                self.on_order.trigger(stop_order)
        self.on_order.trigger(Order(
            self,  # Брокер
            order_state.order_id,  # Уникальный код заявки
            order_state.direction == OrderDirection.ORDER_DIRECTION_BUY,  # Покупка/продажа
            order_type,  # Тип заявки
            symbol.dataname,  # Название тикера
            symbol.decimals,  # Кол-во десятичных знаков в цене
            (order_state.lots_requested - order_state.lots_executed) * symbol.lot_size,  # Кол-во в штуках
            self.provider.money_value_to_float(order_state.average_position_price) if order_type == Order.Limit else 0,  # Цена
            0,  # Цена срабатывания стоп заявки
            order_status))  # Статус заявки
