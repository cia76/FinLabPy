# Курс Мультиброкер: Контроль https://finlab.vip/wpm-category/mbcontrol/

from datetime import datetime, timezone, timedelta  # Дата и время, временнАя зона, временной интервал
from threading import Thread  # Запускаем поток подписки
from math import log10  # Кол-во десятичных знаков будем получать из шага цены через десятичный логарифм
from typing import Union  # Объединение типов
from uuid import uuid4  # Номера заявок должны быть уникальными во времени и пространстве

from google.protobuf.json_format import MessageToDict

from FinLabPy.Core import Broker, Bar, Position, Order, Symbol  # Брокер, бар, позиция, заявка, тикер
from TinkoffPy import TinkoffPy  # Работа с Tinkoff Invest API из Python
from TinkoffPy.grpc.operations_pb2 import PortfolioRequest, PortfolioResponse  # Портфель
from TinkoffPy.grpc.marketdata_pb2 import (
    MarketDataRequest, SubscribeCandlesRequest, SubscriptionAction, CandleInstrument,  # Подписка на свечи
    GetCandlesRequest, GetCandlesResponse, Candle, GetLastPricesRequest, GetLastPricesResponse)  # Свечи, стакан
from TinkoffPy.grpc.orders_pb2 import (
    GetOrdersRequest, GetOrdersResponse, PostOrderRequest, CancelOrderRequest, OrderType, ORDER_DIRECTION_BUY, ORDER_DIRECTION_SELL)  # Заявки
from TinkoffPy.grpc.stoporders_pb2 import (
    GetStopOrdersRequest, GetStopOrdersResponse, PostStopOrderRequest, CancelStopOrderRequest, StopOrderExpirationType, StopOrderType, STOP_ORDER_DIRECTION_BUY, STOP_ORDER_DIRECTION_SELL)  # Стоп заявки


class SymbolEx(Symbol):
    """Тикер Т-Инвестиции"""
    figi: str  # Уникальный код тикера


class Tinkoff(Broker):
    """Брокер Т-Инвестиции"""
    def __init__(self, code: str, name: str, provider: TinkoffPy, account_id: int = 0):
        super().__init__(code, name, provider, account_id)
        self.provider = provider  # Уже инициирован в базовом классе. Выполням для того, чтобы работать с типом провайдера
        self.provider.on_candle = self.tk_new_bar  # Перехватываем управление события получения нового бара
        self.account_id = self.provider.accounts[account_id].id  # Номер счета по порядковому номеру

    def get_symbol_by_dataname(self, dataname: str) -> Union[SymbolEx, None]:
        class_code, symbol = self.provider.dataname_to_class_code_symbol(dataname)  # Код режима торгов и тикер
        if (class_code, symbol) not in self.symbols:  # Если в справочнике нет информации о тикере
            si = self.provider.get_symbol_info(class_code, symbol)  # Поиск тикера по коду площадки и тикеру
            if not si:  # Если тикер не найден
                print(f'Информация о тикере {dataname} не найдена')
                return None  # то возвращаем пустое значение
            self.symbols[(class_code, symbol)] = si  # Заносим информацию о тикере в справочник
        si = self.symbols[(class_code, symbol)]  # Получаем информацию о тикера из справочника
        min_step = self.provider.quotation_to_float(si.min_price_increment)  # Шаг цены
        decimals = int(log10(1 / min_step + 0.99))  # Из шага цены получаем кол-во десятичных знаков
        symbol_ex = SymbolEx(class_code, symbol, dataname, si.name, decimals, min_step, si.lot)
        symbol_ex.figi = si.figi  # Уникальный код тикера
        return symbol_ex

    def get_history(self, dataname: str, tf: str, dt_from: datetime = None, dt_to: datetime = None):
        class_code, security_code = self.provider.dataname_to_class_code_symbol(dataname)  # Код режима торгов и тикер
        si = self.provider.get_symbol_info(class_code, security_code)  # Информация о тикере
        time_frame, intraday = self.provider.timeframe_to_tinkoff_timeframe(tf)  # Временной интервал Tinkoff, внутридневной интервал
        _, td = self.provider.tinkoff_timeframe_to_timeframe(time_frame)  # Временной интервал для имени файла и максимальный период запроса
        next_bar_open_utc = None if dt_from is None else self.provider.msk_to_utc_datetime(dt_from, True)  # Первый возможный бар по UTC
        if next_bar_open_utc is None:  # Если он не задан, то возьмем
            next_bar_open_utc = datetime.fromtimestamp(si.first_1min_candle_date.seconds, timezone.utc) if intraday else \
                datetime.fromtimestamp(si.first_1day_candle_date.seconds, timezone.utc)  # Первый минутный/дневной бар истории
        todate_utc = datetime.utcnow().replace(tzinfo=timezone.utc) if dt_to is None else self.provider.msk_to_utc_datetime(dt_to, True)  # Последний возможный бар по UTC
        bars = []  # Список полученных бар
        while True:  # Будем получать бары пока не получим все
            request = GetCandlesRequest(instrument_id=si.figi, interval=time_frame)  # Запрос на получение бар
            from_ = getattr(request, 'from')  # т.к. from - ключевое слово в Python, то получаем атрибут from из атрибута интервала
            to_ = getattr(request, 'to')  # Аналогично будем работать с атрибутом to для единообразия
            from_.seconds = int(next_bar_open_utc.timestamp())  # Дата и время начала интервала UTC
            todate_min_utc = min(todate_utc, next_bar_open_utc + td)  # До какой даты можем делать запрос
            to_.seconds = int(todate_min_utc.timestamp())  # Дата и время окончания интервала UTC
            candles: GetCandlesResponse = self.provider.call_function(self.provider.stub_marketdata.GetCandles, request)  # Получаем ответ на запрос бар
            if not candles:  # Если бары не получены
                print('Ошибка при получении истории: История не получена')
                return None  # то выходим, дальше не продолжаем
            candles_dict = MessageToDict(candles, always_print_fields_with_no_presence=True)  # Переводим в словарь из JSON
            if 'candles' not in candles_dict:  # Если бар нет в словаре
                print(f'Ошибка при получении истории: {candles_dict}')
                return None  # то выходим, дальше не продолжаем
            new_bars_dict = candles_dict['candles']  # Переводим в словарь/список
            if len(new_bars_dict) > 0:  # Если пришли новые бары
                # Дату/время UTC получаем в формате ISO 8601. Пример: 2023-06-16T20:01:00Z
                # В статье https://stackoverflow.com/questions/127803/how-do-i-parse-an-iso-8601-formatted-date описывается проблема, что Z на конце нужно убирать
                first_bar_dt_utc = datetime.fromisoformat(new_bars_dict[0]['time'][:-1])  # Дата и время начала первого полученного бара в UTC
                first_bar_open_dt = self.provider.utc_to_msk_datetime(first_bar_dt_utc) if intraday else \
                    datetime(first_bar_dt_utc.year, first_bar_dt_utc.month, first_bar_dt_utc.day)  # Дату/время переводим из UTC в МСК
                last_bar_dt_utc = datetime.fromisoformat(new_bars_dict[-1]['time'][:-1])  # Дата и время начала последнего полученного бара в UTC
                last_bar_open_dt = self.provider.utc_to_msk_datetime(last_bar_dt_utc) if intraday else \
                    datetime(last_bar_dt_utc.year, last_bar_dt_utc.month, last_bar_dt_utc.day)  # Дату/время переводим из UTC в МСК
                print(f'Получены бары с {first_bar_open_dt} по {last_bar_open_dt}')
                for new_bar in new_bars_dict:  # Пробегаемся по всем полученным барам
                    if not new_bar['isComplete']:  # Если добрались до незавершенного бара
                        break  # то это последний бар, больше бары обрабатывать не будем
                    dt_utc = datetime.fromisoformat(new_bar['time'][:-1])  # Дата и время начала бара в UTC
                    dt = self.provider.utc_to_msk_datetime(dt_utc) if intraday else datetime(dt_utc.year, dt_utc.month, dt_utc.day)  # Дату/время переводим из UTC в МСК
                    open_ = self.provider.tinkoff_price_to_price(class_code, security_code, self.provider.dict_quotation_to_float(new_bar['open']))
                    high = self.provider.tinkoff_price_to_price(class_code, security_code, self.provider.dict_quotation_to_float(new_bar['high']))
                    low = self.provider.tinkoff_price_to_price(class_code, security_code, self.provider.dict_quotation_to_float(new_bar['low']))
                    close = self.provider.tinkoff_price_to_price(class_code, security_code, self.provider.dict_quotation_to_float(new_bar['close']))
                    volume = int(new_bar['volume']) * si.lot  # Объем в шутках
                    bars.append(Bar(class_code, security_code, dataname, tf, dt, open_, high, low, close, volume))  # Добавляем бар
            next_bar_open_utc = todate_min_utc + timedelta(minutes=1) if intraday else todate_min_utc + timedelta(days=1)  # Смещаем время на возможный следующий бар UTC
            if next_bar_open_utc > todate_utc:  # Если пройден весь интервал
                break  # то выходим из цикла получения бар
        if len(bars) == 0:  # Если новых записей нет
            print('Новых записей нет')
            return None  # то выходим, дальше не продолжаем
        return bars

    def subscribe_history(self, dataname: str, time_frame: str):
        class_code, security_code = self.provider.dataname_to_class_code_symbol(dataname)  # Код режима торгов и тикер из названия тикера
        si = self.provider.get_symbol_info(class_code, security_code)  # Спецификация тикера
        Thread(target=self.provider.subscriptions_marketdata_handler, name='TKSubscriptionsMarketdataThread').start()  # Создаем и запускаем поток обработки подписок сделок по заявке
        self.provider.subscription_marketdata_queue.put(  # Ставим в буфер команд подписки на биржевую информацию
            MarketDataRequest(subscribe_candles_request=SubscribeCandlesRequest(  # запрос на новые бары
                subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,  # подписка
                instruments=(CandleInstrument(interval=self.provider.timeframe_to_tinkoff_subscription_timeframe(time_frame),
                                              instrument_id=si.figi),),  # на тикер по временному интервалу 1 минута
                waiting_close=True)))  # по закрытию бара

    def tk_new_bar(self, candle: Candle):
        si = self.provider.figi_to_symbol_info(candle.figi)  # Спецификация тикера по уникальному коду инструмента
        dataname = self.provider.class_code_symbol_to_dataname(si.class_code, si.ticker)  # Название тикера из кода режима торгов и кода тикера
        time_frame = self.provider.tinkoff_subscription_timeframe_to_timeframe(candle.interval)  # Временной интервал
        dt_msk = self.provider.utc_to_msk_datetime(datetime.utcfromtimestamp(candle.time.seconds))
        open_ = self.provider.quotation_to_float(candle.open)
        high = self.provider.quotation_to_float(candle.high)
        low = self.provider.quotation_to_float(candle.low)
        close = self.provider.quotation_to_float(candle.close)
        volume = int(candle.volume) * si.lot  # Объем в шутках
        self.on_new_bar(Bar(si.class_code, si.ticker, dataname, time_frame, dt_msk, open_, high, low, close, volume))  # Вызываем событие добавления нового бара

    def get_last_price(self, dataname: str):
        si = self.get_symbol_by_dataname(dataname)  # Тикер по названию
        request = GetLastPricesRequest(instrument_id=[si.figi])
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
            instrument = self.provider.figi_to_symbol_info(position.figi)  # Поиск тикера по уникальному коду
            class_code = instrument.class_code  # Код площадки
            if class_code == 'CETS':  # Валюты
                continue  # за позиции не считаем
            dataname = self.provider.class_code_symbol_to_dataname(class_code, instrument.ticker)  # Название тикера
            si = self.get_symbol_by_dataname(dataname)  # Тикер
            self.positions.append(Position(  # Добавляем текущую позицию в список
                self,  # Брокер
                dataname,  # Название тикера
                si.description,  # Описание тикера
                si.decimals,  # Кол-во десятичных знаков в цене
                int(self.provider.quotation_to_float(position.quantity)),  # Кол-во в штуках
                self.provider.money_value_to_float(position.average_position_price),  # Средняя цена входа в рублях
                self.provider.money_value_to_float(position.current_price)))  # Последняя цена в рублях
        return self.positions

    def get_orders(self):
        self.orders = []  # Активные заявки
        request = GetOrdersRequest(account_id=self.account_id)
        response: GetOrdersResponse = self.provider.call_function(self.provider.stub_orders.GetOrders, request)  # Получаем активные заявки
        for order in response.orders:  # Пробегаемся по всем заявкам
            si = self.provider.figi_to_symbol_info(order.figi)  # Поиск тикера по уникальному коду
            min_step = self.provider.quotation_to_float(si.min_price_increment)  # Шаг цены
            decimals = int(log10(1 / min_step) + 0.99)  # Кол-во десятичных знаков
            self.orders.append(Order(  # Добавляем заявки в список
                self,  # Брокер
                order.order_id,  # Уникальный код заявки (номер транзакции)
                order.direction == ORDER_DIRECTION_BUY,  # Покупка/продажа
                Order.Limit if order.order_type == OrderType.ORDER_TYPE_LIMIT else Order.Market,  # Лимит/по рынку
                self.provider.class_code_symbol_to_dataname(si.class_code, si.ticker),  # Название тикера
                decimals,  # Кол-во десятичных знаков в цене
                order.lots_requested * si.lot,  # Кол-во в штуках
                self.provider.money_value_to_float(order.initial_security_price)))  # Цена
        request = GetStopOrdersRequest(account_id=self.account_id)
        response: GetStopOrdersResponse = self.provider.call_function(self.provider.stub_stop_orders.GetStopOrders, request)  # Получаем активные стоп заявки
        for stop_order in response.stop_orders:  # Пробегаемся по всем стоп заявкам
            si = self.provider.figi_to_symbol_info(stop_order.figi)  # Поиск тикера по уникальному коду
            min_step = self.provider.quotation_to_float(si.min_price_increment)  # Шаг цены
            decimals = int(log10(1 / min_step) + 0.99)  # Кол-во десятичных знаков
            self.orders.append(Order(  # Добавляем заявки в список
                self,  # Брокер
                stop_order.stop_order_id,  # Уникальный код заявки (номер транзакции)
                stop_order.direction == STOP_ORDER_DIRECTION_BUY,  # Покупка/продажа
                Order.StopLimit if stop_order.order_type == StopOrderType.STOP_ORDER_TYPE_STOP_LIMIT else Order.Stop,  # Лимит/по рынку
                self.provider.class_code_symbol_to_dataname(si.class_code, si.ticker),  # Название тикера
                decimals,  # Кол-во десятичных знаков в цене
                stop_order.lots_requested * si.lot,  # Кол-во в штуках
                self.provider.money_value_to_float(stop_order.stop_price)))  # Цена
        return self.orders

    def new_order(self, order: Order):
        class_code, symbol = self.provider.dataname_to_class_code_symbol(order.dataname)  # Код площадки и тикер
        si = self.provider.get_symbol_info(class_code, symbol)  # Поиск тикера по площадке
        quantity: int = abs(order.quantity // si.lot)  # Размер позиции в лотах. В Тинькофф всегда передается положительный размер лота
        price = 0 if order.exec_type == Order.Market else self.provider.float_to_quotation(order.price)  # Для рыночной заявки цену не ставим
        stop_price = self.provider.float_to_quotation(order.stop_price)  # Стоп цена
        order_id = str(uuid4())  # Уникальный идентификатор заявки
        if order.exec_type == Order.Market:  # Рыночная заявка
            direction = ORDER_DIRECTION_BUY if order.buy else ORDER_DIRECTION_SELL  # Покупка/продажа
            request = PostOrderRequest(instrument_id=si.figi, quantity=quantity, direction=direction,
                                       account_id=self.account_id, order_type=OrderType.ORDER_TYPE_MARKET, order_id=order_id)
            self.provider.call_function(self.provider.stub_orders.PostOrder, request)  # Отправляем рыночную заявку брокеру
        elif order.exec_type == Order.Limit:  # Лимитная заявка
            direction = ORDER_DIRECTION_BUY if order.buy else ORDER_DIRECTION_SELL  # Покупка/продажа
            request = PostOrderRequest(instrument_id=si.figi, quantity=quantity, price=price, direction=direction,
                                       account_id=self.account_id, order_type=OrderType.ORDER_TYPE_LIMIT, order_id=order_id)
            self.provider.call_function(self.provider.stub_orders.PostOrder, request)  # Отправляем лимитную заявку брокеру
        elif order.exec_type == Order.Stop:  # Стоп заявка
            direction = STOP_ORDER_DIRECTION_BUY if order.buy else STOP_ORDER_DIRECTION_SELL  # Покупка/продажа
            request = PostStopOrderRequest(instrument_id=si.figi, quantity=quantity, stop_price=stop_price,
                                           direction=direction, account_id=self.account_id,
                                           expiration_type=StopOrderExpirationType.STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_CANCEL,
                                           stop_order_type=StopOrderType.STOP_ORDER_TYPE_STOP_LOSS)
            self.provider.call_function(self.provider.stub_stop_orders.PostStopOrder, request)  # Отправляем стоп заявку брокеру
        elif order.exec_type == Order.StopLimit:  # Стоп-лимитная заявка
            direction = STOP_ORDER_DIRECTION_BUY if order.buy else STOP_ORDER_DIRECTION_SELL  # Покупка/продажа
            request = PostStopOrderRequest(instrument_id=si.figi, quantity=quantity, stop_price=stop_price, price=price,
                                           direction=direction, account_id=self.account_id,
                                           expiration_type=StopOrderExpirationType.STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_CANCEL,
                                           stop_order_type=StopOrderType.STOP_ORDER_TYPE_STOP_LIMIT)
            self.provider.call_function(self.provider.stub_stop_orders.PostStopOrder, request)  # Отправляем стоп-лимитную заявку брокеру

    def cancel_order(self, order: Order):
        if order.exec_type in (Order.Market, Order.Limit):  # Заявка
            request = CancelOrderRequest(account_id=self.account_id, order_id=order.id)
            self.provider.call_function(self.provider.stub_orders.CancelOrder, request)  # Отменяем активную заявку
        else:  # Стоп заявка
            request = CancelStopOrderRequest(account_id=self.account_id, stop_order_id=order.id)
            self.provider.call_function(self.provider.stub_stop_orders.CancelStopOrder, request)  # Отменяем активную стоп заявку

    def close(self):
        self.provider.close_channel()  # Закрываем канал перед выходом
