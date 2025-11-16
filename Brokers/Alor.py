import logging
from datetime import datetime, UTC

from FinLabPy.Core import Broker, Bar, Position, Trade, Order, Symbol  # Брокер, бар, позиция, сделка, заявка, тикер
from AlorPy import AlorPy  # Работа с Alor OpenAPI V2 из Python через REST/WebSockets


class Alor(Broker):
    """Брокер Алор"""
    def __init__(self, code, name, provider: AlorPy, account_id=0, exchange=AlorPy.exchanges[0], storage='file'):
        super().__init__(code, name, provider, account_id, storage)
        logging.getLogger('urllib3').setLevel(logging.CRITICAL + 1)  # Не получаем сообщения подключений и отправки запросов в лог
        logging.getLogger('websockets').setLevel(logging.CRITICAL + 1)  # Не получаем сообщения поддерживания подключения в лог
        self.provider = provider  # Уже инициирован в базовом классе. Выполням для того, чтобы работать с типом провайдера
        account = self.provider.accounts[self.account_id]  # Номер счета по порядковому номеру
        self.portfolio = account['portfolio']  # Портфель
        self.exchange = exchange  # Биржа

        self.provider.on_new_bar.subscribe(self._on_new_bar)  # Подписка на новые бары
        self.provider.on_order.subscribe(self._on_order)  # Подписка на заявки
        self.provider.on_stop_order.subscribe(self._on_stop_order_v2)  # Подписка на стоп заявки
        self.provider.on_trade.subscribe(self._on_trade)  # Подписка на сделки
        self.provider.on_position.subscribe(self._on_position)  # Подписка на позиции

    def get_symbol_by_dataname(self, dataname):
        symbol = self.storage.get_symbol(dataname)  # Проверяем, есть ли спецификация тикера в хранилище
        if symbol is not None:  # Если есть тикер
            return symbol  # то возвращаем его, выходим, дальше не продолжаем
        alor_board, alor_symbol = self.provider.dataname_to_alor_board_symbol(dataname)  # Код режима торгов Алора и тикер
        exchange = self.provider.get_exchange(alor_board, alor_symbol)  # Биржа
        return self._get_symbol_info(exchange, alor_symbol)

    def get_history(self, symbol, time_frame, dt_from=None, dt_to=None):
        bars = super().get_history(symbol, time_frame, dt_from, dt_to)  # Получаем бары из хранилища
        if bars is None:  # Если бары из хранилища не получены
            bars = []  # Пока список полученных бар пустой
            seconds_from = 0 if dt_from is None else self.provider.msk_datetime_to_timestamp(dt_from)  # Первый возможный бар
        else:  # Если бары из хранилища получены
            seconds_from = self.provider.msk_datetime_to_timestamp(bars[-1].datetime)  # Дата и время открытия последнего бара
            del bars[-1]  # Удаляем последний бар. Он перепишется первым полученным баром за период
        seconds_to = self.provider.msk_datetime_to_timestamp(datetime.now() if dt_to is None else dt_to)  # Последний возможный бар
        alor_tf, intraday = self.provider.timeframe_to_alor_timeframe(time_frame)  # Временной интервал Алор с признаком внутридневного интервала
        exchange = symbol.broker_info['exchange']  # Биржа
        history = self.provider.get_history(exchange, symbol.symbol, alor_tf, seconds_from, seconds_to)  # Запрос истории рынка
        if 'history' not in history:  # Если в полученной истории нет ключа history
            return None  # то выходим, дальше не продолжаем
        for bar in history['history']:  # Пробегаемся по всем пришедшим барам
            dt_msk = self.provider.timestamp_to_msk_datetime(bar['time']) if intraday else datetime.fromtimestamp(bar['time'], UTC).replace(tzinfo=None)  # Дневные бары и выше ставим на начало дня по UTC. Остальные - по МСК
            open_ = self.provider.alor_price_to_price(exchange, symbol.symbol, bar['open'])  # Конвертируем цены
            high = self.provider.alor_price_to_price(exchange, symbol.symbol, bar['high'])  # из цен Алор
            low = self.provider.alor_price_to_price(exchange, symbol.symbol, bar['low'])  # в зависимости от
            close = self.provider.alor_price_to_price(exchange, symbol.symbol, bar['close'])  # режима торгов
            volume = self.provider.lots_to_size(exchange, symbol.symbol, int(bar['volume']))  # Объем в штуках
            bars.append(Bar(symbol.board, symbol.symbol, symbol.dataname, time_frame, dt_msk, open_, high, low, close, volume))  # Добавляем бар
        self.storage.set_bars(bars)  # Сохраняем бары в хранилище
        return bars

    def subscribe_history(self, symbol, time_frame):
        if (symbol, time_frame) in self.history_subscriptions.keys():  # Если подписка уже есть
            return  # то выходим, дальше не продолжаем
        exchange = symbol.broker_info['exchange']  # Биржа
        alor_tf, _ = self.provider.timeframe_to_alor_timeframe(time_frame)  # Временной интервал Алор
        seconds_from = int(datetime.now(UTC).timestamp())  # Изначально подписываемся с текущего момента времени по UTC
        self.history_subscriptions[(symbol, time_frame)] = self.provider.bars_get_and_subscribe(exchange, symbol.symbol, alor_tf, seconds_from=seconds_from, frequency=1_000_000_000)  # Подписываемся на бары, добавляем уникальный идентификатор подписки в справочник подписок

    def unsubscribe_history(self, symbol, time_frame):
        self.provider.unsubscribe(self.history_subscriptions[(symbol, time_frame)])  # Отменяем подписку на бары
        del self.history_subscriptions[(symbol, time_frame)]  # Удаляем из справочника подписок

    def get_last_price(self, symbol):
        exchange = symbol.broker_info['exchange']  # Биржа
        quotes = self.provider.get_quotes(f'{exchange}:{symbol.symbol}')[0]  # Последнюю котировку получаем через запрос
        return quotes['last_price'] if quotes else None  # Последняя цена сделки

    def get_value(self):
        value = self.provider.get_risk(self.portfolio, self.exchange)['portfolioLiquidationValue']  # Общая стоимость портфеля
        return round(value - self.get_cash(), 2)  # Стоимость позиций = Общая стоимость портфеля - Свободные средства

    def get_cash(self):
        if self.portfolio[0:3] == '750':  # Для счета срочного рынка
            cash = round(self.provider.get_forts_risk(self.portfolio, self.exchange)['moneyFree'], 2)  # Свободные средства. Сумма рублей и залогов, дисконтированных в рубли, доступная для открытия позиций. (MoneyFree = MoneyAmount + VmInterCl – MoneyBlocked – VmReserve – Fee)
        else:  # Для остальных счетов
            cash = next((position['qtyUnits'] for position in self.provider.get_positions(self.portfolio, self.exchange, False) if position['symbol'] == 'RUB'), 0)  # Свободные средства через денежную позицию
        return round(cash, 2)

    def get_positions(self):
        self.positions = []  # Текущие позиции
        for position in self.provider.get_positions(self.portfolio, self.exchange, True):  # Пробегаемся по всем позициям без денежной позиции
            if position['qty'] == 0:  # Если кол-во нулевое (позиция закрыта)
                continue  # то переходим на следующую позицию, дальше не продолжаем
            alor_symbol = position['symbol']  # Тикер
            exchange = position['exchange']  # Биржа
            symbol = self._get_symbol_info(exchange, alor_symbol)  # Спецификация тикера по бирже и тикеру Алора
            size = self.provider.lots_to_size(exchange, symbol.symbol, position['qty'])  # Кол-во в штуках
            entry_price = self.provider.alor_price_to_price(exchange, symbol.symbol, position['avgPrice'])  # Цена входа
            # last_price = position['currentVolume'] / size  # Последняя цена по bid/ask
            last_price = entry_price + position['unrealisedPl'] / size  # Последняя цена по бумажной прибыли/убытку
            self.positions.append(Position(  # Добавляем текущую позицию в список
                self,  # Брокер
                symbol.dataname,  # Название тикера
                symbol.description,  # Описание тикера
                symbol.decimals,  # Кол-во десятичных знаков в цене
                size,  # Кол-во в штуках
                entry_price,  # Средняя цена входа в рублях
                last_price))  # Последняя цена в рублях
        return self.positions

    def get_orders(self):
        self.orders = []  # Активные заявки
        orders = self.provider.get_orders(self.portfolio, self.exchange)  # Получаем список активных заявок
        for order in orders:  # Пробегаемся по всем активным заявкам
            if order['status'] != 'working':  # Если заявка исполнена/отменена/отклонена
                continue  # то переходим к следующей заявке, дальше не продолжаем
            alor_symbol = order['symbol']  # Тикер
            exchange = order['exchange']  # Биржа
            symbol = self._get_symbol_info(exchange, alor_symbol)  # Спецификация тикера по бирже и тикеру Алора
            self.orders.append(Order(  # Добавляем заявки в список
                self,  # Брокер
                order['id'],  # Уникальный код заявки
                order['side'] == 'buy',  # Покупка/продажа
                Order.Limit if order['price'] else Order.Market,  # Лимит/по рынку
                symbol.dataname,  # Название тикера
                symbol.decimals,  # Кол-во десятичных знаков в цене
                self.provider.lots_to_size(exchange, symbol.symbol, order['qty']),  # Кол-во в штуках
                self.provider.alor_price_to_price(exchange, symbol.symbol, order['price']),  # Цена
                0,  # Цена срабатывания стоп заявки
                Order.Accepted if int(order['filledQtyBatch']) == 0 else Order.Partial))  # Статус
        stop_orders = self.provider.get_stop_orders(self.portfolio, self.exchange)  # Получаем список активных стоп заявок
        for stop_order in stop_orders:  # Пробегаемся по всем активным стоп заявкам
            if stop_order['status'] != 'working':  # Если заявка исполнена/отменена/отклонена
                continue  # то переходим к следующей стоп заявке, дальше не продолжаем
            alor_symbol = stop_order['symbol']  # Тикер
            exchange = stop_order['exchange']  # Биржа
            symbol = self._get_symbol_info(exchange, alor_symbol)  # Спецификация тикера по бирже и тикеру Алора
            self.orders.append(Order(  # Добавляем заявки в список
                self,  # Брокер
                stop_order['id'],  # Уникальный код заявки
                stop_order['side'] == 'buy',  # Покупка/продажа
                Order.StopLimit if stop_order['price'] else Order.Stop,  # Стоп-лимит/стоп
                symbol.dataname,  # Название тикера
                symbol.decimals,  # Кол-во десятичных знаков в цене
                self.provider.lots_to_size(exchange, symbol.symbol, stop_order['qty']),  # Кол-во в штуках
                self.provider.alor_price_to_price(exchange, symbol.symbol, stop_order['price']),  # Цена
                self.provider.alor_price_to_price(exchange, symbol.symbol, stop_order['stopPrice']),  # Цена срабатывания стоп заявки
                Order.Accepted))  # Статус
        return self.orders

    def new_order(self, order):
        symbol = self.get_symbol_by_dataname(order.dataname)  # Тикер
        exchange = symbol.broker_info['exchange']  # Биржа
        side = 'buy' if order.buy else 'sell'  # Покупка/продажа
        quantity = self.provider.size_to_lots(exchange, symbol.symbol, order.quantity)  # Кол-во в лотах
        price = self.provider.price_to_alor_price(exchange, symbol.symbol, order.price)  # Цена
        stop_price = self.provider.price_to_alor_price(exchange, symbol.symbol, order.stop_price)  # Стоп цена
        condition = 'MoreOrEqual' if order.buy else 'LessOrEqual'  # Условие срабатывания стоп цены
        response = None  # Результат запроса
        if order.exec_type == Order.Market:  # Рыночная заявка
            response = self.provider.create_market_order(self.portfolio, exchange, symbol.symbol, side, quantity, symbol.board)
        elif order.exec_type == Order.Limit:  # Лимитная заявка
            response = self.provider.create_limit_order(self.portfolio, exchange, symbol.symbol, side, quantity, price, symbol.board)
        elif order.exec_type == Order.Stop:  # Стоп заявка
            response = self.provider.create_stop_order(self.portfolio, exchange, symbol.symbol, side, quantity, stop_price, symbol.board, condition)
        elif order.exec_type == Order.StopLimit:  # Стоп-лимитная заявка
            response = self.provider.create_stop_limit_order(self.portfolio, exchange, symbol.symbol, side, quantity, stop_price, price, symbol.board, condition)
        if not response:  # Если при отправке заявки на биржу произошла веб ошибка
            return False  # Операция завершилась с ошибкой
        order.id = response['orderNumber']  # Сохраняем пришедший номер заявки на бирже
        order.status = Order.Submitted  # Заявка отправлена брокеру
        self.orders.append(order)  # Добавляем новую заявку в список заявок
        return True  # Операция завершилась успешно

    def cancel_order(self, order):
        symbol = self.get_symbol_by_dataname(order.dataname)  # Тикер
        exchange = symbol.broker_info['exchange']  # Биржа
        stop = order.exec_type in (Order.Stop, Order.StopLimit)  # Удаляем стоп заявку
        self.provider.delete_order(self.portfolio, exchange, int(order.id), stop)  # Отменяем заявку по номеру

    def subscribe_transactions(self):
        self.provider.positions_get_and_subscribe_v2(self.portfolio, self.exchange)  # Подписка на позиции (получение свободных средств и стоимости позиций)
        self.provider.trades_get_and_subscribe_v2(self.portfolio, self.exchange)  # Подписка на сделки (изменение статусов заявок)
        self.provider.orders_get_and_subscribe_v2(self.portfolio, self.exchange)  # Подписка на заявки (снятие заявок с биржи)
        self.provider.stop_orders_get_and_subscribe_v2(self.portfolio, self.exchange)  # Подписка на стоп заявки (исполнение или снятие заявок с биржи)

    def unsubscribe_transactions(self):
        subscriptions = self.provider.subscriptions.copy()  # Работаем с копией подписок, т.к. будем удалять элементы
        for guid, subscription_request in subscriptions.items():  # Пробегаемся по всем подпискам
            if subscription_request['opcode'] in \
                    ('PositionsGetAndSubscribeV2',  # Если это подписка на позиции (получение свободных средств и стоимости позиций)
                     'TradesGetAndSubscribeV2',  # или подписка на сделки (изменение статусов заявок)
                     'OrdersGetAndSubscribeV2',  # или подписка на заявки (снятие заявок с биржи)
                     'StopOrdersGetAndSubscribeV2'):  # или подписка на стоп заявки (исполнение или снятие заявок с биржи)
                self.provider.unsubscribe(guid)  # то отменяем подписку

    def close(self):
        self.provider.on_new_bar.unsubscribe(self._on_new_bar)  # Отмена подписки на новые бары
        self.provider.on_order.unsubscribe(self._on_order)  # Отмена подписки на заявки
        self.provider.on_stop_order.unsubscribe(self._on_stop_order_v2)  # Отмена подписки на стоп заявки
        self.provider.on_trade.unsubscribe(self._on_trade)  # Отмена подписки на сделки
        self.provider.on_position.unsubscribe(self._on_position)  # Отмена подписки на позиции

        self.provider.close_web_socket()  # Перед выходом закрываем соединение с сервером WebSocket

    # Внутренние функции

    def _get_symbol_info(self, exchange: str, alor_symbol: str) -> Symbol | None:
        """Спецификация тикера по бирже и коду Алора"""
        symbol = next((symbol for symbol in self.storage.symbols.values() if symbol.symbol == alor_symbol and symbol.broker_info['exchange'] == exchange), None)  # Проверяем, есть ли спецификация тикера в хранилище по тикеру и бирже
        if symbol is not None:  # Если есть тикер
            return symbol  # то возвращаем его, выходим, дальше не продолжаем
        si = self.provider.get_symbol_info(exchange, alor_symbol)  # Спецификация тикера
        if 'board' not in si:  # Если тикер не получен
            return None  # то выходим, дальше не продолжаем
        board = self.provider.alor_board_to_board(si['board'])  # Канонический код режима торгов
        dataname = self.provider.alor_board_symbol_to_dataname(si['board'], alor_symbol)  # Название тикера
        broker_info = {'exchange': exchange}  # Информация брокера
        symbol = Symbol(board, alor_symbol, dataname, si['shortname'], si['decimals'], si['minstep'], si['lotsize'], broker_info)  # Составляем спецификацию тикера
        self.storage.set_symbol(symbol)  # Добавляем спецификацию тикера в хранилище
        return symbol

    def _on_new_bar(self, response):
        """Получение нового бара по подписке"""
        response_data = response['data']  # Данные бара
        utc_timestamp = response_data['time']  # Время в Alor OpenAPI V2 передается в секундах, прошедших с 01.01.1970 00:00 UTC
        subscription = self.provider.subscriptions[response['guid']]  # Получаем данные подписки
        exchange = subscription['exchange']  # Биржа
        alor_symbol = subscription['code']  # Тикер
        symbol = self._get_symbol_info(exchange, alor_symbol)  # Спецификация тикера
        alor_tf = subscription['tf']  # Временной интервал Алор
        time_frame, intraday = self.provider.alor_timeframe_to_timeframe(alor_tf)  # Временной интервал с признаком внутридневного интервала
        dt_msk = self.provider.timestamp_to_msk_datetime(utc_timestamp) if intraday else datetime.fromtimestamp(utc_timestamp)  # Дневные бары и выше ставим на начало дня по UTC. Остальные - по МСК
        open_ = self.provider.alor_price_to_price(exchange, symbol.symbol, response_data['open'])  # Конвертируем цены
        high = self.provider.alor_price_to_price(exchange, symbol.symbol, response_data['high'])  # из цен Алор
        low = self.provider.alor_price_to_price(exchange, symbol.symbol, response_data['low'])  # в зависимости от
        close = self.provider.alor_price_to_price(exchange, symbol.symbol, response_data['close'])  # режима торгов
        volume = self.provider.lots_to_size(exchange, symbol.symbol, int(response_data['volume']))  # Объем в штуках
        self.on_new_bar.trigger(Bar(symbol.board, symbol.symbol, symbol.dataname, time_frame, dt_msk, open_, high, low, close, volume))  # Вызываем событие добавления нового бара

    def _on_position(self, response):
        """Получение позиции по подписке"""
        position = response['data']  # Данные позиции
        if position['isCurrency']:  # Если пришли валютные остатки (деньги)
            return  # то выходим, дальше не продолжаем
        exchange = position['exchange']  # Биржа
        alor_symbol = position['symbol']  # Тикер
        symbol = self._get_symbol_info(exchange, alor_symbol)  # Спецификация тикера
        self.on_position.trigger(Position(
            self,  # Брокер
            symbol.dataname,  # Название тикера
            symbol.description,  # Описание тикера
            symbol.decimals,  # Кол-во десятичных знаков в цене
            self.provider.lots_to_size(exchange, symbol.symbol, position['qty']),  # Кол-во в штуках
            self.provider.alor_price_to_price(exchange, symbol.symbol, position['avgPrice']),  # Цена входа в рублях за штуку
            self.get_last_price(symbol)))  # Последняя цена в рублях за штуку

    def _on_trade(self, response):
        """Получение сделки по подписке"""
        trade = response['data']  # Данные сделки
        if trade['existing']:  # При (пере)подключении к серверу передаются сделки как из истории, так и новые. Если сделка из истории
            return  # то выходим, дальше не продолжаем
        exchange = trade['exchange']  # Биржа
        alor_symbol = trade['symbol']  # Тикер
        symbol = self._get_symbol_info(exchange, alor_symbol)  # Спецификация тикера
        str_utc = trade['date'][:19]  # Возвращается значение типа: '2023-02-16T09:25:01.4335364Z'. Берем первые 20 символов до точки перед наносекундами
        dt_utc = datetime.strptime(str_utc, '%Y-%m-%dT%H:%M:%S')  # Переводим в дату и время UTC
        dt = self.provider.utc_to_msk_datetime(dt_utc)  # Дата и время сделки по времени биржи (МСК)
        quantity = self.provider.lots_to_size(exchange, symbol.symbol, trade['qty'])  # Кол-во в штуках. Всегда положительное
        if trade['side'] == 'sell':  # Если сделка на продажу
            quantity *= -1  # то кол-во ставим отрицательным
        self.on_trade.trigger(Trade(
            self,  # Брокер
            trade['orderno'],  # Номер заявки из сделки
            symbol.dataname,  # Название тикера
            symbol.description,  # Описание тикера
            symbol.decimals,  # Кол-во десятичных знаков в цене
            dt,  # Дата и время сделки по времени биржи (МСК)
            quantity,  # Кол-во в штуках
            self.provider.alor_price_to_price(exchange, symbol.symbol, trade['price'])))  # Цена сделки

    def _on_order(self, response):
        """Получение рыночной/лимитной заявки по подписке"""
        order = response['data']  # Данные заявки
        exchange = order['exchange']  # Биржа
        symbol = self._get_symbol_info(exchange, order['symbol'])  # Спецификация тикера
        status = order['status']  # Статус заявки: working - на исполнении, filled - исполнена, canceled - отменена, rejected - отклонена
        if status == 'working':  # На исполнении
            order_status = Order.Accepted if int(order['filledQtyBatch']) == 0 else Order.Partial  # Если нет исполненных лотов, то заявка принята, иначе частично исполнена
        elif status == 'filled':  # Исполнена
            order_status = Order.Completed
        elif status == 'canceled':  # Отменена
            order_status = Order.Canceled
        else:  # Отклонена
            order_status = Order.Rejected
        self.on_order.trigger(Order(
            self,  # Брокер
            order['id'],  # Уникальный код заявки
            order['side'] == 'buy',  # Покупка/продажа
            Order.Limit if order['price'] else Order.Market,  # Лимит/по рынку
            symbol.dataname,  # Название тикера
            symbol.decimals,  # Кол-во десятичных знаков в цене
            self.provider.lots_to_size(exchange, symbol.symbol, order['qty']),  # Кол-во в штуках
            self.provider.alor_price_to_price(exchange, symbol.symbol, order['price']),  # Цена
            0,  # Цена срабатывания стоп заявки
            order_status))  # Статус заявки (без Expired/Margin)

    def _on_stop_order_v2(self, response):
        """Получение стоп/стоп лимитной заявки по подписке"""
        stop_order = response['data']  # Данные заявки
        exchange = stop_order['exchange']  # Биржа
        symbol = self._get_symbol_info(exchange, stop_order['symbol'])  # Спецификация тикера
        status = stop_order['status']  # Статус заявки: working - на исполнении, filled - исполнена, canceled - отменена, rejected - отклонена
        if status == 'working':  # На исполнении
            order_status = Order.Accepted if int(stop_order['filledQtyBatch']) == 0 else Order.Partial  # Если нет исполненных лотов, то заявка принята, иначе частично исполнена
        elif status == 'filled':  # Исполнена
            order_status = Order.Completed
        elif status == 'canceled':  # Отменена
            order_status = Order.Canceled
        else:  # Отклонена
            order_status = Order.Rejected
        self.on_order.trigger(Order(
            self,  # Брокер
            stop_order['id'],  # Уникальный код заявки
            stop_order['side'] == 'buy',  # Покупка/продажа
            Order.StopLimit if stop_order['price'] else Order.Stop,  # Стоп-лимит/стоп
            symbol.dataname,  # Название тикера
            symbol.decimals,  # Кол-во десятичных знаков в цене
            stop_order['qty'] * symbol.lot_size,  # Кол-во в штуках
            self.provider.alor_price_to_price(exchange, symbol.symbol, stop_order['price']),  # Цена
            self.provider.alor_price_to_price(exchange, symbol.symbol, stop_order['stopPrice']),  # Цена срабатывания стоп заявки
            order_status))  # Статус заявки (без Expired/Margin)
