from datetime import datetime
import itertools  # Итератор для уникальных номеров транзакций

from FinLabPy.Core import Broker, Bar, Position, Trade, Order, Symbol  # Брокер, бар, позиция, сделка, заявка, тикер
from QuikPy.QuikPy import QuikPy  # Работа с QUIK из Python через LUA скрипты QuikSharp


class Quik(Broker):
    """Брокер QUIK"""
    def __init__(self, code, name, provider: QuikPy, account_id=0, limit_kind=1, lots=True, storage='file'):
        super().__init__(code, name, provider, account_id, storage)
        self.provider = provider  # Уже инициирован в базовом классе. Выполням для того, чтобы работать с типом провайдера
        self.account = next((account for account in self.provider.accounts if account['account_id'] == account_id), self.provider.accounts[0])  # Счет
        self.limit_kind = limit_kind  # Срок расчетов
        self.lots = lots  # Входящий остаток в лотах (задается брокером)
        self.class_codes = self.provider.get_classes_list()['data']  # Режимы торгов через запятую
        self.trans_id = itertools.count(1)  # Номер транзакции задается пользователем. Он будет начинаться с 1 и каждый раз увеличиваться на 1
        self.trade_nums = {}  # Список номеров сделок по тикеру для фильтрации дублей сделок

        self.provider.on_new_candle.subscribe(self._on_new_bar)  # Обработка нового бара
        self.provider.on_trans_reply.subscribe(self._on_trans_reply)  # Обработка транзакций
        self.provider.on_order.subscribe(self._on_order)  # Обработка заявок
        self.provider.on_stop_order.subscribe(self._on_stop_order)  # Обработка стоп заявок
        self.provider.on_trade.subscribe(self._on_trade)  # Обработка сделок

    def get_symbol_by_dataname(self, dataname):
        symbol = self.storage.get_symbol(dataname)  # Проверяем, есть ли спецификация тикера в хранилище
        if symbol is not None:  # Если есть тикер
            return symbol  # то возвращаем его, дальше не продолжаем
        class_code, sec_code = self.provider.dataname_to_class_sec_codes(dataname)  # Код режима торгов и тикер
        if not class_code:  # Если код режима торгов не найден
            return None  # То выходим, дальше не продолжаем
        return self._get_symbol_info(class_code, sec_code)

    def get_history(self, symbol, time_frame, dt_from=None, dt_to=None):
        quik_tf, _ = self.provider.timeframe_to_quik_timeframe(time_frame)  # Временной интервал QUIK
        history = self.provider.get_candles_from_data_source(symbol.board, symbol.symbol, quik_tf)  # Получаем все бары из QUIK. Фильтрацию по дате и времени будем делать при разборе баров
        if not history:  # Если бары не получены
            return None  # то выходим, дальше не продолжаем
        if 'data' not in history:  # Если бар нет в словаре
            return None  # то выходим, дальше не продолжаем
        bars = []  # Список полученных бар
        for bar in history['data']:  # Пробегаемся по всем полученным барам
            dt = datetime(bar['datetime']['year'], bar['datetime']['month'], bar['datetime']['day'], bar['datetime']['hour'], bar['datetime']['min'])  # Собираем дату и время бара до минут
            if dt_from and dt_from > dt:  # Если задана дата начала, и она позже даты и времени бара
                continue  # то пропускаем этот бар
            if dt_to and dt_to < dt:  # Если задана дата окончания, и она раньше даты и времени бара
                continue  # то пропускаем этот бар
            bars.append(Bar(symbol.board, symbol.symbol, symbol.dataname, time_frame, dt, bar['open'], bar['high'], bar['low'], bar['close'], int(bar['volume'])))  # Добавляем бар
        self.storage.set_bars(bars)  # Сохраняем бары в хранилище
        return bars

    def subscribe_history(self, symbol, time_frame):
        if (symbol, time_frame) in self.history_subscriptions.keys():  # Если подписка уже есть
            return  # то выходим, дальше не продолжаем
        quik_tf, _ = self.provider.timeframe_to_quik_timeframe(time_frame)  # Временной интервал QUIK
        self.provider.subscribe_to_candles(symbol.board, symbol.symbol, quik_tf)  # Подписываемся на бары
        self.history_subscriptions[(symbol, time_frame)] = True  # Ставим отметку в справочнике подписок

    def unsubscribe_history(self, symbol, time_frame):
        quik_tf, _ = self.provider.timeframe_to_quik_timeframe(time_frame)  # Временной интервал QUIK
        self.provider.unsubscribe_from_candles(symbol.board, symbol.symbol, quik_tf)  # Отменяем подписку на бары
        del self.history_subscriptions[(symbol, time_frame)]  # Удаляем из справочника подписок

    def get_last_price(self, symbol):
        last_price = float(self.provider.get_param_ex(symbol.board, symbol.symbol, 'LAST')['data']['param_value'])  # Последняя цена сделки
        return self.provider.quik_price_to_price(symbol.board, symbol.symbol, last_price)  # Цена в рублях за штуку

    def get_value(self):
        if self.account['futures']:  # Для срочного рынка
            # noinspection PyBroadException
            try:  # Пытаемся получить стоимость позиций
                return float(self.provider.get_futures_limit(self.account['firm_id'], self.account['trade_account_id'], 0, self.provider.currency)['data']['cbplused'])  # Тек.чист.поз. (Заблокированное ГО под открытые позиции)
            except Exception:  # При ошибке Futures limit returns nil
                return 0  # Выдаем пустое значение. Получим стоимость позиций когда сервер будет работать
        # Для остальных рынков
        self.get_positions()  # Получаем текущие позиции
        return round(sum([position.current_price * position.quantity for position in self.positions]), 2)  # Суммируем текущую ст-сть всех позиций

    def get_cash(self):
        if self.account['futures']:  # Для срочного рынка
            # Видео: https://www.youtube.com/watch?v=u2C7ElpXZ4k
            # Баланс = Лимит откр.поз. + Вариац.маржа + Накоплен.доход
            # Лимит откр.поз. = Сумма, которая была на счету вчера в 19:00 МСК (после вечернего клиринга)
            # Вариац.маржа = Рассчитывается с 19:00 предыдущего дня без учета комисии. Перейдет в Накоплен.доход и обнулится в 14:00 (на дневном клиринге)
            # Накоплен.доход включает Биржевые сборы
            # Тек.чист.поз. = Заблокированное ГО под открытые позиции
            # План.чист.поз. = На какую сумму можете открыть еще позиции
            # noinspection PyBroadException
            try:
                futures_limit = self.provider.get_futures_limit(self.account['firm_id'], self.account['trade_account_id'], 0, self.provider.currency)['data']  # Фьючерсные лимиты
                return float(futures_limit['cbplimit']) + float(futures_limit['varmargin']) + float(futures_limit['accruedint'])  # Лимит откр.поз. + Вариац.маржа + Накоплен.доход
            except Exception:  # При ошибке Futures limit returns nil
                return 0  # Выдаем пустое значение. Получим стоимость позиций когда сервер будет работать
        # Для остальных рынков
        money_limits = self.provider.get_money_limits()['data']  # Все денежные лимиты (остатки на счетах)
        if len(money_limits) == 0:  # Если денежных лимитов нет
            return 0
        cash = [money_limit for money_limit in money_limits  # Из всех денежных лимитов
                if money_limit['client_code'] == self.account['client_code'] and  # выбираем по коду клиента
                money_limit['firmid'] == self.account['firm_id'] and  # фирме
                money_limit['limit_kind'] == self.limit_kind and  # дню лимита
                money_limit["currcode"] == self.provider.currency]  # и валюте
        if len(cash) != 1:  # Если ни один денежный лимит не подходит
            return 0
        return float(cash[0]['currentbal'])  # Денежный лимит (остаток) по счету

    def get_positions(self):
        self.positions = []  # Текущие позиции
        if self.account['futures']:  # Для срочного рынка
            active_futures_holdings = [futures_holding for futures_holding in self.provider.get_futures_holdings()['data'] if futures_holding['totalnet'] != 0]  # Активные фьючерсные позиции
            for active_futures_holding in active_futures_holdings:  # Пробегаемся по всем активным фьючерсным позициям
                class_code = 'SPBFUT'  # Код режима торгов
                sec_code = active_futures_holding['sec_code']  # Код тикера
                symbol = self._get_symbol_info(class_code, sec_code)  # Спецификация тикера
                size = active_futures_holding['totalnet']  # Кол-во
                if self.lots:  # Если входящий остаток в лотах
                    size *= symbol.lot_size  # то переводим кол-во из лотов в штуки
                entry_price = self.provider.quik_price_to_price(class_code, sec_code, float(active_futures_holding['avrposnprice']))  # Цена входа в рублях за штуку
                last_price = self.provider.quik_price_to_price(class_code, sec_code, float(self.provider.get_param_ex(class_code, sec_code, 'LAST')['data']['param_value']))  # Последняя цена сделки в рублях за штуку
                self.positions.append(Position(  # Добавляем текущую позицию в список
                    self,  # Брокер
                    symbol.dataname,  # Название тикера
                    symbol.description,  # Описание тикера
                    symbol.decimals,  # Кол-во десятичных знаков в цене
                    size,  # Кол-во в штуках
                    entry_price,  # Средняя цена входа в рублях
                    last_price))  # Последняя цена в рублях
        else:  # Для остальных рынков
            depo_limits = self.provider.get_all_depo_limits()['data']  # Все лимиты по бумагам (позиции по инструментам)
            firm_kind_depo_limits = [depo_limit for depo_limit in depo_limits if  # Бумажный лимит
                                     depo_limit['client_code'] == self.account['client_code'] and  # выбираем по коду клиента
                                     depo_limit['firmid'] == self.account['firm_id'] and  # фирме
                                     depo_limit['limit_kind'] == self.limit_kind and  # и дню лимита
                                     depo_limit['currentbal'] != 0]  # только открытые позиции
            for firm_kind_depo_limit in firm_kind_depo_limits:  # Пробегаемся по всем позициям
                sec_code = firm_kind_depo_limit['sec_code']  # Код тикера
                class_code = self.provider.get_security_class(self.class_codes, sec_code)['data']  # Код режима торгов из режимов торгов счета
                symbol = self._get_symbol_info(class_code, sec_code)  # Спецификация тикера
                size = int(firm_kind_depo_limit['currentbal'])  # Кол-во
                if self.lots:  # Если входящий остаток в лотах
                    size *= symbol.lot_size  # то переводим кол-во из лотов в штуки
                entry_price = self.provider.quik_price_to_price(class_code, sec_code, float(firm_kind_depo_limit["wa_position_price"]))  # Цена входа в рублях за штуку
                last_price = self.provider.quik_price_to_price(class_code, sec_code, float(self.provider.get_param_ex(class_code, sec_code, 'LAST')['data']['param_value']))  # Последняя цена сделки в рублях за штуку
                self.positions.append(Position(  # Добавляем текущую позицию в список
                    self,  # Брокер
                    symbol.dataname,  # Название тикера
                    symbol.description,  # Описание тикера
                    symbol.decimals,  # Кол-во десятичных знаков в цен
                    size,  # Кол-во в штуках
                    entry_price,  # Средняя цена входа в рублях
                    last_price))  # Последняя цена в рублях
        return self.positions

    def get_orders(self) -> list[Order]:
        self.orders = []  # Активные заявки
        firm_orders = [order for order in self.provider.get_all_orders()['data'] if order['firmid'] == self.account['firm_id'] and order['flags'] & 0b1 == 0b1]  # Активные заявки по фирме
        for firm_order in firm_orders:  # Пробегаемся по всем заявкам
            buy = firm_order['flags'] & 0b100 != 0b100  # Заявка на покупку
            class_code = firm_order['class_code']  # Код режима торгов
            sec_code = firm_order['sec_code']  # Тикер
            symbol = self._get_symbol_info(class_code, sec_code)  # Спецификация тикера
            order_price = self.provider.quik_price_to_price(class_code, sec_code, firm_order['price'])  # Цена заявки в рублях за штуку
            status = self._ext_order_status_to_status(int(firm_order['ext_order_status']))  # Статус заявки по расширенному статусу заявки
            self.orders.append(Order(  # Добавляем заявки в список
                self,  # Брокер
                firm_order['order_num'],  # Уникальный код заявки
                buy,  # Покупка/продажа
                Order.Limit if order_price else Order.Market,  # Лимит/по рынку. Для фьючерсов задается текущая рыночная цена. Все заявки по ним будут лимитные
                symbol.dataname,  # Название тикера
                symbol.decimals,  # Кол-во десятичных знаков в цене
                firm_order['qty'] * symbol.lot_size,  # Кол-во в штуках
                order_price,  # Цена
                status=status))  # Статус
        firm_stop_orders = [stopOrder for stopOrder in self.provider.get_all_stop_orders()['data'] if stopOrder['firmid'] == self.account['firm_id'] and stopOrder['flags'] & 0b1 == 0b1]  # Активные стоп заявки по фирме
        for firm_stop_order in firm_stop_orders:  # Пробегаемся по всем стоп заявкам
            buy = firm_stop_order['flags'] & 0b100 != 0b100  # Заявка на покупку
            class_code = firm_stop_order['class_code']  # Код режима торгов
            sec_code = firm_stop_order['sec_code']  # Тикер
            symbol = self._get_symbol_info(class_code, sec_code)  # Спецификация тикера
            condition_price = self.provider.quik_price_to_price(class_code, sec_code, firm_stop_order['condition_price'])  # Цена срабатывания стоп заявки в рублях за штуку
            order_price = self.provider.quik_price_to_price(class_code, sec_code, firm_stop_order['price'])  # Цена заявки в рублях за штуку
            self.orders.append(Order(  # Добавляем заявки в список
                self,  # Брокер
                firm_stop_order['order_num'],  # Уникальный код заявки
                buy,  # Покупка/продажа
                Order.StopLimit if order_price else Order.Stop,  # Стоп лимит/стоп
                symbol.dataname,  # Название тикера
                symbol.decimals,  # Кол-во десятичных знаков в цене
                firm_stop_order['qty'] * symbol.lot_size,  # Кол-во в штуках
                order_price,  # Цена
                condition_price,  # Цена срабатывания стоп заявки
                Order.Accepted))  # Статус
        return self.orders

    def new_order(self, order):
        class_code, sec_code = self.provider.dataname_to_class_sec_codes(order.dataname)  # Код режима торгов и тикер из названия тикера
        action = 'NEW_STOP_ORDER' if order.exec_type in (Order.Stop, Order.StopLimit) else 'NEW_ORDER'  # Действие над заявкой
        quantity = self.provider.size_to_lots(class_code, sec_code, order.quantity)  # Кол-во в лотах
        trans_id = str(next(self.trans_id))  # Следующий номер транзакции
        transaction = {  # Все значения должны передаваться в виде строк
            'TRANS_ID': trans_id,  # Номер транзакции
            'CLIENT_CODE': self.account['client_code'],  # Код клиента
            'ACCOUNT': self.account['trade_account_id'],  # Счет
            'ACTION': action,  # Тип заявки: Новая лимитная/рыночная заявка
            'CLASSCODE': class_code,  # Код режима торгов
            'SECCODE': sec_code,  # Код тикера
            'OPERATION': 'B' if order.buy else 'S',  # B = покупка, S = продажа
            'PRICE': str(order.price),  # Цена исполнения
            'QUANTITY': str(quantity),  # Кол-во в лотах
            'TYPE': 'M'}  # L = лимитная заявка (по умолчанию), M = рыночная заявка
        if order.exec_type in (Order.Stop, Order.StopLimit):  # Для стоп заявки
            transaction['STOPPRICE'] = str(order.stop_price)  # Стоп цена исполнения
            transaction['EXPIRY_DATE'] = 'GTC'  # Срок действия до отмены
        elif order.exec_type == Order.Limit:  # Для лимитной заявки
            transaction['TYPE'] = 'L'  # L = лимитная заявка (по умолчанию)
        else:  # Для рыночной заявки
            transaction['TYPE'] = 'M'  # M = рыночная заявка
        self.provider.send_transaction(transaction)
        order.id = trans_id  # Пока у заявки нет номера, ставим номер транзакции. Номер заявки придет в _on_trans_reply
        order.status = Order.Submitted  # Заявка отправлена брокеру
        self.orders.append(order)  # Добавляем новую заявку в список заявок
        return True  # Операция завершилась успешно

    def cancel_order(self, order):
        class_code, sec_code = self.provider.dataname_to_class_sec_codes(order.dataname)  # Код режима торгов и тикер из названия тикера
        action = 'KILL_STOP_ORDER' if order.exec_type in (Order.Stop, Order.StopLimit) else 'KILL_ORDER'  # Действие над заявкой
        order_key = 'STOP_ORDER_KEY' if order.exec_type in (Order.Stop, Order.StopLimit) else 'ORDER_KEY'  # Номер заявки
        transaction = {  # Все значения должны передаваться в виде строк
            'TRANS_ID': str(next(self.trans_id)),  # Следующий номер транзакции
            'ACTION': action,  # Тип заявки: Удаление существующей заявки
            'CLASSCODE': class_code,  # Код режима торгов
            'SECCODE': sec_code,  # Код тикера
            order_key: order.id}  # Номер заявки
        self.provider.send_transaction(transaction)

    def subscribe_transactions(self):
        pass  # Подписки на позиции, сделки, заявки автоматически запускаются в QuikPy

    def unsubscribe_transactions(self):
        pass  # Подписки на позиции, сделки, заявки автоматически закроются при закрытии соединения в функции close

    def close(self):
        self.provider.on_new_candle.unsubscribe(self._on_new_bar)  # Обработка нового бара
        self.provider.on_trans_reply.unsubscribe(self._on_trans_reply)  # Обработка транзакций
        self.provider.on_order.unsubscribe(self._on_order)  # Обработка заявок
        self.provider.on_stop_order.unsubscribe(self._on_stop_order)  # Обработка стоп заявок
        self.provider.on_trade.unsubscribe(self._on_trade)  # Обработка сделок

        self.provider.close_connection_and_thread()  # Перед выходом закрываем соединение для запросов и поток обработки функций обратного вызова

    # Внутренние функции

    def _get_symbol_info(self, class_code: str, sec_code: str) -> Symbol | None:
        """Спецификация тикера по режиму торгов и коду"""
        si = self.provider.get_symbol_info(class_code, sec_code)  # Спецификация тикера
        if si is None:  # Если тикер не найден
            return None  # то выходим, дальше не продолжаем
        dataname = self.provider.class_sec_codes_to_dataname(class_code, sec_code)  # Название тикера
        symbol = Symbol(class_code, sec_code, dataname, si['short_name'], si['scale'], si['min_price_step'], si['lot_size'])
        self.storage.set_symbol(symbol)  # Добавляем спецификацию тикера в хранилище
        return symbol

    def _on_new_bar(self, data):
        """Получение нового бара по подписке"""
        bar = data['data']  # Данные бара
        class_code = bar['class']  # Код режима торгов
        sec_code = bar['sec']  # Тикер
        dataname = self.provider.class_sec_codes_to_dataname(class_code, sec_code)  # Название тикера
        time_frame, _ = self.provider.quik_timeframe_to_timeframe(bar['interval'])  # Временной интервал
        dt_json = bar['datetime']  # Получаем составное значение даты и времени открытия бара
        dt = datetime(dt_json['year'], dt_json['month'], dt_json['day'], dt_json['hour'], dt_json['min'])  # Время открытия бара
        self.on_new_bar.trigger(Bar(class_code, sec_code, dataname, time_frame, dt, bar['open'], bar['high'], bar['low'], bar['close'], int(bar['volume'])))  # Вызываем событие добавления нового бара

    def _on_trans_reply(self, data):
        """Получение ответа на транзакцию пользователя"""
        trans_reply = data['data']  # Ответ на транзакцию
        trans_id = int(trans_reply['trans_id'])  # Номер транзакции заявки
        if trans_id == 0:  # Заявки, выставленные не из автоторговли / только что (с нулевыми номерами транзакции)
            return  # не обрабатываем, пропускаем
        order_num = int(trans_reply['order_num'])  # Номер заявки на бирже
        order = next((order for order in self.orders if order.id == trans_id), None)  # Ищем заявку по номеру транзакции
        if order is None:  # Если заявка не найдена
            return  # то выходим, дальше не продолжаем
        order.id = order_num  # Ставим номер заявки
        # TODO Есть поле flags, но оно не документировано. Лучше вместо текстового результата транзакции разбирать по нему
        result_msg = str(trans_reply['result_msg']).lower()  # По результату исполнения транзакции (очень плохое решение)
        status = int(trans_reply['status'])  # Статус транзакции
        if status == 15 or 'зарегистрирован' in result_msg:  # Если пришел ответ по новой заявке
            order.status = Order.Accepted  # Заявка принята брокером
        elif 'снят' in result_msg:  # Если пришел ответ по отмене существующей заявки
            order.status = Order.Canceled  # Заявка отменена
        elif status in (2, 4, 5, 10, 11, 12, 13, 14, 16):  # Транзакция не выполнена (ошибка заявки):
            # - Не найдена заявка для удаления
            # - Вы не можете снять данную заявку
            # - Превышен лимит отправки транзакций для данного логина
            if status == 4 and 'не найдена заявка' in result_msg or \
               status == 5 and 'не можете снять' in result_msg or 'превышен лимит' in result_msg:
                return  # то заявку не отменяем, выходим, дальше не продолжаем
            order.status = Order.Rejected  # Заявка отклонена брокером
        elif status == 6:  # Транзакция не прошла проверку лимитов сервера QUIK
            order.status = Order.Margin  # Недостаточно средств
        self.on_order.trigger(order)

    def _on_order(self, data):
        """Получение заявки по подписке"""
        order = data['data']  # Заявка
        buy = order['flags'] & 0b100 != 0b100  # Заявка на покупку
        class_code = order['class_code']  # Код режима торгов
        sec_code = order['sec_code']  # Тикер
        symbol = self._get_symbol_info(class_code, sec_code)  # Спецификация тикера
        quantity = self.provider.lots_to_size(class_code, sec_code, order['qty'])  # Кол-во в штуках
        order_price = self.provider.quik_price_to_price(class_code, sec_code, order['price'])  # Цена заявки в рублях за штуку
        status = self._ext_order_status_to_status(int(order['ext_order_status']))  # Статус заявки по расширенному статусу заявки
        self.on_order.trigger(Order(
            self,  # Брокер
            order['order_num'],  # Уникальный код заявки
            buy,  # Покупка/продажа
            Order.Limit if order_price else Order.Market,  # Лимит/по рынку. Для фьючерсов задается текущая рыночная цена. Все заявки по ним будут лимитные
            symbol.dataname,  # Название тикера
            symbol.decimals,  # Кол-во десятичных знаков в цене
            quantity,  # Кол-во в штуках
            order_price,  # Цена
            status=status))  # Статус)

    def _on_stop_order(self, data):
        """Получение стоп заявки по подписке"""
        stop_order = data['data']  # Стоп заявка
        buy = stop_order['flags'] & 0b100 != 0b100  # Заявка на покупку
        class_code = stop_order['class_code']  # Код режима торгов
        sec_code = stop_order['sec_code']  # Тикер
        symbol = self._get_symbol_info(class_code, sec_code)  # Спецификация тикера
        quantity = self.provider.lots_to_size(class_code, sec_code, stop_order['qty'])  # Кол-во в штуках
        condition_price = self.provider.quik_price_to_price(class_code, sec_code, stop_order['condition_price'])  # Цена срабатывания стоп заявки в рублях за штуку
        order_price = self.provider.quik_price_to_price(class_code, sec_code, stop_order['price'])  # Цена заявки в рублях за штуку
        status = Order.Accepted if int(stop_order['filled_qty']) == 0 else Order.Completed  # Статус
        self.on_order.trigger(Order(
            self,  # Брокер
            stop_order['order_num'],  # Уникальный код заявки
            buy,  # Покупка/продажа
            Order.StopLimit if order_price else Order.Stop,  # Стоп лимит/стоп
            symbol.dataname,  # Название тикера
            symbol.decimals,  # Кол-во десятичных знаков в цене
            quantity,  # Кол-во в штуках
            order_price,  # Цена
            condition_price,  # Цена срабатывания стоп заявки
            status))  # Статус

    def _on_trade(self, data):
        """Получение сделки по подписке"""
        trade = data['data']  # Сделка
        trans_id = int(trade['trans_id'])  # Номер транзакции из заявки на бирже. Не используем GetOrderByNumber, т.к. он может вернуть 0
        if trans_id == 0:  # Заявки, выставленные не из автоторговли / только что (с нулевыми номерами транзакции)
            return  # не обрабатываем, пропускаем
        trade_num = int(trade['trade_num'])  # Номер сделки (дублируется 3 раза)
        class_code = trade['class_code']  # Код режима торгов
        sec_code = trade['sec_code']  # Код тикера
        symbol = self._get_symbol_info(class_code, sec_code)  # Спецификация тикера
        if symbol.dataname not in self.trade_nums.keys():  # Если это первая сделка по тикеру
            self.trade_nums[symbol.dataname] = []  # то ставим пустой список сделок
        elif trade_num in self.trade_nums[symbol.dataname]:  # Если номер сделки есть в списке (фильтр для дублей)
            return  # то выходим, дальше не продолжаем
        self.trade_nums[symbol.dataname].append(trade_num)  # Запоминаем номер сделки по тикеру, чтобы в будущем ее не обрабатывать (фильтр для дублей)
        order_num = trade['order_num']  # Номер заявки на бирже
        order = next((order for order in self.orders if order.id == order_num), None)  # Ищем заявку по номеру
        if order is None:  # Если заявка не найдена
            return  # то выходим, дальше не продолжаем
        dt = trade['datetime']
        dt_msk = datetime(int(dt['year']), int(dt['month']), int(dt['day']), int(dt['hour']), int(dt['min']), int(dt['sec']))
        quantity = int(trade['qty'])  # Абсолютное кол-во
        if self.lots:  # Если входящий остаток в лотах
            quantity = self.provider.lots_to_size(class_code, sec_code, quantity)  # то переводим кол-во из лотов в штуки
        if trade['flags'] & 0b100 == 0b100:  # Если сделка на продажу (бит 2)
            quantity *= -1  # то кол-во ставим отрицательным
        self.on_trade.trigger(Trade(
            self,  # Брокер
            order_num,  # Номер заявки из сделки
            symbol.dataname,  # Название тикера
            symbol.description,  # Описание тикера
            symbol.decimals,  # Кол-во десятичных знаков в цене
            dt_msk,  # Дата и время сделки по времени биржи (МСК)
            quantity,  # Кол-во в штуках
            self.provider.quik_price_to_price(class_code, sec_code, float(trade['price']))))  # Цена сделки
        self.on_position.trigger(self.get_position(symbol))  # При любой сделке позиция изменяется. Отправим текущую или пустую позицию по тикеру по подписке

    @staticmethod
    def _ext_order_status_to_status(ext_order_status: int):
        if ext_order_status in (1, 8):  # заявка активна / приостановлено исполнение
            return Order.Accepted
        elif ext_order_status == 2:  # заявка частично исполнена
            return Order.Partial
        elif ext_order_status == 3:  # заявка исполнена
            return Order.Completed
        elif ext_order_status in (4, 5, 6, 11):  # заявка отменена / заменена / в состоянии отмены / в состоянии замены
            return Order.Canceled
        elif ext_order_status == 7:  # заявка отвергнута
            return Order.Rejected
        elif ext_order_status == 9:  # заявка в состоянии регистрации
            return Order.Submitted
        else:  # 10 – заявка снята по времени действия
            return Order.Expired
