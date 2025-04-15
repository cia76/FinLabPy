# Курс Мультиброкер: Контроль https://finlab.vip/wpm-category/mbcontrol/

from datetime import datetime
import itertools  # Итератор для уникальных номеров транзакций

from FinLabPy.Core import Broker, Position, Symbol, Order, Bar  # Брокер, позиция, заявка, тикер
from QuikPy import QuikPy  # Работа с QUIK из Python через LUA скрипты QuikSharp


class Quik(Broker):
    """Брокер QUIK"""

    def __init__(self, code: str, name: str, provider: QuikPy, account_id: int = 0, limit_kind: int = 1, lots=True):
        super().__init__(code, name, provider, account_id)
        self.provider = provider  # Уже инициирован в базовом классе. Выполням для того, чтобы работать с типом провайдера
        self.provider.on_new_candle = self.qk_new_bar  # Обработчик получения новой свечки
        self.provider.on_trans_reply = self.on_trans_reply  # Ответ на транзакцию пользователя. Если транзакция выполняется из QUIK, то не вызывается
        self.account = next((account for account in self.provider.accounts if account['account_id'] == account_id), self.provider.accounts[0])  # Счет
        self.limit_kind = limit_kind  # Срок расчетов
        self.lots = lots  # Входящий остаток в лотах (задается брокером)
        self.class_codes = self.provider.get_classes_list()['data']  # Режимы торгов через запятую
        self.trans_id = itertools.count(1)  # Номер транзакции задается пользователем. Он будет начинаться с 1 и каждый раз увеличиваться на 1

    def get_symbol_by_dataname(self, dataname: str):
        class_code, sec_code = self.provider.dataname_to_class_sec_codes(dataname)  # Код режима торгов и тикер
        if not class_code:  # Если код режима торгов не найден
            print(f'Код режима торгов тикера {dataname} не найден')
            return None  # То выходим, дальше не продолжаем
        si = self.provider.get_symbol_info(class_code, sec_code)  # Спецификация тикера
        if not si:  # Если тикер не найден
            print(f'Информация о тикере {dataname} не найдена')
            return None  # То выходим, дальше не продолжаем
        return Symbol(class_code, sec_code, dataname, si['short_name'], si['scale'], si['min_price_step'], si['lot_size'])

    def get_history(self, dataname: str, tf: str, dt_from: datetime = None, dt_to: datetime = None):
        class_code, security_code = self.provider.dataname_to_class_sec_codes(dataname)  # Код режима торгов и тикер из названия тикера
        time_frame, _ = self.provider.timeframe_to_quik_timeframe(tf)  # Временной интервал QUIK
        history = self.provider.get_candles_from_data_source(class_code, security_code, time_frame)  # Получаем все бары из QUIK. Фильтрацию по дате и времени будем делать при разборе баров
        if not history:  # Если бары не получены
            print('Ошибка при получении истории: История не получена')
            return None  # то выходим, дальше не продолжаем
        if 'data' not in history:  # Если бар нет в словаре
            print(f'Ошибка при получении истории: {history}')
            return None  # то выходим, дальше не продолжаем
        bars = []  # Список полученных бар
        for bar in history['data']:  # Пробегаемся по всем полученным барам
            dt = datetime(bar['datetime']['year'], bar['datetime']['month'], bar['datetime']['day'], bar['datetime']['hour'], bar['datetime']['min'])  # Собираем дату и время бара до минут
            if dt_from and dt_from > dt:  # Если задана дата начала, и она позже даты и времени бара
                continue  # то пропускаем этот бар
            if dt_to and dt_to < dt:  # Если задана дата окончания, и она раньше даты и времени бара
                continue  # то пропускаем этот бар
            bars.append(Bar(class_code, security_code, dataname, tf, dt, bar['open'], bar['high'], bar['low'], bar['close'], int(bar['volume'])))  # Добавляем бар
        return bars

    def subscribe_history(self, dataname: str, time_frame: str):
        class_code, security_code = self.provider.dataname_to_class_sec_codes(dataname)  # Код режима торгов и тикер из названия тикера
        interval, _ = self.provider.timeframe_to_quik_timeframe(time_frame)  # Временной интервал QUIK
        self.provider.subscribe_to_candles(class_code, security_code, interval)  # Подписываемся на бары

    def qk_new_bar(self, data):
        bar = data['data']  # Данные бара
        class_code = bar['class']  # Код режима торгов
        sec_code = bar['sec']  # Тикер
        dataname = self.provider.class_sec_codes_to_dataname(class_code, sec_code)  # Получаем название тикера по коду режима торгов и тикера
        time_frame, _ = self.provider.quik_timeframe_to_timeframe(bar['interval'])  # Временной интервал
        dt_json = bar['datetime']  # Получаем составное значение даты и времени открытия бара
        dt = datetime(dt_json['year'], dt_json['month'], dt_json['day'], dt_json['hour'], dt_json['min'])  # Время открытия бара
        self.on_new_bar(Bar(class_code, sec_code, dataname, time_frame, dt, bar['open'], bar['high'], bar['low'], bar['close'], int(bar['volume'])))  # Вызываем событие добавления нового бара

    def get_last_price(self, dataname: str):
        si = self.get_symbol_by_dataname(dataname)  # Тикер по названию
        last_price = float(self.provider.get_param_ex(si.board, si.symbol, 'LAST')['data']['param_value'])  # Последняя цена сделки
        return self.provider.quik_price_to_price(si.board, si.symbol, last_price)  # Цена в рублях за штуку

    def get_value(self):
        if self.account['futures']:  # Для срочного рынка
            try:  # Пытаемся получить стоимость позиций
                return float(self.provider.get_futures_limit(self.account['firm_id'], self.account['trade_account_id'], 0, self.provider.currency)['data']['cbplused'])  # Тек.чист.поз. (Заблокированное ГО под открытые позиции)
            except Exception:  # При ошибке Futures limit returns nil
                print(f'get_value: QUIK не вернул фьючерсные лимиты с firm_id={self.account["firm_id"]}, trade_account_id={self.account["trade_account_id"]}, currency_code={self.provider.currency}. Проверьте правильность значений')
                return 0  # Выдаем пустое значение. Получим стоимость позиций когда сервер будет работать
        # Для остальных рынков
        self.get_positions()  # Получаем текущие позиции
        return round(sum([position.current_price * position.quantity for position in self.positions]), 2)  #

    def get_cash(self):
        if self.account['futures']:  # Для срочного рынка
            # Видео: https://www.youtube.com/watch?v=u2C7ElpXZ4k
            # Баланс = Лимит откр.поз. + Вариац.маржа + Накоплен.доход
            # Лимит откр.поз. = Сумма, которая была на счету вчера в 19:00 МСК (после вечернего клиринга)
            # Вариац.маржа = Рассчитывается с 19:00 предыдущего дня без учета комисии. Перейдет в Накоплен.доход и обнулится в 14:00 (на дневном клиринге)
            # Накоплен.доход включает Биржевые сборы
            # Тек.чист.поз. = Заблокированное ГО под открытые позиции
            # План.чист.поз. = На какую сумму можете открыть еще позиции
            try:
                futures_limit = self.provider.get_futures_limit(self.account['firm_id'], self.account['trade_account_id'], 0, self.provider.currency)['data']  # Фьючерсные лимиты
                return float(futures_limit['cbplimit']) + float(futures_limit['varmargin']) + float(futures_limit['accruedint'])  # Лимит откр.поз. + Вариац.маржа + Накоплен.доход
            except Exception:  # При ошибке Futures limit returns nil
                print(f'get_cash: QUIK не вернул фьючерсные лимиты с firm_id={self.account["firm_id"]}, trade_account_id={self.account["trade_account_id"]}, currency_code={self.provider.currency}. Проверьте правильность значений')
                return 0  # Выдаем пустое значение. Получим стоимость позиций когда сервер будет работать
        # Для остальных рынков
        money_limits = self.provider.get_money_limits()['data']  # Все денежные лимиты (остатки на счетах)
        if len(money_limits) == 0:  # Если денежных лимитов нет
            print('get_cash: QUIK не вернул денежные лимиты (остатки на счетах). Свяжитесь с брокером')
            return 0
        cash = [money_limit for money_limit in money_limits  # Из всех денежных лимитов
                if money_limit['client_code'] == self.account['client_code'] and  # выбираем по коду клиента
                money_limit['firmid'] == self.account['firm_id'] and  # фирме
                money_limit['limit_kind'] == self.limit_kind and  # дню лимита
                money_limit["currcode"] == self.provider.currency]  # и валюте
        if len(cash) != 1:  # Если ни один денежный лимит не подходит
            # print(f'Полученные денежные лимиты: {money_limits}')  # Для отладки, если нужно разобраться, что указано неверно
            print(f'get_cash: Денежный лимит не найден с client_code={self.account["client_code"]}, firm_id={self.account["firm_id"]}, limit_kind={self.limit_kind}, currency_code={self.provider.currency}. Проверьте правильность значений')
            return 0
        return float(cash[0]['currentbal'])  # Денежный лимит (остаток) по счету

    def get_positions(self):
        self.positions = []  # Текущие позиции
        if self.account['futures']:  # Для срочного рынка
            futures_holdings = self.provider.get_futures_holdings()['data']  # Все фьючерсные позиции
            active_futures_holdings = [futures_holding for futures_holding in futures_holdings if futures_holding['totalnet'] != 0]  # Активные фьючерсные позиции
            for active_futures_holding in active_futures_holdings:  # Пробегаемся по всем активным фьючерсным позициям
                class_code = 'SPBFUT'  # Код режима торгов
                sec_code = active_futures_holding['sec_code']  # Код тикера
                dataname = self.provider.class_sec_codes_to_dataname(class_code, sec_code)  # Получаем название тикера по коду режима торгов и тикера
                size = active_futures_holding['totalnet']  # Кол-во
                si = self.provider.get_symbol_info(class_code, sec_code)  # Спецификация тикера
                if self.lots:  # Если входящий остаток в лотах
                    size *= si['lot_size']  # то переводим кол-во из лотов в штуки
                entry_price = self.provider.quik_price_to_price(class_code, sec_code, float(active_futures_holding['avrposnprice']))  # Цена входа в рублях за штуку
                last_price = self.provider.quik_price_to_price(class_code, sec_code, float(self.provider.get_param_ex(class_code, sec_code, 'LAST')['data']['param_value']))  # Последняя цена сделки в рублях за штуку
                self.positions.append(Position(  # Добавляем текущую позицию в список
                    self,  # Брокер
                    dataname,  # Название тикера
                    si['short_name'],  # Описание тикера
                    si['scale'],  # Кол-во десятичных знаков в цене
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
                sec_code = firm_kind_depo_limit["sec_code"]  # Код тикера
                class_code = self.provider.get_security_class(self.class_codes, sec_code)['data']  # Код режима торгов из режимов торгов счета
                dataname = self.provider.class_sec_codes_to_dataname(class_code, sec_code)  # Получаем название тикера по коду режима торгов и тикера
                size = int(firm_kind_depo_limit['currentbal'])  # Кол-во
                si = self.provider.get_symbol_info(class_code, sec_code)  # Спецификация тикера
                if self.lots:  # Если входящий остаток в лотах
                    size *= si['lot_size']  # то переводим кол-во из лотов в штуки
                entry_price = self.provider.quik_price_to_price(class_code, sec_code, float(firm_kind_depo_limit["wa_position_price"]))  # Цена входа в рублях за штуку
                last_price = self.provider.quik_price_to_price(class_code, sec_code, float(self.provider.get_param_ex(class_code, sec_code, 'LAST')['data']['param_value']))  # Последняя цена сделки в рублях за штуку
                self.positions.append(Position(  # Добавляем текущую позицию в список
                    self,  # Брокер
                    dataname,  # Название тикера
                    si['short_name'],  # Описание тикера
                    si['scale'],  # Кол-во десятичных знаков в цене
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
            sec_code = firm_order["sec_code"]  # Тикер
            dataname = self.provider.class_sec_codes_to_dataname(class_code, sec_code)  # Получаем название тикера по коду режима торгов и тикера
            order_price = self.provider.quik_price_to_price(class_code, sec_code, firm_order['price'])  # Цена заявки в рублях за штуку
            si = self.provider.get_symbol_info(class_code, sec_code)  # Спецификация тикера
            order_qty = firm_order['qty'] * si['lot_size']  # Кол-во в штуках
            self.orders.append(Order(  # Добавляем заявки в список
                self,  # Брокер
                firm_order['order_num'],  # Уникальный код заявки
                buy,  # Покупка/продажа
                Order.Limit if order_price else Order.Market,  # Лимит/по рынку. Для фьючерсов задается текущая рыночная цена. Все заявки по ним будут лимитные
                dataname,  # Название тикера
                si.decimals,  # Кол-во десятичных знаков в цене
                order_qty,  # Кол-во в штуках
                order_price))  # Цена заявки
        firm_stop_orders = [stopOrder for stopOrder in self.provider.get_all_stop_orders()['data'] if stopOrder['firmid'] == self.account['firm_id'] and stopOrder['flags'] & 0b1 == 0b1]  # Активные стоп заявки по фирме
        for firm_stop_order in firm_stop_orders:  # Пробегаемся по всем стоп заявкам
            buy = firm_stop_order['flags'] & 0b100 != 0b100  # Заявка на покупку
            class_code = firm_stop_order['class_code']  # Код режима торгов
            sec_code = firm_stop_order['sec_code']  # Тикер
            dataname = self.provider.class_sec_codes_to_dataname(class_code, sec_code)  # Получаем название тикера по коду режима торгов и тикера
            stop_order_price = self.provider.quik_price_to_price(class_code, sec_code, firm_stop_order['price'])  # Цена срабатывания стоп заявки в рублях за штуку
            si = self.provider.get_symbol_info(class_code, sec_code)  # Спецификация тикера
            stop_order_qty = firm_stop_order['qty'] * si['lot_size']  # Кол-во в штуках
            self.orders.append(Order(  # Добавляем заявки в список
                self,  # Брокер
                firm_stop_order['order_num'],  # Уникальный код заявки
                buy,  # Покупка/продажа
                Order.Limit if stop_order_price else Order.Market,  # Лимит/по рынку
                dataname,  # Название тикера
                si.decimals,  # Кол-во десятичных знаков в цене
                stop_order_qty,  # Кол-во в штуках
                stop_order_price))  # Цена заявки
        return self.orders

    def new_order(self, order: Order):
        class_code, sec_code = self.provider.dataname_to_class_sec_codes(order.dataname)  # Код режима торгов и тикер из названия тикера
        action = 'NEW_STOP_ORDER' if order.exec_type in (Order.Stop, Order.StopLimit) else 'NEW_ORDER'  # Действие над заявкой
        quantity = self.provider.size_to_lots(class_code, sec_code, order.quantity)  # Кол-во в лотах
        transaction = {  # Все значения должны передаваться в виде строк
            'TRANS_ID': str(next(self.trans_id)),  # Следующий номер транзакции
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

    def cancel_order(self, order: Order):
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

    def on_trans_reply(self, data):
        """Обработчик события ответа на транзакцию пользователя"""
        trans_id = data['data']['trans_id']  # Номер транзакции
        order_num = data['data']['order_num']  # Номер заявки
        order = next((order for order in self.orders if order.id == trans_id), None)  # Ищем заявку по номеру транзакции
        if not order:  # Если заявка не найдена
            print(f'Заявка {order_num} с номером транзакции {trans_id} не найдена')
            return  # то выходим, дальше не продолжаем
        order.id = order_num  # Ставим номер заявки

    def close(self):
        self.provider.close_connection_and_thread()  # Перед выходом закрываем соединение для запросов и поток обработки функций обратного вызова
