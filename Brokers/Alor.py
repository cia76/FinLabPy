# Курс Мультиброкер: Контроль https://finlab.vip/wpm-category/mbcontrol/

from datetime import datetime  # Дата и время
from typing import Union  # Объединение типов

from FinLabPy.Core import Broker, Bar, Position, Order, Symbol  # Брокер, бар, позиция, заявка, тикер
from AlorPy import AlorPy  # Работа с Alor OpenAPI V2 из Python через REST/WebSockets


class SymbolEx(Symbol):
    """Тикер Алор"""
    exchange: str  # Биржа


class Alor(Broker):
    """Брокер Алор"""
    def __init__(self, code: str, name: str, provider: AlorPy, account_id: int = 0, exchange: str = AlorPy.exchanges[0]):
        super().__init__(code, name, provider, account_id)
        self.provider = provider  # Уже инициирован в базовом классе. Выполням для того, чтобы работать с типом провайдера
        self.provider.on_new_bar = self.al_new_bar  # Перехватываем управление события получения нового бара
        account = self.provider.accounts[self.account_id]  # Номер счета по порядковому номеру
        self.portfolio = account['portfolio']  # Портфель
        self.exchange = exchange  # Биржа

    def get_symbol_by_dataname(self, dataname: str) -> Union[SymbolEx, None]:
        board, symbol = self.provider.dataname_to_board_symbol(dataname)  # Код режима торгов Алора и тикер из названия тикера
        exchange = self.provider.get_exchange(board, symbol)  # Биржа
        if (exchange, symbol) not in self.symbols:  # Если в справочнике нет информации о тикере
            si = self.provider.get_symbol(exchange, symbol)  # Получаем информацию о тикере из Алор
            if not si:  # Если тикер не найден
                print(f'Информация о тикере {dataname} на бирже {exchange} не найдена')
                return None  # то возвращаем пустое значение
            self.symbols[(exchange, symbol)] = si  # Заносим информацию о тикере в справочник
        si = self.symbols[(exchange, symbol)]  # Получаем информацию о тикера из справочника
        if board == 'RFUD':  # Для фьючерсов
            board = 'SPBFUT'  # Меняем код режима торгов Алор на канонический
        elif board == 'ROPD':  # Для опционов
            board = 'SPBOPT'  # Меняем код режима торгов Алор на канонический
        symbol_ex = SymbolEx(board, symbol, dataname, si['shortname'], si['decimals'], si['minstep'], si['lotsize'])
        symbol_ex.exchange = exchange  # Биржа
        return symbol_ex

    def get_history(self, dataname: str, time_frame: str, dt_from: datetime = None, dt_to: datetime = None):
        board, symbol = self.provider.dataname_to_board_symbol(dataname)  # Код режима торгов Алора и тикер из названия тикера
        exchange = self.provider.get_exchange(board, symbol)  # Биржа
        alor_tf, intraday = self.provider.timeframe_to_alor_timeframe(time_frame)  # Временной интервал Alor с признаком внутридневного интервала
        seconds_from = 0  # Дата и время начала добавления в секундах, прошедших с 01.01.1970 00:00 UTC
        seconds_to = 32536799999  # Максимально возможное кол-во секунд в Алор
        if dt_from:  # Если задана дата и время начала добавления
            seconds_from = self.provider.msk_datetime_to_utc_timestamp(dt_from)  # то переводим ее в секунды, прошедших с 01.01.1970 00:00 UTC
        if dt_to:  # Если задана дата и время окончания добавления
            seconds_to = self.provider.msk_datetime_to_utc_timestamp(dt_to)  # то переводим ее в секунды, прошедших с 01.01.1970 00:00 UTC
        history = self.provider.get_history(exchange, symbol, alor_tf, seconds_from, seconds_to)  # Запрос истории рынка
        if 'history' not in history:  # Если в полученной истории нет ключа history
            print('Ошибка при получении истории: История не получена')
            return None  # то выходим, дальше не продолжаем
        bars = []  # Список полученных бар
        si = self.get_symbol_by_dataname(dataname)  # Тикер по названию
        for bar in history['history']:  # Пробегаемся по всем барам
            dt_msk = self.provider.utc_timestamp_to_msk_datetime(bar['time']) if intraday else datetime.utcfromtimestamp(bar['time'])  # Дневные бары и выше ставим на начало дня по UTC. Остальные - по МСК
            volume = bar['volume'] * si.lot_size  # Объем в штуках
            bars.append(Bar(board, symbol, dataname, time_frame, dt_msk, bar['open'], bar['high'], bar['low'], bar['close'], volume))  # Добавляем бар
        return bars

    def subscribe_history(self, dataname: str, time_frame: str):
        board, symbol = self.provider.dataname_to_board_symbol(dataname)  # Код режима торгов Алора и тикер из названия тикера
        exchange = self.provider.get_exchange(board, symbol)  # Биржа
        alor_tf, _ = self.provider.timeframe_to_alor_timeframe(time_frame)  # Временной интервал Alor
        seconds_from = int(datetime.utcnow().timestamp())  # Изначально подписываемся с текущего момента времени по UTC
        _ = self.provider.bars_get_and_subscribe(exchange, symbol, alor_tf, seconds_from, frequency=1_000_000_000)  # Подписываемся на бары

    def al_new_bar(self, response):
        response_data = response['data']  # Данные бара
        utc_timestamp = response_data['time']  # Время в Alor OpenAPI V2 передается в секундах, прошедших с 01.01.1970 00:00 UTC
        subscription = self.provider.subscriptions[response['guid']]  # Получаем данные подписки
        symbol = subscription['code']  # Тикер
        exchange = subscription['exchange']  # Биржа
        alor_tf = subscription['tf']  # Временной интервал Alor
        si = self.provider.get_symbol_info(exchange, symbol)  # Из спецификации
        board = si['board']  # получаем режим торгов Алора
        dataname = self.provider.board_symbol_to_dataname(board, symbol)  # Название тикера
        time_frame, intraday = self.provider.alor_timeframe_to_timeframe(alor_tf)  # Временной интервал с признаком внутридневного интервала
        dt_msk = self.provider.utc_timestamp_to_msk_datetime(utc_timestamp) if intraday else datetime.utcfromtimestamp(utc_timestamp)  # Дневные бары и выше ставим на начало дня по UTC. Остальные - по МСК
        volume = response_data['volume'] * si.lot_size  # Объем в штуках
        self.on_new_bar(Bar(board, symbol, dataname, time_frame, dt_msk, response_data['open'], response_data['high'], response_data['low'], response_data['close'], volume))  # Вызываем событие добавления нового бара

    def get_last_price(self, dataname: str):
        si = self.get_symbol_by_dataname(dataname)  # Тикер по названию
        quotes = self.provider.get_quotes(f'{si.exchange}:{si.symbol}')[0]  # Последнюю котировку получаем через запрос
        return quotes['last_price'] if quotes else None  # Последняя цена сделки

    def get_value(self):
        value = self.provider.get_risk(self.portfolio, self.exchange)['portfolioLiquidationValue']  # Общая стоимость портфеля
        return round(value - self.get_cash(), 2)  # Стоимость позиций = Общая стоимость портфеля - Свободные средства

    def get_cash(self):
        if self.portfolio[0:3] == '750':  # Для счета срочного рынка
            cash = round(self.provider.get_forts_risk(self.portfolio, self.exchange)['moneyFree'], 2)  # Свободные средства. Сумма рублей и залогов, дисконтированных в рубли, доступная для открытия позиций. (MoneyFree = MoneyAmount + VmInterCl – MoneyBlocked – VmReserve – Fee)
        else:  # Для остальных счетов
            cash = next((position['volume'] for position in self.provider.get_positions(self.portfolio, self.exchange, False) if position['symbol'] == 'RUB'), 0)  # Свободные средства через денежную позицию
        return round(cash, 2)

    def get_positions(self):
        self.positions = []  # Текущие позиции
        for position in self.provider.get_positions(self.portfolio, self.exchange, True):  # Пробегаемся по всем позициям без денежной позиции
            if position['qty'] == 0:  # Если кол-во нулевое (позиция закрыта)
                continue  # то переходим на следующую позицию, дальше не продолжаем
            symbol = position['symbol']  # Тикер
            board = self.provider.get_symbol(self.exchange, symbol)['board']  # Режим торгов Алора
            dataname = self.provider.board_symbol_to_dataname(board, symbol)  # Название тикера
            si = self.get_symbol_by_dataname(dataname)  # Тикер по названию
            size = position['qty'] * si.lot_size  # Кол-во в штуках
            entry_price = self.provider.alor_price_to_price(self.exchange, symbol, position['avgPrice'])  # Цена входа
            # last_price = position['currentVolume'] / size  # Последняя цена по bid/ask
            last_price = entry_price + position['unrealisedPl'] / size  # Последняя цена по бумажной прибыли/убытку
            self.positions.append(Position(  # Добавляем текущую позицию в список
                self,  # Брокер
                dataname,  # Название тикера
                si.description,  # Описание тикера
                si.decimals,  # Кол-во десятичных знаков в цене
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
            symbol = order['symbol']  # Тикер
            board = self.provider.get_symbol(self.exchange, symbol)['board']  # Режим торгов Алора
            dataname = self.provider.board_symbol_to_dataname(board, symbol)  # Название тикера
            si = self.get_symbol_by_dataname(dataname)  # Информация о тикере с кол-вом десятичных знаков
            if not si:  # Если информация о тикере не найдена
                continue  # то переходим к следующей заявке, дальше не продолжаем
            self.orders.append(Order(  # Добавляем заявки в список
                self,  # Брокер
                order['id'],  # Уникальный код заявки
                order['side'] == 'buy',  # Покупка/продажа
                Order.Limit if order['price'] else Order.Market,  # Лимит/по рынку
                dataname,  # Название тикера
                si.decimals,  # Кол-во десятичных знаков в цене
                order['qty'] * si.lot_size,  # Кол-во в штуках
                self.provider.alor_price_to_price(self.exchange, si.symbol, order['price'])))  # Цена заявки
        stop_orders = self.provider.get_stop_orders(self.portfolio, self.exchange)  # Получаем список активных стоп заявок
        for stop_order in stop_orders:  # Пробегаемся по всем активным стоп заявкам
            if stop_order['status'] != 'working':  # Если заявка исполнена/отменена/отклонена
                continue  # то переходим к следующей стоп заявке, дальше не продолжаем
            symbol = stop_order['symbol']  # Тикер
            board = self.provider.get_symbol(self.exchange, symbol)['board']  # Режим торгов Алора
            dataname = self.provider.board_symbol_to_dataname(board, symbol)  # Название тикера
            si = self.get_symbol_by_dataname(dataname)  # Информация о тикере с кол-вом десятичных знаков
            if not si:  # Если информация о тикере не найдена
                continue  # то переходим к следующей стоп заявке, дальше не продолжаем
            self.orders.append(Order(  # Добавляем заявки в список
                self,  # Брокер
                stop_order['id'],  # Уникальный код заявки
                stop_order['side'] == 'buy',  # Покупка/продажа
                Order.StopLimit if stop_order['price'] else Order.Stop,  # Стоп-лимит/стоп
                dataname,  # Название тикера
                si.decimals,  # Кол-во десятичных знаков в цене
                stop_order['qty'] * si.lot_size,  # Кол-во в штуках
                self.provider.alor_price_to_price(self.exchange, si.symbol, stop_order['price'])))  # Цена срабатывания стоп заявки
        return self.orders

    def new_order(self, order: Order):
        response = None  # Результат запроса
        si = self.get_symbol_by_dataname(order.dataname)  # Тикер
        side = 'buy' if order.buy else 'sell'  # Покупка/продажа
        quantity = order.quantity // si.lot_size  # Кол-во в лотах
        price = self.provider.price_to_alor_price(si.exchange, si.symbol, order.price)  # Цена
        stop_price = self.provider.price_to_alor_price(si.exchange, si.symbol, order.stop_price)  # Стоп цена
        condition = 'MoreOrEqual' if order.buy else 'LessOrEqual'  # Условие срабатывания стоп цены
        if order.exec_type == Order.Market:  # Рыночная заявка
            response = self.provider.create_market_order(self.portfolio, si.exchange, si.symbol, side, quantity)
        elif order.exec_type == Order.Limit:  # Лимитная заявка
            response = self.provider.create_limit_order(self.portfolio, si.exchange, si.symbol, side, quantity, price)
        elif order.exec_type == Order.Stop:  # Стоп заявка
            response = self.provider.create_stop_order(self.portfolio, si.exchange, si.symbol, si.board, side, quantity, stop_price, condition)
        elif order.exec_type == Order.StopLimit:  # Стоп-лимитная заявка
            response = self.provider.create_stop_limit_order(self.portfolio, si.exchange, si.symbol, si.board, side, quantity, stop_price, price, condition)
        order.id = response['orderNumber']  # Сохраняем пришедший номер заявки на бирже

    def cancel_order(self, order: Order):
        si = self.get_symbol_by_dataname(order.dataname)  # Тикер
        stop = order.exec_type not in (Order.Market, Order.Limit)  # Удаляем стоп заявку
        self.provider.delete_order(self.portfolio, si.exchange, int(order.id), stop)  # Отменяем заявку по номеру

    def close(self):
        self.provider.close_web_socket()  # Перед выходом закрываем соединение с
