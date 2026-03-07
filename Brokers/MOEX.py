import logging
from datetime import datetime

from FinLabPy.Core import Broker, Bar, Symbol  # Брокер, бар, тикер
from MOEXPy import MOEXPy  # Работа с Algopack API Московской Биржи из Python через REST/WebSockets


class MOEX(Broker):
    """Московская Биржа"""

    def __init__(self, code='МБ', name='МосБиржа', provider=MOEXPy(), storage='file'):
        super().__init__(code, name, provider, 0, storage)
        logging.getLogger('urllib3').setLevel(logging.CRITICAL + 1)  # Не получаем сообщения подключений и отправки запросов в лог
        logging.getLogger('websockets').setLevel(logging.CRITICAL + 1)  # Не получаем сообщения поддерживания подключения в лог
        self.provider = provider  # Уже инициирован в базовом классе. Выполням для того, чтобы работать с типом провайдера
        self.last_bars = {}  # Последний бар. Он может быть не завершен
        self.provider.on_message.subscribe(self._on_new_bar)  # Подписка на новые бары

    def get_symbol_by_dataname(self, dataname: str):
        symbol = self.storage.get_symbol(dataname)  # Проверяем, есть ли спецификация тикера в хранилище
        if symbol is not None:  # Если есть тикер
            return symbol  # то возвращаем его, дальше не продолжаем
        board, symbol = self.provider.dataname_to_board_symbol(dataname)  # Код режима торгов и тикер из названия тикера
        si = self.provider.get_ticker(board, symbol)  # Получаем информацию о тикере (спецификация и рыночные данные)
        if si is None:  # Если информация о тикере не найдена
            return None  # то выходим, дальше не продолжаем
        col_securities = {col: idx for idx, col in enumerate(si['securities']['columns'])}  # Колонки спецификации тикера с их порядковыми номерами
        market, _, _ = self.provider.get_market_engine(board)  # Рынок и торговая площадка
        data_securities = si['securities']['data'][0]  # Спецификация тикера
        symbol = Symbol(
            board, symbol, dataname,
            data_securities[col_securities['SHORTNAME']],
            data_securities[col_securities['DECIMALS']],
            data_securities[col_securities['MINSTEP']],
            data_securities[col_securities['LOTSIZE']] if market == 'shares' else data_securities[col_securities['LOTVOLUME']])
        self.storage.set_symbol(symbol)  # Добавляем спецификацию тикера в хранилище
        return symbol

    def get_history(self, symbol, time_frame, dt_from=None, dt_to=None):
        moex_tf = self.provider.timeframe_to_moex_timeframe(time_frame)  # Временной интервал Московской Биржи (REST)
        bars = self.provider.get_candles(symbol.board, symbol.symbol, dt_from, dt_to, moex_tf)  # Получаем всю историю тикера
        col_bars = {col: idx for idx, col in enumerate(bars['candles']['columns'])}  # Колонки истории тикера с их порядковыми номерами
        data_bars = bars['candles']['data']  # Данные истории тикера
        if len(data_bars) == 0:  # Если бары не получены
            return None  # то выходим, дальше не продолжаем
        for bar in data_bars:  # Пробегаемся по всем барам
            bars.append(Bar(
                symbol.board, symbol.symbol, symbol.dataname, time_frame,
                datetime.fromisoformat(bar[col_bars['begin']]),
                bar[col_bars['open']],
                bar[col_bars['high']],
                bar[col_bars['low']],
                bar[col_bars['close']],
                int(bar[col_bars['volume']])))  # Добавляем бар
        self.storage.set_bars(bars)  # Сохраняем бары в хранилище
        return bars

    def subscribe_history(self, symbol, time_frame):
        moex_ws_tf = self.provider.timeframe_to_moex_ws_timeframe(time_frame)  # Временной интервал Московской Биржи (WebSockets)
        _, marketplace, _ = self.provider.get_market_engine(symbol.board)  # Рынок и торговая площадка
        self.provider.send_websocket(
            cmd='SUBSCRIBE',  # Подписываемся
            params={
                'destination': f'{marketplace}.candles',  # на бары
                'selector': dict(ticker=f'{marketplace}.{symbol.dataname}', interval=moex_ws_tf),  # тикера по временнОму интервалу МосБиржи
            })

    def unsubscribe_history(self, symbol, time_frame):
        moex_ws_tf = self.provider.timeframe_to_moex_ws_timeframe(time_frame)  # Временной интервал Московской Биржи (WebSockets)
        _, marketplace, _ = self.provider.get_market_engine(symbol.board)  # Рынок и торговая площадка
        subscription_id = next((  # Код подписки
            k for k, v in self.provider.subscriptions.items()  # из всех подписок
            if v['destination'] == f'{marketplace}.candles'  # на бары
            and v['selector']['ticker'] == f'{marketplace}.{symbol.dataname}'  # тикера
            and v['selector']['interval'] == moex_ws_tf), None)  # по временнОму интервалу МосБиржи
        if subscription_id is None:  # Если подписка не найдена
            return  # то выходим, дальше не продолжаем
        self.provider.send_websocket(
            cmd='UNSUBSCRIBE',  # Отписываемся
            params={
                'id': subscription_id,  # по коду подписки
            })

    def get_last_price(self, symbol):
        si = self.provider.get_ticker(symbol.board, symbol.symbol)  # Получаем информацию о тикере (спецификация и рыночные данные)
        col_marketdata = {col: idx for idx, col in enumerate(si['marketdata']['columns'])}  # Колонки рыночных данных тикера с их порядковыми номерами
        data_marketdata = si['marketdata']['data'][0]  # Рыночные данные тикера
        return data_marketdata[col_marketdata['LAST']]

    # Внутренние функции

    def _on_new_bar(self, headers, body):  # Обработчик события прихода нового бара
        if '.candles' not in headers['destination']:  # Если пришла подписка не на новый бар
            return  # то выходим, дальше не продолжаем
        _, board, symbol = headers['selector']['ticker'].split('.')[0]  # Режим торгов и тикер
        dataname = self.provider.board_symbol_to_dataname(board, symbol)  # Название тикера
        symbol = self.get_symbol_by_dataname(dataname)  # Спецификация тикера
        time_frame = self.provider.moex_ws_timeframe_to_timeframe(headers['selector']['interval'])  # Временной интервал
        last_bar: Bar = None if (symbol.dataname, time_frame) not in self.last_bars else self.last_bars[(symbol.dataname, time_frame)]  # Последний бар. Он может быть не завершен
        for row in body['data']:  # Пробегаемся по всем строкам
            row_dict = dict(zip(body['columns'], row))  # Переводим строку бара в словарь
            dt_bar = datetime.fromisoformat(row_dict['FROM'])  # Дата/время полученного бара
            if last_bar is not None and last_bar.datetime < dt_bar:  # Если время бара стало больше (предыдущий бар закрыт, новый бар открыт)
                self.on_new_bar.trigger(Bar(symbol.board, symbol.symbol, symbol.dataname, time_frame, last_bar.datetime, last_bar.open, last_bar.high, last_bar.low, last_bar.close, last_bar.volume))  # Вызываем событие добавления нового бара
            open_ = round(float(row_dict['OPEN'][0]), row_dict['OPEN'][1])
            high = round(float(row_dict['HIGH'][0]), row_dict['HIGH'][1])
            low = round(float(row_dict['LOW'][0]), row_dict['LOW'][1])
            close = round(float(row_dict['CLOSE'][0]), row_dict['CLOSE'][1])
            self.last_bars[(symbol.dataname, time_frame)] = Bar(symbol.board, symbol.symbol, symbol.dataname, time_frame, dt_bar, open_, high, low, close, int(float(row_dict['VOLUME'])))  # Запоминаем бар
