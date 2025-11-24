import logging  # Будем вести лог
from collections import defaultdict, OrderedDict, deque  # Словари и очередь

from backtrader import BrokerBase, Order as BTOrder, BuyOrder, SellOrder
from backtrader.position import Position as BTPosition
from backtrader.utils.py3 import with_metaclass

from FinLabPy.BackTrader import Store, Data  # Хранилище и данные для BackTrader
from FinLabPy.Core import Order as FLOrder, Trade as FLTrade, Position as FLPosition   # Заявка, сделка, позиция


# noinspection PyArgumentList,PyMethodParameters
class MetaBroker(BrokerBase.__class__):
    def __init__(cls, name, bases, dct):
        super(MetaBroker, cls).__init__(name, bases, dct)  # Инициализируем класс брокера
        Store.BrokerCls = cls  # Регистрируем класс брокера в хранилище


# noinspection PyProtectedMember,PyArgumentList
class Broker(with_metaclass(MetaBroker, BrokerBase)):
    """Брокер BackTrader"""

    def __init__(self, **kwargs):
        """Инициализация"""
        super(Broker, self).__init__()
        self.store = Store(**kwargs)  # Хранилище BackTrader
        self.logger = logging.getLogger(f'BTBroker.{self.store.broker.code}')  # Будем вести лог
        self.notifs = deque()  # Очередь уведомлений брокера о заявках
        self.orders = OrderedDict()  # Справочник заявок, отправленных на биржу
        self.ocos = {}  # Справочник связанных заявок (One Cancel Others)
        self.pcs = defaultdict(deque)  # Справочник очереди всех родительских/дочерних заявок (Parent - Children)
        self.positions = defaultdict(BTPosition)  # Список позиций
        self.startingcash = self.cash = self.store.broker.get_cash()  # Стартовые и текущие свободные средства
        self.value = self.store.broker.get_value()  # Текущая стоимость позиций

        self.store.broker.on_order.subscribe(self._on_order)  # Получение заявки по подписке
        self.store.broker.on_trade.subscribe(self._on_trade)  # Получение сделки по подписке
        self.store.broker.on_position.subscribe(self._on_position)  # Получение позиции по подписке

    def start(self):
        """Запуск"""
        super(Broker, self).start()
        for position in self.store.broker.get_positions():  # Пробегаемся по всем открытым позициям
            self.positions[position.dataname] = self._position_to_bt_position(position)  # Получаем все открытые позиции. Обновлять будем через совершенные сделки

    def getcash(self) -> float:
        """Свободные средства. Запрос вызывается каждый раз при отправке уведомлений из Strategy._notify"""
        return 0 if self.store.BrokerCls is None else self.cash  # Если брокера нет в хранилище, то 0. Иначе, получаем его свободные средства

    def getvalue(self, datas: list[Data] = None) -> float:
        """Стоимость всех позиций, выбранных позиций, выбранной позиции. Запрос вызывается каждый раз при отправке уведомлений из Strategy._notify"""
        if self.store.BrokerCls is None:  # Если брокера нет в хранилище
            return 0  # то стоимость 0
        if datas is None:  # Если стоимость всех позиций
            return self.value  # то получаем стоимость всех позиций брокера
        datanames = [data.p.dataname for data in datas]  # Список тикеров
        return sum([position.price * position.size for key, position in self.positions.items() if key in datanames])  # Стоимость позиций тикеров

    def getposition(self, data: Data) -> BTPosition | None:
        """Позиция по тикеру
        В BackTrader класс позиций никак не связан с остальными классами. В нем содержится только цена и кол-во
        Используется в strategy.py для закрытия (close) и ребалансировки (увеличения/уменьшения) позиции:
        - В процентах от портфеля (order_target_percent)
        - До нужного кол-ва (order_target_size)
        - До нужного объема (order_target_value)
        """
        return self.positions[data.p.dataname]  # Получаем позицию по тикеру или нулевую позицию, если тикера в списке позиций нет

    def buy(self, owner, data, size, price=None, plimit=None, exectype=None, valid=None, tradeid=0, oco=None, trailamount=None, trailpercent=None, parent=None, transmit=True, **kwargs) -> BuyOrder:
        """Заявка на покупку"""
        order: BuyOrder = self._create_order(owner, data, size, price, plimit, exectype, valid, oco, parent, transmit, True, **kwargs)
        self.notifs.append(order.clone())  # Уведомляем брокера о принятии/отклонении зявки на бирже
        return order

    def sell(self, owner, data, size, price=None, plimit=None, exectype=None, valid=None, tradeid=0, oco=None, trailamount=None, trailpercent=None, parent=None, transmit=True, **kwargs) -> SellOrder:
        """Заявка на продажу"""
        order: SellOrder = self._create_order(owner, data, size, price, plimit, exectype, valid, oco, parent, transmit, False, **kwargs)
        self.notifs.append(order.clone())  # Уведомляем брокера о принятии/отклонении зявки на бирже
        return order

    def cancel(self, order: BTOrder):
        """Отмена заявки"""
        return self._cancel_order(order)

    def get_notification(self):
        """Получение уведомления"""
        return self.notifs.popleft() if self.notifs else None  # Удаляем и возвращаем крайний левый элемент списка уведомлений или ничего

    def next(self):
        """Приход нового бара"""
        self.notifs.append(None)  # Добавляем в список уведомлений пустой элемент

    def stop(self):
        """Остановка брокера"""
        super(Broker, self).stop()
        self.store.BrokerCls = None  # Удаляем класс брокера из хранилища

    # Внутренние функции

    def _get_order(self, order_number) -> BTOrder | None:
        """Заявка BackTrader по номеру заявки на бирже

        :param order_number: Номер заявки на бирже
        :return: Заявка BackTrader или None
        """
        return next((order for order in self.orders.values() if order.info['order_number'] == order_number), None)  # Пробегаемся по всем заявкам на бирже. Если нашли совпадение с номером заявки на бирже, то возвращаем заявку BackTrader. Иначе, ничего не найдено

    def _create_order(self, owner, data: Data, size, price=None, plimit=None, exectype=None, valid=None, oco=None, parent=None, transmit=True, is_buy=True, **kwargs) -> BuyOrder | SellOrder:
        """Создание заявки: Created/Rejected. Привязка параметров счета и тикера. Обработка связанных и родительской/дочерних заявок"""
        order = BuyOrder(owner=owner, data=data, size=size, price=price, pricelimit=plimit, exectype=exectype, valid=valid, oco=oco, parent=parent, transmit=transmit) if is_buy \
            else SellOrder(owner=owner, data=data, size=size, price=price, pricelimit=plimit, exectype=exectype, valid=valid, oco=oco, parent=parent, transmit=transmit)  # Заявка на покупку/продажу (Order.Created)
        order.addcomminfo(self.getcommissioninfo(data))  # По тикеру выставляем комиссии в заявку. Нужно для исполнения заявки в BackTrader
        order.addinfo(**kwargs)  # Передаем в заявку все дополнительные параметры
        if order.exectype in (BTOrder.Close, BTOrder.StopTrail, BTOrder.StopTrailLimit, BTOrder.Historical):  # Эти типы заявок не реализованы
            self.logger.warning(f'Постановка заявки {order.ref} по тикеру {data.p.dataname} отклонена. Работа с заявками {order.exectype} не реализована')
            order.reject(self)  # то отклоняем заявку
            self._oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки (Неверный тип заявки)
            return order  # Возвращаем отклоненную заявку
        if order.exectype != BTOrder.Market and not order.price:  # Если цена заявки не указана для всех заявок, кроме рыночной
            price_type = 'Лимитная' if order.exectype == BTOrder.Limit else 'Стоп'  # Для стоп заявок это будет триггерная (стоп) цена
            self.logger.warning(f'Постановка заявки {order.ref} по тикеру {data.p.dataname} отклонена. {price_type} цена (price) не указана для заявки типа {order.exectype}')
            order.reject(self)  # то отклоняем заявку
            self._oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки (Не указана цена исполнения)
            return order  # Возвращаем отклоненную заявку
        if order.exectype == BTOrder.StopLimit and not order.pricelimit:  # Если лимитная цена не указана для стоп-лимитной заявки
            self.logger.warning(f'Постановка заявки {order.ref} по тикеру {data.p.dataname} отклонена. Лимитная цена (pricelimit) не указана для заявки типа {order.exectype}')
            order.reject(self)  # то отклоняем заявку
            self._oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки (Не указана лимитная цена для стоп лимитной заявки)
            return order  # Возвращаем отклоненную заявку

        if oco:  # Если есть связанная заявка
            self.ocos[order.ref] = oco.ref  # то заносим в список связанных заявок
        if not transmit or parent:  # Для родительской/дочерних заявок
            parent_ref = getattr(order.parent, 'ref', order.ref)  # Номер транзакции родительской заявки или номер заявки, если родительской заявки нет
            if order.ref != parent_ref and parent_ref not in self.pcs:  # Если есть родительская заявка, но она не найдена в очереди родительских/дочерних заявок
                self.logger.warning(f'Постановка заявки {order.ref} по тикеру {data.p.dataname} отклонена. Родительская заявка не найдена')
                order.reject(self)  # то отклоняем заявку
                self._oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
                return order  # Возвращаем отклоненную заявку
            pcs = self.pcs[parent_ref]  # В очередь к родительской заявке
            pcs.append(order)  # добавляем заявку (родительскую или дочернюю)
        if transmit:  # Если обычная заявка или последняя дочерняя заявка
            if not parent:  # Для обычных заявок
                return self._place_order(order)  # Отправляем заявку на биржу
            else:  # Если последняя заявка в цепочке родительской/дочерних заявок
                self.notifs.append(order.clone())  # Удедомляем брокера о создании новой заявки
                return self._place_order(order.parent)  # Отправляем родительскую заявку на биржу
        # Если не последняя заявка в цепочке родительской/дочерних заявок (transmit=False)
        return order  # то возвращаем созданную заявку со статусом Created. На биржу ее пока не отправляем

    def _place_order(self, order: BTOrder):
        """Отправка заявки на биржу: Submitted/Accepted/Rejected"""
        order.submit(self)  # Отправляем заявку на биржу (Order.Submitted)
        self.notifs.append(order.clone())  # Уведомляем брокера об отправке заявки на биржу
        fl_order = self._bt_order_to_order(order)  # Переводим заявку BackTrader в заявку
        result = self.store.broker.new_order(fl_order)  # Отправляем заявку через брокера на биржу
        if not result:  # Если при отправке заявки на биржу произошла ошибка
            self.logger.warning(f'Постановка заявки по тикеру {order.data.p.dataname} отклонена. Ошибка веб сервиса')
            order.reject(self)  # то отклоняем заявку
            self._oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки (Отклонение заявки при постановке)
            return order  # Возвращаем отклоненную заявку
        order.addinfo(order_number=fl_order.id)  # Сохраняем пришедший номер заявки на бирже
        order.accept(self)  # Заявка принята на бирже (Order.Accepted)
        self.orders[order.ref] = order  # Сохраняем заявку в списке заявок, отправленных на биржу
        return order  # Возвращаем заявку

    def _cancel_order(self, order: BTOrder) -> bool:
        """Отмена заявки"""
        if not order.alive():  # Если заявка уже была завершена
            return False  # то выходим, дальше не продолжаем
        if order.ref not in self.orders:  # Если заявка не найдена
            return False  # то выходим, дальше не продолжаем
        self.store.broker.cancel_order(self._bt_order_to_order(order))  # Снятие заявки
        return True  # В список уведомлений ничего не добавляем. Ждем события on_order

    def _oco_pc_check(self, order: BTOrder):
        """Проверка связанных и родительской/дочерних заявок"""
        ocos = self.ocos.copy()  # Пока ищем связанные заявки, они могут измениться. Поэтому, работаем с копией
        for order_ref, oco_ref in ocos.items():  # Пробегаемся по списку связанных заявок
            if oco_ref == order.ref:  # Если в заявке номер эта заявка указана как связанная (по номеру транзакции)
                self.cancel_order(self.orders[order_ref])  # то отменяем заявку
        if order.ref in ocos.keys():  # Если у этой заявки указана связанная заявка
            oco_ref = ocos[order.ref]  # то получаем номер транзакции связанной заявки
            self.cancel_order(self.orders[oco_ref])  # отменяем связанную заявку

        if not order.parent and not order.transmit and order.status == BTOrder.Completed:  # Если исполнена родительская заявка
            pcs = self.pcs[order.ref]  # Получаем очередь родительской/дочерних заявок
            for child in pcs:  # Пробегаемся по всем заявкам
                if child.parent:  # Пропускаем первую (родительскую) заявку
                    self._place_order(child)  # Отправляем дочернюю заявку на биржу
        elif order.parent:  # Если исполнена/отменена дочерняя заявка
            pcs = self.pcs[order.parent.ref]  # Получаем очередь родительской/дочерних заявок
            for child in pcs:  # Пробегаемся по всем заявкам
                if child.parent and child.ref != order.ref:  # Пропускаем первую (родительскую) заявку и исполненную заявку
                    self.cancel_order(child)  # Отменяем дочернюю заявку

    def _bt_order_to_order(self, order: BTOrder) -> FLOrder | None:
        """Заявка BackTrader -> Заявка FinLabPy"""
        price = 0  # Лимитная цена
        stop_price = 0  # Цена срабатывания стоп заявки
        if order.exectype == BTOrder.Market:  # Рыночная заявка
            exec_type = FLOrder.Market
        elif order.exectype == BTOrder.Limit:  # Лимитная заявка
            exec_type = FLOrder.Limit
            price = order.price  # Лимитная цена
        elif order.exectype == BTOrder.Stop:  # Стоп заявка
            exec_type = FLOrder.Stop
            stop_price = order.price  # Цена срабатывания стоп заявки
        elif order.exectype == BTOrder.StopLimit:  # Стоп-лимитная заявка
            exec_type = FLOrder.StopLimit
            stop_price = order.price  # Цена срабатывания стоп заявки
            price = order.pricelimit  # Лимитная цена после срабатывания стоп заявки
        else:  # Для остальных типов заявок: Close, StopTrail, StopTrailLimit, Historical
            self.logger.error(f'Тип заявки {order.getordername()} не реализован. Используйте типы заявок: Market, Limit, Stop, StopLimit')
            return None  # Конвертация невозможна. Выходим, дальше не продолжаем
        dataname = order.data.p.dataname  # Название тикера
        if order.status == BTOrder.Created:  # Создана
            status = FLOrder.Created
        elif order.status == BTOrder.Submitted:  # Отправлена брокеру
            status = FLOrder.Submitted
        elif order.status == BTOrder.Accepted:  # Принята брокером
            status = FLOrder.Accepted
        elif order.status == BTOrder.Partial:  # Частично исполнена
            status = FLOrder.Partial
        elif order.status == BTOrder.Completed:  # Исполнена
            status = FLOrder.Completed
        elif order.status == BTOrder.Canceled:  # Отменена
            status = FLOrder.Canceled
        elif order.status == BTOrder.Expired:  # Снята по времени
            status = FLOrder.Expired
        elif order.status == BTOrder.Margin:  # Недостаточно средств
            status = FLOrder.Margin
        elif order.status == BTOrder.Rejected:  # Отклонена брокером
            status = FLOrder.Rejected
        else:  # Все статусы разобраны. Проверка на всякий случай
            self.logger.error(f'Неизвестный статус заявки {order.getstatusname()}')
            return None  # Конвертация невозможна. Выходим, дальше не продолжаем
        return FLOrder(
            self.store.broker,  # Брокер
            order.info['order_number'] if 'order_number' in order.info.keys() else None,  # Номер заявки будет известен только после постановки на бирже
            order.isbuy(),  # Покупка = True, продажа = False
            exec_type,  # Тип заявки
            dataname,  # Название тикера
            self.store.broker.get_symbol_by_dataname(dataname).decimals,  # Кол-во десятичных знаков в цене
            order.size,  # Кол-во в штуках
            price,  # Лимитная цена для лимитных и стоп лимитных заявок
            stop_price,  # Стоп цена срабатывания для стоп и стоп лимитных заявок
            status)  # Статус заявки

    def _on_order(self, order: FLOrder):
        """Получение заявки по подписке: Canceled/Expired/Margin/Rejected"""
        bt_order = self._get_order(order.id)  # Заявка BackTrader по номеру заявки на бирже
        if bt_order is None:  # Если заявка не найдена
            return  # то выходим, дальше не продолжаем
        if bt_order.Status == BTOrder.Canceled:  # Отменена
            bt_order.cancel()  # Отменяем существующую заявку (Order.Canceled)
        elif bt_order.Status == BTOrder.Expired:  # Снята по времени. Нужно установить в заявке свойство valid
            bt_order.expire()  # Отменяем текущую заявку по времени (Order.Expired)
        elif bt_order.Status == BTOrder.Margin:  # Недостаточно средств
            bt_order.margin()  # Отменяем существующую заявку по Margin Call (Order.Margin)
        elif bt_order.Status == BTOrder.Rejected:  # Отклонена брокером
            bt_order.reject(self)  # Отменяем существующую заявку от брокера (Order.Rejected)
        else:  # Исполнена/Частично исполнена
            return  # Обработаем заявку при приходе сделки _on_trade. Выходим, дальше не продолжаем
        self.notifs.append(bt_order.clone())  # Уведомляем брокера об отмене заявки
        self._oco_pc_check(bt_order)  # Проверяем связанные и родительскую/дочерние заявки (Снятие заявки)

    def _on_trade(self, trade: FLTrade):
        """Получение сделки по подписке. Исполнение заявки: Partial/Completed"""
        bt_order = self._get_order(trade.order_id)  # Заявка BackTrader по номеру заявки на бирже из сделки
        if bt_order is None:  # Если заявка не найдена
            return  # то выходим, дальше не продолжаем
        bt_position = self.getposition(bt_order.data)  # Получаем позицию по тикеру или нулевую позицию если тикера в списке позиций нет
        psize, pprice, opened, closed = bt_position.update(trade.quantity, trade.price)  # Обновляем размер/цену позиции на размер/цену сделки
        bt_order.execute(trade.datetime, trade.quantity, trade.price, closed, 0, 0, opened, 0, 0, 0, 0, psize, pprice)  # Исполняем заявку в BackTrader
        if bt_order.executed.remsize:  # Если осталось что-то к исполнению
            if bt_order.status == bt_order.Partial:  # Если заявка переходит в статус частичного исполнения (может исполняться несколькими частями)
                bt_order.partial()  # то заявка частично исполнена
                self.notifs.append(bt_order.clone())  # Уведомляем брокера о частичном исполнении заявки
        else:  # Если ничего нет к исполнению
            bt_order.completed()  # то заявка полностью исполнена
            self.notifs.append(bt_order.clone())  # Уведомляем брокера о полном исполнении заявки
            # Снимаем oco-заявку только после полного исполнения заявки
            # Если нужно снять oco-заявку на частичном исполнении, то прописываем это правило в ТС
            self._oco_pc_check(bt_order)  # Проверяем связанные и родительскую/дочерние заявки (Completed)
        self.cash = self.store.broker.get_cash()  # Текущие свободные средства
        self.value = self.store.broker.get_value()  # Текущая стоимость позиций

    @staticmethod
    def _position_to_bt_position(position: FLPosition) -> BTPosition:
        """Позиция FinLabPy -> позиция BackTrader"""
        return BTPosition(position.quantity, position.average_price)

    def _on_position(self, position: FLPosition):
        """Получение позиции по подписке"""
        self.positions[position.dataname] = BTPosition(position.quantity, position.average_price)  # Сохраняем в списке открытых позиций с текущим кол-вом и средней ценой входа
