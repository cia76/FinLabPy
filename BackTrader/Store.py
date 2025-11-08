from collections import deque

from backtrader.metabase import MetaParams
from backtrader.utils.py3 import with_metaclass

from FinLabPy.Config import default_broker  # Брокер по умолчанию
from FinLabPy.Core import Broker as FLBroker, Bar as FLBar


class MetaSingleton(MetaParams):
    """Метакласс для создания Singleton классов"""
    def __init__(cls, *args, **kwargs):
        super(MetaSingleton, cls).__init__(*args, **kwargs)
        cls._singleton = None  # Экземпляра класса еще нет

    def __call__(cls, *args, **kwargs):
        if cls._singleton is None:  # Если класса нет в экземплярах класса
            cls._singleton = super(MetaSingleton, cls).__call__(*args, **kwargs)  # то создаем зкземпляр класса
        return cls._singleton  # Возвращаем экземпляр класса


class Store(with_metaclass(MetaSingleton, object)):
    """Хранилище BackTrader"""

    BrokerCls = None  # Класс брокера будет задан из брокера
    DataCls = None  # Класс данных будет задан из данных

    @classmethod
    def getdata(cls, *args, **kwargs):
        """Новый экземпляр класса данных с заданными параметрами"""
        return cls.DataCls(*args, **kwargs)

    @classmethod
    def getbroker(cls, *args, **kwargs):
        """Новый экземпляр класса брокера с заданными параметрами"""
        return cls.BrokerCls(*args, **kwargs)

    def __init__(self, **kwargs):
        super(Store, self).__init__()
        self.broker: FLBroker = kwargs['broker'] if 'broker' in kwargs.keys() else default_broker  # Подключаемся к брокеру если указан. Иначе, используем брокера по умолчанию
        self.notifs = deque()  # Очередь уведомлений
        self.new_bars: list[FLBar] = []  # Спиоск новых бар по всем подпискам на тикеры

    def start(self):
        self.broker.on_new_bar.subscribe(self._on_new_bar)  # Подписываемся на новые бары

    def put_notification(self, msg, *args, **kwargs):
        """Добавление уведомлений в хранилище"""
        self.notifs.append((msg, args, kwargs))  # Добавляем уведомление

    def get_notifications(self):
        """Выдача накопленных уведомлений из хранилища"""
        self.notifs.append(None)  # Добавляем пустое уведомление
        return [x for x in iter(self.notifs.popleft, None)]  # Собираем накопленные уведомления в порядке их поступления до пустого элемента (до конца)

    def stop(self):
        self.broker.on_new_bar.unsubscribe(self._on_new_bar)  # Отписываемся от новых бар
        self.broker.close()  # Перед выходом закрываем провайдер брокера

    def _on_new_bar(self, bar: FLBar): self.new_bars.append(bar)  # При поступлении нового бара добавляем его в список новых бар
