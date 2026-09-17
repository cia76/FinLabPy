Расширим пример из [предыдущего урока](01%20-%20Hello%20BackTrader.md). Подключим вашего брокера, и посмотрим свободные средства и стоимость позиций на нём.

Внимание! Для успешного выполнения этого и следующих примеров нужно, чтобы универсальная система автоторговли [FinLabPy](https://github.com/cia76/FinLabPy) и коннекторы к вашим брокерам были настроены и работали.

Из FinLabPy импортируем всех ваших брокеров и брокера по умолчанию.

```python
from FinLabPy.Config import brokers, default_broker  # Все брокеры и брокер по умолчанию
```

В BackTrader рекомендуется операции по получению исторических данных объединять с операциями выставления заявок в хранилища (Store). Мы следуем рекомендациям. Импортируем хранилище BackTrader для FinLabPy.

```python
from FinLabPy.BackTrader import Store  # Хранилище BackTrader
```

Сделаем хранилище для брокера по умолчанию.

```python
store = Store(broker=default_broker)  # Хранилище брокера по умолчанию
```

Можно сделать хранилище для любого брокера FinLabPy. Нужно просто указать его ключ.

```python
store = Store(broker=brokers['<Ключ словаря brokers из Config.py>'])  # Хранилище выбранного брокера
```

Из хранилища получаем брокера.

```python
broker = store.getbroker()  # Брокер
```

Устанавливаем его в "движок" BackTrader.

```python
cerebro.setbroker(broker)  # Устанавливаем брокера
```

При запуске скрипта выводится реальное состояние счета брокера.

Полный код примера:

```python
import backtrader as bt  # Библиотека BackTrader

from FinLabPy.Config import brokers, default_broker  # Все брокеры и брокер по умолчанию
from FinLabPy.BackTrader import Store  # Хранилище BackTrader


def get_cash_value():
    cash = cerebro.broker.getcash()  # Свободные средства
    value = cerebro.broker.getvalue()  # Стоимость портфеля
    print(f'Свободные средства : {cash}')
    print(f'Стоимость позиций  : {value - cash}')
    print(f'Стоимость портфеля : {value}')


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    cerebro = bt.Cerebro()  # Инициируем "движок" BackTrader (Cerebro = Мозг на испанском)
    store = Store(broker=default_broker)  # Хранилище брокера по умолчанию
    # store = Store(broker=brokers['<Ключ словаря brokers из Config.py>'])  # Хранилище выбранного брокера
    broker = store.getbroker()  # Брокер
    cerebro.setbroker(broker)  # Устанавливаем брокера

    print('\nСтартовый капитал')
    get_cash_value()  # Отображаем статистику портфеля до запуска ТС
    cerebro.run()  # Запуск ТС. Пока ее у нас нет
    print('\nКонечный капитал')
    get_cash_value()  # Отображаем статистику портфеля после запуска ТС
```
