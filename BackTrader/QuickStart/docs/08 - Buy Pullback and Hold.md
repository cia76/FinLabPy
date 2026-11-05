[Видео](https://vkvideo.ru/video_ext.php?oid=-215710796&id=456239070&hash=1d88206e4c903f37&hd=3)

Приступаем к созданию торговых систем.

Внивание! В этом курсе мы будем создавать учебные торговые системы. Их главная задача - показать реализации базовых торговых техник. Не рекомендуется их использовать в реальной торговле.

Напишем первую учебную торговую систему набора позиции. Будем покупать заданным лотом, если цена падает 3-ий раз подряд. Далее будем просто держать позицию. Продажа не предусмотрена.

## Ряды цен

К каждому инструменту, который попадает в торговую систему, можно обратиться через **self.datas[<Порядковый номер инструкмента с 0>]**.
К первому добавленному инструменту можно обратиться **self.datas[0]**. Ко второму - **self.datas[1]**, и т.д.

Если в торговой системе будем работать только с ценами закрытия, то при инициализации можно сразу это правило определить.

```python
    def __init__(self):
        """Инициализация торговой системы"""
        self.close = self.datas[0].close  # Цены закрытия
```

## Правило входа в позицию

При приходе новой свечи рекомендую сразу же считать логику на вход и выход из позиции. Так, вход в позицию, если цена падает 3-ий раз подряд, будет таким:

```python
    def next(self):
        """Получение следующего бара"""
        signal_buy = self.close[0] < self.close[-1] < self.close[-2] < self.close[-3]  # Сигнал на покупку, когда цена падает 3 сессии подряд
```

Когда вся логика посчитана, можно покупать через **self.buy()** или продавать через **self.sell()**

```python
        if signal_buy:  # Если пришла заявка на покупку
            self.buy()  # Заявка на покупку по рыночной цене
```

## Размер покупки

Размер покупки в штуках мы можем задать параметром **stake** при конфигурировании размера позиции **cerebro.addsizer**

```python
cerebro.addsizer(bt.sizers.FixedSize, stake=100)  # Кол-во акций для покупки/продажи
```

## Отрисовка графика

После запуска торговой системы можно вывести графики через **cerebro.plot**
Т.к. мы не работаем с объемами, то их график нам не нужен. Убираем график объемов параметром **volume=False**

```python
cerebro.plot(volume=False)  # Рисуем график
```

## Полный код примера:

```python
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import backtrader as bt


logger = logging.getLogger('BuyPullbackAndHold')  # Будем вести лог


class BuyPullbackAndHold(bt.Strategy):
    """Покупка на откатах и удержание"""

    def __init__(self):
        """Инициализация торговой системы"""
        self.close = self.datas[0].close  # Цены закрытия

    def next(self):
        """Получение следующего бара"""
        logger.info(f'{bt.num2date(self.data.datetime[0]):%d.%m.%Y %H:%M:%S} O:{self.data.open[0]} H:{self.data.high[0]} L:{self.data.low[0]} C:{self.close[0]} V:{int(self.data.volume[0])}')
        signal_buy = self.close[0] < self.close[-1] < self.close[-2] < self.close[-3]  # Сигнал на покупку, когда цена падает 3 сессии подряд
        if signal_buy:  # Если пришла заявка на покупку
            logger.info('Buy Market @ Pullback')
            self.buy()  # Заявка на покупку по рыночной цене


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.INFO,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('BuyPullbackAndHold.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=ZoneInfo('Europe/Moscow')).timetuple()  # В логе время указываем по МСК

    cerebro = bt.Cerebro()  # Инициируем "движок" BackTrader
    cerebro.addstrategy(BuyPullbackAndHold)  # Привязываем торговую систему
    data = bt.feeds.GenericCSVData(
        # Можно принимать любые CSV файлы с разделителем десятичных знаков в виде точки https://backtrader.com/docu/datafeed-develop-csv/
        dataname='TQBR.SBER_D1.txt',  # Файл для импорта
        separator='\t',  # Колонки разделены табуляцией
        dtformat='%d.%m.%Y %H:%M',  # Формат даты/времени DD.MM.YYYY HH:MI
        openinterest=-1,  # Открытого интереса в файле нет
        fromdate=datetime(2024, 1, 1),  # Начальная дата приема исторических данных (Входит)
        todate=datetime(2026, 1, 1))  # Конечная дата приема исторических данных (Не входит)
    cerebro.adddata(data)  # Привязываем исторические данные
    cerebro.broker.setcash(1_000_000)  # Стартовый капитал для "бумажной" торговли
    cerebro.addsizer(bt.sizers.FixedSize, stake=100)  # Кол-во акций для покупки/продажи
    logger.info(f'Старовый капитал: {cerebro.broker.getvalue():.2f}')
    cerebro.run()  # Запуск торговой системы
    logger.info(f'Конечный капитал: {cerebro.broker.getvalue():.2f}')
    cerebro.plot(volume=False)  # Рисуем график
```
