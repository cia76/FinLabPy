# Отображение биржевых данных в TradingView Lightweight Charts https://tradingview.github.io/lightweight-charts/
# Что не может делать из коробки:
# - Рисовать на панелях индикаторов. Можно рисовать только на панели цен
# - Отображать легенду линии на своей панели. Все линии в легенде в левом верхнем углу
# - Не отображать в легенде уровни индикаторов
# - Закрашивать зоны. Например, перекупленность/перепроданность, полосы. Можно сделать градиент от 0 до значений
# - Рисовать нестандартные графики. Например, горизонтальные объемы
# - Рисовать многомерные графики. Например, тепловые карты

from itertools import cycle

import pandas as pd
from backtrader import with_metaclass, MetaParams, num2date, TimeFrame

from FinLabPy.Plot.LightweightCharts import Chart, JupyterChart
from FinLabPy.Plot.LightweightCharts.util import MARKER_POSITION, MARKER_SHAPE


class Plot(with_metaclass(MetaParams, object)):
    color_palette = (
        '#FF0000',  # Чистый красный. Резкий контраст по тону и яркости; привлекает внимание
        '#0000FF',  # Насыщенный синий. Глубокий цвет, хорошо читается на тёмном
        '#FF8C00',  # Темно-оранжевый
        '#800080',  # Пурпурный (фуксия). Насыщенный холодный тон; контрастен к серо‑чёрной базе
        '#00FFFF',  # Циан (голубой). Яркий, «световой» цвет; отлично виден на тёмном
        '#FFC0CB',  # Розовый. Светлый и тёплый; создаёт мягкий, но заметный контраст
        '#FFFFE0',  # Кремовый (светло‑жёлтый). Мягче чистого белого, но всё ещё хорошо читается; снижает зрительную нагрузку
        '#FFFF00',  # Лимонный жёлтый. Очень светлый и яркий; максимальный контраст по яркости
        '#00FF00',  # Яркий зелёный. Комплементарный к красному, высокий контраст на тёмном фоне
        '#FFFFFF',  # Белый. Максимальный контраст по свету; универсален для текста и акцентов
    )

    def __init__(self, **kwargs):  # Получаем доп. параметры BackTrader
        for param_name, param_value in kwargs.items():  # Пробегаемся по всем доп. параметрам
            setattr(self, param_name, param_value)  # Устанавливаем параметр как атрибут класса
        self.dataname = None  # Название тикера
        self.time_frame = None  # Временной интервал
        self.intraday = False  # Внутридневной интервал
        self.pd_bars = None  # pandas DataFrame с барами и индикаторами
        self.plot_params = {}  # Параметры отображения индикаторов
        self.jupyter = False  # Запущен ли код из Jupyter Notebook

    def plot(self, strategy, figid=0, numfigs=1, iplot=True, start=None, end=None, **kwargs):
        data = strategy.datas[0]  # TODO Сделать работу для всех данных ТС
        self.dataname = data._name
        self.time_frame = data._bt_timeframe_to_tf(data.p.timeframe, data.p.compression)  # Временной интервал FinLabPy
        self.intraday = data.p.timeframe <= TimeFrame.Minutes  # Внутридневной интервал
        self.pd_bars = pd.DataFrame(data={
            'open': data.open.array,
            'high': data.high.array,
            'low': data.low.array,
            'close': data.close.array,
            'volume': data.volume.array
        }, index=[num2date(dt) for dt in data.datetime.array])  # Переводим в дату и время, а затем в строку)  # Приводим к pandas DataFrame
        if not getattr(self, 'volume', True):  # Если не нужно отображать объемы
            self.pd_bars.drop('volume', axis=1, inplace=True)  # то удаляем объемы
        self.pd_bars.index.name = 'datetime'  # Название индекса

        auto_pane = 1  # Автоматическое размещение на отдельных панелях
        color_cycle = cycle(self.color_palette)  # Автоматически задаваемый цвет линий
        for indicator in strategy.getindicators():  # Пробегаемся по всем индикаторам
            indicator._plotinit()  # Инициируем программно задаваемые уровни индикатора
            str_params = '(' + ', '.join(str(value) for value in indicator.params._getvalues()) + ')'  # Строка параметров
            subplot = getattr(indicator.plotinfo, 'subplot', None)  # Отображать линии индикатора на отдельной панели
            lines = getattr(indicator.plotinfo, 'lines', None)  # Параметр отображения линий индикатора
            hlines = getattr(indicator.plotinfo, 'plothlines') + getattr(indicator.plotinfo, 'plotyhlines')  # Все уровни индикатора
            lines_names = [indicator.lines._getlinealias(line_id) for line_id in range(indicator.size())]  # Список названий линий индикатора
            is_auto_pane = False  # Пока автоматически не размещаем на отдельных панелях. Посмотрим, что будет дальше
            for i in range(len(lines_names)):  # Пробегаемся по всем линиям индикатора
                line_name = lines_names[i]  # Линия индикатора
                plot_params = {}  # Будем заполнять параметры отображения
                if lines is not None:  # Если есть параметры отображения линий индикатора
                    plot_params = lines.get(line_name)  # то пробуем получить параметры отображения линии индикатора
                elif subplot:  # Если параметров отображения линий индикатора нет, и нужно отобразить на отдельной панели
                    plot_params['pane_id'] = -auto_pane  # то задаем автоматическое размещение
                    is_auto_pane = True  # Было задано автоматическое размещение
                plot_params['color'] = plot_params.get('color', next(color_cycle))  # Если цвет линии не задан, то берем следующий цвет из палитры
                self.plot_params[line_name + ' ' + str_params] = plot_params  # Сохраняем параметры отображения линии индикатора
                self.pd_bars[line_name + ' ' + str_params] = indicator.lines[i].array  # Добавляем значения линии индикатора к барам
                if i == len(lines_names) - 1 and len(hlines) > 0:  # Если находимся на последней линии индикатора и у него есть горизонтальные линии
                    h_plot_params = dict(color='gray', style='dotted', width=1, pane_id=plot_params['pane_id'])  # Параметры отображения горизонтальной линии
                    for j in range(len(hlines)):  # Пробегаемся по каждой горизонтальной линии
                        self.plot_params[str(j) + ' ' + line_name] = h_plot_params  # Сохраняем параметры отображения горизонтальной линии индикатора
                        self.pd_bars[str(j) + ' ' + line_name] = hlines[j]  # Добавляем значения горизонтальной линии индикатора к барам
            if is_auto_pane:  # Если автоматически размещали линии индикатора
                auto_pane += 1  # то делаем смещение на следующую панель для следующего индикатора
        lines_without_pane_id = {k: v for k, v in self.plot_params.items() if 'pane_id' not in v}  # Линии, которые будут отображаться на панели цен (нет pane_id)
        lines_with_pane_id = {k: v for k, v in self.plot_params.items() if v.get('pane_id', 0) > 0}  # Линии, которые будут отображаться на своих панелях (есть pane_id > 0)
        max_pane_id = 0 if lines_with_pane_id == {} else max(lines_with_pane_id.values(), key=lambda x: x['pane_id'])['pane_id']  # Максимальный номер панели, где задано отображение линий индикатора
        lines_with_new_pane_id = {  # Линии, которые будут отображаться на новых панелях (есть pane_id < 0)
            k: {**v, 'pane_id': max_pane_id - v['pane_id']}  # Делаем копию словаря v, чтобы не изменять исходные значения, изменяем номер панели
            for k, v in self.plot_params.items() if v.get('pane_id', 0) < 0}  # Берем только данные с отрицательным номером панели
        lines_with_pane_id = {**lines_with_pane_id, **lines_with_new_pane_id}  # Объединяем все линии, которые будут отображаться на своих панелях
        lines_sorted_by_pane_id = dict(sorted(lines_with_pane_id.items(), key=lambda item: item[1]['pane_id']))  # Сортируем по возрастанию номера панели
        self.plot_params = {**lines_without_pane_id, **lines_sorted_by_pane_id}  # Объединяем сначала линии без панели, потом с панелями по возрастанию

        for observer in strategy.getobservers():  # Пробегаемся по всем панелям статистики
            color_cycle = cycle(self.color_palette)  # Автоматически задаваемый цвет линий
            lines = [observer.lines._getlinealias(line_id) for line_id in range(observer.lines.size())]  # Все линии статистики
            for i in range(len(lines)):  # Пробегаемся по всем линиям статистики
                line_name = lines[i]  # Название линии
                self.plot_params[line_name] = dict(pane_id=max_pane_id, color=next(color_cycle))  # Ставим номер панели максимально возможный (текущая панель) с цветом из палитры
                self.pd_bars[line_name] = observer.lines[i].array[:len(self.pd_bars)]  # Добавляем значение линии статистики к барам
            max_pane_id += 1  # Отображаем статистику на следюущей панели для каждой панели статистики (следующая панель)

    def show(self):
        try:
            from IPython import get_ipython  # В Jupyter Notebook всегда доступен объект IPython
            self.jupyter = get_ipython() is not None  # Если запущен в Jupyter Notebook, то возвращает объект ядра
        except ImportError:  # Если не возвращает
            self.jupyter = False  # то запуск не из Jupyter Notebook
        chart = JupyterChart(1080, 720, toolbox=True) if self.jupyter else Chart(toolbox=True)  # График с элементами рисования в т.ч. для Jupyter Notebook
        chart.layout(background_color='#222', text_color='#C3BCDB', font_family='sans-serif')  # Цвет фона и текста, шрифт
        chart.grid(color='#444')  # Цвет сетки
        chart.price_scale(scale_margin_top=0.1)  # От верха цен отступаем на 10%, от низа на 20% (по умолчанию)
        chart.time_scale(min_bar_spacing=8, border_color='#71649C')  # Минимальный размер бара и цвет горизонтальной линии над временем
        chart.crosshair(vert_width=6, vert_color='#C3BCDB44', vert_style='solid', vert_label_background_color='#9B7DFF', horz_color='#9B7DFF', horz_label_background_color='#9B7DFF')
        chart.watermark(f'{self.dataname} @ {self.time_frame}', color='rgba(255, 255, 255, 0.2)')  # Водяной знак с названием и временным интервалом инструмента
        chart.legend(True, font_family='sans-serif')
        chart.candle_style(up_color='rgba(54, 116, 217, 0.5)', down_color='rgba(225, 50, 85, 0.5)')  # Цвет свечей
        chart.volume_config(up_color='rgba(54, 116, 217, 0.3)', down_color='rgba(225, 50, 85, 0.3)')  # Цвет объемов с прозрачностью
        time_visible = 'true' if self.intraday else 'false'  # Для внутридневного интервала отображаем дату и время
        chart.run_script(f"""
        {chart.id}.chart.applyOptions({{
            localization: {{
                dateFormat: 'dd.MM.yyyy',
                }},
            timeScale: {{
              timeVisible: {time_visible},
              secondsVisible: false
            }},
        }})""")  # Формат отображения дат. Должен вызываться после основных настроек, т.к. они могут переписать значения

        chart.set(self.pd_bars)  # Отправляем бары на график

        for line_name, plot_params in self.plot_params.items():  # Пробегаемся по всем параметрам отображения
            color = plot_params.get('color', 'red')  # Цвет индикатора или цвет по умолчанию
            style = plot_params.get('style', 'solid')  # Стиль линии или по умолчанию
            width = plot_params.get('width', 2)  # Толщина линии или по умолчанию
            pane_index = plot_params.get('pane_id')  # Номер панели. None, если отображать на панели цен
            if line_name in ('buy', 'sell'):  # Покупка/продажа
                position: MARKER_POSITION = 'below' if line_name == 'buy' else 'above'  # Стрелку покупки рисуем ниже бара, стрелку продажи - выше
                shape: MARKER_SHAPE = 'arrow_up' if line_name == 'buy' else 'arrow_down'  # Стрелка вверх для покупки, стрелка вниз для продажи
                color = 'green' if line_name == 'buy' else 'red'  # Зеленым цветом рисуем покупку, красным - продажу
                # Цену покупки/продажи мы не знаем. price[1] показывает где поставить маркер, чтобы не задеть свечку
                [chart.marker(time=price[0], position=position, shape=shape, color=color) for price in self.pd_bars[[line_name]].dropna().itertuples()]  # Ставим маркер покупки/продажи
            elif line_name in ('pnlplus', 'pnlminus'):  # Сделка с прибылью/убытком
                color = 'green' if line_name == 'pnlplus' else 'red'  # Зеленым цветом рисуем прибыль, красным - убыток
                histogram = chart.create_histogram(line_name, color, price_line=False, price_label=False, scale_margin_top=0.05, scale_margin_bottom=0.05, pane_index=pane_index)  # Отображаем в виде гистограммы
                histogram.set(self.pd_bars[[line_name]])  # Заполняем ее значениями MACD
            else:  # Для остальных линий
                line = chart.create_line(line_name, color, style, width, pane_index=pane_index)  # На графике цен создаем линию
                pane_height = plot_params.get('pane_height')  # Высота панели
                if pane_index and pane_height:  # Если индикатор отображается на отдельной панели, и у нее задана высота
                    chart.resize_pane(pane_index, pane_height)  # то задаем высоту панели
                line.set(self.pd_bars[[line_name]])  # Заполняем линию значениями индикатора

        if self.jupyter:  # Если запущен в Jupyter Notebook
            chart.load()  # то загружаем график
        else:  # В остальных случаях
            chart.show(block=True)  # отображаем график. Блокируем дальнейшее исполнение кода, пока его не закроем
