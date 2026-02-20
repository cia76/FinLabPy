from FinLabPy.Config import brokers, default_broker  # Все брокеры и брокер по умолчанию
from FinLabPy.Core import bars_to_df  # Перевод бар в pandas DataFrame
from FinLabPy.Plot.LightweightCharts import Chart  # TradingView Lightweight Charts


if __name__ == '__main__':
    dataname = 'TQBR.SBER'
    time_frame = 'D1'

    broker = default_broker  # Брокер по умолчанию
    # broker = brokers['Т']  # Брокер по ключу из Config.py словаря brokers
    symbol = broker.get_symbol_by_dataname(dataname)  # Тикер по названию
    bars = broker.get_history(symbol, time_frame)  # Получаем всю историю тикера
    broker.close()  # Закрываем брокера

    # Настроим график по примеру https://tradingview.github.io/lightweight-charts/tutorials/customization/intro
    # Не сделано:
    # - Отображение цен в Евро. Не требуется
    # - Выделение цен по условию другим цветом. Не требуется
    # - Градиент под графиком. Будет мешаться, когда на график наложатся индикаторы
    chart = Chart()  # График
    chart.layout(background_color='#222', text_color='#C3BCDB', font_family='sans-serif')  # Цвет фона и текста, шрифт
    chart.grid(color='#444')  # Цвет сетки
    chart.price_scale(scale_margin_top=0.1, border_visible=True, border_color='#71649C')  # От верха цен отступаем на 10%, от низа на 20% (по умолчанию). Показваем вертикальную линию слева от цен
    chart.time_scale(min_bar_spacing=8, border_color='#71649C')  # Минимальный размер бара и цвет горизонтальной линии над временем
    chart.crosshair(vert_width=6, vert_color='#C3BCDB44', vert_style='solid', vert_label_background_color='#9B7DFF', horz_color='#9B7DFF', horz_label_background_color='#9B7DFF')
    chart.watermark(f'{dataname} @ {time_frame}', color='#555')  # Водяной знак с названием и временным интервалом инструмента
    chart.legend(True, font_family='sans-serif')
    chart.candle_style(up_color='rgb(54, 116, 217)', down_color='rgb(225, 50, 85)')  # Цвет свечей
    chart.volume_config(up_color='rgba(54, 116, 217, 0.5)', down_color='rgba(225, 50, 85, 0.5)')  # Цвет объемов с прозрачностью
    chart.run_script(f"""
    {chart.id}.chart.applyOptions({{
        localization: {{
            dateFormat: 'dd.MM.yyyy',
            }},
        timeScale: {{
          timeVisible: false,
          secondsVisible: false
        }},
    }})""")  # Формат отображения дат. Для дневного интервала не ставим время и секунды. Должен вызываться после основных настроек, т.к. они могут переписать значения

    pd_bars = bars_to_df(bars)  # Бары в pandas DataFrame
    chart.set(pd_bars)  # Отправляем бары на график

    chart.show(block=True)
