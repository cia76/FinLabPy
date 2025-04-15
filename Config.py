# Курс Мультиброкер: Контроль https://finlab.vip/wpm-category/mbcontrol/

from AlorPy import AlorPy  # Провайдер Алор
from FinLabPy.Brokers.Alor import Alor  # Брокер Алор

from FinamPy import FinamPyOld  # Провайдер Финам
from FinLabPy.Brokers.FinamOld import Finam  # Брокер Финам

from TinkoffPy import TinkoffPy  # Провайдер Т-Инвестиции
from FinLabPy.Brokers.Tinkoff import Tinkoff  # Брокер Т-Инвестиции

# from QuikPy import QuikPy  # Провайдер QUIK
# from FinLabPy.Brokers.Quik import Quik  # Брокер QUIK


# Провайдеры
ap_provider = AlorPy()  # Провайдер Алор. Для демо счета AlorPy(demo=True)
fp_provider = FinamPyOld()  # Провайдер Финам
tp_provider = TinkoffPy()  # Провайдер Т-Инвестиции. Для демо счета TinkoffPy(demo=True). Курс Мультиброкер: Облигации https://finlab.vip/wpm-category/mbbonds/
# qp_provider = QuikPy()  # Провайдер QUIK

# Брокеры
brokers = {
    'АС': Alor(code='АС', name='Алор - Срочный рынок', provider=ap_provider, account_id=0),  # Алор - Портфель срочного рынка
    'АФ': Alor(code='АФ', name='Алор - Фондовый рынок', provider=ap_provider, account_id=1),  # Алор - Портфель фондового рынка
    # 'АВ': Alor(code='АВ', name='Алор - Валютный рынок', provider=ap_provider, account_id=2),  # Алор - Портфель валютного рынка
    # 'ИС': Alor(code='ИС', name='Алор ИИС - Срочный рынок', provider=ap_provider, account_id=3),  # Алор ИИС - Портфель срочного рынка
    'ИФ': Alor(code='ИФ', name='Алор ИИС - Фондовый рынок', provider=ap_provider, account_id=4),  # Алор ИИС - Портфель фондового рынка
    # 'ИВ': Alor(code='ИВ', name='Алор ИИС - Валютный рынок', provider=ap_provider, account_id=5),  # Алор ИИС - Портфель валютного рынка
    'Ф': Finam(code='Ф', name='Финам', provider=fp_provider),  # Финам
    'Т': Tinkoff(code='Т', name='Т-Инвестиции', provider=tp_provider),  # Т-Инвестиции
    # 'КС': Quik(code='КС', name='QUIK - Срочный рынок', provider=qp_provider, account_id=0),  # QUIK - Портфель срочного рынка
    # 'КФ': Quik(code='КФ', name='QUIK - Фондовый рынок', provider=qp_provider, account_id=1),  # QUIK - Портфель фондового рынка
    # 'КВ': Quik(code='КВ', name='QUIK - Валютный рынок', provider=qp_provider, account_id=2),  # QUIK - Портфель валютного рынка
}
default_broker = brokers['АС']  # Брокер по умолчанию для выполнения технических операций
