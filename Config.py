# Курс Мультиброкер: Контроль https://finlab.vip/wpm-category/mbcontrol/

from AlorPy import AlorPy  # Провайдер Алор
from FinLabPy.Brokers.Alor import Alor  # Брокер Алор

from FinamPy import FinamPy  # Провайдер Финам
from FinLabPy.Brokers.Finam import Finam  # Брокер Финам

from TinvestPy import TinvestPy  # Провайдер Т-Инвестиции
from FinLabPy.Brokers.Tinvest import Tinvest  # Брокер Т-Инвестиции

# from QuikPy import QuikPy  # Провайдер QUIK
# from FinLabPy.Brokers.Quik import Quik  # Брокер QUIK

# Провайдеры
ap_provider = AlorPy()  # Провайдер Алор. Для демо счета AlorPy(demo=True)
fp_provider = FinamPy()  # Провайдер Финам
tp_provider = TinvestPy()  # Провайдер Т-Инвестиции. Для демо счета TinkoffPy(demo=True). Курс Мультиброкер: Облигации https://finlab.vip/wpm-category/mbbonds/
# qp_provider = QuikPy()  # Провайдер QUIK

# Брокеры
storage = 'file'  # Файловое хранилище
# storage = 'db'  # Курс Базы данных для трейдеров https://finlab.vip/wpm-category/databases/
brokers = {
    'АФ': Alor(code='АФ', name='Алор - Фондовый рынок', provider=ap_provider, account_id=1, storage=storage),  # Алор - Портфель фондового рынка
    'АС': Alor(code='АС', name='Алор - Срочный рынок', provider=ap_provider, account_id=0, storage=storage),  # Алор - Портфель срочного рынка
    # 'АВ': Alor(code='АВ', name='Алор - Валютный рынок', provider=ap_provider, account_id=2, storage=storage),  # Алор - Портфель валютного рынка
    'ИФ': Alor(code='ИФ', name='Алор ИИС - Фондовый рынок', provider=ap_provider, account_id=4, storage=storage),  # Алор ИИС - Портфель фондового рынка
    # 'ИС': Alor(code='ИС', name='Алор ИИС - Срочный рынок', provider=ap_provider, account_id=3, storage=storage),  # Алор ИИС - Портфель срочного рынка
    # 'ИВ': Alor(code='ИВ', name='Алор ИИС - Валютный рынок', provider=ap_provider, account_id=5, storage=storage),  # Алор ИИС - Портфель валютного рынка
    'Ф': Finam(code='Ф', name='Финам', provider=fp_provider, storage=storage),  # Финам
    'Т': Tinvest(code='Т', name='Т-Инвестиции', provider=tp_provider, storage=storage),  # Т-Инвестиции
    # 'КФ': Quik(code='КФ', name='QUIK - Фондовый рынок', provider=qp_provider, account_id=0, storage=storage),  # QUIK - Портфель фондового рынка
    # 'КС': Quik(code='КС', name='QUIK - Срочный рынок', provider=qp_provider, account_id=1, storage=storage),  # QUIK - Портфель срочного рынка
    # 'КВ': Quik(code='КВ', name='QUIK - Валютный рынок', provider=qp_provider, account_id=2, storage=storage),  # QUIK - Портфель валютного рынка
}
default_broker = brokers['АФ']  # Брокер по умолчанию для выполнения технических операций
