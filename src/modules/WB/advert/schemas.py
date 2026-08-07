# Модуль описывает таблицы в БД
from sqlalchemy import String, DateTime, Numeric


# === vector_db ===

# --- Данные из раздела Документы на ВБ --- 
weekly_implementation_report_dict = {
    "weekly_implementation_report": {
        '№': String(255),
        'Наименование': String(255),
        'Документ основание': String(255),
        'Дата': DateTime(),
        '№ документа': Numeric(12,2),
        'Сумма, руб.': Numeric(12,2),
        'в т.ч НДС, руб.': Numeric(12,2),
        'account': String(255)
    }
}
