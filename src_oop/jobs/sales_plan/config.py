from sqlalchemy import text

annual_procurement_plan = {
                        "title": "Годовой план закупа 2026",
                        "orders_sheet": "БД_ЗАКАЗЫ",
                        "unit_sheet": "Данные_Юнитки",
                        "supply_sheet": "Данные_Поставки",
                        "parfume_sheet": "Данные_Парфюм",
                        "quarter_sheet": "Поквартально"
                            }

funnel_query = text(
            """
            SELECT
        """
)