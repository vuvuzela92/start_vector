"""SQL-запросы для unit-джоб.

Сейчас в этом модуле оставлен только запрос для сценария, который обновляет
в UNIT признак участия артикула в рекламе.
"""

query_adv_spend = """
SELECT a.article_id,
	sum(a.sum) AS adv_spend
FROM advert_stat a
WHERE a."date" = CURRENT_DATE - INTERVAL '1 days'
GROUP BY a.article_id
HAVING sum(a.sum) > 0;"""
