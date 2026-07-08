"""SQL-запросы для unit-джоб.

Для функциональности бывшего `competitors_prices.py` здесь важны два запроса:

1. `query_competitors_positions`
   Читает наблюдения по конкурентным слотам из ClickHouse. Дальше в Python эти
   данные превращаются в lookup-словарь вида `wild -> конкурент/цена`.

2. `query_our_article_prices`
   Читает последнюю известную "нашу цену после СПП" по каждому артикулу.
   Для этого на стороне ClickHouse используются агрегаты `argMax` и `max`
   по `processed_at`.
"""

query_adv_spend = """
SELECT a.article_id,
	sum(a.sum) AS adv_spend
FROM advert_stat a
WHERE a."date" = CURRENT_DATE - INTERVAL '1 days'
GROUP BY a.article_id
HAVING sum(a.sum) > 0;"""


# Источник для заполнения трёх конкурентных колонок в UNIT.
# Запрос возвращает "сырые" строки, а дедупликация по (`Конкурент`, `wild`)
# делается уже в Python, чтобы логика была явно видна в коде сервиса.
query_competitors_positions = """
SELECT
    pp.processed_at AS `дата`,
    pp.article_id AS `Наш артикул`,
    pp.found_article AS `Артикул конкурента`,
    pp.price AS `цена конкурента`,
    pp.wild AS `wild`,
    pp.position AS `Позиция в полках конкурента`,
    pp.concurrent AS `Конкурент`
FROM product_positions pp
WHERE pp.processed_at >= now()
  AND pp.price IS NOT NULL
  AND pp.concurrent IS NOT NULL
ORDER BY pp.processed_at;
"""


# Источник для колонки "Наша цена после СПП".
# На стороне ClickHouse сразу сворачиваем историю до одной последней записи
# на каждый наш артикул.
query_our_article_prices = """
SELECT
    max(pp.processed_at) AS `дата`,
    pp.article_id AS `Наш артикул`,
    argMax(pp.found_article, pp.processed_at) AS `Артикул конкурента`,
    argMax(pp.price, pp.processed_at) AS `цена конкурента`,
    argMax(pp.wild, pp.processed_at) AS `wild`,
    argMax(pp.position, pp.processed_at) AS `Позиция в полках конкурента`,
    argMax(pp.concurrent, pp.processed_at) AS `Конкурент`
FROM product_positions pp
WHERE pp.price IS NOT NULL
  AND pp.concurrent IS NOT NULL
GROUP BY pp.article_id;
"""
