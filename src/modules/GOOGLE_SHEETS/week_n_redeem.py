import pandas as pd
from sqlalchemy import text
from src.core.google_sheets_scheme import week_n_redeem
from src.core.utils_gspread import safe_open_spreadsheet
from src.core.utils_sql import get_db_engine
# from gspread.utils import rowcol_to_a1
from gspread_dataframe import set_with_dataframe 

def update_week_n_redeem():
	# Получаем подключение к базе данных
	engine = get_db_engine()
	# Выполняем SQL-запрос для получения данных по еженедельным отчетам реализации и уведомлениях о выпкупе
	query = text("""-- === Для бухгалтерии ===
WITH week_rep AS ( -- Данные еженедельного отчета реализации
	SELECT 
		-- Достаю данные по различным статьям еженедельного отчета в разрезе документа
		SUM(
		CASE 
			WHEN w.title = 'Итого стоимость реализованного товара и услуг'
				THEN COALESCE(sum_rub, 0)
		END) AS total_sum,
		ROUND(SUM(
		    CASE 
		        WHEN w.title = 'Итого стоимость реализованного товара и услуг'
		            THEN COALESCE(sum_rub, 0) / (1.0 + v.vat)  
		    END
		), 2) AS total_sum_without_vat,
			SUM(
		CASE 
			WHEN w.title IN ('Компенсация ущерба', 'Прочие выплаты')
				THEN COALESCE(sum_rub, 0)
		END) AS damages_comp_other,
			SUM(
		CASE 
			WHEN w.title = 'Компенсация скидки по программе лояльности'
				THEN COALESCE(sum_rub, 0)
		END) AS discount_loyalty,
		SUM(
		CASE 
			WHEN w.title IN ('Сумма вознаграждения Вайлдберриз за текущий период (ВВ), без НДС', 'НДС с вознаграждения Вайлдберриз')
				THEN COALESCE(sum_rub, 0)
		END) AS award,
		SUM(
		CASE 
			WHEN w.title IN ('Сумма, удержанная в счёт обеспечения организации платежа')
				THEN COALESCE(sum_rub, 0)
		END) AS amount_withheld_to_org,
		SUM(
		CASE 
			WHEN w.title IN ('Возмещение расходов по перевозке')
				THEN COALESCE(sum_rub, 0)
		END) AS reimbursement_of_transp_costs,
		SUM(
		CASE 
			WHEN w.title IN ('Возмещение за выдачу и возврат товаров на ПВЗ')
				THEN COALESCE(sum_rub, 0)
		END) AS reimbursement_for_delivery_and_return_of_goods_to_pvz,	
		SUM(
		CASE 
			WHEN w.title IN ('Штрафы')
				THEN COALESCE(sum_rub, 0)
		END) AS penalties,
		SUM(
		CASE 
			WHEN w.title IN ('Прочие удержания')
				THEN COALESCE(sum_rub, 0)
		END) other_deductions,	
		SUM(
		CASE 
			WHEN w.title IN ('Удержания в пользу третьих лиц')
				THEN COALESCE(sum_rub, 0)
		END) retentions_in_favor_of_third_parties,	
		-- Выделяю данные для группировки
		w.doc_num AS weekly_rep,
		DATE(DATE_TRUNC('week', w."date")) AS week_start,
		DATE(w."date") AS report_date,
		DATE((DATE_TRUNC('week', w."date") + INTERVAL '7 days' - INTERVAL '1 microsecond')) AS week_end,
		w.account
	FROM weekly_implementation_report w
	LEFT JOIN vat_guide v
		ON v.account = UPPER(w.account)
	GROUP BY w.doc_num, w."date", w.account),
	fin_rep AS -- Данные еженедельного финансового отчета
		(SELECT 
			f.realizationreport_id,
			f.date_from,
			f.date_to,
			f.create_dt,
			sum
			(CASE
				WHEN f.doc_type_name ILIKE '%возврат%'
				THEN (f.ppvz_for_pay)
				ELSE 0
			END) AS return_pay,
			f.account
		FROM fin_reports_full f
		WHERE f.report_type = 2
		GROUP BY f.realizationreport_id, f.date_from, f.date_to, f.create_dt, f.account),
	redeem_not AS( -- Данные уведомления о выкупе
		SELECT sum(sum_rub_with_vat) AS sum_rub_with_vat,
		-- Считаем в зависимости от ставки НДС
		ROUND(sum(sum_rub_with_vat)/(1.0 + v.vat), 2) AS sum_rub_without_vat,
		SUBSTRING(r.doc_name FROM '№(\d+)') AS redeem_notif, -- из полного названия документа извлекаю номер
		DATE(SUBSTRING(r.doc_name FROM ' от (\d{4}-\d{2}-\d{2})')) AS redeem_notif_date
	FROM redeem_notification r
	LEFT JOIN vat_guide v
		ON v.account = UPPER(r.account)
	GROUP BY r.doc_name, v.vat)
	SELECT 
		w.account,
		w.weekly_rep AS "Номер_еженедельного_отчета",
		w.week_start AS "Начало_периода",
		w.report_date AS "Конец_периода",
		w.total_sum AS "Всего_товара",
		w.total_sum_without_vat AS "Всего_товара_БЕЗ_НДС",
		w.damages_comp_other AS "Компенсации_ущерба_и_прочие_выплаты",
		w.discount_loyalty AS "Компенсация_скидки_по_программе_лояльности",
		r.redeem_notif AS "Уведомление_о_выкупе_№",
		r.sum_rub_with_vat AS "Выкуплено_по_уведомлению",
		r.sum_rub_without_vat AS "Выкуплено_по_уведомлению_без_НДС",
		f.return_pay AS "Вовзрат_выкупа",
		CASE 
			WHEN w.award < 0
			THEN w.award*-1
			ELSE 0 
		END AS "Вознагрождение_в_доход",
		CASE 
			WHEN w.award < 0
			-- Расчет в зависимости от налоговой ставки
				THEN ROUND((w.award*-1)/(1.0 + v.vat), 2)
			ELSE 0
		END AS "Вознагрождение_в_доход_БЕЗ_НДС",	
		w.award AS "Вознаграждение",
		COALESCE(amount_withheld_to_org, 0) AS "Сумма_удержанная_в_счёт_обеспечения_организации_платежа",
		reimbursement_of_transp_costs AS "Возмещение расходов по перевозке",
		reimbursement_for_delivery_and_return_of_goods_to_pvz AS "Возмещение_за_выдачу_и_возврат_товаров_на_ПВЗ", 
		penalties AS "Штрафы",
		other_deductions AS "Прочие удержания",
		retentions_in_favor_of_third_parties AS "Удержания_в_пользу_третьих_лиц"
	FROM week_rep w
	LEFT JOIN fin_rep f ON UPPER(w.account) = UPPER(f.account) 
		AND w.report_date = f.date_to
	LEFT JOIN redeem_not r
		ON f.realizationreport_id = r.redeem_notif::INT
	LEFT JOIN vat_guide v
		ON v.account = UPPER(w.account)
	WHERE f.date_to > '2025-12-31'
	ORDER BY w.account, w.week_start DESC;""")

	# Загружаем данные в DataFrame
	df = pd.read_sql(query, engine)
	df = df.rename(columns={
							"Всего_товара":             "Всего_стоимость_реализованного_товара",
							"Всего_товара_БЕЗ_НДС": "Всего_стоимость_реализованного_товара_без_НДС",
							"Компенсации_ущерба_и_прочие_выпла": "Компенсации_ущерба_и_прочие_выплаты",
							"Компенсация_скидки_по_программе_л": "Компенсация_скидки_по_программе_лояльности",
							"Возмещение_за_выдачу_и_возврат_тов": "Возмещение_за_выдачу_и_возврат_товара_ПВЗ",
							"Сумма_удержанная_в_счёт_обеспечен":"Сумма_удержанная_в_счёт_обеспечения_организации_платежа",
							"Возмещение_за_выдачу_и_возврат_тов":"Возмещение_за_выдачу_и_возврат_товаров_на_ПВЗ"
							})
	# Сортировка по неделям и аккаунтам
	df = df.sort_values(by=['Начало_периода', 'account'], ascending=[True, True])


	import numpy as np

	df['Итого_расходы'] = np.where(
    df['Вознаграждение'] < 0,
    df['Вовзрат_выкупа'] + 
    df['Возмещение расходов по перевозке'] +
    df['Возмещение_за_выдачу_и_возврат_товаров_на_ПВЗ'] + 
    df['Штрафы'] + 
    df['Прочие удержания'] + 
    df['Удержания_в_пользу_третьих_лиц'] + 
    df['Сумма_удержанная_в_счёт_обеспечения_организации_платежа'],
    # Иначе (ВОЗНАГРАЖДЕНИЕ >= 0):
    df['Вовзрат_выкупа'] + 
    df['Возмещение расходов по перевозке'] +
    df['Возмещение_за_выдачу_и_возврат_товаров_на_ПВЗ'] + 
    df['Штрафы'] + 
    df['Прочие удержания'] + 
    df['Удержания_в_пользу_третьих_лиц'] + 
    df['Сумма_удержанная_в_счёт_обеспечения_организации_платежа'] +
    df['Вознаграждение'],

)


	df['К_перечислению_по_отчетам'] = np.where(
    df['Вознаграждение'] >= 0,
    # Если вознаграждение >= 0
    df['Всего_стоимость_реализованного_товара'] +
    df['Компенсации_ущерба_и_прочие_выплаты'] + 
    df['Выкуплено_по_уведомлению'] +
    df['Вознаграждение'] - 
    df['Итого_расходы'],
    # Если вознаграждение < 0
    df['Всего_стоимость_реализованного_товара'] +
    df['Компенсации_ущерба_и_прочие_выплаты'] + 
    df['Выкуплено_по_уведомлению'] -
    df['Вознаграждение'] - 
    df['Итого_расходы']
	)

	# Открываем таблицу 
	report_table = safe_open_spreadsheet(week_n_redeem['title'])
	# Обновляем данные гугл таблицы
	report_sheet = report_table.worksheet(week_n_redeem['data'])
	set_with_dataframe(report_sheet, df)