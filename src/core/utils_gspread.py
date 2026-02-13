import re
import os
import time
import logging
import gspread
import requests
import pandas as pd
from datetime import datetime


from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[2]
CREDS_PATH = BASE_DIR / os.getenv("CREDS_DIR") / os.getenv("CREDS_FILE")

# CREDS_PATH = os.getenv('CREDS_PATH')

# -------------------------------- ПОДКЛЮЧЕНИЕ К ТАБЛИЦАМ --------------------------------

def init_client(creds_file_name = CREDS_PATH):
    '''Инициализирует аккаунт для работы с Google Sheets'''
    return gspread.service_account(filename=creds_file_name)

def get_table_by_url(table_url):
    '''Получение таблицы из Google Sheets по ссылке'''
    client = init_client()
    return client.open_by_url(table_url)

def get_table_by_id(client, table_url):
    '''Получение таблицы из Google Sheets по id'''
    return client.open_by_key(table_url)

def connect_to_local_sheet(table_url = None, sheet_name = None, table = None):
    if not table:
        table = get_table_by_url(table_url)
    sh = table.worksheet(sheet_name)
    return sh

def connect_to_remote_sheet(table_name, sheet_name, creds_file = CREDS_PATH):
    '''Подключение к таблице и листу'''    
    table = safe_open_spreadsheet(table_name, creds_file = creds_file)
    return table.worksheet(sheet_name)

def safe_open_spreadsheet(title, retries=5, delay=5, creds_file = CREDS_PATH):
    """
    Пытается открыть таблицу с повторными попытками при APIError 503.
    """
    gc = gspread.service_account(filename = creds_file)
    for attempt in range(1, retries + 1):
        # print(f"Попытка {attempt} открыть доступ к таблице")
        try:
            return gc.open(title)
        except gspread.exceptions.APIError as e:
            if "503" in str(e):
                print(f"[Попытка {attempt}/{retries}] APIError 503 — повтор через {delay} сек.")
                time.sleep(delay)
            else:
                raise  # если ошибка не 503 — пробрасываем дальше
    raise RuntimeError(f"Не удалось открыть таблицу '{title}' после {retries} попыток.")







# -------------------------------- ПОЛУЧЕНИЕ ДАННЫХ --------------------------------


def get_articles_and_clients_df(wild = False):
    '''
    Возвращает df с артикулами с указанием ЛК из таблицы UNIT
    '''

    # в перспективе можно поставить цикл, чтобы парсил любые колонки
    try: 
        sh = connect_to_remote_sheet('UNIT 2.0 (tested)', 'MAIN (tested)')
            
        if wild:
            df = pd.DataFrame({'Артикул':[int(x) for x in sh.col_values(1)[1:]], 'ЛК':[x.capitalize() for x in sh.col_values(2)[1:]], 'wild':sh.col_values(3)[1:]})
            # df = pd.DataFrame({'Артикул':[int(x) for x in sh.col_values(1)[1:]], 'ЛК':sh.col_values(2)[1:], 'wild':sh.col_values(3)[1:]})
        else:
            df = pd.DataFrame({'Артикул':[int(x) for x in sh.col_values(1)[1:]], 'ЛК':[x.capitalize() for x in sh.col_values(2)[1:]]})
    
    except Exception as e:
        print('Ошибка при попытке парсинга артикулов и ЛК из таблицы UNIT: {e}')
        raise
    
    clients_num = len(df['ЛК'].unique())
    if clients_num != 8:
        raise ValueError(f'Неправильное количество клиентов - в загруженных данных {clients_num} клиентов, проверьте таблицу')
    return df


def get_articles_and_clients_dict(filter_articles=None, sh = None):
    '''
    Возвращает словарь {Артикул: ЛК} из таблицы UNIT.
    Для тестов: можно передать артикулы в filter_articles, тогда вернёт значения только этих артикулов.
    '''
    try:
        if not sh:
            sh = connect_to_remote_sheet('UNIT 2.0 (tested)', 'MAIN (tested)')
        articles = [int(x) for x in sh.col_values(1)[1:]] 
        clients = sh.col_values(2)[1:]
        clients = [client.capitalize() for client in clients]                   
        result =  dict(zip(articles, clients))
        if filter_articles:
            result = {art: lk for art, lk in result.items() if art in filter_articles}
        return result
    except Exception as e:
        print(f'Ошибка при парсинге артикулов и ЛК из таблицы UNIT: {e}')
        raise


def get_articles_autopilot(sh = None, remote = False):
    '''
    Возвращает лист с артикулами в нужном порядке из таблицы Автопилот
    '''
    try:

        # если не передано подключение к таблице
        if not sh:
            if remote:
                sh = connect_to_remote_sheet('Панель управления продажами Вектор', 'Автопилот')
            else:
                sh = connect_to_local_sheet('https://docs.google.com/spreadsheets/d/1Cpxi7HbND5JuDz18FzDcm6Kdx5Ks8THf80cWt4hwFtc/edit?gid=1348704165#gid=1348704165', 'Автопилот')
        
        articles_raw = sh.col_values(1)[3:]
        
        try:
            articles = [int(n) for n in articles_raw]
        except Exception as e:
            for i, val in enumerate(articles_raw):
                if not val.strip():
                    logging.error(f"At index {i} the value is '{val}' - fix!")
            raise ValueError(f"Failed to parse articles: {e}")

    except Exception as e:
        logging.error(f'Ошибка при попытке парсинга артикулов из таблицы Автопилот:\n{e}')
        raise

    return articles



def get_data_offset(url, headers, extract_callback=lambda x: x, limit = 1000, return_keys = None, other_params=None):
    '''
    Функция, позволяющая получать все данные по одному клиенту, если стоит лимит на кол-во получаемых записей.
    
    Параметры:
    extract_callback - lambda-функция для получения чистого ответа при вложенности json.
    return_key - если указан, возвращает только значения этого ключа из каждого элемента
    '''

    all_data = []
    offset = 0

    while True:
        params = {
            'limit': limit,
            'offset': offset}
        if other_params:
            params.update(other_params)

        response = requests.get(url, params=params, headers=headers, timeout=30).json()
        batch = extract_callback(response)
        all_data.extend(batch)

        if len(batch) < limit:
            break
        offset += limit

    if return_keys:
        if isinstance(return_keys, str):
            all_data = [d[return_keys] for d in all_data]
        else:
            all_data = [{k: d[k] for k in return_keys} for d in all_data]

    return all_data


def get_purchase_price(sh = None):
    '''
    Возвращает словарь с ценами закупки в формате {wild : цена закупки}
    '''
    if not sh:
        sh = connect_to_remote_sheet('UNIT 2.0 (tested)', 'Сопост')
    article_cost_range = define_range(target_header_name = 'wild',
                                      all_headers = list(sh.row_values(1)),
                                      number_of_columns = 2,
                                      values_first_row = 2,
                                      sh_len = sh.row_count)
    clean_prices = {row[0]: clean_number(row[1]) for row in sh.get_values(article_cost_range)}
    return clean_prices


def get_skus_unit(unit_sh = None):
    '''
    Возвращает отсортированный и отформатированный список артикулов из UNIT
    '''
    if not unit_sh:
        unit_sh = connect_to_remote_sheet('UNIT 2.0 (tested)', 'MAIN (tested)')
    
    try:
        unit_skus = unit_sh.col_values(1)[1:]
        unit_skus = [int(i) for i in unit_skus]
    except gspread.exceptions.APIError as e:
        # Проверяем, что это именно лимит
        if e.response.status_code == 429:
            print("Google Sheets API: превышен лимит чтения. Ждём 60 секунд...")
            time.sleep(60)
            # Повторяем запрос ОДИН раз (или в цикле)
            unit_skus = unit_sh.col_values(1)[1:]
            unit_skus = [int(i) for i in unit_skus]
        else:
            # Все остальные ошибки — пробрасываем дальше
            raise

    return unit_skus


def find_duplicates_gs(sh, col_num=None, col_name=None, col_letter=None, col_values = None, header_row_num=1, start_row=0, return_all=False):
    """
    Находит дубликаты в столбце по номеру, букве или названию.
    Возвращает {row_index: value} для дублирующихся значений.
    """
    if col_num is None and col_name is None and col_letter is None and col_values is None:
        raise ValueError("Нужно передать один из аргументов: col_num, col_name, col_letter или col_values")
    
    if not col_values:
        if not col_num:
            if col_letter:
                col_num = col_letter_to_num(col_letter)
            else:
                header = sh.row_values(header_row_num)
                if col_name not in header:
                    raise ValueError(f"Столбец '{col_name}' не найден в строке {header_row_num}")
                col_num = header.index(col_name) + 1
        col_values = sh.col_values(col_num)

    return find_duplicates(col_values, start_row=start_row, return_all=return_all)

def get_col_index(sh, col_name, header_row=1, zero_based=False, header=None):
    """
    Возвращает номер столбца по имени заголовка.
    
    :param sh: лист Google Sheets
    :param col_name: имя столбца
    :param header_row: строка с заголовками (по умолчанию 1)
    :param zero_based: если True — возвращает индекс с 0, иначе с 1
    :return: номер столбца (int)
    :raises ValueError: если столбец не найден
    """
    if not header:
        header = sh.row_values(header_row)
    try:
        index = header.index(col_name)
        return index if zero_based else index + 1
    except ValueError:
        raise ValueError(f"Столбец '{col_name}' не найден в строке {header_row}")





# -------------------------------- ДОБАВЛЕНИЕ ДАННЫХ  --------------------------------


def add_data_to_range(sheet, data, sh_range, clean_range = True, headers = False):
    '''
    Обновляет данные в заданном диапазоне.
    Тип data: df, list
    '''

    # добавить проверку на размер данных?
    
    # сохраняем исходные данные
    backup_data = sheet.get(sh_range, value_render_option="FORMULA")
    
    try:
        
        if clean_range:
            # удаление старых записей
            sheet.batch_clear([sh_range])

        # добавление полученных данных
        if hasattr(data, 'values'):
            # если df
            data = my_pandas.process_decimal(data)
            data_to_insert = data.values.tolist()
            if headers:
                col_names = data.columns.tolist()
                data_to_insert = [col_names] + data_to_insert
        else:
            # если список
            data_to_insert = data
            
        sheet.update(data_to_insert, sh_range)
    
    except Exception as e:
        sheet.batch_clear([sh_range])
        sheet.update(backup_data, sh_range,  value_input_option="USER_ENTERED")
        print(f'Ошибка при работе с Google Sheets. Прежние данные восстановлены. \n{e}')
        raise



def add_data_to_google_sheet(sheet, data, take_headers_from_google_sheet = True):
    '''
    Обновляет данные во всей таблице
    '''
    try: 
        # сохраняем исходные данные
        backup_data = sheet.get_all_values(value_render_option="FORMULA")

        if take_headers_from_google_sheet == True:
            # берём названия колонок
            headers = sheet.row_values(row = 1)
        else: 
            headers = list(data.columns)

        # удаление старых записей
        sheet.clear()

        # добавление полученных данных
        data = my_pandas.process_decimal(data)
        data_to_insert = data.values.tolist()
        sheet.update([headers], 'A1')
        sheet.update(data_to_insert, 'A2')
    
    except Exception as e:
        sheet.clear()
        sheet.update(backup_data, 'A1',  value_input_option="USER_ENTERED")
        print(f'Ошибка при работе с Google Sheets. Прежние данные восстановлены. \n{e}')
        raise




# -------------------------------- УДАЛЕНИЕ ДАННЫХ --------------------------------

def clean_extra_rows(sh, inserted_data, sheet_name="Sheet", logger=None):
    """
    Cleans rows in the worksheet below the inserted data.
    
    sh: gspread worksheet object
    inserted_data: list of lists that was just inserted
    sheet_name: optional, for logging
    """
    # Number of rows inserted
    n_inserted = len(inserted_data)
    # Total number of rows in the sheet
    total_rows = sh.row_count

    # If there are extra rows below inserted data, clear them
    if total_rows > n_inserted:
        range_to_clear = f"A{n_inserted+1}:Z{total_rows}"  # Adjust Z if your sheet has more columns
        sh.batch_clear([range_to_clear])
        n_cleared = total_rows - n_inserted
    else:
        n_cleared = 0

    if logger is not None:
        logger.info(f"Cleaned {n_cleared} rows in {sheet_name}")



def delete_rows_by_index(sh, row_indices, trash_sheet=None, dont_delete = False):
    """
    Удаляет строки по индексам.
    При trash_sheet — сохраняет данные с именем таблицы, листа и временем.
    При dont_delete=True — только копирует в корзину, не удаляя.
    """
    if not row_indices:
        return

    all_rows = sh.get_all_values()
    deleted_rows = [all_rows[i - 1].copy() for i in sorted(row_indices)]  # копируем, чтобы не сломать

    if trash_sheet:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        spreadsheet_name = sh.spreadsheet.title  # имя таблицы
        worksheet_name = sh.title          # имя листа

        # Подготавливаем строки: [table, sheet, timestamp] + данные
        for row in deleted_rows:
            row[:0] = [spreadsheet_name, worksheet_name, now]  # вставляем в начало

        # Добавляем данные в конец
        trash_sheet.append_rows(deleted_rows)

    # Удаляем строки (снизу вверх)
    if not dont_delete:
        for idx in sorted(row_indices, reverse=True):
            sh.delete_rows(idx)
            logging.info(f'Deleted row {idx}')
            

def delete_rows_based_on_values(sh, values_to_delete, col_num, transform_to_str = True, trash_sheet = None):
    '''
    Удаляет строки из таблицы sh, основываясь на значениях values_to_delete из столбца col_num.
    Принимает gs таблицу, значения для удаления и номер столбца.
    Перед чеком преобразует values_to_delete в строки.

    Аргументы:
    sh - gs sheet
    values_to_delete - list of target values which have to be deleted
    col_num - number of column in sh to delete values from (starting with 1, not 0)
    transform_to_str - если нужно преобразовать values_to_delete в str (False на случай, если данные до этого преобразуются в строки)
    '''
    if values_to_delete is None:
        raise ValueError("values_to_delete can't be None") 
    elif not isinstance(values_to_delete, (list, tuple)):
        values_to_delete = [values_to_delete]

    col_values = sh.col_values(col_num)

    if transform_to_str:
        values_str = [str(value) for value in values_to_delete]

    rows_to_delete = [
        i + 1 for i, value in enumerate(col_values)
        if value in values_str
    ]

    if not rows_to_delete:
        logging.info(f"{sh.title}: Duplicates aren't found, no rows to delete.")
        return

    # сортируем в обратном порядке, чтобы избежать смещения строк
    rows_to_delete.sort(reverse=True)
    rows_num = len(rows_to_delete)
    logging.info(f'Found {rows_num} rows to delete')
    print(rows_to_delete)

    try:
        c = 1
        for row_num in rows_to_delete:
            sh.delete_rows(row_num)
            logging.info(f'Deleted row {row_num}: proccessed {c}/{rows_num} rows')

            # trash_sheet.update()
            c += 1
        logging.info(f"Successfully deleted {len(rows_to_delete)} rows: {sorted(rows_to_delete, reverse=False)}")
    except Exception as e:
        logging.error(f"Error during deletion: {e}")


def remove_duplicates_by_val(sh, values_to_exclude, col_values_to_delete_from=None, col_num_to_delete_from=None, trash_sheet = None):
    """
    Удаляет строки, где значение в указанной колонке присутствует в values_to_exclude.
    Оставляет первое вхождение не обязательно — удаляет все совпадения.
    
    :param sh: gspread Worksheet
    :param values_to_exclude: список значений для удаления (например, уже существующие артикулы)
    :param col_values_to_delete_from: список значений из колонки (например, sh.col_values(1))
    :param col_num_to_delete_from: номер колонки, если col_values_to_delete_from не передан
    :param return_values: вернуть удалённые значения
    :param trash_sheet: лист для архивации удалённых строк
    :return: словарь {row_index: value} удалённых, если return_values=True
    """
    # Получаем значения колонки, если не переданы
    if col_values_to_delete_from is None:
        if col_num_to_delete_from:
            col_values_to_delete_from = sh.col_values(col_num_to_delete_from)
        else:
            logging.error('At least one of the arguments should be given: col_values_to_delete_from или col_num_to_delete_from')
            return

    # Находим индексы строк (1-индексированные), где значение есть в values_to_exclude
    duplicates = {i + 1: item for i, item in enumerate(col_values_to_delete_from) if item in values_to_exclude}
    idxs = list(duplicates.keys())

    if idxs:
        logging.info(f"{len(idxs)} duplicates of the provided values are found: {duplicates}")
        delete_rows_by_index(sh=sh, row_indices=idxs, trash_sheet=trash_sheet)
    else:
        logging.info(f"{sh.title}: the provided values aren't found in the column")

    return duplicates
    

def find_duplicates_by_val_and_warn(sh, values_to_exclude, col_values_to_delete_from=None, col_num_to_delete_from=None, raise_absent_error = False):
    """
    Удаляет строки, где значение в указанной колонке присутствует в values_to_exclude.
    Оставляет первое вхождение не обязательно — удаляет все совпадения.
    
    :param sh: gspread Worksheet
    :param values_to_exclude: список значений для удаления (например, уже существующие артикулы)
    :param col_values_to_delete_from: список значений из колонки (например, sh.col_values(1))
    :param col_num_to_delete_from: номер колонки, если col_values_to_delete_from не передан
    :param return_values: вернуть удалённые значения
    :param trash_sheet: лист для архивации удалённых строк
    :return: словарь {row_index: value} удалённых, если return_values=True
    """
    # Получаем значения колонки, если не переданы
    if col_values_to_delete_from is None:
        if col_num_to_delete_from:
            col_values_to_delete_from = sh.col_values(col_num_to_delete_from)
        else:
            logging.error('Необходимо передать один из аргументов: col_values_to_delete_from или col_num_to_delete_from')
            return

    # Находим индексы строк (1-индексированные), где значение есть в values_to_exclude
    duplicates = {i + 1: item for i, item in enumerate(col_values_to_delete_from) if item in values_to_exclude}
    idxs = list(duplicates.keys())

    if idxs:
        logging.error(f"{len(idxs)} duplicates of the provided values are found: {duplicates}")
        if raise_absent_error:
            raise ValueError(f'The provided values are found in {sh.title}:\n{duplicates}\n. Delete the duplicates to continue')
    else:
        logging.info(f"{sh.title}: the provided values aren't found in the column")

    return duplicates


def remove_duplicates_from_col(sh, col_num=None, col_name=None, col_letter=None, col_values = None, header_row_num=1, start_row=2, trash_sheet=None, dont_delete=False):
    """
    Удаляет дубликаты по столбцу (оставляет первое вхождение).
    Сохраняет удалённые строки в trash_sheet, если указан.
    При dont_delete=True — только копирует, не удаляя.
    """
    # Находим дубликаты (все кроме первого вхождения)
    duplicates = find_duplicates_gs(
        sh=sh,
        col_num=col_num,
        col_name=col_name,
        col_letter=col_letter,
        col_values = col_values,
        header_row_num=header_row_num,
        start_row=start_row,
        return_all=False
    )
    
    if duplicates:
        logging.info(f"{sh.title}: {len(duplicates)} duplicates are found: {duplicates}. Deleting...")
        delete_rows_by_index(
            sh=sh,
            row_indices=duplicates.keys(),
            trash_sheet=trash_sheet,
            dont_delete=dont_delete
        )
        logging.info(f"{sh.title}: {len(duplicates)} rows are deleted")
    else:
        logging.info(f"{sh.title}: Duplicates not found")





# -------------------------------- ДИАПАЗОНЫ: ПОИСК, КОНВЕРТИРОВАНИЕ --------------------------------


# находит колонку через num_col колонок
def calculate_range_end(range_start, num_col):
    '''
    Принимает начальную колонку (например, 'A') и количество колонок (num_col),
    возвращает конечную колонку диапазона.

    Пример:
        calculate_range_end('Z', 3) → 'AB'  (Z + 3 колонки = Z,AA,AB)
    '''

    col = 0
    for c in range_start:
        col = col * 26 + (ord(c.upper()) - ord('A') + 1)
    
    end_col = col + num_col - 1
    range_end = ''
    while end_col > 0:
        end_col -= 1
        range_end = chr(ord('A') + end_col % 26) + range_end
        end_col //= 26
    
    return range_end


# конвертирует номер колонки (int) в буквенное представление гугл таблицы
def column_number_to_letter(col_num):
    """
    Конвертирует номер колонки в её буквенное представление в гугл-таблице (e.g., 1 → 'A', 28 → 'AB')
    """
    if col_num < 0:
        raise ValueError("Column number must be ≥ 0")
    col_letter = ''
    while col_num >= 0:
        col_letter = chr(ord('A') + (col_num % 26)) + col_letter
        col_num = (col_num // 26) - 1
    return col_letter


def col_letter_to_num(letter):
    """Convert Excel-style column letter to number (e.g., 'A' -> 1, 'AB' -> 28)."""
    num = 0
    for c in letter.upper():
        num = num * 26 + ord(c) - ord('A') + 1
    return num


def define_range(target_header_name, all_headers, number_of_columns, values_first_row, sh_len, all_col = True):
    """
    Определяет диапазон ячеек в Google Sheets для указанного заголовка.
    Возвращает строку диапазона в формате 'A1:B10' для использования в gspread.
    При all_col = True возвращает весь диапазон (от первой до последней колонки),
    при all_col = False возвращает только последнюю колонку.
    """
    if target_header_name in all_headers:
        # находит номер столбца в заголовках
        column_num = all_headers.index(target_header_name)

        # переводит в буквенное представление
        range_start = column_number_to_letter(column_num)

        # считает окончание диапазона
        range_end = calculate_range_end(range_start, number_of_columns)
        
        if not all_col:
            range_start = range_end

        # форматирование для gspread
        full_range = f'{range_start}{values_first_row}:{range_end}{sh_len}'

    else:
        print(f'{target_header_name} Не найдена в заданном диапазоне')
        raise ValueError
    
    return full_range


def find_gscol_num_by_name(col_name, sh, headers_col = 1, headers = None, **kwargs):
    '''
    Находит номер колонки по её названию в строке заголовков.
    
    Параметры:
    col_name - название колонки, как в гугл таблице
    sh - объект листа Google Sheets (worksheet)
    headers_col - номер строки с заголовками (по умолчанию 1)
    **kwargs - доп. параметры для метода row_values()

    Возвращает:
    int - номер колонки
    '''
    if not headers:
        headers = sh.row_values(headers_col, **kwargs)
    if col_name in headers:
        column_num = headers.index(col_name) + 1
        return column_num
    else:
        print('Колонка не найдена в заголовках.')


def col_values_by_name(col_name, sh, headers_col = 1, offset = 0, **kwargs):
    '''
    Получает значения колонки по её названию.
    
    Параметры:
    col_name - название колонки, как в гугл таблице
    sh - объект листа Google Sheets (worksheet)
    headers_col - номер строки с заголовками (по умолчанию 1)
    offset - смещение от найденной колонки  (по умолчанию 0) -- не помню, зачем добавляла --
    **kwargs: Дополнительные параметры для методов
    
    Возвращает:
    list - значения из колонки/колонок
    '''
    col_num = find_gscol_num_by_name(col_name, sh, headers_col, **kwargs)
    if offset:
        col_num += offset
    return sh.col_values(col_num)





# -------------------------------- ФОРМАТИРОВАНИЕ --------------------------------


def format_headers(sheet, data_len):
    sheet.format("1:1", {"textFormat": {"bold": True}})
    sheet.format(f"2:{data_len+1}", {"textFormat": {"bold": False}})


def clean_number(value):
    '''
    Функция для очистки форматирования тысячных гугл таблицы
    '''
    if isinstance(value, str):
        cleaned = re.sub(r'[^\d]', '', value)
        return int(cleaned) if cleaned else 0
    return value

def clean_float_number(value):
    if not isinstance(value, str):
        return value

    # replace non-breaking spaces with normal spaces
    value = value.replace("\xa0", " ")

    # remove spaces
    value = value.replace(" ", "")

    # replace comma with dot for float
    value = value.replace(",", ".")

    # keep digits and dot only
    cleaned = re.sub(r"[^0-9.]", "", value)

    # convert
    try:
        return float(cleaned)
    except ValueError:
        return 0