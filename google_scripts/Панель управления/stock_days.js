const IMPORT_DAY_OSTATOK_CONFIG = {
  sourceSpreadsheetId: '1JktihEfzUY_aOC0SQ9__Hr9D3JNa3xS8_mnnSYnAnVo',
  sourceSheetName: 'Расчет закупки',
  targetSheetName: 'статус дней по вилду',
  sourceHeaderRow: 2,
  sourceHeaders: {
    wild: 'wild',
    loadingLevel: 'Уровень загрузки (висячие, менее 7 дней, более 7 дней, более 1 мес)',
  },
  targetStatusCell: 'A1',
  targetDataStartRow: 3,
  targetDataStartColumn: 1,
};

/**
 * Импортирует статусы дней по wild из исходной таблицы в активную.
 * Колонки в источнике определяются по заголовкам, а не по буквам диапазонов.
 */
function importdayostatok() {
  Logger.log('Старт импорта статуса дней по wild.');

  try {
    const sourceSheet = SpreadsheetApp.openById(
      IMPORT_DAY_OSTATOK_CONFIG.sourceSpreadsheetId
    ).getSheetByName(IMPORT_DAY_OSTATOK_CONFIG.sourceSheetName);
    const targetSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(
      IMPORT_DAY_OSTATOK_CONFIG.targetSheetName
    );

    if (!sourceSheet) {
      throw new Error(
        'Не найден исходный лист "' +
          IMPORT_DAY_OSTATOK_CONFIG.sourceSheetName +
          '".'
      );
    }

    if (!targetSheet) {
      throw new Error(
        'Не найден целевой лист "' +
          IMPORT_DAY_OSTATOK_CONFIG.targetSheetName +
          '".'
      );
    }

    const sourceData = getImportDayOstatokSourceData_(sourceSheet);
    const preparedData = buildImportDayOstatokRows_(sourceData);

    writeImportDayOstatokStatus_(targetSheet);
    clearImportDayOstatokData_(targetSheet);

    if (preparedData.length > 0) {
      targetSheet
        .getRange(
          IMPORT_DAY_OSTATOK_CONFIG.targetDataStartRow,
          IMPORT_DAY_OSTATOK_CONFIG.targetDataStartColumn,
          preparedData.length,
          preparedData[0].length
        )
        .setValues(preparedData);
    }

    Logger.log(
      'Импорт завершен успешно. Перенесено строк: %s.',
      preparedData.length
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    Logger.log('Ошибка при выполнении importdayostatok: %s', message);
    throw error;
  }
}

/**
 * Возвращает все данные источника вместе с индексами нужных колонок.
 *
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sourceSheet
 * @return {{rows: Array<Array<*>>, wildColumnIndex: number, loadingLevelColumnIndex: number}}
 */
function getImportDayOstatokSourceData_(sourceSheet) {
  const lastRow = sourceSheet.getLastRow();
  const lastColumn = sourceSheet.getLastColumn();

  if (lastRow < IMPORT_DAY_OSTATOK_CONFIG.sourceHeaderRow) {
    throw new Error('В исходном листе нет строки заголовков.');
  }

  const headerValues = sourceSheet
    .getRange(
      IMPORT_DAY_OSTATOK_CONFIG.sourceHeaderRow,
      1,
      1,
      lastColumn
    )
    .getValues()[0];
  const headerMap = buildImportDayOstatokHeaderMap_(headerValues);

  const wildColumnIndex = getImportDayOstatokHeaderIndex_(
    headerMap,
    IMPORT_DAY_OSTATOK_CONFIG.sourceHeaders.wild
  );
  const loadingLevelColumnIndex = getImportDayOstatokHeaderIndex_(
    headerMap,
    IMPORT_DAY_OSTATOK_CONFIG.sourceHeaders.loadingLevel
  );

  if (lastRow === IMPORT_DAY_OSTATOK_CONFIG.sourceHeaderRow) {
    return {
      rows: [],
      wildColumnIndex,
      loadingLevelColumnIndex,
    };
  }

  const rows = sourceSheet
    .getRange(
      IMPORT_DAY_OSTATOK_CONFIG.sourceHeaderRow + 1,
      1,
      lastRow - IMPORT_DAY_OSTATOK_CONFIG.sourceHeaderRow,
      lastColumn
    )
    .getValues();

  return {
    rows,
    wildColumnIndex,
    loadingLevelColumnIndex,
  };
}

/**
 * Собирает итоговый массив из двух колонок и пропускает полностью пустые строки.
 *
 * @param {{rows: Array<Array<*>>, wildColumnIndex: number, loadingLevelColumnIndex: number}} sourceData
 * @return {Array<Array<*>>}
 */
function buildImportDayOstatokRows_(sourceData) {
  const result = [];

  for (let index = 0; index < sourceData.rows.length; index += 1) {
    const row = sourceData.rows[index];
    const wildValue = row[sourceData.wildColumnIndex] || '';
    const loadingLevelValue = row[sourceData.loadingLevelColumnIndex] || '';

    if (
      String(wildValue).trim() === '' &&
      String(loadingLevelValue).trim() === ''
    ) {
      continue;
    }

    result.push([wildValue, loadingLevelValue]);
  }

  return result;
}

/**
 * Строит map заголовков: нормализованное имя -> индекс колонки в массиве.
 *
 * @param {Array<*>} headers
 * @return {Object<string, number>}
 */
function buildImportDayOstatokHeaderMap_(headers) {
  /** @type {Object<string, number>} */
  const result = {};

  for (let index = 0; index < headers.length; index += 1) {
    const normalizedHeader = normalizeImportDayOstatokHeader_(headers[index]);

    if (normalizedHeader !== '') {
      result[normalizedHeader] = index;
    }
  }

  return result;
}

/**
 * Возвращает индекс колонки по имени заголовка.
 *
 * @param {Object<string, number>} headerMap
 * @param {string} headerName
 * @return {number}
 */
function getImportDayOstatokHeaderIndex_(headerMap, headerName) {
  const normalizedHeaderName = normalizeImportDayOstatokHeader_(headerName);

  if (!(normalizedHeaderName in headerMap)) {
    throw new Error(
      'Не найдена колонка с заголовком "' + headerName + '" в исходном листе.'
    );
  }

  return headerMap[normalizedHeaderName];
}

/**
 * Нормализует заголовок для надежного поиска.
 *
 * @param {*} value
 * @return {string}
 */
function normalizeImportDayOstatokHeader_(value) {
  return String(value).trim().toLowerCase();
}

/**
 * Записывает время последнего успешного обновления.
 *
 * @param {GoogleAppsScript.Spreadsheet.Sheet} targetSheet
 */
function writeImportDayOstatokStatus_(targetSheet) {
  const now = new Date();
  const dateTimeString = Utilities.formatDate(
    now,
    Session.getScriptTimeZone(),
    'dd.MM.yyyy HH:mm:ss'
  );

  targetSheet
    .getRange(IMPORT_DAY_OSTATOK_CONFIG.targetStatusCell)
    .setValue('Обновлено ' + dateTimeString);
}

/**
 * Очищает только область данных, не затрагивая ячейку статуса.
 *
 * @param {GoogleAppsScript.Spreadsheet.Sheet} targetSheet
 */
function clearImportDayOstatokData_(targetSheet) {
  const maxRows = targetSheet.getMaxRows();
  const maxColumns = Math.max(targetSheet.getMaxColumns(), 2);
  const rowsToClear =
    maxRows - IMPORT_DAY_OSTATOK_CONFIG.targetDataStartRow + 1;

  if (rowsToClear <= 0) {
    return;
  }

  targetSheet
    .getRange(
      IMPORT_DAY_OSTATOK_CONFIG.targetDataStartRow,
      IMPORT_DAY_OSTATOK_CONFIG.targetDataStartColumn,
      rowsToClear,
      maxColumns
    )
    .clearContent();
}
