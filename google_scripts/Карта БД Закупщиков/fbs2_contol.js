/**
 * Добавляет над рабочей областью столько строк, сколько найдено строк с крайней указанной датой в колонке B.
 *
 * Бизнес-сценарий:
 * скрипт помогает быстро подготовить свободное место в таблице "Карта БД Закупщиков"
 * перед вставкой новых данных. Он ориентируется на лист "ФБС2 контроль отгрузок",
 * анализирует колонку B, начиная с 3-й строки, берёт дату из верхнего актуального блока
 * и вставляет над строкой 3 столько строк, сколько подряд записей относится именно к этой дате.
 */
function addRowsAboveByDatesInColumnB() {
  const spreadsheetId = '1md1hQgysVfh36KSkiqnLjO8SXVuJl5ZvnNEEvtKSAXQ';
  const sheetName = 'ФБС2 контроль отгрузок';
  const headerRow = 2;
  const startRow = 3;
  const targetColumn = 2;
  const defaultDayColor = '#fff2cc';
  const alternateDayColor = '#ffffff';

  const spreadsheet = SpreadsheetApp.openById(spreadsheetId);
  const sheet = spreadsheet.getSheetByName(sheetName);

  if (!sheet) {
    throw new Error(`Лист "${sheetName}" не найден в таблице.`);
  }

  const lastRow = sheet.getLastRow();

  if (lastRow < startRow) {
    Logger.log('В таблице нет строк для анализа, начиная с 3-й строки.');
    return;
  }

  const rowCount = lastRow - startRow + 1;
  const headerFormulas = sheet
    .getRange(headerRow, 1, 1, sheet.getLastColumn())
    .getFormulas()[0];
  const range = sheet.getRange(startRow, targetColumn, rowCount, 1);
  const values = range.getValues();
  const displayValues = range.getDisplayValues();
  const normalizedDates = values.map(([cellValue], index) =>
    normalizeDateValue(cellValue, displayValues[index][0])
  );

  const firstDate = normalizedDates.find((cellValue) => cellValue !== null);

  if (firstDate === undefined) {
    Logger.log('В колонке B, начиная с 3-й строки, не найдено ни одной корректной даты.');
    return;
  }

  const rowsWithFirstDateCount = countLeadingRowsWithDate(normalizedDates, firstDate);
  const lastColumn = sheet.getLastColumn();
  const currentTopRowColor = sheet.getRange(startRow, 1).getBackground();
  const sourceColumnAValues = sheet
    .getRange(startRow, 1, rowsWithFirstDateCount, 1)
    .getValues();
  const nextDateValues = buildRepeatedDateColumn(
    getNextDate(firstDate),
    rowsWithFirstDateCount
  );
  const newBlockColor = getAlternateDayColor(
    currentTopRowColor,
    defaultDayColor,
    alternateDayColor
  );

  sheet.insertRowsBefore(startRow, rowsWithFirstDateCount);
  sheet
    .getRange(startRow, 1, rowsWithFirstDateCount, lastColumn)
    .setBackground(newBlockColor);
  sheet
    .getRange(startRow, 1, rowsWithFirstDateCount, 1)
    .setValues(sourceColumnAValues);
  sheet
    .getRange(startRow, targetColumn, rowsWithFirstDateCount, 1)
    .setValues(nextDateValues)
    .setNumberFormat('@');
  applyFormulasToNewBlock(
    sheet,
    startRow,
    rowsWithFirstDateCount,
    lastColumn
  );
  restoreHeaderFormulas(sheet, headerRow, headerFormulas);
  Logger.log(
    'Над строкой %s добавлено %s строк по количеству подряд идущих записей с датой %s в колонке B. Новый блок окрашен в %s, значения из колонки A скопированы, в колонку B записана следующая дата, формулы перенесены только в те ячейки, где они были в исходном блоке, а формулы заголовков восстановлены для расчёта от 3-й строки.',
    startRow,
    rowsWithFirstDateCount,
    firstDate,
    newBlockColor
  );
}

/**
 * Восстанавливает формулы заголовков во 2-й строке после вставки новых строк.
 *
 * Бизнес-правило:
 * формулы в строке заголовков должны всегда считать полный диапазон данных, начиная
 * с 3-й строки. После вставки нового блока Google Sheets автоматически сдвигает
 * абсолютные ссылки вниз, поэтому формулы нужно вернуть к их исходному виду.
 *
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet Лист с рабочими данными.
 * @param {number} headerRow Номер строки с заголовками и формулами.
 * @param {string[]} headerFormulas Исходные формулы заголовков до вставки строк.
 * @returns {void}
 */
function restoreHeaderFormulas(sheet, headerRow, headerFormulas) {
  headerFormulas.forEach((formula, index) => {
    if (!formula) {
      return;
    }

    sheet.getRange(headerRow, index + 1).setFormula(formula);
  });
}

/**
 * Переносит формулы в новый блок только для тех ячеек, где формулы были в исходных строках.
 *
 * Бизнес-правило:
 * автоматически должны переноситься только расчётные ячейки, а поля без формул должны
 * оставаться пустыми, чтобы сотрудники могли заполнять их вручную без лишней очистки.
 *
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet Лист, в который вставлен новый блок.
 * @param {number} startRow Первая строка нового блока.
 * @param {number} rowCount Количество строк в новом блоке.
 * @param {number} lastColumn Последняя заполненная колонка листа.
 * @returns {void}
 */
function applyFormulasToNewBlock(sheet, startRow, rowCount, lastColumn) {
  const formulaStartColumn = 3;
  const formulaColumnCount = lastColumn - (formulaStartColumn - 1);

  if (formulaColumnCount <= 0) {
    return;
  }

  const sourceRange = sheet.getRange(
    startRow + rowCount,
    formulaStartColumn,
    rowCount,
    formulaColumnCount
  );
  const targetRange = sheet.getRange(startRow, formulaStartColumn, rowCount, formulaColumnCount);

  sourceRange.copyTo(targetRange, SpreadsheetApp.CopyPasteType.PASTE_FORMULA, false);
}

/**
 * Возвращает следующую календарную дату относительно даты верхнего блока.
 *
 * Бизнес-правило:
 * новый добавленный блок должен создаваться уже для следующего дня, чтобы сотруднику
 * не приходилось вручную менять дату после подготовки строк.
 *
 * @param {string} normalizedDate Дата в формате yyyy-MM-dd.
 * @returns {string} Следующая дата в формате dd.MM.yyyy для надёжной записи в таблицу.
 */
function getNextDate(normalizedDate) {
  const [year, month, day] = normalizedDate.split('-').map(Number);
  const nextDate = new Date(year, month - 1, day);

  nextDate.setDate(nextDate.getDate() + 1);
  return Utilities.formatDate(nextDate, Session.getScriptTimeZone(), 'dd.MM.yyyy');
}

/**
 * Строит колонку с повторяющимся значением даты для нового блока строк.
 *
 * Бизнес-правило:
 * все строки нового дня должны сразу получить одинаковую дату, чтобы блок был готов
 * к дальнейшему заполнению без ручных массовых правок.
 *
 * @param {string} dateValue Дата, которую нужно записать в новый блок.
 * @param {number} rowCount Количество строк в новом блоке.
 * @returns {string[][]} Двумерный массив для пакетной записи в колонку Google Sheets.
 */
function buildRepeatedDateColumn(dateValue, rowCount) {
  return Array.from({ length: rowCount }, () => [dateValue]);
}

/**
 * Возвращает цвет для нового дневного блока по правилу чередования.
 *
 * Бизнес-правило:
 * строки за соседние даты должны визуально отличаться, поэтому новый верхний блок
 * получает цвет, противоположный цвету текущего верхнего блока.
 *
 * @param {string} currentTopRowColor Текущий цвет верхней строки листа.
 * @param {string} defaultDayColor Базовый цвет дневного блока.
 * @param {string} alternateDayColor Альтернативный цвет дневного блока.
 * @returns {string} Цвет, который нужно применить к новому вставленному блоку.
 */
function getAlternateDayColor(currentTopRowColor, defaultDayColor, alternateDayColor) {
  const normalizedCurrentColor = currentTopRowColor.toLowerCase();
  const normalizedDefaultColor = defaultDayColor.toLowerCase();
  const normalizedAlternateColor = alternateDayColor.toLowerCase();

  if (normalizedCurrentColor === normalizedDefaultColor) {
    return alternateDayColor;
  }

  if (normalizedCurrentColor === normalizedAlternateColor) {
    return defaultDayColor;
  }

  return defaultDayColor;
}

/**
 * Считает длину верхнего непрерывного блока строк с одной и той же датой.
 *
 * Бизнес-правило:
 * для подготовки новых строк в начале листа нужно ориентироваться на текущий верхний блок
 * данных за одну дату. Как только встречается другая дата или пустое значение после начала
 * блока, подсчёт должен остановиться.
 *
 * @param {(string | null)[]} normalizedDates Нормализованные значения дат по колонке B.
 * @param {string} targetDate Дата верхнего блока, для которого нужен подсчёт.
 * @returns {number} Количество подряд идущих строк с целевой датой.
 */
function countLeadingRowsWithDate(normalizedDates, targetDate) {
  let count = 0;
  let blockStarted = false;

  for (const currentDate of normalizedDates) {
    if (currentDate === null) {
      if (blockStarted) {
        break;
      }

      continue;
    }

    if (!blockStarted) {
      if (currentDate !== targetDate) {
        continue;
      }

      blockStarted = true;
      count += 1;
      continue;
    }

    if (currentDate !== targetDate) {
      break;
    }

    count += 1;
  }

  return count;
}

/**
 * Приводит значение ячейки к единому виду даты для группировки по крайней указанной дате.
 *
 * Бизнес-правило:
 * в колонке B учитываются только те строки, где дату можно уверенно распознать.
 * Это защищает сценарий подготовки строк от ложного подсчёта по служебному тексту
 * и позволяет сравнивать даты между собой без влияния времени и формата отображения.
 *
 * @param {*} value Значение ячейки из Google Sheets.
 * @param {string} displayValue Отображаемое значение ячейки из Google Sheets.
 * @returns {string | null} Дата в формате yyyy-MM-dd или null, если дата не распознана.
 */
function normalizeDateValue(value, displayValue) {
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return Utilities.formatDate(value, Session.getScriptTimeZone(), 'yyyy-MM-dd');
  }

  if (typeof displayValue === 'string' && displayValue.trim() !== '') {
    const parsedDateFromDisplay = parseRussianDateString(displayValue);

    if (parsedDateFromDisplay !== null) {
      return parsedDateFromDisplay;
    }
  }

  return null;
}

/**
 * Разбирает дату из строкового значения в привычном для таблицы формате.
 *
 * Бизнес-правило:
 * если дата в Google Sheets отображается строкой вроде "26.08.2026" или
 * "26.08.2026 14:30:00", скрипт должен распознать её без зависимости от локали
 * среды выполнения, чтобы корректно посчитать количество строк для вставки.
 *
 * @param {string} value Отображаемое значение ячейки.
 * @returns {string | null} Дата в формате yyyy-MM-dd или null, если распознать дату не удалось.
 */
function parseRussianDateString(value) {
  const normalizedValue = value.trim();
  const match = normalizedValue.match(/^(\d{2})\.(\d{2})\.(\d{4})/);

  if (!match) {
    return null;
  }

  const [, day, month, year] = match;
  return `${year}-${month}-${day}`;
}
