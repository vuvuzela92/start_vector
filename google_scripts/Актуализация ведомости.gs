const CONFIG = {
  SOURCE_SHEET: 'октябрь 2026',

  PAYROLL_SHEETS: [
    {
      name: 'Октябрь ЗП Вектор',
      legalEntityCell: 'B13',
    },
    {
      name: 'Октябрь ЗП Старт',
      legalEntityCell: 'B14',
    },
  ],

  DIRECTORY_SHEET: 'Справочник',

  HEADER_ROW: 2,
  DATE_HEADER_ROW: 1,
  SOURCE_HEADER_ROW: 4,
  DATA_START_ROW: 4,

  FIO_COLUMN: 2,
  POSITION_COLUMN: 3,
  LEGAL_ENTITY_COLUMN: 4,
  GROUP_COLUMN: 5,

  FIRST_CALC_COLUMN: 6,

  TOTAL_PREFIX: 'ИТОГО ',

  GROUPS: [
    {
      title: 'ОФИС',
      sourceName: 'Офис',
      totalName: 'ИТОГО ОФИС',
    },
    {
      title: 'СКЛАД. ПЕРСОНАЛ',
      sourceName: 'СКЛАД. Персонал',
      totalName: 'ИТОГО СКЛАД. ПЕРСОНАЛ',
    },
    {
      title: 'СКЛАД. ВОДИТЕЛЬ',
      sourceName: 'Склад. Водитель',
      totalName: 'ИТОГО СКЛАД. ВОДИТЕЛЬ',
    },
    {
      title: 'СКЛАД. ОФИС',
      sourceName: 'Склад. Офис',
      totalName: 'ИТОГО СКЛАД. ОФИС',
    },
  ],

  TOTAL_HEADERS: [
    'ОКЛАД',
    'Премии постоянные',
    'Начислено 1 половина месяца',
    'Аванс',
    'Компенсация1',
    'Отпуск/больничный1',
    'Премия1',
    'Удержания/штраф1',
    'На карту первая половина мес',
    'На руки первая половина мес',
    'округление1',
    'Компенсация2',
    'Отпуск/больничный2',
    'Премия2',
    'Удержания/штраф2',
    'На карту вторая половина мес',
    'На руки вторая половина мес',
    'округление2',
    'Итого начисленно за месяц',
    'Итого выплаченно за месяц',
  ],
};


/**
 * Создаёт меню, через которое бухгалтер запускает актуализацию двух октябрьских ведомостей.
 */
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Актуализация ведомости')
    .addItem('Обновить ведомости', 'actualizePayrollSheets')
    .addToUi();
}


/**
 * Перестраивает октябрьские ведомости Вектора и Старта, восстанавливает формулы дней,
 * начислений и выплат, а также заново считает итоги групп. Для каждого юрлица берётся
 * отдельное значение из справочника; отсутствие ставки оставляет оклад пустым.
 */
function actualizePayrollSheets() {
  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  const lock = LockService.getDocumentLock();

  if (!lock.tryLock(30000)) {
    throw new Error('Не удалось получить блокировку таблицы.');
  }

  try {
    const sourceSheet = spreadsheet.getSheetByName(CONFIG.SOURCE_SHEET);
    const directorySheet = spreadsheet.getSheetByName(
      CONFIG.DIRECTORY_SHEET
    );

    if (!sourceSheet) {
      throw new Error(`Не найден лист «${CONFIG.SOURCE_SHEET}».`);
    }

    if (!directorySheet) {
      throw new Error(`Не найден лист «${CONFIG.DIRECTORY_SHEET}».`);
    }

    for (const payrollConfig of CONFIG.PAYROLL_SHEETS) {
      const payrollSheet = spreadsheet.getSheetByName(payrollConfig.name);

      if (!payrollSheet) {
        console.log(
          `Лист «${payrollConfig.name}» отсутствует. Обновление пропущено.`
        );
        continue;
      }

      const legalEntity = directorySheet
        .getRange(payrollConfig.legalEntityCell)
        .getDisplayValue()
        .trim();

      if (!legalEntity) {
        throw new Error(
          `Не заполнено значение ${payrollConfig.legalEntityCell} ` +
          `на листе «${CONFIG.DIRECTORY_SHEET}».`
        );
      }

      actualizePayrollSheet_(payrollSheet, sourceSheet, legalEntity);
    }

    SpreadsheetApp.flush();
    SpreadsheetApp.getUi().alert(
      'Актуализация завершена',
      'Октябрьские ведомости, формулы и итоги групп обновлены.',
      SpreadsheetApp.getUi().ButtonSet.OK
    );
  } finally {
    lock.releaseLock();
  }
}


/**
 * Перестраивает одну зарплатную ведомость для выбранного юрлица, сохраняя ручные
 * выплаты и комментарии значениями, а вычисляемые поля возвращая формулами.
 */
function actualizePayrollSheet_(sheet, sourceSheet, legalEntity) {
  const lastColumn = sheet.getLastColumn();
  const lastRow = sheet.getMaxRows();
  const savedData = saveExistingData_(sheet, lastColumn);
  const formulaTemplates = saveFormulaTemplates_(sheet, lastColumn);
  const sourceEmployees = readSourceEmployees_(sourceSheet, legalEntity);
  const outputRows = buildOutputRows_(sourceEmployees);
  const oldRowCount = lastRow - CONFIG.DATA_START_ROW + 1;

  sheet
    .getRange(CONFIG.DATA_START_ROW, 2, oldRowCount, lastColumn - 1)
    .clearContent();

  if (!outputRows.length) {
    return;
  }

  sheet
    .getRange(CONFIG.DATA_START_ROW, 2, outputRows.length, 4)
    .setValues(outputRows.map((row) => row.identity));

  const employeeRows = [];
  const totalRows = [];

  outputRows.forEach((row, index) => {
    const rowNumber = CONFIG.DATA_START_ROW + index;

    if (row.type === 'employee') {
      employeeRows.push(rowNumber);
    }

    if (row.type === 'total') {
      totalRows.push(rowNumber);
    }
  });

  restoreManualData_(sheet, outputRows, savedData);
  applyFormulaTemplates_(sheet, employeeRows, formulaTemplates);
  applyMissingDailyFormulas_(
    sheet,
    sourceSheet,
    employeeRows,
    formulaTemplates
  );
  applyCalculatedFormulaRules_(sheet, employeeRows);
  writeGroupTotals_(sheet, employeeRows, totalRows);

  sheet
    .getRange(CONFIG.DATA_START_ROW, 1, outputRows.length, lastColumn)
    .setFontWeight('normal');

  for (const totalRow of totalRows) {
    sheet.getRange(totalRow, 1, 1, lastColumn).setFontWeight('bold');
  }
}


/**
 * Сохраняет только значения без формул, чтобы ручные начисления и даты выплат
 * сотрудников пережили перестройку строк ведомости.
 */
function saveExistingData_(sheet, lastColumn) {
  const lastRow = sheet.getMaxRows();
  const rowCount = lastRow - CONFIG.DATA_START_ROW + 1;
  const range = sheet.getRange(
    CONFIG.DATA_START_ROW,
    2,
    rowCount,
    lastColumn - 1
  );
  const values = range.getValues();
  const formulas = range.getFormulas();
  const result = new Map();

  for (let index = 0; index < values.length; index += 1) {
    const row = values[index];
    const formulaRow = formulas[index];
    const fio = String(row[0] || '').trim();
    const legalEntity = String(row[2] || '').trim();

    if (!fio || !legalEntity || fio.startsWith(CONFIG.TOTAL_PREFIX)) {
      continue;
    }

    const key = makeEmployeeKey_(fio, legalEntity);
    const manualValues = {};

    for (
      let column = CONFIG.FIRST_CALC_COLUMN;
      column <= lastColumn;
      column += 1
    ) {
      const relativeIndex = column - 2;

      if (!formulaRow[relativeIndex]) {
        manualValues[column] = row[relativeIndex];
      }
    }

    result.set(key, manualValues);
  }

  return result;
}


/**
 * Собирает шаблон отдельно для каждого столбца из первой найденной формулы этого
 * столбца, чтобы пустая ячейка в первой строке не обнуляла расчёт всей ведомости.
 */
function saveFormulaTemplates_(sheet, lastColumn) {
  const rowCount = sheet.getMaxRows() - CONFIG.DATA_START_ROW + 1;
  const identities = sheet
    .getRange(CONFIG.DATA_START_ROW, 2, rowCount, 3)
    .getValues();
  const formulas = sheet
    .getRange(
      CONFIG.DATA_START_ROW,
      CONFIG.FIRST_CALC_COLUMN,
      rowCount,
      lastColumn - CONFIG.FIRST_CALC_COLUMN + 1
    )
    .getFormulasR1C1();
  const templates = Array(lastColumn - CONFIG.FIRST_CALC_COLUMN + 1).fill('');

  for (let rowIndex = 0; rowIndex < identities.length; rowIndex += 1) {
    const fio = String(identities[rowIndex][0] || '').trim();
    const legalEntity = String(identities[rowIndex][2] || '').trim();

    if (!fio || !legalEntity || fio.startsWith(CONFIG.TOTAL_PREFIX)) {
      continue;
    }

    for (let columnIndex = 0; columnIndex < templates.length; columnIndex += 1) {
      if (!templates[columnIndex] && formulas[rowIndex][columnIndex]) {
        templates[columnIndex] = formulas[rowIndex][columnIndex];
      }
    }

    if (templates.every(Boolean)) {
      break;
    }
  }

  return templates;
}


/**
 * Читает сотрудников исходного табеля только для юрлица, выбранного текущей
 * зарплатной ведомостью; строки без ФИО или без группы не создают сотрудников.
 */
function readSourceEmployees_(sourceSheet, legalEntity) {
  const lastRow = sourceSheet.getLastRow();

  if (lastRow < 5) {
    return [];
  }

  const values = sourceSheet.getRange(5, 1, lastRow - 4, 4).getValues();

  return values
    .filter((row) => {
      const fio = String(row[0] || '').trim();
      const rowLegalEntity = String(row[2] || '').trim();
      const group = String(row[3] || '').trim();

      return fio && rowLegalEntity === legalEntity && group;
    })
    .map((row) => ({
      fio: String(row[0] || '').trim(),
      position: row[1],
      legalEntity: String(row[2] || '').trim(),
      group: String(row[3] || '').trim(),
    }));
}


/**
 * Формирует для каждой заданной группы её заголовок, сотрудников и отдельную
 * строку итогов; состав сотрудников определяется текущими данными исходного листа.
 */
function buildOutputRows_(employees) {
  const rows = [];

  for (const group of CONFIG.GROUPS) {
    const groupEmployees = employees
      .filter((employee) =>
        normalizeHeader_(employee.group) ===
        normalizeHeader_(group.sourceName)
      )
      .sort((first, second) => first.fio.localeCompare(second.fio, 'ru'));

    rows.push({
      type: 'header',
      identity: [group.title, '', '', ''],
    });

    for (const employee of groupEmployees) {
      rows.push({
        type: 'employee',
        key: makeEmployeeKey_(employee.fio, employee.legalEntity),
        identity: [
          employee.fio,
          employee.position,
          employee.legalEntity,
          employee.group,
        ],
      });
    }

    rows.push({
      type: 'total',
      identity: [group.totalName, '', '', group.sourceName],
    });
  }

  return rows;
}


/**
 * Возвращает ранее сохранённые ручные выплаты и даты в строки тех же сотрудников,
 * даже если после сортировки или изменения группы номер строки изменился.
 */
function restoreManualData_(sheet, outputRows, savedData) {
  for (let index = 0; index < outputRows.length; index += 1) {
    const row = outputRows[index];

    if (row.type !== 'employee') {
      continue;
    }

    const manualValues = savedData.get(row.key);

    if (!manualValues) {
      continue;
    }

    for (const [column, value] of Object.entries(manualValues)) {
      sheet
        .getRange(CONFIG.DATA_START_ROW + index, Number(column))
        .setValue(value);
    }
  }
}


/**
 * Протягивает существующие формулы из шаблонов по отдельным столбцам на все
 * строки сотрудников ведомости.
 */
function applyFormulaTemplates_(sheet, employeeRows, formulaTemplates) {
  for (const rowNumber of employeeRows) {
    for (let index = 0; index < formulaTemplates.length; index += 1) {
      const formula = formulaTemplates[index];

      if (!formula) {
        continue;
      }

      sheet
        .getRange(rowNumber, CONFIG.FIRST_CALC_COLUMN + index)
        .setFormulaR1C1(formula);
    }
  }
}


/**
 * Восстанавливает формулы дневных часов, если формула потеряна во всём столбце.
 * День связывается с исходным сводом по дате в заголовке, а сотрудник — по точным
 * ФИО и юрлицу; условные обозначения отпуска и больничного остаются текстом.
 */
function applyMissingDailyFormulas_(
  sheet,
  sourceSheet,
  employeeRows,
  formulaTemplates
) {
  const lastColumn = sheet.getLastColumn();
  const headerWidth = lastColumn - CONFIG.FIRST_CALC_COLUMN + 1;
  const payrollDates = sheet
    .getRange(
      CONFIG.DATE_HEADER_ROW,
      CONFIG.FIRST_CALC_COLUMN,
      1,
      headerWidth
    )
    .getValues()[0];
  const sourceLastColumn = sourceSheet.getLastColumn();
  const sourceHeaders = sourceSheet
    .getRange(
      CONFIG.SOURCE_HEADER_ROW,
      1,
      1,
      sourceLastColumn
    )
    .getDisplayValues()[0];
  const sourceColumnsByDay = new Map();

  sourceHeaders.forEach((header, index) => {
    const match = String(header).match(/СВОД\s+день\s+(\d{2})\./i);

    if (match) {
      sourceColumnsByDay.set(Number(match[1]), index + 1);
    }
  });

  const quotedSourceName = sourceSheet.getName().replace(/'/g, "''");

  payrollDates.forEach((dateValue, index) => {
    if (formulaTemplates[index]) {
      return;
    }

    const day = dayNumberFromHeader_(dateValue);
    const sourceColumn = sourceColumnsByDay.get(day);

    if (!sourceColumn) {
      return;
    }

    const payrollColumn = CONFIG.FIRST_CALC_COLUMN + index;
    const sourceColumnLetter = columnToLetter_(sourceColumn);

    for (const rowNumber of employeeRows) {
      const formula =
        `=IFERROR(LET(значение_свода;INDEX('${quotedSourceName}'!` +
        `$${sourceColumnLetter}:$${sourceColumnLetter};` +
        `MATCH(1;('${quotedSourceName}'!$A:$A=$B${rowNumber})*` +
        `('${quotedSourceName}'!$C:$C=$D${rowNumber});0));` +
        `IF(ISNUMBER(значение_свода);ROUND(значение_свода;0);` +
        `значение_свода));"")`;

      sheet.getRange(rowNumber, payrollColumn).setFormula(formula);
    }
  });
}


/**
 * Преобразует числовой или датный заголовок зарплатного столбца в день месяца,
 * включая дату 31-го числа, которая в таблице может храниться серийным числом.
 */
function dayNumberFromHeader_(value) {
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.getDate();
  }

  const number = Number(value);

  if (!Number.isFinite(number)) {
    return 0;
  }

  if (Number.isInteger(number) && number >= 1 && number <= 31) {
    return number;
  }

  if (number > 31) {
    const date = new Date(Date.UTC(1899, 11, 30) + number * 86400000);
    return date.getUTCDate();
  }

  return 0;
}


/**
 * Восстанавливает расчётные поля по названиям заголовков. Все формулы начислений
 * проверяют оклад: если ставка не найдена, связанные суммы остаются пустыми.
 */
function applyCalculatedFormulaRules_(sheet, employeeRows) {
  const lastColumn = sheet.getLastColumn();
  const headers = sheet
    .getRange(CONFIG.HEADER_ROW, 1, 1, lastColumn)
    .getDisplayValues()[0];
  const columnByHeader = new Map();

  headers.forEach((header, index) => {
    const normalized = normalizeHeader_(header);

    if (normalized && !columnByHeader.has(normalized)) {
      columnByHeader.set(normalized, index + 1);
    }
  });

  const formulaRules = [
    {
      header: 'Начислено 1 половина месяца',
      build: (row) =>
        `=IF(${cellRef_(columnByHeader, 'ОКЛАД', row)}="";"";` +
        `${cellRef_(columnByHeader, 'Аванс', row)}+` +
        `${cellRef_(columnByHeader, 'Компенсация1', row)}+` +
        `${cellRef_(columnByHeader, 'Отпуск/больничный1', row)}+` +
        `${cellRef_(columnByHeader, 'Премия1', row)}-` +
        `${cellRef_(columnByHeader, 'Удержания/штраф1', row)})`,
    },
    {
      header: 'На руки первая половина мес',
      build: (row) =>
        `=IF(${cellRef_(columnByHeader, 'ОКЛАД', row)}="";"";` +
        `${cellRef_(columnByHeader, 'Начислено 1 половина месяца', row)}-` +
        `${cellRef_(columnByHeader, 'На карту первая половина мес', row)})`,
    },
    {
      header: 'округление1',
      build: (row) =>
        `=IF(${cellRef_(columnByHeader, 'На руки первая половина мес', row)}="";"";` +
        `ROUND(${cellRef_(columnByHeader, 'На руки первая половина мес', row)};-2))`,
    },
    {
      header: 'Начислено 2 половина месяца',
      build: (row) =>
        `=IF(${cellRef_(columnByHeader, 'ОКЛАД', row)}="";"";` +
        `${cellRef_(columnByHeader, 'Зарплата', row)}+` +
        `${cellRef_(columnByHeader, 'Компенсация2', row)}+` +
        `${cellRef_(columnByHeader, 'Отпуск/больничный2', row)}+` +
        `${cellRef_(columnByHeader, 'Премия2', row)}-` +
        `${cellRef_(columnByHeader, 'Удержания/штраф2', row)})`,
    },
    {
      header: 'На руки вторая половина мес',
      build: (row) =>
        `=IF(${cellRef_(columnByHeader, 'ОКЛАД', row)}="";"";` +
        `${cellRef_(columnByHeader, 'Начислено 2 половина месяца', row)}-` +
        `${cellRef_(columnByHeader, 'На карту вторая половина мес', row)})`,
    },
    {
      header: 'округление2',
      build: (row) =>
        `=IF(${cellRef_(columnByHeader, 'На руки вторая половина мес', row)}="";"";` +
        `ROUND(${cellRef_(columnByHeader, 'На руки вторая половина мес', row)};-2))`,
    },
    {
      header: 'Итого начисленно за месяц',
      build: (row) =>
        `=IF(${cellRef_(columnByHeader, 'ОКЛАД', row)}="";"";` +
        `${cellRef_(columnByHeader, 'Начислено 1 половина месяца', row)}+` +
        `${cellRef_(columnByHeader, 'Начислено 2 половина месяца', row)})`,
    },
    {
      header: 'Итого выплаченно за месяц',
      build: (row) =>
        `=IF(${cellRef_(columnByHeader, 'ОКЛАД', row)}="";"";` +
        `${cellRef_(columnByHeader, 'На карту первая половина мес', row)}+` +
        `${cellRef_(columnByHeader, 'округление1', row)}+` +
        `${cellRef_(columnByHeader, 'На карту вторая половина мес', row)}+` +
        `${cellRef_(columnByHeader, 'округление2', row)})`,
    },
    {
      header: 'Сверка',
      build: (row) =>
        `=IF(${cellRef_(columnByHeader, 'ОКЛАД', row)}="";"";` +
        `${cellRef_(columnByHeader, 'Итого начисленно за месяц', row)}-` +
        `${cellRef_(columnByHeader, 'Итого выплаченно за месяц', row)})`,
    },
  ];

  for (const rule of formulaRules) {
    const column = columnByHeader.get(normalizeHeader_(rule.header));

    if (!column) {
      continue;
    }

    for (const row of employeeRows) {
      sheet.getRange(row, column).setFormula(rule.build(row));
    }
  }
}


/**
 * Возвращает ссылку на ячейку нужного поля по заголовку; отсутствие обязательного
 * заголовка останавливает актуализацию, чтобы не записать суммы не в тот столбец.
 */
function cellRef_(columnByHeader, header, row) {
  const column = columnByHeader.get(normalizeHeader_(header));

  if (!column) {
    throw new Error(`Не найден обязательный заголовок «${header}».`);
  }

  return `${columnToLetter_(column)}${row}`;
}


/**
 * Создаёт суммы только по сотрудникам непосредственно над соответствующей
 * разделительной строкой; пустая группа не наследует диапазон предыдущей группы.
 */
function writeGroupTotals_(sheet, employeeRows, totalRows) {
  const employeeRowSet = new Set(employeeRows);
  const totalColumns = findTotalColumns_(sheet);

  for (const totalRow of totalRows) {
    let firstEmployeeRow = totalRow - 1;

    while (employeeRowSet.has(firstEmployeeRow)) {
      firstEmployeeRow -= 1;
    }

    firstEmployeeRow += 1;
    const lastEmployeeRow = totalRow - 1;

    if (firstEmployeeRow > lastEmployeeRow) {
      continue;
    }

    for (const column of totalColumns) {
      const columnLetter = columnToLetter_(column);
      sheet
        .getRange(totalRow, column)
        .setFormula(
          `=SUM(${columnLetter}${firstEmployeeRow}:` +
          `${columnLetter}${lastEmployeeRow})`
        );
    }
  }
}


/**
 * Находит разрешённые поля групповых итогов по заголовкам второй строки, не
 * создавая суммы в днях, служебных колонках и ручных полях, не входящих в список.
 */
function findTotalColumns_(sheet) {
  const lastColumn = sheet.getLastColumn();
  const headers = sheet
    .getRange(CONFIG.HEADER_ROW, 1, 1, lastColumn)
    .getDisplayValues()[0];
  const requiredHeaders = new Set(
    CONFIG.TOTAL_HEADERS.map(normalizeHeader_)
  );
  const columns = [];

  headers.forEach((header, index) => {
    if (requiredHeaders.has(normalizeHeader_(header))) {
      columns.push(index + 1);
    }
  });

  return columns;
}


/**
 * Формирует уникальный ключ сотрудника из нормализованных ФИО и юрлица, чтобы
 * ручные значения не переходили сотруднику с совпадающим именем из другой компании.
 */
function makeEmployeeKey_(fio, legalEntity) {
  return `${normalizeHeader_(fio)}|${normalizeHeader_(legalEntity)}`;
}


/**
 * Убирает лишние пробелы и различия регистра при сравнении названий групп,
 * заголовков, ФИО и юридических лиц.
 */
function normalizeHeader_(value) {
  return String(value || '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}


/**
 * Преобразует номер столбца Google Таблиц в буквенное обозначение для формул.
 */
function columnToLetter_(columnNumber) {
  let column = columnNumber;
  let result = '';

  while (column > 0) {
    const remainder = (column - 1) % 26;
    result = String.fromCharCode(65 + remainder) + result;
    column = Math.floor((column - 1) / 26);
  }

  return result;
}
