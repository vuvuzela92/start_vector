const CONFIG = {
  SALARY_DIRECTORY_SHEET: 'Справочник окладов офис',
  SUPPLEMENTAL_ROLE_COLUMN: 12,
  SUPPLEMENTAL_ENTITY_COLUMN: 13,
  SUPPLEMENTAL_GROUP_COLUMN: 14,

  PAYROLL_SHEETS: [
    {
      name: 'Октябрь ЗП Вектор',
      sourceSheet: 'октябрь 2026 new',
      legalEntityCell: 'B13',
    },
    {
      name: 'Октябрь ЗП Старт',
      sourceSheet: 'октябрь 2026 new',
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
    'Зачёт аванса',
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
 * Перестраивает октябрьские ведомости Вектора и Старта по актуальному листу табеля,
 * объединяет дополнительные назначения с основной строкой сотрудника и восстанавливает
 * формулы дней, выплат и групповых итогов. Ручные выплаты сохраняются.
 */
function actualizePayrollSheets() {
  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  const lock = LockService.getDocumentLock();

  if (!lock.tryLock(30000)) {
    throw new Error('Не удалось получить блокировку таблицы.');
  }

  try {
    const directorySheet = spreadsheet.getSheetByName(
      CONFIG.DIRECTORY_SHEET
    );
    const salaryDirectorySheet = spreadsheet.getSheetByName(
      CONFIG.SALARY_DIRECTORY_SHEET
    );

    if (!directorySheet) {
      throw new Error(`Не найден лист «${CONFIG.DIRECTORY_SHEET}».`);
    }

    if (!salaryDirectorySheet) {
      throw new Error(`Не найден лист «${CONFIG.SALARY_DIRECTORY_SHEET}».`);
    }

    for (const payrollConfig of CONFIG.PAYROLL_SHEETS) {
      const payrollSheet = spreadsheet.getSheetByName(payrollConfig.name);
      const sourceSheet = spreadsheet.getSheetByName(
        payrollConfig.sourceSheet
      );

      if (!payrollSheet) {
        console.log(
          `Лист «${payrollConfig.name}» отсутствует. Обновление пропущено.`
        );
        continue;
      }

      if (!sourceSheet) {
        throw new Error(`Не найден лист «${payrollConfig.sourceSheet}».`);
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

      const supplementalAssignments = readSupplementalAssignments_(
        salaryDirectorySheet,
        legalEntity
      );
      actualizePayrollSheet_(
        payrollSheet,
        sourceSheet,
        legalEntity,
        supplementalAssignments
      );
    }

    SpreadsheetApp.flush();
    SpreadsheetApp.getUi().alert(
      'Актуализация завершена',
      'Октябрьские ведомости, дополнительные назначения и итоги групп обновлены.',
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
function actualizePayrollSheet_(
  sheet,
  sourceSheet,
  legalEntity,
  supplementalAssignments
) {
  const lastColumn = sheet.getLastColumn();
  const lastRow = sheet.getMaxRows();
  const savedData = saveExistingData_(
    sheet,
    lastColumn,
    supplementalAssignments
  );
  const formulaTemplates = saveFormulaTemplates_(sheet, lastColumn);
  const sourceEmployees = readSourceEmployees_(
    sourceSheet,
    legalEntity,
    supplementalAssignments
  );
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
  writeEmployeeNumbers_(sheet, outputRows);

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
  applyHourlySalaryFormulaRules_(
    sheet,
    sourceSheet,
    employeeRows,
    supplementalAssignments
  );
  applyCalculatedFormulaRules_(sheet, employeeRows);
  applyBaseSalaryFormulaRules_(sheet, employeeRows);
  applySupplementalFormulaRules_(
    sheet,
    sourceSheet,
    employeeRows,
    supplementalAssignments
  );
  writeGroupTotals_(sheet, employeeRows, totalRows);

  sheet
    .getRange(CONFIG.DATA_START_ROW, 1, outputRows.length, lastColumn)
    .setFontWeight('normal');

  for (const totalRow of totalRows) {
    sheet.getRange(totalRow, 1, 1, lastColumn).setFontWeight('bold');
  }
}


/**
 * Нумерует сотрудников после перестройки ведомости; разделители групп и строки
 * итогов остаются без номера, а дополнительные назначения входят в строку сотрудника.
 */
function writeEmployeeNumbers_(sheet, outputRows) {
  let employeeNumber = 0;
  const numbers = outputRows.map((row) => {
    if (row.type === 'employee') {
      employeeNumber += 1;
      return [employeeNumber];
    }
    return [''];
  });

  sheet
    .getRange(
      CONFIG.DATA_START_ROW,
      1,
      sheet.getMaxRows() - CONFIG.DATA_START_ROW + 1,
      1
    )
    .clearContent();

  sheet
    .getRange(CONFIG.DATA_START_ROW, 1, numbers.length, 1)
    .setValues(numbers);
}


/**
 * Сохраняет только значения без формул, чтобы ручные начисления и даты выплат
 * сотрудников пережили перестройку строк ведомости.
 */
function saveExistingData_(sheet, lastColumn, supplementalAssignments) {
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
  const employeeCounts = new Map();
  const supplementalKeys = new Set(
    supplementalAssignments.map((assignment) =>
      makeEmployeeKey_(
        assignment.fio,
        assignment.legalEntity,
        assignment.designation
      )
    )
  );

  for (const row of values) {
    const fio = String(row[0] || '').trim();
    const position = String(row[1] || '').trim();
    const legalEntity = String(row[2] || '').trim();

    if (!fio || !legalEntity || fio.startsWith(CONFIG.TOTAL_PREFIX)) {
      continue;
    }

    if (supplementalKeys.has(makeEmployeeKey_(fio, legalEntity, position))) {
      continue;
    }

    const legacyKey = makeLegacyEmployeeKey_(fio, legalEntity);
    employeeCounts.set(legacyKey, (employeeCounts.get(legacyKey) || 0) + 1);
  }

  for (let index = 0; index < values.length; index += 1) {
    const row = values[index];
    const formulaRow = formulas[index];
    const fio = String(row[0] || '').trim();
    const position = String(row[1] || '').trim();
    const legalEntity = String(row[2] || '').trim();

    if (!fio || !legalEntity || fio.startsWith(CONFIG.TOTAL_PREFIX)) {
      continue;
    }

    const key = makeEmployeeKey_(fio, legalEntity, position);
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

    const legacyKey = makeLegacyEmployeeKey_(fio, legalEntity);
    if (
      !supplementalKeys.has(key) &&
      employeeCounts.get(legacyKey) === 1
    ) {
      result.set(legacyKey, manualValues);
    }
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
 * Читает основных сотрудников выбранного юрлица из табеля. Строки дополнительных
 * назначений из справочника исключаются, чтобы зарплатный скрипт не создавал для
 * совмещения вторую строку поверх уже формируемой строки назначения.
 */
function readSourceEmployees_(sourceSheet, legalEntity, supplementalAssignments) {
  const lastRow = sourceSheet.getLastRow();

  if (lastRow < 5) {
    return [];
  }

  const values = sourceSheet.getRange(5, 1, lastRow - 4, 4).getValues();

  const supplementalKeys = new Set(
    supplementalAssignments.map((assignment) =>
      makeEmployeeKey_(
        assignment.fio,
        assignment.legalEntity,
        assignment.designation
      )
    )
  );

  return values
    .filter((row) => {
      const fio = String(row[0] || '').trim();
      const group = String(row[1] || '').trim();
      const rowLegalEntity = String(row[2] || '').trim();
      const designation = String(row[3] || '').trim();

      return (
        fio &&
        rowLegalEntity === legalEntity &&
        group &&
        !supplementalKeys.has(
          makeEmployeeKey_(fio, rowLegalEntity, designation)
        )
      );
    })
    .map((row) => ({
      fio: String(row[0] || '').trim(),
      position: row[3],
      legalEntity: String(row[2] || '').trim(),
      group: String(row[1] || '').trim(),
    }));
}


/**
 * Читает дополнительные должности из справочника окладов только для текущего
 * юрлица. Запись считается отдельной работой, когда заполнены назначение,
 * ФИО, месячный оклад и группа; даты действия проверяются формулами ведомости.
 */
function readSupplementalAssignments_(salarySheet, legalEntity) {
  const lastRow = salarySheet.getLastRow();

  if (lastRow < 3) {
    return [];
  }

  const rows = salarySheet.getRange(3, 1, lastRow - 2, 14).getValues();
  const assignmentsByKey = new Map();

  for (const row of rows) {
    const fio = String(row[0] || '').trim();
    const monthlySalary = Number(row[1]);
    const designation = String(
      row[CONFIG.SUPPLEMENTAL_ROLE_COLUMN - 1] || ''
    ).trim();
    const assignmentEntity = String(
      row[CONFIG.SUPPLEMENTAL_ENTITY_COLUMN - 1] || ''
    ).trim();
    const group = String(
      row[CONFIG.SUPPLEMENTAL_GROUP_COLUMN - 1] || ''
    ).trim();

    if (
      !fio ||
      !designation ||
      !group ||
      assignmentEntity !== legalEntity ||
      !Number.isFinite(monthlySalary) ||
      monthlySalary <= 0
    ) {
      continue;
    }

    const key = makeEmployeeKey_(fio, assignmentEntity, designation);
    assignmentsByKey.set(key, {
      fio,
      monthlySalary,
      startDate: row[2],
      endDate: row[3],
      designation,
      legalEntity: assignmentEntity,
      group,
    });
  }

  return [...assignmentsByKey.values()];
}


/**
 * Формирует для каждой заданной группы заголовок, основных сотрудников и итоги.
 * Дополнительные назначения не создают строк: их начисления прибавляются к строке
 * основного сотрудника, чтобы бухгалтерия видела одну итоговую выплату.
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

    const groupRows = [
      ...groupEmployees.map((employee) => ({
        type: 'employee',
        key: makeEmployeeKey_(
          employee.fio,
          employee.legalEntity,
          employee.position
        ),
        identity: [
          employee.fio,
          employee.position,
          employee.legalEntity,
          employee.group,
        ],
      })),
    ].sort((first, second) => {
      const fioOrder = first.identity[0].localeCompare(
        second.identity[0],
        'ru'
      );
      return fioOrder || String(first.identity[1] || '').localeCompare(
        String(second.identity[1] || ''),
        'ru'
      );
    });

    for (const row of groupRows) {
      rows.push(row);
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

    const manualValues =
      savedData.get(row.key) ||
      (row.type === 'employee'
        ? savedData.get(
          makeLegacyEmployeeKey_(row.identity[0], row.identity[2])
        )
        : null);

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
 * Восстанавливает денежные поля основной строки: офисные начисления требуют
 * оклад из справочника, а складские — суммы за половины месяца из исходного табеля.
 */
function applyCalculatedFormulaRules_(sheet, employeeRows) {
  const columnByHeader = getColumnMap_(sheet);
  const formulaRules = [
    {
      header: 'Начислено 1 половина месяца',
      build: (row) => {
        const hourlyGroup = hourlyGroupCondition_(row, columnByHeader);
        const hasPayrollBasis = payrollBasisCondition_(row, columnByHeader);
        const amount =
          `${cellRef_(columnByHeader, 'Аванс', row)}+` +
          `${cellRef_(columnByHeader, 'Компенсация1', row)}+` +
          `${cellRef_(columnByHeader, 'Отпуск/больничный1', row)}+` +
          `${cellRef_(columnByHeader, 'Премия1', row)}-` +
          `${cellRef_(columnByHeader, 'Удержания/штраф1', row)}`;

        return `=IF($B${row}="";"";IF(${hasPayrollBasis};${amount};""))`;
      },
    },
    {
      header: 'На руки первая половина мес',
      build: (row) =>
        `=IF(${payrollBasisCondition_(row, columnByHeader)};` +
        `${cellRef_(columnByHeader, 'Начислено 1 половина месяца', row)}-` +
        `${cellRef_(columnByHeader, 'На карту первая половина мес', row)};"")`,
    },
    {
      header: 'округление1',
      build: (row) =>
        `=IF(${cellRef_(columnByHeader, 'На руки первая половина мес', row)}="";"";` +
        `ROUND(${cellRef_(columnByHeader, 'На руки первая половина мес', row)};-2))`,
    },
    {
      header: 'Начислено 2 половина месяца',
      build: (row) => {
        const amount =
          `${cellRef_(columnByHeader, 'Зарплата', row)}+` +
          `${cellRef_(columnByHeader, 'Компенсация2', row)}+` +
          `${cellRef_(columnByHeader, 'Отпуск/больничный2', row)}+` +
          `${cellRef_(columnByHeader, 'Премия2', row)}-` +
          `${cellRef_(columnByHeader, 'Удержания/штраф2', row)}`;

        return `=IF(${payrollBasisCondition_(row, columnByHeader)};${amount};"")`;
      },
    },
    {
      header: 'На руки вторая половина мес',
      build: (row) =>
        `=IF(${payrollBasisCondition_(row, columnByHeader)};` +
        `${cellRef_(columnByHeader, 'Начислено 2 половина месяца', row)}-` +
        `${cellRef_(columnByHeader, 'На карту вторая половина мес', row)}-` +
        `N(${cellRef_(columnByHeader, 'Зачёт аванса', row)});"")`,
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
        `=IF(${payrollBasisCondition_(row, columnByHeader)};` +
        `${cellRef_(columnByHeader, 'Начислено 1 половина месяца', row)}+` +
        `${cellRef_(columnByHeader, 'Начислено 2 половина месяца', row)};"")`,
    },
    {
      header: 'Итого выплаченно за месяц',
      build: (row) =>
        `=IF(${payrollBasisCondition_(row, columnByHeader)};` +
        `${cellRef_(columnByHeader, 'На карту первая половина мес', row)}+` +
        `${cellRef_(columnByHeader, 'округление1', row)}+` +
        `${cellRef_(columnByHeader, 'На карту вторая половина мес', row)}+` +
        `${cellRef_(columnByHeader, 'округление2', row)};"")`,
    },
    {
      header: 'Сверка',
      build: (row) =>
        `=IF(${payrollBasisCondition_(row, columnByHeader)};` +
        `${cellRef_(columnByHeader, 'Итого начисленно за месяц', row)}-` +
        `${cellRef_(columnByHeader, 'Итого выплаченно за месяц', row)};"")`,
    },
  ];

  for (const rule of formulaRules) {
    for (const row of employeeRows) {
      setFormulaByHeader_(sheet, columnByHeader, rule.header, row, rule.build(row));
    }
  }
}


/**
 * Подключает начисления складских групп из итогов табеля: офис склада получает
 * базовые суммы двух периодов по ФИО, юрлицу и группе, а персонал и водители —
 * почасовые суммы с учётом дополнительных назначений.
 */
function applyHourlySalaryFormulaRules_(
  sheet,
  sourceSheet,
  employeeRows,
  supplementalAssignments
) {
  if (!employeeRows.length) {
    return;
  }

  const columnByHeader = getColumnMap_(sheet);
  const sourceLastColumn = sourceSheet.getLastColumn();
  const sourceHeaders = sourceSheet
    .getRange(CONFIG.SOURCE_HEADER_ROW, 1, 1, sourceLastColumn)
    .getDisplayValues()[0];
  const firstHalfIndex = sourceHeaders.indexOf('Начислено 15Д') + 1;
  const secondHalfIndex = sourceHeaders.indexOf('Начислено 2-я половина') + 1;

  if (!firstHalfIndex || !secondHalfIndex) {
    throw new Error('В исходном табеле не найдены итоговые начисления двух половин месяца.');
  }

  const sourceName = sourceSheet.getName().replace(/'/g, "''");
  const lastSourceColumn = columnToLetter_(sourceLastColumn);
  const sourceRange = `'${sourceName}'!$A:$${lastSourceColumn}`;
  const sourceFio = `'${sourceName}'!$A:$A`;
  const sourceLegalEntity = `'${sourceName}'!$C:$C`;
  const sourceHeadersRange = `'${sourceName}'!$${CONFIG.SOURCE_HEADER_ROW}:$${CONFIG.SOURCE_HEADER_ROW}`;
  const salarySheet = `'${CONFIG.SALARY_DIRECTORY_SHEET.replace(/'/g, "''")}'`;
  const salaryFields = {
    fio: `${salarySheet}!$A$3:$A`,
    amount: `${salarySheet}!$B$3:$B`,
    start: `${salarySheet}!$C$3:$C`,
    end: `${salarySheet}!$D$3:$D`,
    designation: `${salarySheet}!$L$3:$L`,
    legalEntity: `${salarySheet}!$M$3:$M`,
    group: `${salarySheet}!$N$3:$N`,
  };
  const groupColumn = columnByHeader.get(normalizeHeader_('Группа'));
  const firstEmployeeRow = Math.min(...employeeRows);
  const lastEmployeeRow = Math.max(...employeeRows);
  const groupValues = sheet
    .getRange(firstEmployeeRow, groupColumn, lastEmployeeRow - firstEmployeeRow + 1, 1)
    .getDisplayValues()
    .flat();

  for (const row of employeeRows) {
    const group = normalizeHeader_(groupValues[row - firstEmployeeRow]);
    if (group === normalizeHeader_('Склад. Офис')) {
      const rowMatch =
        `MATCH(1;(${sourceFio}=$B${row})*` +
        `(${sourceLegalEntity}=$D${row})*` +
        `('${sourceName}'!$B:$B=$E${row});0)`;
      const firstHalfFormula =
        `=IFERROR(INDEX(${sourceRange};${rowMatch};` +
        `MATCH("Начислено 15Д";${sourceHeadersRange};0));0)`;
      const secondHalfFormula =
        `=IFERROR(INDEX(${sourceRange};${rowMatch};` +
        `MATCH("Начислено 2-я половина";${sourceHeadersRange};0));0)`;

      setFormulaByHeader_(sheet, columnByHeader, 'Аванс', row, firstHalfFormula);
      setFormulaByHeader_(sheet, columnByHeader, 'Зарплата', row, secondHalfFormula);
      continue;
    }

    if (
      group !== normalizeHeader_('СКЛАД. Персонал') &&
      group !== normalizeHeader_('Склад. Водитель')
    ) {
      continue;
    }

    const rowMatch =
      `MATCH(1;(${sourceFio}=$B${row})*` +
      `(${sourceLegalEntity}=$D${row});0)`;
    const firstHalfFormula =
      `=IFERROR(INDEX(${sourceRange};${rowMatch};` +
      `MATCH("Начислено 15Д";${sourceHeadersRange};0));0)`;
    const secondHalfFormula =
      `=IFERROR(INDEX(${sourceRange};${rowMatch};` +
      `MATCH("Начислено 2-я половина";${sourceHeadersRange};0));0)`;

    const rowIdentity = sheet.getRange(row, 2, 1, 4).getDisplayValues()[0];
    const matchingAssignments = supplementalAssignments.filter(
      (assignment) =>
        normalizeHeader_(assignment.fio) === normalizeHeader_(rowIdentity[0]) &&
        normalizeHeader_(assignment.legalEntity) === normalizeHeader_(rowIdentity[2])
    );
    const assignmentCriteria =
      `(${salaryFields.fio}=$B${row})*` +
      `(${salaryFields.designation}<>"")*` +
      `(${salaryFields.legalEntity}=$D${row})*` +
      `(${salaryFields.group}=$E${row})`;

    let combinedFirstHalfFormula = firstHalfFormula;
    let combinedSecondHalfFormula = secondHalfFormula;

    if (matchingAssignments.length) {
      const fixedAdvance =
        `SUMPRODUCT(${assignmentCriteria}*` +
        `(${salaryFields.start}<=EOMONTH($F$1;0))*` +
        `((${salaryFields.end}="")+(${salaryFields.end}>=$F$1))*` +
        `${salaryFields.amount}/2)`;
      const secondHalfSupplement = buildCalendarAccrualFormula_(
        salaryFields,
        assignmentCriteria,
        `$AN${row}`,
        '$V$1',
        '$AK$1'
      ).slice(1);

      combinedFirstHalfFormula =
        `=IFERROR(INDEX(${sourceRange};${rowMatch};` +
        `MATCH("Начислено 15Д";${sourceHeadersRange};0));0)+` +
        `IFERROR(${fixedAdvance};0)`;
      combinedSecondHalfFormula =
        `=IFERROR(INDEX(${sourceRange};${rowMatch};` +
        `MATCH("Начислено 2-я половина";${sourceHeadersRange};0));0)+` +
        `IFERROR(${secondHalfSupplement};0)`;
    }

    setFormulaByHeader_(
      sheet,
      columnByHeader,
      'Аванс',
      row,
      combinedFirstHalfFormula
    );
    setFormulaByHeader_(
      sheet,
      columnByHeader,
      'Зарплата',
      row,
      combinedSecondHalfFormula
    );
  }
}


/**
 * Проверяет, относится ли строка к складу, где дневные начисления рассчитываются
 * по почасовой ставке и уже собраны отдельно в исходном табеле.
 */
function hourlyGroupCondition_(row, columnByHeader) {
  const group = cellRef_(columnByHeader, 'Группа', row);
  return `OR(${group}="СКЛАД. Персонал";${group}="Склад. Водитель")`;
}


/**
 * Разрешает денежные итоги строки, если найден офисный оклад либо сотрудник
 * относится к одной из складских групп с начислением по часовой ставке.
 */
function payrollBasisCondition_(row, columnByHeader) {
  const salary = cellRef_(columnByHeader, 'ОКЛАД', row);
  const hourlyGroup = hourlyGroupCondition_(row, columnByHeader);
  return `OR(${salary}<>"";${hourlyGroup})`;
}


/**
 * Считает месячный оклад основной должности и активных дополнительных назначений
 * одной суммой в строке сотрудника, чтобы итоговая ведомость содержала одну выплату.
 */
function applyBaseSalaryFormulaRules_(sheet, employeeRows) {
  const salarySheetName = CONFIG.SALARY_DIRECTORY_SHEET.replace(/'/g, "''");
  const salaryRange = `'${salarySheetName}'`;
  const columnByHeader = getColumnMap_(sheet);

  for (const row of employeeRows) {
    const formula =
      `=IF($B${row}="";"";LET(` +
      `фио;$B${row};` +
      `юрлицо;$D${row};` +
      `группа;$E${row};` +
      `начало_месяца;$F$1;` +
      `конец_месяца;EOMONTH($F$1;0);` +
      `основная_ставка;SUMPRODUCT(` +
      `(${salaryRange}!$A$3:$A=фио)*` +
      `(${salaryRange}!$C$3:$C<=конец_месяца)*` +
      `((${salaryRange}!$D$3:$D="")+(${salaryRange}!$D$3:$D>=начало_месяца))*` +
      `(${salaryRange}!$L$3:$L="")*` +
      `${salaryRange}!$B$3:$B` +
      `);` +
      `дополнительные_ставки;SUMPRODUCT(` +
      `(${salaryRange}!$A$3:$A=фио)*` +
      `(${salaryRange}!$M$3:$M=юрлицо)*` +
      `(${salaryRange}!$N$3:$N=группа)*` +
      `(${salaryRange}!$L$3:$L<>"")*` +
      `(${salaryRange}!$C$3:$C<=конец_месяца)*` +
      `((${salaryRange}!$D$3:$D="")+(${salaryRange}!$D$3:$D>=начало_месяца))*` +
      `${salaryRange}!$B$3:$B` +
      `);` +
      `итоговый_оклад;основная_ставка+дополнительные_ставки;` +
      `IF(итоговый_оклад=0;"";итоговый_оклад)))`;

    setFormulaByHeader_(sheet, columnByHeader, 'ОКЛАД', row, formula);
  }
}


/**
 * Добавляет начисления дополнительных должностей к основной зарплатной строке:
 * фиксированный аванс равен половине месячной ставки, начисления делятся по
 * календарным дням, а разница аванса зачитывается во второй половине месяца.
 */
function applySupplementalFormulaRules_(
  sheet,
  sourceSheet,
  employeeRows,
  supplementalAssignments
) {
  if (!employeeRows.length || !supplementalAssignments.length) {
    return;
  }

  const columnByHeader = getColumnMap_(sheet);
  const salarySheet = `'${CONFIG.SALARY_DIRECTORY_SHEET.replace(/'/g, "''")}'`;
  const salaryFields = {
    fio: `${salarySheet}!$A$3:$A`,
    amount: `${salarySheet}!$B$3:$B`,
    start: `${salarySheet}!$C$3:$C`,
    end: `${salarySheet}!$D$3:$D`,
    designation: `${salarySheet}!$L$3:$L`,
    legalEntity: `${salarySheet}!$M$3:$M`,
    group: `${salarySheet}!$N$3:$N`,
  };
  const sourceName = sourceSheet.getName().replace(/'/g, "''");
  const sourceLastColumn = columnToLetter_(sourceSheet.getLastColumn());
  const sourceRange = `'${sourceName}'!$A:$${sourceLastColumn}`;
  const sourceFio = `'${sourceName}'!$A:$A`;
  const sourceLegalEntity = `'${sourceName}'!$C:$C`;
  const sourceHeadersRange = `'${sourceName}'!$${CONFIG.SOURCE_HEADER_ROW}:$${CONFIG.SOURCE_HEADER_ROW}`;

  for (const row of employeeRows) {
    const [fioValue, , legalEntityValue, groupValue] = sheet
      .getRange(row, 2, 1, 4)
      .getDisplayValues()[0];
    const hasSupplement = supplementalAssignments.some((assignment) =>
      normalizeHeader_(assignment.fio) === normalizeHeader_(fioValue) &&
      normalizeHeader_(assignment.legalEntity) === normalizeHeader_(legalEntityValue) &&
      normalizeHeader_(assignment.group) === normalizeHeader_(groupValue)
    );

    if (!hasSupplement) {
      continue;
    }

    const sourceMatch =
      `MATCH(1;(${sourceFio}=$B${row})*` +
      `(${sourceLegalEntity}=$D${row});0)`;
    const baseFirstHalf =
      `IFERROR(INDEX(${sourceRange};${sourceMatch};` +
      `MATCH("Начислено 15Д";${sourceHeadersRange};0));0)`;
    const assignmentCriteria =
      `(${salaryFields.fio}=$B${row})*` +
      `(${salaryFields.designation}<>"")*` +
      `(${salaryFields.legalEntity}=$D${row})*` +
      `(${salaryFields.group}=$E${row})`;
    const supplementalAccrual = buildCalendarAccrualFormula_(
      salaryFields,
      assignmentCriteria,
      `$AN${row}`,
      '$F$1',
      '$T$1'
    ).slice(1);
    const fixedAdvance =
      `SUMPRODUCT(${assignmentCriteria}*` +
      `(${salaryFields.start}<=EOMONTH($F$1;0))*` +
      `((${salaryFields.end}="")+(${salaryFields.end}>=$F$1))*` +
      `${salaryFields.amount}/2)`;
    const advance = cellRef_(columnByHeader, 'Аванс', row);
    const compensation = cellRef_(columnByHeader, 'Компенсация1', row);
    const leave = cellRef_(columnByHeader, 'Отпуск/больничный1', row);
    const bonus = cellRef_(columnByHeader, 'Премия1', row);
    const deduction = cellRef_(columnByHeader, 'Удержания/штраф1', row);
    const card = cellRef_(columnByHeader, 'На карту первая половина мес', row);
    const firstHalfAccrual = cellRef_(
      columnByHeader,
      'Начислено 1 половина месяца',
      row
    );
    const firstHalfCash = cellRef_(
      columnByHeader,
      'На руки первая половина мес',
      row
    );
    const advanceOffset = cellRef_(columnByHeader, 'Зачёт аванса', row);

    setFormulaByHeader_(
      sheet,
      columnByHeader,
      'Начислено 1 половина месяца',
      row,
      `=IF($B${row}="";"";${baseFirstHalf}+` +
      `IFERROR(${supplementalAccrual};0)+N(${compensation})+N(${leave})+` +
      `N(${bonus})-N(${deduction}))`
    );
    setFormulaByHeader_(
      sheet,
      columnByHeader,
      'На руки первая половина мес',
      row,
      `=IF($B${row}="";"";${advance}+N(${compensation})+N(${leave})+` +
      `N(${bonus})-N(${deduction})-N(${card}))`
    );
    setFormulaByHeader_(
      sheet,
      columnByHeader,
      'Зачёт аванса',
      row,
      `=IF($B${row}="";"";IFERROR(${fixedAdvance};0)-` +
      `IFERROR(${supplementalAccrual};0))`
    );
  }
}


/**
 * Строит формулу календарного начисления за выбранный полуинтервал месяца,
 * учитывая даты действия каждого оклада дополнительной должности.
 */
function buildCalendarAccrualFormula_(
  salaryFields,
  assignmentCriteria,
  monthlySalaryCell,
  periodStart,
  periodEnd
) {
  const overlapDays =
    `IF(${salaryFields.end}="";${periodEnd};` +
    `IF(${salaryFields.end}<${periodEnd};${salaryFields.end};${periodEnd}))-` +
    `IF(${salaryFields.start}>${periodStart};${salaryFields.start};${periodStart})+1`;

  return (
    `=IF(${monthlySalaryCell}="";"";` +
    `SUMPRODUCT(${assignmentCriteria}*` +
    `(${salaryFields.start}<=${periodEnd})*` +
    `((${salaryFields.end}="")+(${salaryFields.end}>=${periodStart}))*` +
    `${salaryFields.amount}*IF(${overlapDays}>0;${overlapDays};0)/` +
    `DAY(EOMONTH($F$1;0))))`
  );
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
 * Читает заголовки ведомости в карту «название → номер столбца», чтобы формулы
 * дополнительных назначений не зависели от фиксированных букв столбцов.
 */
function getColumnMap_(sheet) {
  const headers = sheet
    .getRange(CONFIG.HEADER_ROW, 1, 1, sheet.getLastColumn())
    .getDisplayValues()[0];
  const columnByHeader = new Map();

  headers.forEach((header, index) => {
    const normalized = normalizeHeader_(header);
    if (normalized && !columnByHeader.has(normalized)) {
      columnByHeader.set(normalized, index + 1);
    }
  });

  return columnByHeader;
}


/**
 * Записывает формулу в поле по его заголовку и останавливает актуализацию при
 * отсутствии обязательного поля, чтобы денежный расчёт не попал в соседнюю колонку.
 */
function setFormulaByHeader_(sheet, columnByHeader, header, row, formula) {
  const column = columnByHeader.get(normalizeHeader_(header));

  if (!column) {
    throw new Error(`Не найден обязательный заголовок «${header}».`);
  }

  sheet.getRange(row, column).setFormula(formula);
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
function makeEmployeeKey_(fio, legalEntity, designation = '') {
  return [fio, legalEntity, designation]
    .map(normalizeHeader_)
    .join('|');
}


/**
 * Создаёт ключ прежнего формата только для безопасного переноса ручных значений
 * при первом обновлении строк, до появления отдельных должностей совмещения.
 */
function makeLegacyEmployeeKey_(fio, legalEntity) {
  return `${normalizeHeader_(fio)}|${normalizeHeader_(legalEntity)}|`;
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
