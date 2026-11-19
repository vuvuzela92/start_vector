const CONFIG = {
  SALARY_DIRECTORY_SHEET: 'Справочник окладов офис',
  DRIVER_CONDITIONS_SHEET: 'Справочник условий оплаты водителей',
  SUPPLEMENTAL_ROLE_COLUMN: 12,
  SUPPLEMENTAL_ENTITY_COLUMN: 13,
  SUPPLEMENTAL_GROUP_COLUMN: 14,

  PAYROLL_SHEETS: [
    {
      name: 'Октябрь ЗП Вектор',
      sourceSheet: 'октябрь 2026 new',
      legalEntityCell: 'B13',
      active: false,
    },
    {
      name: 'Октябрь ЗП Старт',
      sourceSheet: 'октябрь 2026 new',
      legalEntityCell: 'B14',
      active: false,
    },
    {
      name: 'Ноябрь ЗП Вектор',
      sourceSheet: 'ноябрь 2026 new',
      legalEntityCell: 'B13',
      active: true,
    },
    {
      name: 'Ноябрь ЗП Старт',
      sourceSheet: 'ноябрь 2026 new',
      legalEntityCell: 'B14',
      active: true,
    },
  ],

  DIRECTORY_SHEET: 'Справочник',

  NOVEMBER_ROSTER_SHEET: 'ноябрь 2026 new',
  TECHNICAL_SHEET: 'Технический лист',
  TECHNICAL_DATA_START_ROW: 2,
  ROSTER_DATA_START_ROW: 5,
  ROSTER_IDENTITY_COLUMN: 1,
  ROSTER_IDENTITY_WIDTH: 4,

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
 * Показывает бухгалтеру единственную команду для совместной актуализации ведомостей и табеля.
 */
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Актуализация ведомости')
    .addItem('Обновить ведомости', 'actualizePayrollSheets')
    .addToUi();
}


/**
 * Готовит состав ноября из технических записей до перестройки строк. Если для
 * сотрудников не найдено юрлицо, показывает список для подтверждения, чтобы
 * бухгалтер понимал, какие строки будут добавлены с пустым юрлицом.
 */
function prepareNovemberRosterUpdate_(spreadsheet) {
  const rosterSheet = spreadsheet.getSheetByName(CONFIG.NOVEMBER_ROSTER_SHEET);
  const technicalSheet = spreadsheet.getSheetByName(CONFIG.TECHNICAL_SHEET);

  if (!rosterSheet) {
    throw new Error(`Не найден лист «${CONFIG.NOVEMBER_ROSTER_SHEET}».`);
  }
  if (!technicalSheet) {
    throw new Error(`Не найден лист «${CONFIG.TECHNICAL_SHEET}».`);
  }

  const existingRows = readNovemberRosterRows_(rosterSheet);
  const employees = readNovemberEmployees_(technicalSheet, existingRows);
  if (!employees.length) {
    throw new Error('В техническом листе не найдено сотрудников с записями за ноябрь 2026 года. Ведомости не изменены.');
  }

  return { rosterSheet, existingRows, employees };
}


/**
 * Спрашивает подтверждение перед совместной актуализацией, если в ноябрьском
 * источнике есть сотрудники без юрлица, необходимого для зарплатных формул.
 */
function confirmNovemberRosterUpdate_(employees) {
  const employeesWithoutEntity = employees.filter((employee) => !employee.legalEntity);
  if (!employeesWithoutEntity.length) {
    return true;
  }

  const sampleNames = employeesWithoutEntity
    .slice(0, 12)
    .map((employee) => employee.fio)
    .join('\n');
  const answer = SpreadsheetApp.getUi().alert(
    'Проверьте юрлица сотрудников ноября',
    `У ${employeesWithoutEntity.length} сотрудников не определено юрлицо. ` +
      'Ведомости не будут изменены, если нажать «Нет».\n\n' +
      `${sampleNames}${employeesWithoutEntity.length > 12 ? '\n…' : ''}\n\n` +
      'Продолжить общую актуализацию ведомостей?',
    SpreadsheetApp.getUi().ButtonSet.YES_NO
  );
  return answer === SpreadsheetApp.getUi().Button.YES;
}


/**
 * Читает сотрудников, присутствующих в технических записях ноября 2026 года,
 * исключая повторы по дням и дополняя незаполненное юрлицо из существующей строки.
 */
function readNovemberEmployees_(technicalSheet, existingRows) {
  const lastRow = technicalSheet.getLastRow();
  if (lastRow < CONFIG.TECHNICAL_DATA_START_ROW) {
    return [];
  }

  const rows = technicalSheet.getRange(
    CONFIG.TECHNICAL_DATA_START_ROW,
    1,
    lastRow - CONFIG.TECHNICAL_DATA_START_ROW + 1,
    6
  ).getDisplayValues();
  const employeesByKey = new Map();
  const technicalEntitiesByPerson = new Map();

  for (const row of rows) {
    if (!/^2026-11-\d{2}$/.test(String(row[0] || '').trim())) {
      continue;
    }
    const fioKey = normalizeHeader_(row[1]);
    const groupKey = normalizeHeader_(row[2]);
    const legalEntity = String(row[3] || '').trim();
    if (!fioKey || !groupKey || !legalEntity) {
      continue;
    }
    const personKey = `${fioKey}|${groupKey}`;
    if (!technicalEntitiesByPerson.has(personKey)) {
      technicalEntitiesByPerson.set(personKey, new Set());
    }
    technicalEntitiesByPerson.get(personKey).add(legalEntity);
  }

  for (const row of rows) {
    const dateText = String(row[0] || '').trim();
    if (!/^2026-11-\d{2}$/.test(dateText)) {
      continue;
    }

    const fio = String(row[1] || '').trim();
    const group = String(row[2] || '').trim();
    let legalEntity = String(row[3] || '').trim();
    if (!fio || !group) {
      continue;
    }

    if (!legalEntity) {
      const normalizedFio = normalizeHeader_(fio);
      const normalizedGroup = normalizeHeader_(group);
      const personKey = `${normalizedFio}|${normalizedGroup}`;
      const entitiesFromTechnical = [
        ...(technicalEntitiesByPerson.get(personKey) || []),
      ];
      const knownEntities = [...existingRows.values()]
        .filter((saved) =>
          normalizeHeader_(saved.identity[0]) === normalizedFio &&
          normalizeHeader_(saved.identity[1]) === normalizedGroup &&
          normalizeHeader_(saved.identity[2])
        )
        .map((saved) => String(saved.identity[2]).trim());
      const uniqueEntities = new Map();
      for (const entity of [...entitiesFromTechnical, ...knownEntities]) {
        uniqueEntities.set(normalizeHeader_(entity), entity);
      }
      if (uniqueEntities.size === 1) {
        legalEntity = [...uniqueEntities.values()][0];
      }
    }

    const key = makeNovemberRosterKey_({ fio, legalEntity, group });
    const savedRow = findNovemberRosterRow_(existingRows, {
      fio,
      legalEntity,
      group,
    });
    const employee = {
      fio,
      group,
      legalEntity,
      position: savedRow ? savedRow.identity[3] : '',
    };
    if (!employeesByKey.has(key)) {
      employeesByKey.set(key, employee);
    }
  }

  return [...employeesByKey.values()].sort((first, second) =>
    first.group.localeCompare(second.group, 'ru') ||
    first.fio.localeCompare(second.fio, 'ru')
  );
}


/**
 * Сохраняет идентификаторы и ручные значения согласованных часов, чтобы при
 * изменении состава строки и их значения оставались привязаны к нужным сотрудникам.
 */
function readNovemberRosterRows_(sheet) {
  const firstRow = CONFIG.ROSTER_DATA_START_ROW;
  const lastRow = sheet.getLastRow();
  const lastColumn = sheet.getLastColumn();
  if (lastRow < firstRow) {
    const emptyRows = new Map();
    emptyRows.manualColumns = [];
    emptyRows.manualCellsToClear = [];
    return emptyRows;
  }

  const range = sheet.getRange(firstRow, 1, lastRow - firstRow + 1, lastColumn);
  const values = range.getValues();
  const formulas = range.getFormulas();
  const result = new Map();
  const headerValues = sheet
    .getRange(CONFIG.SOURCE_HEADER_ROW, 1, 1, lastColumn)
    .getDisplayValues()[0];
  const manualColumns = headerValues
    .map((header, index) =>
      normalizeHeader_(header) === 'Согласовано добавление часов' ? index : -1
    )
    .filter((index) => index >= 0);

  for (let rowIndex = 0; rowIndex < values.length; rowIndex += 1) {
    const row = values[rowIndex];
    const fio = normalizeHeader_(row[0]);
    const group = normalizeHeader_(row[1]);
    const legalEntity = normalizeHeader_(row[2]);
    if (!fio || fio.startsWith('ИТОГО')) {
      continue;
    }

    const manualValues = {};
    for (const column of manualColumns) {
      if (!formulas[rowIndex][column] && row[column] !== '' && row[column] !== null) {
        manualValues[column] = row[column];
      }
    }

    result.set(makeNovemberRosterKey_({ fio, legalEntity, group, position: row[3] }), {
      identity: row.slice(0, CONFIG.ROSTER_IDENTITY_WIDTH),
      manualValues,
      sourceRow: firstRow + rowIndex,
    });
  }

  result.manualColumns = manualColumns;
  result.manualCellsToClear = [...result.values()].flatMap((saved) =>
    Object.keys(saved.manualValues).map((column) => ({
      row: saved.sourceRow,
      column: Number(column) + 1,
    }))
  );
  return result;
}


/**
 * Обновляет ФИО, группу, юрлицо и должность, возвращает ручные согласованные часы,
 * а при нехватке строк расширяет лист формулами и форматом строки-шаблона ноября.
 */
function writeNovemberRoster_(sheet, employees, existingRows) {
  const firstRow = CONFIG.ROSTER_DATA_START_ROW;
  const lastColumn = sheet.getLastColumn();
  const oldRowCount = sheet.getMaxRows() - firstRow + 1;
  const outputRows = buildNovemberRosterRows_(employees);
  const neededRowCount = outputRows.length;
  const previousIdentities = sheet
    .getRange(firstRow, CONFIG.ROSTER_IDENTITY_COLUMN, oldRowCount, CONFIG.ROSTER_IDENTITY_WIDTH)
    .getDisplayValues();
  const arrivalFormulas = sheet
    .getRange(firstRow, 5, oldRowCount, 1)
    .getFormulas()
    .flat();
  const templateOffset = arrivalFormulas.findIndex(Boolean);
  if (templateOffset < 0) {
    throw new Error('В ноябрьском табеле не найдена формула-подсказка для дневных блоков. Список не изменён.');
  }
  const templateRow = firstRow + templateOffset;
  const templateRange = sheet.getRange(templateRow, 1, 1, lastColumn);
  sheet
    .getRange(firstRow, CONFIG.ROSTER_IDENTITY_COLUMN, oldRowCount, CONFIG.ROSTER_IDENTITY_WIDTH)
    .clearContent();
  for (const cell of existingRows.manualCellsToClear) {
    sheet.getRange(cell.row, cell.column).clearContent();
  }
  if (!employees.length) {
    return;
  }

  if (neededRowCount > oldRowCount) {
    const rowsToAdd = neededRowCount - oldRowCount;
    const oldLastRow = sheet.getMaxRows();
    sheet.insertRowsAfter(oldLastRow, rowsToAdd);

    for (let offset = 1; offset <= rowsToAdd; offset += 1) {
      const newRow = oldLastRow + offset;
      templateRange.copyTo(
        sheet.getRange(newRow, 1, 1, lastColumn),
        SpreadsheetApp.CopyPasteType.PASTE_FORMAT,
        false
      );
      templateRange.copyTo(
        sheet.getRange(newRow, 1, 1, lastColumn),
        SpreadsheetApp.CopyPasteType.PASTE_FORMULA,
        false
      );
    }
  }

  for (let index = 0; index < outputRows.length; index += 1) {
    if (outputRows[index].type !== 'employee') {
      continue;
    }
    const targetRow = firstRow + index;
    const wasBlank = !String(previousIdentities[index]?.[0] || '').trim();
    const hasArrivalFormula = Boolean(arrivalFormulas[index]);
    if (wasBlank && !hasArrivalFormula) {
      templateRange.copyTo(
        sheet.getRange(targetRow, 1, 1, lastColumn),
        SpreadsheetApp.CopyPasteType.PASTE_FORMAT,
        false
      );
      templateRange.copyTo(
        sheet.getRange(targetRow, 1, 1, lastColumn),
        SpreadsheetApp.CopyPasteType.PASTE_FORMULA,
        false
      );
    }
  }

  const identities = outputRows.map((row) => row.type === 'separator'
    ? ['', '', '', '']
    : [
      row.employee.fio,
      row.employee.group,
      row.employee.legalEntity,
      row.employee.position,
    ]
  );
  sheet.getRange(firstRow, 1, identities.length, CONFIG.ROSTER_IDENTITY_WIDTH)
    .setValues(identities);

  outputRows.forEach((row, index) => {
    if (row.type !== 'employee') {
      return;
    }
    const previousRow = findNovemberRosterRow_(existingRows, row.employee);
    if (!previousRow) {
      return;
    }
    const rowNumber = firstRow + index;
    for (const [column, value] of Object.entries(previousRow.manualValues)) {
      sheet.getRange(rowNumber, Number(column) + 1).setValue(value);
    }
  });

  updateNovemberSeparatorRule_(sheet);
}


/**
 * Вставляет пустую строку при смене группы, чтобы правило условного форматирования
 * рисовало разделительную полосу между блоками сотрудников.
 */
function buildNovemberRosterRows_(employees) {
  const rows = [];
  let previousGroup = '';

  for (const employee of employees) {
    const currentGroup = normalizeHeader_(employee.group);
    if (previousGroup && currentGroup !== previousGroup) {
      rows.push({ type: 'separator' });
    }
    rows.push({ type: 'employee', employee });
    previousGroup = currentGroup;
  }

  return rows;
}


/**
 * Расширяет правило белой разделительной полосы на все строки ноябрьского листа,
 * включая строки, добавленные при росте списка сотрудников.
 */
function updateNovemberSeparatorRule_(sheet) {
  const separatorRuleFormula = '=AND(COUNTBLANK($A2:$C2)=3;$A1<>"";$A3<>"")';
  const rules = sheet.getConditionalFormatRules();
  const ruleIndex = rules.findIndex((rule) => {
    const condition = rule.getBooleanCondition();
    if (!condition || condition.getCriteriaType() !== SpreadsheetApp.BooleanCriteria.CUSTOM_FORMULA) {
      return false;
    }
    return String(condition.getCriteriaValues()[0] || '').includes(
      'COUNTBLANK($A2:$C2)=3'
    );
  });

  if (ruleIndex < 0) {
    return;
  }

  rules[ruleIndex] = SpreadsheetApp.newConditionalFormatRule()
    .whenFormulaSatisfied(separatorRuleFormula)
    .setBackground('#ffffff')
    .setRanges([sheet.getRange(2, 1, sheet.getMaxRows() - 1, sheet.getLastColumn())])
    .build();
  sheet.setConditionalFormatRules(rules);
}


/**
 * Создаёт устойчивый ключ для дедупликации ноября и переноса ручных значений,
 * различая одинаковые ФИО по юрлицу, группе и должности.
 */
function makeNovemberRosterKey_(employee) {
  return [employee.fio, employee.legalEntity, employee.group, employee.position || '']
    .map((value) => normalizeHeader_(value).toLocaleLowerCase('ru'))
    .join('|');
}


/**
 * Находит предыдущую строку того же сотрудника по ФИО, юрлицу и группе,
 * сохраняя должность и дневные значения при изменении состава или порядка строк.
 */
function findNovemberRosterRow_(existingRows, employee) {
  const exactMatch = [...existingRows.values()].find((saved) =>
    normalizeHeader_(saved.identity[0]) === normalizeHeader_(employee.fio) &&
    normalizeHeader_(saved.identity[1]) === normalizeHeader_(employee.group) &&
    normalizeHeader_(saved.identity[2]) === normalizeHeader_(employee.legalEntity) &&
    normalizeHeader_(saved.identity[3]) !== normalizeHeader_('Уборщица')
  );
  if (exactMatch) {
    return exactMatch;
  }

  const matchingRows = [...existingRows.values()].filter((saved) =>
    normalizeHeader_(saved.identity[0]) === normalizeHeader_(employee.fio) &&
    normalizeHeader_(saved.identity[1]) === normalizeHeader_(employee.group) &&
    normalizeHeader_(saved.identity[2]) === normalizeHeader_(employee.legalEntity)
  );
  return matchingRows.find((saved) =>
    normalizeHeader_(saved.identity[3]) !== normalizeHeader_('Уборщица')
  ) || matchingRows[0] || null;
}


/**
 * Перестраивает только активные ведомости открытого периода и обновляет ноябрьский
 * список по техническому листу. Закрытые периоды с active:false не пересчитываются.
 * Перед изменениями проверяет, можно ли определить юрлица сотрудников.
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

    const novemberUpdate = prepareNovemberRosterUpdate_(spreadsheet);
    if (!confirmNovemberRosterUpdate_(novemberUpdate.employees)) {
      SpreadsheetApp.getUi().alert(
        'Актуализация отменена',
        'Из-за неустановленных юрлиц ведомости и ноябрьский список не изменены.',
        SpreadsheetApp.getUi().ButtonSet.OK
      );
      return;
    }

    for (const payrollConfig of CONFIG.PAYROLL_SHEETS) {
      if (!payrollConfig.active) {
        continue;
      }

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

    writeNovemberRoster_(
      novemberUpdate.rosterSheet,
      novemberUpdate.employees,
      novemberUpdate.existingRows
    );
    SpreadsheetApp.flush();
    SpreadsheetApp.getUi().alert(
      'Актуализация завершена',
      `Активные зарплатные ведомости обновлены. Список ноября актуализирован: ${novemberUpdate.employees.length} сотрудников.`,
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
  applyDriverPaymentFormulaRules_(sheet, sourceSheet, employeeRows);
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
 * итоги двух периодов, а персонал остаётся на почасовом начислении. Водителей
 * пропускает, потому что их аванс и зарплата рассчитываются по отдельным условиям.
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

    if (group === normalizeHeader_('Склад. Водитель')) {
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
 * Подставляет оклад из актуального справочника по группе сотрудника: офисные
 * ставки берутся из справочника окладов офиса, «Склад. Офис» — из справочника
 * нормы окладов склада, а почасовые группы оставляют базовое поле пустым.
 */
function applyBaseSalaryFormulaRules_(sheet, employeeRows) {
  const salarySheetName = CONFIG.SALARY_DIRECTORY_SHEET.replace(/'/g, "''");
  const salaryRange = `'${salarySheetName}'`;
  const warehouseSalaryRange = "'Справочник нормы времени окладов склад'";
  const columnByHeader = getColumnMap_(sheet);

  for (const row of employeeRows) {
    const group = normalizeHeader_(
      sheet.getRange(row, columnByHeader.get(normalizeHeader_('Группа'))).getDisplayValue()
    );
    if (group === normalizeHeader_('Склад. Водитель')) {
      continue;
    }

    let formula = '=IF($B' + row + '="";"";"")';
    if (group === normalizeHeader_('Склад. Офис')) {
      formula =
        `=IF($B${row}="";"";LET(` +
        `фио;$B${row};начало_месяца;$F$1;конец_месяца;EOMONTH($F$1;0);` +
        `оклад;SUMPRODUCT(` +
        `(${warehouseSalaryRange}!$A$3:$A=фио)*` +
        `(${warehouseSalaryRange}!$E$3:$E<=конец_месяца)*` +
        `((${warehouseSalaryRange}!$F$3:$F="")+` +
        `(${warehouseSalaryRange}!$F$3:$F>=начало_месяца))*` +
        `${warehouseSalaryRange}!$B$3:$B);IF(оклад=0;"";оклад)))`;
    } else if (group !== normalizeHeader_('СКЛАД. Персонал')) {
      formula =
        `=IF($B${row}="";"";LET(` +
        `фио;$B${row};начало_месяца;$F$1;конец_месяца;EOMONTH($F$1;0);` +
        `оклад;SUMPRODUCT(` +
        `(${salaryRange}!$A$3:$A=фио)*` +
        `(${salaryRange}!$C$3:$C<=конец_месяца)*` +
        `((${salaryRange}!$D$3:$D="")+` +
        `(${salaryRange}!$D$3:$D>=начало_месяца))*` +
        `${salaryRange}!$B$3:$B);IF(оклад=0;"";оклад)))`;
    }

    setFormulaByHeader_(sheet, columnByHeader, 'ОКЛАД', row, formula);
  }
}


/**
 * Рассчитывает аванс и остаток зарплаты водителей по их персональным условиям:
 * дневной эквивалент берёт фактические часы из табеля без вычета обеда, норму,
 * дневной предел и денежный режим — из отдельного справочника водителей.
 */
function applyDriverPaymentFormulaRules_(sheet, sourceSheet, employeeRows) {
  const columnByHeader = getColumnMap_(sheet);
  const groupColumn = columnByHeader.get(normalizeHeader_('Группа'));
  const driverRows = employeeRows.filter((row) =>
    normalizeHeader_(sheet.getRange(row, groupColumn).getDisplayValue()) ===
    normalizeHeader_('Склад. Водитель')
  );
  if (!driverRows.length) return;

  const directoryName = CONFIG.DRIVER_CONDITIONS_SHEET.replace(/'/g, "''");
  const directory = `'${directoryName}'`;
  const sourceName = sourceSheet.getName().replace(/'/g, "''");
  const sourceHeaders = sourceSheet
    .getRange(CONFIG.SOURCE_HEADER_ROW, 1, 1, sourceSheet.getLastColumn())
    .getDisplayValues()[0];
  const factColumns = sourceHeaders.flatMap((header, index) => {
    const match = String(header).match(/^Факт день (\d{2})\./i);
    return match ? [{ day: Number(match[1]), column: index + 1 }] : [];
  });
  const firstHalfColumns = factColumns.filter((item) => item.day <= 15);
  const allFactColumns = factColumns;
  if (!firstHalfColumns.length || !allFactColumns.length) {
    throw new Error(`На листе «${sourceSheet.getName()}» не найдены столбцы фактических часов водителей.`);
  }

  const directoryMatch = (row) =>
    `MATCH(1;(${directory}!$A$3:$A=$B${row})*` +
    `(${directory}!$B$3:$B=$D${row})*` +
    `(${directory}!$C$3:$C=$E${row})*` +
    `(IF(ISNUMBER(${directory}!$M$3:$M);${directory}!$M$3:$M;` +
    `IFERROR(DATE(VALUE(RIGHT(${directory}!$M$3:$M;4));` +
    `VALUE(MID(${directory}!$M$3:$M;4;2));` +
    `VALUE(LEFT(${directory}!$M$3:$M;2)));0))<=EOMONTH($F$1;0))*` +
    `IF(${directory}!$N$3:$N="";TRUE;` +
    `IF(ISNUMBER(${directory}!$N$3:$N);${directory}!$N$3:$N;` +
    `IFERROR(DATE(VALUE(RIGHT(${directory}!$N$3:$N;4));` +
    `VALUE(MID(${directory}!$N$3:$N;4;2));` +
    `VALUE(LEFT(${directory}!$N$3:$N;2)));0))>=$F$1);0)`;
  const sourceMatch = (row) =>
    `MATCH(1;('${sourceName}'!$A:$A=$B${row})*` +
    `('${sourceName}'!$B:$B=$E${row})*` +
    `('${sourceName}'!$C:$C=$D${row})*` +
    `('${sourceName}'!$D:$D=$C${row});0)`;
  const factHours = (row, items) =>
    `SUM(${items.map(({ column }) =>
      `IFERROR(INDEX('${sourceName}'!$${columnToLetter_(column)}:$${columnToLetter_(column)};${sourceMatch(row)});0)`
    ).join(';')})`;

  for (const row of driverRows) {
    const match = directoryMatch(row);
    const mode = `INDEX(${directory}!$G$3:$G;${match})`;
    const salary = `INDEX(${directory}!$H$3:$H;${match})`;
    const shiftNorm = `INDEX(${directory}!$I$3:$I;${match})`;
    const dailyCap = `INDEX(${directory}!$J$3:$J;${match})`;
    const lowerLimit = `INDEX(${directory}!$K$3:$K;${match})`;
    const upperLimit = `INDEX(${directory}!$L$3:$L;${match})`;
    const monthHours = factHours(row, allFactColumns);
    const salaryCell = cellRef_(columnByHeader, 'ОКЛАД', row);
    const advanceCell = cellRef_(columnByHeader, 'Аванс', row);
    const monthlyWorkdaysColumn = sheet
      .getRange(1, 1, 1, sheet.getLastColumn())
      .getDisplayValues()[0]
      .findIndex((header) => normalizeHeader_(header) === normalizeHeader_('раб дней')) + 1;
    if (!monthlyWorkdaysColumn) {
      throw new Error(`На листе «${sheet.getName()}» не найден показатель «раб дней».`);
    }
    const monthlyWorkdaysCell = `${columnToLetter_(monthlyWorkdaysColumn + 1)}$1`;
    const firstHalfShiftUnits = firstHalfColumns.map(({ column }) => {
      const letter = columnToLetter_(column);
      const hours = `IFERROR(INDEX('${sourceName}'!$${letter}:$${letter};${sourceMatch(row)});0)`;
      return `IFERROR(IF(${dailyCap}="";${hours}/${shiftNorm};MIN(${hours}/${shiftNorm};${dailyCap}));0)`;
    }).join(';');

    const salaryFormula =
      `=IFERROR(IF(INDEX(${directory}!$H$3:$H;${match})="";"";${salary});"")`;
    const advanceFormula =
      `=IFERROR(IF(OR(${salaryCell}="";${monthlyWorkdaysCell}="";${shiftNorm}=0);"";` +
      `${salaryCell}/${monthlyWorkdaysCell}*SUM(${firstHalfShiftUnits}));"")`;
    const salaryRemainderFormula =
      `=IFERROR(LET(режим;${mode};оклад;${salaryCell};` +
      `часы_месяца;${monthHours};нижняя_граница;${lowerLimit};верхняя_граница;${upperLimit};` +
      `IF(оклад="";"";IF(режим="Оклад с диапазоном часов";` +
      `IF(часы_месяца<нижняя_граница;оклад*часы_месяца/нижняя_граница;` +
      `IF(часы_месяца<=верхняя_граница;оклад;` +
      `оклад+(часы_месяца-верхняя_граница)*оклад/верхняя_граница));оклад))-` +
      `${advanceCell});"")`;

    setFormulaByHeader_(sheet, columnByHeader, 'ОКЛАД', row, salaryFormula);
    setFormulaByHeader_(sheet, columnByHeader, 'Аванс', row, advanceFormula);
    setFormulaByHeader_(sheet, columnByHeader, 'Зарплата', row, salaryRemainderFormula);
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
