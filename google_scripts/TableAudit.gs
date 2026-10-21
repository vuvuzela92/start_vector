/**
 * Аудитор производительности таблицы, в которую помещён этот файл.
 * Скрипт только читает исходную таблицу и создаёт отдельный отчёт.
 */

var TABLE_AUDIT_PREFIX = 'TABLE_AUDIT_';
var TABLE_AUDIT_CHUNK_CELLS = 20000;
var TABLE_AUDIT_TIME_LIMIT_MS = 4 * 60 * 1000;
var TABLE_AUDIT_MAX_EXAMPLES = 5;

/** Добавляет меню аудитора без замены существующего обработчика onOpen. */
function tableAuditInstallMenu() {
  try {
    SpreadsheetApp.getUi().createMenu('Аудит производительности')
      .addItem('Начать аудит', 'tableAuditStart')
      .addItem('Продолжить аудит', 'tableAuditContinue')
      .addItem('Показать состояние', 'tableAuditStatus')
      .addItem('Отменить аудит', 'tableAuditCancel')
      .addToUi();
  } catch (error) {
    Logger.log('Меню недоступно в этом контексте. Запускайте функции напрямую: tableAuditStart или tableAuditContinue.');
  }
}

/**
 * Запускает новый аудит текущей таблицы и создаёт отдельную книгу отчёта.
 * Бизнес-сценарий: пользователь получает независимый снимок рекомендаций,
 * не изменяя рабочие данные и существующие автоматизации.
 */
function tableAuditStart() {
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(1000)) throw new Error('Аудит уже выполняется другим запуском.');
  var shouldContinue = false;
  try {
    var source = SpreadsheetApp.getActiveSpreadsheet();
    var report = SpreadsheetApp.create('Аудит производительности: ' + source.getName());
    tableAuditPrepareReport_(report, source);
    var state = tableAuditInitialState_(source, report);
    tableAuditSaveState_(state);
    shouldContinue = true;
  } finally {
    lock.releaseLock();
  }
  if (shouldContinue) tableAuditContinue();
}

/**
 * Продолжает аудит с сохранённого листа и строки.
 * Бизнес-сценарий: крупная таблица проверяется частями, чтобы не упереться
 * в лимит выполнения Apps Script; повторный запуск возобновляет курсор.
 */
function tableAuditContinue() {
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(1000)) throw new Error('Аудит уже выполняется другим запуском.');
  try {
    var state = tableAuditLoadState_();
    if (!state) throw new Error('Нет активного аудита. Сначала выберите «Начать аудит».');
    var source = SpreadsheetApp.openById(state.sourceId);
    var report = SpreadsheetApp.openById(state.reportId);
    var started = Date.now();
    state._deadline = started + TABLE_AUDIT_TIME_LIMIT_MS;
    try {
      while (state.sheetIndex < state.sheetNames.length && Date.now() - started < TABLE_AUDIT_TIME_LIMIT_MS) {
        var sheet = source.getSheetByName(state.sheetNames[state.sheetIndex]);
        if (!sheet) { state.sheetIndex++; state.row = 1; continue; }
        state = tableAuditScanSheet_(sheet, report, state);
        tableAuditSaveState_(state);
      }
    } catch (error) {
      state.status = 'Ошибка с сохранённым прогрессом';
      state.lastError = tableAuditSafeErrorMessage_(error);
      tableAuditSaveState_(state);
      report.getSheetByName('Покрытие').appendRow(['Выполнение', 'Ошибка', 'Лист: ' + state.sheetNames[state.sheetIndex] + '; строка: ' + state.row, state.lastError]);
      throw new Error('Аудит остановлен на листе «' + state.sheetNames[state.sheetIndex] + '», строка ' + state.row + '. ' + state.lastError);
    }
    if (state.sheetIndex >= state.sheetNames.length) {
      tableAuditFinalize_(source, report, state);
      state.status = 'Завершён';
      tableAuditSaveState_(state);
      tableAuditNotify_('Аудит завершён. Отчёт: ' + report.getUrl());
    } else {
      tableAuditNotify_('Сохранён прогресс. Запустите tableAuditContinue.\nОтчёт: ' + report.getUrl());
    }
  } finally {
    lock.releaseLock();
  }
}

/** Показывает пользователю состояние текущего аудита. */
function tableAuditStatus() {
  var state = tableAuditLoadState_();
  if (!state) { tableAuditNotify_('Активный аудит не найден.'); return; }
  tableAuditNotify_('Статус: ' + state.status + '\nЛист: ' + (state.sheetIndex + 1) + ' из ' + state.sheetNames.length + '\nОтчёт: ' + state.reportUrl);
}

/** Отменяет аудит и удаляет только его служебное состояние. */
function tableAuditCancel() {
  PropertiesService.getUserProperties().deleteProperty(TABLE_AUDIT_PREFIX + 'STATE');
  tableAuditNotify_('Аудит отменён. Созданный ранее отчёт не удалён.');
}

/** Показывает уведомление через UI, а при его недоступности пишет его в журнал. */
function tableAuditNotify_(message) {
  try {
    SpreadsheetApp.getUi().alert(message);
  } catch (error) {
    Logger.log(message);
    try { SpreadsheetApp.getActiveSpreadsheet().toast(message, 'Аудит таблицы', 10); } catch (toastError) { Logger.log('Уведомление UI недоступно.'); }
  }
}

/** Создаёт вкладки отчёта и сохраняет их заголовки. */
function tableAuditPrepareReport_(report, source) {
  var first = report.getSheets()[0];
  first.setName('Сводка');
  var tabs = ['Листы', 'Формулы', 'Оформление', 'Скрипты', 'Покрытие'];
  tabs.forEach(function(name) { report.insertSheet(name); });
  tableAuditHeader_(report.getSheetByName('Сводка'), ['Показатель', 'Значение', 'Комментарий']);
  tableAuditHeader_(report.getSheetByName('Листы'), ['Лист', 'Скрыт', 'Строки', 'Столбцы', 'Формулы', 'Непустые ячейки', 'Пустые внутри области', 'Диагностика']);
  tableAuditHeader_(report.getSheetByName('Формулы'), ['Приоритет', 'Лист', 'Адреса-примеры', 'Факт', 'Возможная нагрузка', 'Рекомендация', 'Проверка эффекта']);
  tableAuditHeader_(report.getSheetByName('Оформление'), ['Приоритет', 'Лист', 'Тип объекта', 'Факт', 'Возможная нагрузка', 'Рекомендация', 'Проверка эффекта']);
  tableAuditHeader_(report.getSheetByName('Скрипты'), ['Приоритет', 'Файл/метрика', 'Строка', 'Факт', 'Возможная нагрузка', 'Рекомендация', 'Статус проверки']);
  tableAuditHeader_(report.getSheetByName('Покрытие'), ['Раздел', 'Статус', 'Проверено', 'Ограничения']);
  report.getSheetByName('Сводка').appendRow(['Источник', source.getName(), source.getUrl()]);
}

/** Записывает заголовок вкладки отчёта. */
function tableAuditHeader_(sheet, values) { sheet.getRange(1, 1, 1, values.length).setValues([values]).setFontWeight('bold'); sheet.setFrozenRows(1); }

/** Формирует начальное состояние аудита. */
function tableAuditInitialState_(source, report) {
  return { status: 'Выполняется', sourceId: source.getId(), reportId: report.getId(), reportUrl: report.getUrl(), sheetNames: source.getSheets().map(function(s) { return s.getName(); }), sheetIndex: 0, row: 1, counts: { formulas: 0, nonEmpty: 0, blanks: 0, findings: 0 }, sheetStats: {}, startedAt: new Date().toISOString() };
}

/** Сохраняет компактный курсор и агрегаты в UserProperties. */
function tableAuditSaveState_(state) { PropertiesService.getUserProperties().setProperty(TABLE_AUDIT_PREFIX + 'STATE', JSON.stringify(state)); }

/** Читает состояние текущего пользователя. */
function tableAuditLoadState_() { var raw = PropertiesService.getUserProperties().getProperty(TABLE_AUDIT_PREFIX + 'STATE'); return raw ? JSON.parse(raw) : null; }

/** Сканирует один лист блоками и добавляет найденные факты в отчёт. */
function tableAuditScanSheet_(sheet, report, state) {
  var maxRows = sheet.getMaxRows();
  var maxCols = sheet.getMaxColumns();
  var width = Math.max(1, Math.min(maxCols, Math.floor(TABLE_AUDIT_CHUNK_CELLS / Math.max(1, Math.min(maxRows, 1000)))));
  var lastRow = Math.max(1, sheet.getLastRow());
  var lastCol = Math.max(1, sheet.getLastColumn());
  var row = state.row;
  while (row <= lastRow && Date.now() < state._deadline) {
    var rows = Math.max(1, Math.min(1000, lastRow - row + 1));
    var cols = Math.max(1, Math.min(width, lastCol));
    var range = sheet.getRange(row, 1, rows, cols);
    var formulas = range.getFormulas();
    var values = range.getValues();
    tableAuditAnalyzeBlock_(sheet, report, row, formulas, values, state);
    row += rows;
    state.row = row;
    if (row <= lastRow && Date.now() >= state._deadline) break;
  }
  if (row > lastRow) { tableAuditWriteSheetSummary_(sheet, report, state); state.sheetIndex++; state.row = 1; }
  return state;
}

/** Анализирует блок формул и значений без копирования данных в отчёт. */
function tableAuditAnalyzeBlock_(sheet, report, startRow, formulas, values, state) {
  var formulaRows = report.getSheetByName('Формулы');
  var examples = {};
  var sheetStats = state.sheetStats[sheet.getName()] || { formulas: 0, columns: {} };
  formulas.forEach(function(row, r) { row.forEach(function(formula, c) {
    if (!formula) { if (values[r][c] === '') state.counts.blanks++; return; }
    state.counts.formulas++; if (values[r][c] !== '') state.counts.nonEmpty++;
    sheetStats.formulas++;
    var columnStats = sheetStats.columns[c] || { count: 0, first: startRow + r, last: startRow + r, patterns: {} };
    columnStats.count++; columnStats.first = Math.min(columnStats.first, startRow + r); columnStats.last = Math.max(columnStats.last, startRow + r);
    var address = sheet.getName() + '!' + tableAuditColumn_(c + 1) + (startRow + r);
    var upper = formula.toUpperCase();
    var normalized = tableAuditNormalizeFormula_(formula);
    columnStats.patterns[normalized] = (columnStats.patterns[normalized] || 0) + 1;
    sheetStats.columns[c] = columnStats;
    examples[normalized] = examples[normalized] || []; if (examples[normalized].length < TABLE_AUDIT_MAX_EXAMPLES) examples[normalized].push(address);
    var rules = [
      {re: /IMPORTRANGE|IMPORTDATA|IMPORTHTML|IMPORTXML|IMPORTFEED|GOOGLEFINANCE/, p: 'Высокий', f: 'Внешняя импортирующая формула', load: 'Расчёт зависит от внешнего источника и повторных сетевых обновлений.', rec: 'Сократить число источников, кэшировать данные или вынести импорт в отдельный слой.'},
      {re: /INDIRECT|OFFSET/, p: 'Средний', f: 'Динамическая ссылка', load: 'Зависимости сложнее отслеживаются и могут пересчитывать большие области.', rec: 'Заменить статическими ссылками или ограниченными диапазонами, если бизнес-логика позволяет.'},
      {re: /(^|[^A-Z])NOW\(|TODAY\(|RAND\(|RANDBETWEEN\(/, p: 'Средний', f: 'Волатильная функция', load: 'Формула может пересчитываться при частых изменениях книги.', rec: 'Зафиксировать значение скриптом или ограничить область применения.'},
      {re: /:[A-Z]+|[0-9]+:[0-9]+/, p: 'Средний', f: 'Широкий или открытый диапазон', load: 'Обработка множества пустых строк/столбцов увеличивает объём вычислений.', rec: 'Ограничить диапазон фактическими данными или использовать структурированный блок.'},
      {re: /QUERY\(|FILTER\(|SORT\(|VLOOKUP\(|XLOOKUP\(|INDEX\(|MATCH\(/, p: 'Низкий', f: 'Потенциально тяжёлая обработка диапазона', load: 'Поиск или сортировка большого повторяющегося диапазона может быть дорогой.', rec: 'Проверить размер диапазона и объединить повторяющиеся вычисления.'}
    ];
    rules.forEach(function(rule) { if (rule.re.test(upper)) tableAuditFinding_(formulaRows, [rule.p, sheet.getName(), address, rule.f + ': ' + tableAuditMaskFormula_(formula), rule.load, rule.rec, 'Сравнить время пересчёта до и после изменения.']); });
    if (values[r][c] === '#REF!' || values[r][c] === '#VALUE!' || values[r][c] === '#N/A') tableAuditFinding_(formulaRows, ['Средний', sheet.getName(), address, 'Ошибка результата формулы: ' + values[r][c], 'Ошибки могут запускать повторные исправления и скрывать реальные сбои.', 'Проверить источник ошибки и обработать отсутствующие данные.', 'Сравнить количество ошибок после исправления.']);
  }); });
  state.sheetStats[sheet.getName()] = sheetStats;
  Object.keys(examples).forEach(function(key) { if (examples[key].length > 1) tableAuditFinding_(formulaRows, ['Низкий', sheet.getName(), examples[key].join(', '), 'Повторяется одна структура формулы (' + examples[key].length + ' примеров)', 'Одинаковые расчёты в большом числе ячеек увеличивают объём пересчёта.', 'Рассмотреть одну массивную формулу или предварительно рассчитанный диапазон.', 'Сравнить число формул и время пересчёта.']); });
}

/** Добавляет одну строку замечания в отчёт и увеличивает счётчик. */
function tableAuditFinding_(sheet, row) { sheet.appendRow(row); }

/** Записывает сводку листа и проверяемые объекты оформления. */
function tableAuditWriteSheetSummary_(sheet, report, state) {
  var data = report.getSheetByName('Листы');
  var lastRow = Math.max(1, sheet.getLastRow()), lastCol = Math.max(1, sheet.getLastColumn());
  var stats = state.sheetStats[sheet.getName()] || { formulas: 0, columns: {} };
  var count = stats.formulas;
  data.appendRow([sheet.getName(), sheet.isSheetHidden(), sheet.getMaxRows(), sheet.getMaxColumns(), count, 'См. агрегаты формул', 'См. агрегаты блоков', 'Статический аудит; точное время формул не измеряется.']);
  Object.keys(stats.columns).forEach(function(columnIndex) {
    var column = stats.columns[columnIndex];
    var patterns = Object.keys(column.patterns).sort(function(a, b) { return column.patterns[b] - column.patterns[a]; }).slice(0, 3);
    var address = sheet.getName() + '!' + tableAuditColumn_(Number(columnIndex) + 1);
    tableAuditFinding_(report.getSheetByName('Формулы'), ['Информация', sheet.getName(), address, 'Формул: ' + column.count + '; строки ' + column.first + '–' + column.last + '; шаблоны: ' + patterns.map(function(pattern) { return '×' + column.patterns[pattern] + ' ' + tableAuditMaskFormula_(pattern); }).join(' | '), 'Повторяющиеся формулы показывают объём однотипного пересчёта.', 'Проверить, можно ли заменить повторяющиеся расчёты одной массивной формулой или предварительным расчётом.', 'Сравнить число формул и время пересчёта.']);
  });
  var rules = sheet.getConditionalFormatRules();
  if (rules.length > 0) report.getSheetByName('Оформление').appendRow(['Низкий', sheet.getName(), 'Условное форматирование', 'Правил: ' + rules.length, 'Широкие или перекрывающиеся правила могут увеличивать обработку.', 'Сузить диапазоны и удалить дубли.', 'Сравнить скорость открытия и редактирования.']);
  var validations = sheet.getDataRange().getDataValidations();
  var validationCount = validations.reduce(function(total, row) { return total + row.filter(function(v) { return v !== null; }).length; }, 0);
  if (validationCount > 0) report.getSheetByName('Оформление').appendRow(['Низкий', sheet.getName(), 'Проверка данных', 'Ячеек: ' + validationCount, 'Большое число проверок увеличивает обработку ввода.', 'Ограничить проверки реальными диапазонами.', 'Сравнить задержку ввода.']);
  if (sheet.getCharts().length > 0) report.getSheetByName('Оформление').appendRow(['Низкий', sheet.getName(), 'Диаграммы', 'Количество: ' + sheet.getCharts().length, 'Диаграммы на широких диапазонах могут замедлять обновление.', 'Проверить диапазоны и частоту обновления.', 'Сравнить время открытия.']);
  var merged = sheet.getDataRange().getMergedRanges();
  if (merged.length > 0) report.getSheetByName('Оформление').appendRow(['Низкий', sheet.getName(), 'Объединённые ячейки', 'Диапазонов: ' + merged.length, 'Большое число объединений усложняет массовую обработку.', 'Оставить объединения только для заголовков.', 'Проверить операции вставки и сортировки.']);
}

/** Завершает отчёт: анализирует связанный проект, триггеры и покрытие. */
function tableAuditFinalize_(source, report, state) {
  tableAuditAnalyzeScript_(report, source.getId());
  var triggers = ScriptApp.getProjectTriggers();
  triggers.forEach(function(trigger) { report.getSheetByName('Скрипты').appendRow(['Низкий', 'Триггер', '', trigger.getHandlerFunction() + ' / ' + trigger.getEventType(), 'Частые триггеры могут суммировать нагрузку.', 'Проверить частоту и область обработки.', 'Видимы триггеры текущего пользователя.']); });
  report.getSheetByName('Скрипты').appendRow(['Информация', 'Метрики проекта', '', 'Пробуется Apps Script API projects.getMetrics за 7 дней.', 'Метрики относятся ко всему проекту и могут включать сам аудит.', 'При недоступности включить API и OAuth scope script.metrics.', 'Ограничено доступом API.']);
  var summary = report.getSheetByName('Сводка');
  summary.appendRow(['Формулы', state.counts.formulas, 'Подробности на вкладке «Формулы».']);
  summary.appendRow(['Ограничение', 'Эвристика', 'Найденные признаки требуют проверки замером до/после.']);
  report.getSheetByName('Покрытие').appendRow(['Таблица', 'Завершено', 'Все листы считаны блоками', 'Снимок не атомарный.']);
  report.getSheetByName('Покрытие').appendRow(['Скрипт', 'Частично', 'Доступен связанный проект и текущие триггеры', 'Другие проекты и триггеры других пользователей не видны.']);
}

/** Читает исходники связанного проекта через Apps Script API, если API доступен. */
function tableAuditAnalyzeScript_(report, spreadsheetId) {
  try {
    var token = ScriptApp.getOAuthToken();
    var url = 'https://script.googleapis.com/v1/projects/' + encodeURIComponent(ScriptApp.getScriptId()) + '/content';
    var response = tableAuditFetchWithRetry_(url, token);
    var files = JSON.parse(response.getContentText()).files || [];
    files.forEach(function(file) {
      if (file.type !== 'SERVER_JS' || !file.source || /TableAudit/i.test(file.name)) return;
      tableAuditScanSource_(report.getSheetByName('Скрипты'), file.name, file.source);
    });
  } catch (error) {
    report.getSheetByName('Скрипты').appendRow(['Информация', 'Apps Script API', '', 'Исходники не прочитаны', 'Статический анализ кода пропущен.', 'Включить Apps Script API и выдать разрешение проекту.', 'Продолжена диагностика таблицы.']);
  }
}

/** Выполняет эвристический анализ строк исходника без сохранения кода целиком. */
function tableAuditScanSource_(sheet, fileName, source) {
  var lines = source.split(/\r?\n/);
  var inComment = false;
  lines.forEach(function(line, index) {
    var clean = line.replace(/\/\/.*$/, '').replace(/\/\*[\s\S]*?\*\//g, '');
    if (!clean.trim()) return;
    var loop = /for\s*\(|while\s*\(/.test(clean);
    var expensive = /(getRange\([^)]*\)\.(getValue|getValues)|setValue|setValues|flush\(|UrlFetchApp\.fetch|create(Number|Time|Edit|Open)edTrigger)/.test(clean);
    if (loop && expensive) sheet.appendRow(['Высокий', fileName, index + 1, 'Операция таблицы/HTTP/триггера внутри цикла', 'Много обращений к сервису может привести к задержкам и квотам.', 'Собирать данные в массивы, выполнять batch-операции и ограничивать цикл.', 'Эвристика; проверить профилированием.']);
    if (/function\s+(onEdit|onOpen)\b/.test(clean)) sheet.appendRow(['Средний', fileName, index + 1, 'Обработчик ' + (clean.match(/function\s+(onEdit|onOpen)/) || [])[1], 'Обработчик запускается часто и может задерживать пользователя.', 'Ограничить область по e.range и вынести тяжёлую работу.', 'Эвристика; проверить сценарий редактирования.']);
  });
}

/** Выполняет ограниченные повторы URL-запроса и обрабатывает 429/5xx. */
function tableAuditFetchWithRetry_(url, token) {
  for (var attempt = 0; attempt < 3; attempt++) {
    var response = UrlFetchApp.fetch(url, { method: 'get', headers: { Authorization: 'Bearer ' + token }, muteHttpExceptions: true });
    var code = response.getResponseCode();
    if (code >= 200 && code < 300) return response;
    if (code !== 429 && code < 500) throw new Error('Apps Script API вернул HTTP ' + code);
    Utilities.sleep(Math.pow(2, attempt) * 1000);
  }
  throw new Error('Apps Script API временно недоступен после повторов.');
}

/** Маскирует строковые литералы перед помещением формулы в отчёт. */
function tableAuditMaskFormula_(formula) { return formula.replace(/"(?:[^"]|"")*"/g, '"…"'); }

/** Возвращает безопасное описание ошибки без токенов, URL и содержимого запросов. */
function tableAuditSafeErrorMessage_(error) {
  var message = error && error.message ? String(error.message) : 'Неизвестная ошибка сервиса Google.';
  return message.replace(/https?:\/\/\S+/g, '[URL скрыт]').slice(0, 500);
}

/**
 * Нормализует формулу для группировки повторяющихся строковых шаблонов.
 * Бизнес-правило: номера строк в ссылках считаются переменной частью формулы,
 * но числовые константы, даты и текстовые литералы не должны искажаться.
 */
function tableAuditNormalizeFormula_(formula) {
  var masked = tableAuditMaskFormula_(formula);
  return masked.replace(/(\$?[A-Z]{1,3}\$?)\d+/g, '$1#').replace(/(\$?)\d+:(\$?[A-Z]{1,3}\$?)\d+/g, '$1#:$2#');
}

/** Переводит номер столбца в буквенное обозначение. */
function tableAuditColumn_(number) { var result = ''; while (number > 0) { var rest = (number - 1) % 26; result = String.fromCharCode(65 + rest) + result; number = Math.floor((number - 1) / 26); } return result; }

