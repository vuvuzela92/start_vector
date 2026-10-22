/**
 * Аудитор производительности таблицы по ручному ID источника.
 * Скрипт только читает исходную таблицу и создаёт отдельный отчёт.
 */

var TABLE_AUDIT_PREFIX = 'TABLE_AUDIT_';
// Укажите ID проверяемой таблицы между /d/ и /edit в её URL.
var TABLE_AUDIT_SOURCE_ID = '';
var TABLE_AUDIT_SOURCE_PROPERTY = TABLE_AUDIT_PREFIX + 'SOURCE_ID';
var TABLE_AUDIT_CANCEL_PROPERTY = TABLE_AUDIT_PREFIX + 'CANCEL_REQUESTED';
var TABLE_AUDIT_CHUNK_CELLS = 20000;
var TABLE_AUDIT_TIME_LIMIT_MS = 4 * 60 * 1000;
var TABLE_AUDIT_MAX_EXAMPLES = 5;
var TABLE_AUDIT_TOP_GROUPS = 40;
var TABLE_AUDIT_API_CHUNK_ROWS = 5000;
var TABLE_AUDIT_CACHE_CHUNK_SIZE = 70000;

/** Добавляет меню аудитора без замены существующего обработчика onOpen. */
function tableAuditInstallMenu() {
  try {
    SpreadsheetApp.getUi().createMenu('Аудит производительности')
      .addItem('Начать аудит', 'tableAuditStart')
      .addItem('Аудит формул', 'tableAuditStartFormulas')
      .addItem('Аудит структуры', 'tableAuditStartStructure')
      .addItem('Аудит скриптов', 'tableAuditStartScripts')
      .addItem('Аудит метрик', 'tableAuditStartMetrics')
      .addItem('Продолжить аудит', 'tableAuditContinue')
      .addItem('Выбрать таблицу по ID', 'tableAuditOpenSettings')
      .addItem('Показать состояние', 'tableAuditStatus')
      .addItem('Отменить аудит', 'tableAuditCancel')
      .addToUi();
  } catch (error) {
    Logger.log('Меню недоступно в этом контексте. Запускайте функции напрямую: tableAuditStart или tableAuditContinue.');
  }
}

/** Запускает полный аудит таблицы. */
function tableAuditStart() { tableAuditStartMode_('full'); }

/** Запускает только аудит формул и их потенциальной нагрузки. */
function tableAuditStartFormulas() { tableAuditStartMode_('formulas'); }

/** Запускает только аудит структуры и оформления листов. */
function tableAuditStartStructure() { tableAuditStartMode_('structure'); }

/** Запускает только аудит Apps Script-кода и триггеров. */
function tableAuditStartScripts() { tableAuditStartMode_('scripts'); }

/** Запускает только аудит доступных метрик Apps Script-проекта. */
function tableAuditStartMetrics() { tableAuditStartMode_('metrics'); }

/** Открывает форму выбора таблицы без необходимости редактировать код. */
function tableAuditOpenSettings() {
  var currentId = tableAuditGetSourceId_();
  var html = HtmlService.createHtmlOutput(
    '<!doctype html><html><head><base target="_top"><style>' +
    'body{font:14px Arial,sans-serif;padding:16px;color:#202124}' +
    'input{box-sizing:border-box;width:100%;padding:8px;margin:8px 0 12px}' +
    'button{padding:8px 14px;background:#1a73e8;color:white;border:0;border-radius:4px;cursor:pointer}' +
    '#status{margin-top:12px;color:#5f6368;white-space:pre-wrap}' +
    '</style></head><body><b>Таблица для аудита</b>' +
    '<p>Вставьте ID из URL между <code>/d/</code> и <code>/edit</code>.</p>' +
    '<input id="sourceId" value="' + tableAuditEscapeHtml_(currentId) + '" placeholder="ID или полный URL Google-таблицы">' +
    '<button id="saveButton" type="button" onclick="save()">Сохранить</button><div id="status" role="status"></div>' +
    '<script>function save(){var input=document.getElementById("sourceId");var button=document.getElementById("saveButton");var status=document.getElementById("status");var id=input.value.trim();if(!id){status.textContent="Укажите ID или URL таблицы.";input.focus();return;}button.disabled=true;button.textContent="Проверяю…";status.textContent="Проверяю доступ к таблице…";google.script.run.withSuccessHandler(function(msg){button.disabled=false;button.textContent="Сохранить";status.textContent=msg;}).withFailureHandler(function(err){button.disabled=false;button.textContent="Сохранить";status.textContent="Ошибка: "+(err&&err.message?err.message:String(err));}).tableAuditSaveSourceId(id);}</script>' +
    '</body></html>'
  ).setWidth(420).setHeight(220);
  try {
    SpreadsheetApp.getUi().showModalDialog(html, 'Настройка аудита');
  } catch (error) {
    Logger.log('Форма доступна только из связанной Google-таблицы или её меню.');
  }
}

/** Сохраняет ID таблицы после проверки доступности документа. */
function tableAuditSaveSourceId(sourceId) {
  var normalizedId = tableAuditExtractSourceId_(sourceId);
  if (!/^[A-Za-z0-9_-]{20,}$/.test(normalizedId)) throw new Error('Похоже, ID таблицы указан неверно.');
  var source = SpreadsheetApp.openById(normalizedId);
  PropertiesService.getUserProperties().setProperty(TABLE_AUDIT_SOURCE_PROPERTY, normalizedId);
  return 'Сохранено: «' + source.getName() + '». Теперь можно запускать аудит.';
}

/** Извлекает ID из отдельного значения или полного URL таблицы для сценария выбора источника. */
function tableAuditExtractSourceId_(sourceId) {
  var value = String(sourceId || '').trim();
  var match = value.match(/\/spreadsheets\/d\/([A-Za-z0-9_-]+)/);
  return match ? match[1] : value;
}

/** Возвращает сохранённый ID или значение из конфигурации для первого запуска. */
function tableAuditGetSourceId_() {
  return PropertiesService.getUserProperties().getProperty(TABLE_AUDIT_SOURCE_PROPERTY) || TABLE_AUDIT_SOURCE_ID;
}

/** Экранирует ID перед вставкой в HTML-форму. */
function tableAuditEscapeHtml_(value) {
  return String(value || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

/** Создаёт состояние выбранного режима и запускает его первый этап. */
function tableAuditStartMode_(mode) {
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(1000)) throw new Error('Аудит уже выполняется другим запуском.');
  var shouldContinue = false;
  try {
    var sourceId = tableAuditGetSourceId_();
    if (!sourceId) throw new Error('Сначала выберите таблицу через «Выбрать таблицу по ID».');
    var source = SpreadsheetApp.openById(sourceId);
    var report = SpreadsheetApp.create('Аудит производительности: ' + source.getName());
    tableAuditPrepareReport_(report, source);
    var state = tableAuditInitialState_(source, report, mode);
    PropertiesService.getUserProperties().deleteProperty(TABLE_AUDIT_CANCEL_PROPERTY);
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
    if (state.status === 'Отменён') throw new Error('Этот аудит отменён. Запустите новый аудит.');
    var source = SpreadsheetApp.openById(state.sourceId);
    var report = SpreadsheetApp.openById(state.reportId);
    if (state.mode === 'scripts' || state.mode === 'metrics') {
      tableAuditFinalize_(source, report, state);
      state.status = 'Завершён';
      tableAuditSaveState_(state);
      tableAuditNotify_('Аудит «' + state.mode + '» завершён. Отчёт: ' + report.getUrl());
      return;
    }
    if (state.mode === 'formulas') {
      tableAuditContinueFormulaApi_(source, report, state);
      return;
    }
    var started = Date.now();
    state._deadline = started + TABLE_AUDIT_TIME_LIMIT_MS;
    try {
      while (state.sheetIndex < state.sheetNames.length && Date.now() - started < TABLE_AUDIT_TIME_LIMIT_MS) {
        if (tableAuditCancellationRequested_(state)) { state.status = 'Отменён'; tableAuditSaveState_(state); tableAuditNotify_('Аудит отменён после завершения текущего блока.'); return; }
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

/** Продолжает быстрый аудит формул через Sheets REST API большими блоками. */
function tableAuditContinueFormulaApi_(source, report, state) {
  var started = Date.now();
  state._deadline = started + TABLE_AUDIT_TIME_LIMIT_MS;
  try {
    while (state.sheetIndex < state.sheetNames.length && Date.now() < state._deadline) {
      if (tableAuditCancellationRequested_(state)) { state.status = 'Отменён'; tableAuditSaveState_(state); tableAuditNotify_('Аудит формул отменён после завершения текущего блока.'); return; }
      var sheet = source.getSheetByName(state.sheetNames[state.sheetIndex]);
      if (!sheet) { state.sheetIndex++; state.row = 1; continue; }
      var lastRow = Math.max(1, sheet.getLastRow());
      var lastCol = Math.max(1, sheet.getLastColumn());
      while (state.row <= lastRow && Date.now() < state._deadline) {
        var endRow = Math.min(lastRow, state.row + TABLE_AUDIT_API_CHUNK_ROWS - 1);
        var response = tableAuditSheetsApi_('/values/' + encodeURIComponent("'" + sheet.getName() + "'!" + state.row + ':' + endRow), { valueRenderOption: 'FORMULA' }, state.sourceId);
        tableAuditAggregateFormulaRows_(sheet.getName(), state.row, response.values || [], state);
        state.row = endRow + 1;
        tableAuditSaveState_(state);
      }
      if (state.row > lastRow) {
        tableAuditWriteSheetSummary_(sheet, report, state);
        state.sheetIndex++;
        state.row = 1;
        tableAuditSaveState_(state);
      }
    }
    if (state.sheetIndex >= state.sheetNames.length) {
      tableAuditFinalize_(source, report, state);
      state.status = 'Завершён';
      tableAuditSaveState_(state);
      tableAuditNotify_('Аудит формул завершён. Отчёт: ' + report.getUrl());
    } else {
      tableAuditNotify_('Сохранён прогресс аудита формул. Запустите tableAuditContinue.\nОтчёт: ' + report.getUrl());
    }
  } catch (error) {
    state.status = 'Ошибка с сохранённым прогрессом';
    state.lastError = tableAuditSafeErrorMessage_(error);
    tableAuditSaveState_(state);
    report.getSheetByName('Покрытие').appendRow(['Формулы', 'Ошибка', 'Лист: ' + state.sheetNames[state.sheetIndex] + '; строка: ' + state.row, state.lastError]);
    throw new Error('Аудит формул остановлен на листе «' + state.sheetNames[state.sheetIndex] + '», строка ' + state.row + '. ' + state.lastError);
  }
}

/** Агрегирует формулы REST-блока без обращений к вкладке отчёта. */
function tableAuditAggregateFormulaRows_(sheetName, startRow, rows, state) {
  state.groupStats = state.groupStats || {};
  state.sheetStats = state.sheetStats || {};
  var sheetStats = state.sheetStats[sheetName] || { formulas: 0, columns: {} };
  rows.forEach(function(row, rowOffset) {
    row.forEach(function(formula, colOffset) {
      if (typeof formula !== 'string' || formula.charAt(0) !== '=') return;
      var rowNumber = startRow + rowOffset;
      var colNumber = colOffset + 1;
      var normalized = tableAuditNormalizeFormula_(formula);
      var columnStats = sheetStats.columns[colOffset] || { count: 0, first: rowNumber, last: rowNumber, patterns: {} };
      columnStats.count++;
      columnStats.first = Math.min(columnStats.first, rowNumber);
      columnStats.last = Math.max(columnStats.last, rowNumber);
      columnStats.patterns[normalized] = (columnStats.patterns[normalized] || 0) + 1;
      sheetStats.columns[colOffset] = columnStats;
      sheetStats.formulas++;
      state.counts.formulas++;
      var key = sheetName + '\u0000' + normalized;
      var group = state.groupStats[key] || { sheet: sheetName, formula: formula, normalized: normalized, count: 0, minRow: rowNumber, maxRow: rowNumber, minCol: colNumber, maxCol: colNumber, score: 0, flags: {} };
      group.count++;
      group.minRow = Math.min(group.minRow, rowNumber);
      group.maxRow = Math.max(group.maxRow, rowNumber);
      group.minCol = Math.min(group.minCol, colNumber);
      group.maxCol = Math.max(group.maxCol, colNumber);
      var analysis = tableAuditScoreFormula_(formula, sheetName, state);
      group.score += analysis.score;
      analysis.flags.forEach(function(flag) { group.flags[flag] = true; });
      state.groupStats[key] = group;
    });
  });
  state.sheetStats[sheetName] = sheetStats;
}

/** Выполняет запрос к Sheets API с повтором временных ошибок и HTTP 429. */
function tableAuditSheetsApi_(path, params, sourceId) {
  var url = 'https://sheets.googleapis.com/v4/spreadsheets/' + encodeURIComponent(sourceId || tableAuditGetSourceId_()) + path;
  var query = Object.keys(params || {}).map(function(key) { return encodeURIComponent(key) + '=' + encodeURIComponent(params[key]); }).join('&');
  if (query) url += '?' + query;
  var lastMessage = 'неизвестная ошибка Sheets API';
  for (var attempt = 0; attempt < 5; attempt++) {
    var response = UrlFetchApp.fetch(url, { method: 'get', headers: { Authorization: 'Bearer ' + ScriptApp.getOAuthToken() }, muteHttpExceptions: true });
    var code = response.getResponseCode();
    if (code >= 200 && code < 300) return JSON.parse(response.getContentText());
    lastMessage = 'Sheets API HTTP ' + code;
    if (code !== 429 && code < 500) throw new Error(lastMessage);
    Utilities.sleep(code === 429 ? 30000 : Math.pow(2, attempt) * 1000);
  }
  throw new Error(lastMessage + ' после 5 попыток');
}

/** Показывает пользователю состояние текущего аудита. */
function tableAuditStatus() {
  var state = tableAuditLoadState_();
  if (!state) { tableAuditNotify_('Активный аудит не найден.'); return; }
  tableAuditNotify_('Статус: ' + state.status + '\nРежим: ' + (state.mode || 'full') + '\nЛист: ' + (state.sheetIndex + 1) + ' из ' + state.sheetNames.length + '\nОтчёт: ' + state.reportUrl);
}

/** Помечает текущий аудит отменённым после завершения уже начатого блока. */
function tableAuditCancel() {
  var properties = PropertiesService.getUserProperties();
  var state = tableAuditLoadState_();
  if (!state) { Logger.log('Активный аудит не найден.'); return; }
  state.status = 'Отмена запрошена';
  tableAuditSaveState_(state);
  properties.setProperty(TABLE_AUDIT_CANCEL_PROPERTY, state.runId || '1');
  Logger.log('Запрос отмены сохранён. Текущий блок завершится, после чего аудит остановится.');
}

/** Показывает уведомление через UI, а при его недоступности пишет его в журнал. */
function tableAuditNotify_(message) {
  Logger.log(message);
  try { SpreadsheetApp.getActiveSpreadsheet().toast(message, 'Аудит таблицы', 10); } catch (toastError) { Logger.log('Уведомление UI недоступно.'); }
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
function tableAuditInitialState_(source, report, mode) {
  return { runId: Utilities.getUuid(), status: 'Выполняется', mode: mode || 'full', sourceId: source.getId(), reportId: report.getId(), reportUrl: report.getUrl(), sheetNames: source.getSheets().map(function(s) { return s.getName(); }), sheetIndex: 0, row: 1, counts: { formulas: 0, nonEmpty: 0, blanks: 0, findings: 0 }, sheetStats: {}, groupStats: {}, startedAt: new Date().toISOString() };
}

/** Сохраняет компактный курсор и агрегаты частями, чтобы большой аудит продолжался без превышения лимита кэша. */
function tableAuditSaveState_(state) {
  var cache = CacheService.getUserCache();
  var aggregate = JSON.stringify({ sheetStats: state.sheetStats || {}, groupStats: state.groupStats || {} });
  var previousCount = Number(state.aggregateChunkCount || 0);
  if (previousCount > 0) {
    var oldKeys = [];
    for (var oldIndex = 0; oldIndex < previousCount; oldIndex++) oldKeys.push(TABLE_AUDIT_PREFIX + 'AGGREGATE_' + state.runId + '_' + oldIndex);
    cache.removeAll(oldKeys);
  }
  var chunkCount = Math.max(1, Math.ceil(aggregate.length / TABLE_AUDIT_CACHE_CHUNK_SIZE));
  var chunks = {};
  for (var index = 0; index < chunkCount; index++) {
    chunks[TABLE_AUDIT_PREFIX + 'AGGREGATE_' + state.runId + '_' + index] = aggregate.slice(index * TABLE_AUDIT_CACHE_CHUNK_SIZE, (index + 1) * TABLE_AUDIT_CACHE_CHUNK_SIZE);
  }
  cache.putAll(chunks, 21600);
  var compact = {};
  Object.keys(state).forEach(function(key) { if (key !== 'sheetStats' && key !== 'groupStats' && key !== '_deadline') compact[key] = state[key]; });
  compact.aggregateChunkCount = chunkCount;
  PropertiesService.getUserProperties().setProperty(TABLE_AUDIT_PREFIX + 'STATE', JSON.stringify(compact));
}

/** Читает состояние текущего пользователя. */
function tableAuditLoadState_() {
  var raw = PropertiesService.getUserProperties().getProperty(TABLE_AUDIT_PREFIX + 'STATE');
  if (!raw) return null;
  var state = JSON.parse(raw);
  var cache = CacheService.getUserCache();
  var chunkCount = Number(state.aggregateChunkCount || 0);
  var aggregate = '';
  if (chunkCount > 0) {
    var keys = [];
    for (var index = 0; index < chunkCount; index++) keys.push(TABLE_AUDIT_PREFIX + 'AGGREGATE_' + state.runId + '_' + index);
    var chunks = cache.getAll(keys);
    for (var chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++) aggregate += chunks[keys[chunkIndex]] || '';
  }
  if (aggregate) {
    var parsed = JSON.parse(aggregate);
    state.sheetStats = parsed.sheetStats || {};
    state.groupStats = parsed.groupStats || {};
  } else {
    state.sheetStats = state.sheetStats || {};
    state.groupStats = state.groupStats || {};
  }
  return state;
}

/** Проверяет запрос отмены именно для текущего запуска. */
function tableAuditCancellationRequested_(state) {
  return PropertiesService.getUserProperties().getProperty(TABLE_AUDIT_CANCEL_PROPERTY) === (state.runId || '1');
}

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
    if (state.mode === 'full' || state.mode === 'formulas') {
      tableAuditAnalyzeBlock_(sheet, report, row, formulas, values, state);
    }
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
  state.sheetStats = state.sheetStats || {};
  state.groupStats = state.groupStats || {};
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
    var groupKey = sheet.getName() + '\u0000' + normalized;
    var group = state.groupStats[groupKey] || { sheet: sheet.getName(), formula: formula, normalized: normalized, count: 0, minRow: startRow + r, maxRow: startRow + r, minCol: c + 1, maxCol: c + 1, score: 0, flags: {} };
    group.count++;
    group.minRow = Math.min(group.minRow, startRow + r);
    group.maxRow = Math.max(group.maxRow, startRow + r);
    group.minCol = Math.min(group.minCol, c + 1);
    group.maxCol = Math.max(group.maxCol, c + 1);
    var groupAnalysis = tableAuditScoreFormula_(formula, sheet.getName(), state);
    group.score += groupAnalysis.score;
    groupAnalysis.flags.forEach(function(flag) { group.flags[flag] = true; });
    state.groupStats[groupKey] = group;
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

/**
 * Оценивает одну формулу по признакам потенциальной нагрузки.
 * Бизнес-правило: балл помогает ранжировать повторяющиеся группы, но не
 * заменяет замер фактического времени пересчёта в Google Sheets.
 */
function tableAuditScoreFormula_(formula, sheetName, state) {
  var outside = tableAuditMaskFormula_(formula).toUpperCase();
  var score = 1;
  var flags = [];
  if (/IMPORTRANGE|IMPORTDATA|IMPORTHTML|IMPORTXML|IMPORTFEED|GOOGLEFINANCE/.test(outside)) { score += 20; flags.push('внешний источник'); }
  if (/NOW\(|TODAY\(|RAND\(|RANDBETWEEN\(/.test(outside)) { score += 5; flags.push('волатильная функция'); }
  if (/INDIRECT\(|OFFSET\(/.test(outside)) { score += 5; flags.push('динамическая ссылка'); }
  var heavy = (outside.match(/QUERY\(|FILTER\(|SORT\(|VLOOKUP\(|XLOOKUP\(|MATCH\(|SUMIF\(|SUMIFS\(|COUNTIF\(|COUNTIFS\(|SUMPRODUCT\(/g) || []).length;
  if (heavy) { score += heavy * 2; flags.push('тяжёлая функция ×' + heavy); }
  var openRanges = (outside.match(/(?:[A-Z]{1,3}:[A-Z]{1,3}|[0-9]+:[0-9]+)/g) || []).length;
  if (openRanges) { score += openRanges * 3; flags.push('открытый диапазон'); }
  return { score: score, flags: flags };
}

/** Переводит оценочный балл группы в понятный приоритет отчёта. */
function tableAuditPriority_(score) {
  if (score >= 1000) return 'Высокий';
  if (score >= 200) return 'Средний';
  return 'Низкий';
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
  if (state.mode === 'full' || state.mode === 'formulas') Object.keys(stats.columns).forEach(function(columnIndex) {
    var column = stats.columns[columnIndex];
    var patterns = Object.keys(column.patterns).sort(function(a, b) { return column.patterns[b] - column.patterns[a]; }).slice(0, 3);
    var address = sheet.getName() + '!' + tableAuditColumn_(Number(columnIndex) + 1);
    tableAuditFinding_(report.getSheetByName('Формулы'), ['Информация', sheet.getName(), address, 'Формул: ' + column.count + '; строки ' + column.first + '–' + column.last + '; шаблоны: ' + patterns.map(function(pattern) { return '×' + column.patterns[pattern] + ' ' + tableAuditMaskFormula_(pattern); }).join(' | '), 'Повторяющиеся формулы показывают объём однотипного пересчёта.', 'Проверить, можно ли заменить повторяющиеся расчёты одной массивной формулой или предварительным расчётом.', 'Сравнить число формул и время пересчёта.']);
  });
  var groups = Object.keys(state.groupStats || {}).map(function(key) { return state.groupStats[key]; })
    .filter(function(group) { return group.sheet === sheet.getName(); })
    .sort(function(a, b) { return b.score - a.score; })
    .slice(0, TABLE_AUDIT_TOP_GROUPS);
  if (state.mode === 'full' || state.mode === 'formulas') groups.forEach(function(group) {
    var groupAddress = sheet.getName() + '!' + tableAuditColumn_(group.minCol) + group.minRow + ':' + tableAuditColumn_(group.maxCol) + group.maxRow;
    tableAuditFinding_(report.getSheetByName('Формулы'), [tableAuditPriority_(group.score), sheet.getName(), groupAddress, 'Группа формул: ' + group.count + ' ячеек; оценочный балл ' + group.score + '; ' + tableAuditMaskFormula_(group.formula), 'Оценка учитывает количество ячеек и признаки тяжёлых функций/диапазонов.', 'Проверить группу как единый расчёт и ограничить диапазоны.', 'Сравнить время пересчёта до и после оптимизации.']);
  });
  if (state.mode !== 'formulas') {
  var rules = sheet.getConditionalFormatRules();
  if (rules.length > 0) report.getSheetByName('Оформление').appendRow(['Низкий', sheet.getName(), 'Условное форматирование', 'Правил: ' + rules.length, 'Широкие или перекрывающиеся правила могут увеличивать обработку.', 'Сузить диапазоны и удалить дубли.', 'Сравнить скорость открытия и редактирования.']);
  var validations = sheet.getDataRange().getDataValidations();
  var validationCount = validations.reduce(function(total, row) { return total + row.filter(function(v) { return v !== null; }).length; }, 0);
  if (validationCount > 0) report.getSheetByName('Оформление').appendRow(['Низкий', sheet.getName(), 'Проверка данных', 'Ячеек: ' + validationCount, 'Большое число проверок увеличивает обработку ввода.', 'Ограничить проверки реальными диапазонами.', 'Сравнить задержку ввода.']);
  if (sheet.getCharts().length > 0) report.getSheetByName('Оформление').appendRow(['Низкий', sheet.getName(), 'Диаграммы', 'Количество: ' + sheet.getCharts().length, 'Диаграммы на широких диапазонах могут замедлять обновление.', 'Проверить диапазоны и частоту обновления.', 'Сравнить время открытия.']);
  var merged = sheet.getDataRange().getMergedRanges();
  if (merged.length > 0) report.getSheetByName('Оформление').appendRow(['Низкий', sheet.getName(), 'Объединённые ячейки', 'Диапазонов: ' + merged.length, 'Большое число объединений усложняет массовую обработку.', 'Оставить объединения только для заголовков.', 'Проверить операции вставки и сортировки.']);
  }
}

/** Завершает отчёт: анализирует связанный проект, триггеры и покрытие. */
function tableAuditFinalize_(source, report, state) {
  if (state.mode === 'full' || state.mode === 'scripts') {
    tableAuditAnalyzeScript_(report, source.getId());
    var triggers = ScriptApp.getProjectTriggers();
    triggers.forEach(function(trigger) { report.getSheetByName('Скрипты').appendRow(['Низкий', 'Триггер', '', trigger.getHandlerFunction() + ' / ' + trigger.getEventType(), 'Частые триггеры могут суммировать нагрузку.', 'Проверить частоту и область обработки.', 'Видимы триггеры текущего пользователя.']); });
  }
  if (state.mode === 'full' || state.mode === 'metrics') {
    tableAuditWriteMetrics_(report, state);
  }
  var summary = report.getSheetByName('Сводка');
  summary.appendRow(['Формулы', state.counts.formulas, 'Подробности на вкладке «Формулы».']);
  summary.appendRow(['Ограничение', 'Эвристика', 'Найденные признаки требуют проверки замером до/после.']);
  report.getSheetByName('Покрытие').appendRow(['Таблица', 'Завершено', 'Все листы считаны блоками', 'Снимок не атомарный.']);
  report.getSheetByName('Покрытие').appendRow(['Скрипт', 'Частично', 'Доступен связанный проект и текущие триггеры', 'Другие проекты и триггеры других пользователей не видны.']);
}

/** Получает дневные метрики проекта Apps Script и записывает их в отчёт. */
function tableAuditWriteMetrics_(report, state) {
  var sheet = report.getSheetByName('Скрипты');
  try {
    var url = 'https://script.googleapis.com/v1/projects/' + encodeURIComponent(ScriptApp.getScriptId()) + '/metrics?metricsGranularity=DAILY';
    var response = tableAuditFetchWithRetry_(url, ScriptApp.getOAuthToken());
    var metrics = JSON.parse(response.getContentText());
    ['totalExecutions', 'failedExecutions', 'activeUsers'].forEach(function(metricName) {
      (metrics[metricName] || []).forEach(function(item) {
        sheet.appendRow(['Информация', 'Метрика ' + metricName, '', item.value + ' (' + item.startTime + ' — ' + item.endTime + ')', 'Метрики относятся ко всему проекту и могут включать сам аудит.', 'Сопоставить пики запусков с триггерами и изменениями таблицы.', 'Доступно через Apps Script API.']);
      });
    });
    if (!metrics.totalExecutions && !metrics.failedExecutions && !metrics.activeUsers) sheet.appendRow(['Информация', 'Метрики проекта', '', 'За 7 дней данные не возвращены.', 'Метрики могут быть недоступны для проекта или пользователя.', 'Проверить OAuth scope script.metrics.', 'Ответ API пуст.']);
  } catch (error) {
    sheet.appendRow(['Информация', 'Метрики проекта', '', 'Не удалось получить дневные метрики.', 'Метрики относятся ко всему проекту и могут включать сам аудит.', 'Включить Apps Script API и выдать scope script.metrics.', 'Продолжена диагностика остальных разделов.']);
  }
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

