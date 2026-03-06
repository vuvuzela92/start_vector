/**
 * Добавляет 4 новые колонки на лист "ВБ Финанс (auto)",
 * копируя формулы и форматы из 4-х предыдущих колонок.
 */
function addFourColumnsWithFormulas() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName("ВБ Финанс (auto)");
  
  if (!sheet) {
    SpreadsheetApp.getUi().alert("Лист 'ВБ Финанс (auto)' не найден!");
    return;
  }

  const lastCol = sheet.getLastColumn();
  const lastRow = sheet.getLastRow();
  
  // Если в таблице меньше 4 колонок, копировать нечего
  if (lastCol < 4) {
    SpreadsheetApp.getUi().alert("Недостаточно колонок для копирования.");
    return;
  }

  // 1. Вставляем 4 пустые колонки справа
  sheet.insertColumnsAfter(lastCol, 4);
  
  // 2. Определяем диапазон-источник (последние 4 колонки)
  // getRange(row, column, numRows, numColumns)
  const sourceRange = sheet.getRange(1, lastCol - 3, lastRow, 4);
  
  // 3. Определяем целевой диапазон (новые 4 колонки)
  const targetRange = sheet.getRange(1, lastCol + 1, lastRow, 4);
  
  // 4. Копируем всё: формулы, форматы и валидацию данных
  // Ссылки в формулах (например, =A1+B1) изменятся автоматически (относительная адресация)
  sourceRange.copyTo(targetRange);
  
  // Опционально: прокручиваем лист к новым колонкам
  sheet.activate();
  targetRange.activate();
}