// Функция для подсчета данных о количестве ячеек, существующих в гугл-таблице
function countCellsInSpreadsheet() {
  const sheets = SpreadsheetApp.getActiveSpreadsheet().getSheets();
  let totalCells = 0;

  // Подсчёт общего количества ячеек
  sheets.forEach(sheet => {
    const rows = sheet.getMaxRows();
    const cols = sheet.getMaxColumns();
    totalCells += rows * cols;
  });

  Logger.log(`Общее количество ячеек: ${totalCells}`);
  
  // Запись результата в ячейку L2 на вкладке "Расчет пополнения"
  const sheetName = "Админка"; // Название вкладки
  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = spreadsheet.getSheetByName(sheetName);

  if (!sheet) {
    Logger.log(`Лист с именем "${sheetName}" не найден.`);
    return;
  }

  sheet.getRange("A2").setValue(totalCells);

  return totalCells;
}