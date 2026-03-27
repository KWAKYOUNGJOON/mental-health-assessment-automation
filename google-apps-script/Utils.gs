function getSpreadsheet_() {
  return SpreadsheetApp.getActiveSpreadsheet();
}

function getSheetIfExists_(sheetName) {
  return getSpreadsheet_().getSheetByName(sheetName);
}

function getOrCreateSheet_(sheetName) {
  const spreadsheet = getSpreadsheet_();
  return spreadsheet.getSheetByName(sheetName) || spreadsheet.insertSheet(sheetName);
}

function getManagedSheetNames_() {
  const counselingAiSheetName = getSettingValue_(
    "counseling_ai_output_sheet_name",
    APP_CONFIG.sheetNames.counselingAi
  );

  return [
    APP_CONFIG.sheetNames.base,
    APP_CONFIG.sheetNames.hospital,
    APP_CONFIG.sheetNames.facility,
    APP_CONFIG.sheetNames.service,
    APP_CONFIG.sheetNames.publicService,
    APP_CONFIG.sheetNames.counselingAi,
    counselingAiSheetName,
    APP_CONFIG.sheetNames.error,
    APP_CONFIG.sheetNames.dashboard,
    APP_CONFIG.sheetNames.settings,
    APP_CONFIG.sheetNames.monthly
  ].filter(function(value, index, values) {
    return value && values.indexOf(value) === index;
  });
}

function getSheetMap() {
  const settingsSheet = getOrCreateSheet_(APP_CONFIG.sheetNames.settings);
  ensureSettingsSheet_(settingsSheet);

  return {
    rawInput: resolveRawInputSheet_(),
    base: getOrCreateSheet_(APP_CONFIG.sheetNames.base),
    hospital: getOrCreateSheet_(APP_CONFIG.sheetNames.hospital),
    facility: getOrCreateSheet_(APP_CONFIG.sheetNames.facility),
    service: getOrCreateSheet_(APP_CONFIG.sheetNames.service),
    publicService: getOrCreateSheet_(APP_CONFIG.sheetNames.publicService),
    counselingAi: getOrCreateSheet_(
      getSettingValue_("counseling_ai_output_sheet_name", APP_CONFIG.sheetNames.counselingAi)
    ),
    monthly: getOrCreateSheet_(APP_CONFIG.sheetNames.monthly),
    error: getOrCreateSheet_(APP_CONFIG.sheetNames.error),
    dashboard: getOrCreateSheet_(APP_CONFIG.sheetNames.dashboard),
    settings: settingsSheet
  };
}

function ensureRawInputSheetForSetup_() {
  const spreadsheet = getSpreadsheet_();
  const configuredName = getSettingValue_("raw_input_sheet_name", APP_CONFIG.sheetNames.rawInput);
  let sheet = spreadsheet.getSheetByName(configuredName);

  if (!sheet && configuredName !== APP_CONFIG.sheetNames.rawInput) {
    sheet = spreadsheet.getSheetByName(APP_CONFIG.sheetNames.rawInput);
  }

  if (!sheet) {
    sheet = findExistingRawInputSheetForSetup_();
    if (sheet) {
      setSettingValue_("raw_input_sheet_name", sheet.getName(), "초기 셋업에서 감지한 원본 입력 시트");
    }
  }

  if (!sheet) {
    sheet = findBlankRawInputSheetForSetup_();
    if (sheet) {
      sheet.setName(APP_CONFIG.sheetNames.rawInput);
    } else {
      sheet = spreadsheet.insertSheet(APP_CONFIG.sheetNames.rawInput);
    }
    setSettingValue_("raw_input_sheet_name", sheet.getName(), "원본 입력 시트 이름");
  }

  return {
    sheet: sheet,
    seededTemplate: ensureRawInputTemplate_(sheet)
  };
}

function ensureManagedOutputSheetsForSetup_(sheetMap) {
  const seededSheets = [];
  [
    { sheet: sheetMap.base, name: APP_CONFIG.sheetNames.base, headers: APP_CONFIG.headers.base },
    { sheet: sheetMap.hospital, name: APP_CONFIG.sheetNames.hospital, headers: APP_CONFIG.headers.hospital },
    { sheet: sheetMap.facility, name: APP_CONFIG.sheetNames.facility, headers: APP_CONFIG.headers.facility },
    { sheet: sheetMap.service, name: APP_CONFIG.sheetNames.service, headers: APP_CONFIG.headers.service },
    { sheet: sheetMap.publicService, name: APP_CONFIG.sheetNames.publicService, headers: APP_CONFIG.headers.publicService },
    { sheet: sheetMap.counselingAi, name: sheetMap.counselingAi.getName(), headers: APP_CONFIG.headers.counselingAi },
    { sheet: sheetMap.monthly, name: APP_CONFIG.sheetNames.monthly, headers: APP_CONFIG.headers.monthly },
    { sheet: sheetMap.error, name: APP_CONFIG.sheetNames.error, headers: APP_CONFIG.headers.error }
  ].forEach(function(spec) {
    if (ensureSheetHeaderRows_(spec.sheet, spec.headers)) {
      seededSheets.push(spec.name);
    }
  });

  return seededSheets;
}

function resolveRawInputSheet_() {
  const spreadsheet = getSpreadsheet_();
  const managedNames = getManagedSheetNames_();
  const configuredName = getSettingValue_("raw_input_sheet_name", APP_CONFIG.sheetNames.rawInput);
  const candidates = dedupeStrings_([configuredName].concat(APP_CONFIG.rawSheetFallbackNames));

  for (let index = 0; index < candidates.length; index += 1) {
    const sheetName = candidates[index];
    if (!sheetName || managedNames.indexOf(sheetName) !== -1) {
      continue;
    }

    const sheet = spreadsheet.getSheetByName(sheetName);
    if (sheet) {
      return sheet;
    }
  }

  const fallbackSheet = spreadsheet.getSheets().filter(function(sheet) {
    return managedNames.indexOf(sheet.getName()) === -1;
  })[0];

  if (fallbackSheet) {
    return fallbackSheet;
  }

  return spreadsheet.insertSheet(APP_CONFIG.sheetNames.rawInput);
}

function findExistingRawInputSheetForSetup_() {
  const spreadsheet = getSpreadsheet_();
  const managedNames = getManagedSheetNames_();
  const candidateSheets = spreadsheet.getSheets().filter(function(sheet) {
    return managedNames.indexOf(sheet.getName()) === -1;
  });
  const preferredNames = dedupeStrings_([APP_CONFIG.sheetNames.rawInput].concat(APP_CONFIG.rawSheetFallbackNames));

  for (let index = 0; index < preferredNames.length; index += 1) {
    const preferredSheet = spreadsheet.getSheetByName(preferredNames[index]);
    if (preferredSheet && managedNames.indexOf(preferredSheet.getName()) === -1 && !isSheetEffectivelyBlank_(preferredSheet)) {
      return preferredSheet;
    }
  }

  for (let index = 0; index < candidateSheets.length; index += 1) {
    if (!isSheetEffectivelyBlank_(candidateSheets[index])) {
      return candidateSheets[index];
    }
  }

  return null;
}

function findBlankRawInputSheetForSetup_() {
  const spreadsheet = getSpreadsheet_();
  const managedNames = getManagedSheetNames_();
  const preferredNames = dedupeStrings_(APP_CONFIG.rawSheetFallbackNames);

  for (let index = 0; index < preferredNames.length; index += 1) {
    const preferredSheet = spreadsheet.getSheetByName(preferredNames[index]);
    if (preferredSheet && managedNames.indexOf(preferredSheet.getName()) === -1 && isSheetEffectivelyBlank_(preferredSheet)) {
      return preferredSheet;
    }
  }

  const blankSheet = spreadsheet.getSheets().filter(function(sheet) {
    return managedNames.indexOf(sheet.getName()) === -1 && isSheetEffectivelyBlank_(sheet);
  })[0];

  return blankSheet || null;
}

function ensureSettingsSheet_(sheet) {
  const isSeeded =
    sheet.getRange("A1").getDisplayValue() === "구분" &&
    sheet.getRange("F1").getDisplayValue() === "코드그룹";
  if (!isSeeded) {
    sheet.clear();
    sheet.setFrozenRows(1);
    sheet.getRange(1, 1, 1, 4).setValues([["구분", "키", "값", "설명"]]);
    styleHeaderRow_(sheet, 1, 4);

    sheet.getRange(1, 6, 1, 4).setValues([["코드그룹", "코드값", "활성", "설명"]]);
    styleCustomHeaderRow_(sheet, 1, 6, 4);
  }

  ensureMissingSettingsOptionRows_(sheet);
  ensureMissingSettingsCodeRows_(sheet);

  sheet.setColumnWidth(1, 110);
  sheet.setColumnWidth(2, 180);
  sheet.setColumnWidth(3, 180);
  sheet.setColumnWidth(4, 420);
  sheet.setColumnWidth(6, 130);
  sheet.setColumnWidth(7, 170);
  sheet.setColumnWidth(8, 70);
  sheet.setColumnWidth(9, 280);
}

function ensureMissingSettingsOptionRows_(sheet) {
  const existingKeys = {};
  const lastRow = sheet.getLastRow();

  if (lastRow >= 2) {
    const values = sheet.getRange(2, 1, lastRow - 1, 4).getDisplayValues();
    values.forEach(function(row) {
      const key = normalizeText_(row[1]);
      if (key) {
        existingKeys[key] = true;
      }
    });
  }

  const rowsToAppend = APP_CONFIG.settingsOptions.filter(function(option) {
    return !existingKeys[option.key];
  }).map(function(option) {
    return [option.section, option.key, option.defaultValue, option.description];
  });

  if (!rowsToAppend.length) {
    return;
  }

  const startRow = Math.max(sheet.getLastRow(), 1) + 1;
  sheet.getRange(startRow, 1, rowsToAppend.length, 4).setValues(rowsToAppend);
}

function ensureMissingSettingsCodeRows_(sheet) {
  const existingKeys = {};
  const lastRow = sheet.getLastRow();

  if (lastRow >= 2 && sheet.getLastColumn() >= 7) {
    const values = sheet.getRange(2, 6, lastRow - 1, 2).getDisplayValues();
    values.forEach(function(row) {
      const groupName = normalizeText_(row[0]);
      const codeValue = normalizeText_(row[1]);
      if (groupName && codeValue) {
        existingKeys[groupName + "::" + codeValue] = true;
      }
    });
  }

  const rowsToAppend = [];
  APP_CONFIG.settingsCodeGroups.forEach(function(group) {
    group.values.forEach(function(item) {
      const compositeKey = group.name + "::" + item.value;
      if (!existingKeys[compositeKey]) {
        rowsToAppend.push([group.name, item.value, "Y", item.description || ""]);
      }
    });
  });

  if (!rowsToAppend.length) {
    return;
  }

  const startRow = Math.max(sheet.getLastRow(), 1) + 1;
  sheet.getRange(startRow, 6, rowsToAppend.length, 4).setValues(rowsToAppend);
}

function getSettingValue_(key, fallbackValue) {
  const sheet = getSheetIfExists_(APP_CONFIG.sheetNames.settings);
  if (!sheet || sheet.getLastRow() < 2) {
    return fallbackValue;
  }

  const values = sheet.getRange(2, 1, sheet.getLastRow() - 1, 4).getDisplayValues();
  for (let index = 0; index < values.length; index += 1) {
    if (values[index][1] === key) {
      return values[index][2] === "" ? fallbackValue : values[index][2];
    }
  }

  return fallbackValue;
}

function setSettingValue_(key, value, description) {
  const sheet = getOrCreateSheet_(APP_CONFIG.sheetNames.settings);
  ensureSettingsSheet_(sheet);

  const lastRow = sheet.getLastRow();
  if (lastRow < 2) {
    return;
  }

  const values = sheet.getRange(2, 1, lastRow - 1, 4).getDisplayValues();
  for (let index = 0; index < values.length; index += 1) {
    if (values[index][1] === key) {
      sheet.getRange(index + 2, 3).setValue(value);
      return;
    }
  }

  sheet.getRange(lastRow + 1, 1, 1, 4).setValues([[
    "운영설정",
    key,
    value,
    description || ""
  ]]);
}

function getBooleanSetting_(key, fallbackValue) {
  const rawValue = String(getSettingValue_(key, fallbackValue ? "TRUE" : "FALSE") || "").toUpperCase();
  if (!rawValue) {
    return fallbackValue;
  }
  return ["TRUE", "Y", "YES", "1", "예"].indexOf(rawValue) !== -1;
}

function clearSheet_(sheet) {
  const filter = sheet.getFilter();
  if (filter) {
    filter.remove();
  }
  sheet.clearContents();
  sheet.clearFormats();
  sheet.clearNotes();
}


function ensureSheetSize_(sheet, minRows, minColumns) {
  const targetRows = Math.max(minRows || 1, 1);
  const targetColumns = Math.max(minColumns || 1, 1);

  if (sheet.getMaxRows() < targetRows) {
    sheet.insertRowsAfter(sheet.getMaxRows(), targetRows - sheet.getMaxRows());
  }
  if (sheet.getMaxColumns() < targetColumns) {
    sheet.insertColumnsAfter(sheet.getMaxColumns(), targetColumns - sheet.getMaxColumns());
  }
}

function isSheetEffectivelyBlank_(sheet) {
  if (!sheet) {
    return true;
  }

  const lastRow = sheet.getLastRow();
  const lastColumn = sheet.getLastColumn();
  if (lastRow === 0 || lastColumn === 0) {
    return true;
  }

  const values = sheet.getRange(1, 1, lastRow, lastColumn).getDisplayValues();
  for (let rowIndex = 0; rowIndex < values.length; rowIndex += 1) {
    for (let colIndex = 0; colIndex < values[rowIndex].length; colIndex += 1) {
      if (normalizeText_(values[rowIndex][colIndex])) {
        return false;
      }
    }
  }

  return true;
}

function hasMatchingHeaderRows_(sheet, expectedRows) {
  if (!sheet || !expectedRows || !expectedRows.length) {
    return false;
  }

  const rowCount = expectedRows.length;
  const columnCount = expectedRows[0].length;
  if (sheet.getLastRow() < rowCount || sheet.getLastColumn() < columnCount) {
    return false;
  }

  const currentValues = sheet.getRange(1, 1, rowCount, columnCount).getDisplayValues();
  return expectedRows.every(function(expectedRow, rowIndex) {
    return expectedRow.every(function(expectedValue, colIndex) {
      return currentValues[rowIndex][colIndex] === expectedValue;
    });
  });
}

function ensureSheetHeaderRows_(sheet, headers) {
  if (!sheet || !headers || !headers.length) {
    return false;
  }

  if (hasMatchingHeaderRows_(sheet, [headers])) {
    return false;
  }
  if (!isSheetEffectivelyBlank_(sheet)) {
    return false;
  }

  ensureSheetSize_(sheet, 2, headers.length);
  writeBatchData(sheet, headers, []);
  return true;
}

function ensureRawInputTemplate_(sheet) {
  const template = APP_CONFIG.rawInputTemplate;
  if (!sheet || !template) {
    return false;
  }

  const matchesTemplate = hasMatchingHeaderRows_(sheet, [
    template.groupHeaders,
    template.detailHeaders
  ]);
  const headerOnlySheet = matchesTemplate && sheet.getLastRow() <= APP_CONFIG.sourceHeaderRows;

  if (!matchesTemplate && !isSheetEffectivelyBlank_(sheet)) {
    return false;
  }

  if (!matchesTemplate) {
    clearSheet_(sheet);
    ensureSheetSize_(sheet, APP_CONFIG.sourceHeaderRows + 1, template.detailHeaders.length);
    sheet.getRange(1, 1, 1, template.groupHeaders.length).setValues([template.groupHeaders]);
    sheet.getRange(2, 1, 1, template.detailHeaders.length).setValues([template.detailHeaders]);
  }

  if (!matchesTemplate || headerOnlySheet) {
    formatRawInputTemplateSheet_(sheet, template);
  }

  return !matchesTemplate;
}

function formatRawInputTemplateSheet_(sheet, template) {
  const dataStartRow = APP_CONFIG.sourceHeaderRows + 1;
  const dataRowCount = Math.max(sheet.getMaxRows() - APP_CONFIG.sourceHeaderRows, 1);
  const columnCount = template.detailHeaders.length;
  const headerMap = buildCompositeHeaderMap(template.groupHeaders, template.detailHeaders);

  sheet.setFrozenRows(APP_CONFIG.sourceHeaderRows);
  sheet.getRange(1, 1, 1, columnCount)
    .setFontWeight("bold")
    .setBackground("#d9ead3")
    .setHorizontalAlignment("center")
    .setVerticalAlignment("middle");
  sheet.getRange(2, 1, 1, columnCount)
    .setFontWeight("bold")
    .setBackground("#ddebf7")
    .setHorizontalAlignment("center")
    .setVerticalAlignment("middle");
  sheet.getRange(1, 1, APP_CONFIG.sourceHeaderRows, columnCount).setWrap(true);
  sheet.getRange(1, 1).setNote("1행은 그룹 헤더, 2행은 상세 헤더이며 데이터는 3행부터 붙여 넣습니다.");
  sheet.getRange(dataStartRow, 2, dataRowCount, 1).setNumberFormat("yyyy-mm-dd");

  template.checkboxFields.forEach(function(compositeKey) {
    const columnIndex = headerMap[compositeKey];
    if (columnIndex === undefined) {
      return;
    }

    sheet.getRange(dataStartRow, columnIndex + 1, dataRowCount, 1).insertCheckboxes();
  });

  ensureRawInputBirthValidation_(sheet, headerMap, { forcePlainTextFormat: true });
  applyColumnWidths_(sheet, template.columnWidths);
}

function ensureRawInputBirthValidation_(sheet, headerMap, options) {
  if (!sheet) {
    return false;
  }

  const lastColumn = sheet.getLastColumn();
  if (lastColumn < 1 || sheet.getLastRow() < APP_CONFIG.sourceHeaderRows) {
    return false;
  }

  const resolvedHeaderMap = headerMap || buildCompositeHeaderMap(
    sheet.getRange(1, 1, 1, lastColumn).getDisplayValues()[0],
    sheet.getRange(2, 1, 1, lastColumn).getDisplayValues()[0]
  );
  const birthColumnIndex = resolvedHeaderMap["공통_생년월일"];
  if (birthColumnIndex === undefined) {
    return false;
  }

  const birthColumnNumber = birthColumnIndex + 1;
  const dataStartRow = APP_CONFIG.sourceHeaderRows + 1;
  const rowCount = Math.max(sheet.getMaxRows() - APP_CONFIG.sourceHeaderRows, 1);
  const targetRange = sheet.getRange(dataStartRow, birthColumnNumber, rowCount, 1);
  const firstCellA1 = sheet.getRange(dataStartRow, birthColumnNumber).getA1Notation();
  const validationRule = SpreadsheetApp.newDataValidation()
    .requireFormulaSatisfied(buildBirthValidationFormula_(firstCellA1))
    .setAllowInvalid(false)
    .setHelpText(getBirthInputGuidanceMessage_())
    .build();

  if (options && options.forcePlainTextFormat) {
    targetRange.setNumberFormat("@");
  }
  targetRange.setDataValidation(validationRule);
  sheet.getRange(2, birthColumnNumber).setNote(getBirthInputGuidanceMessage_());

  return true;
}

function buildBirthValidationFormula_(cellA1) {
  return "=OR(" + cellA1 + "=\"\",AND(" +
    "REGEXMATCH(TO_TEXT(" + cellA1 + "),\"^\\d{6}$\")," +
    "TEXT(DATE(2000+VALUE(LEFT(TO_TEXT(" + cellA1 + "),2)),VALUE(MID(TO_TEXT(" + cellA1 + "),3,2)),VALUE(RIGHT(TO_TEXT(" + cellA1 + "),2))),\"yyMMdd\")=" +
    "TO_TEXT(" + cellA1 + ")" +
    "))";
}

function showToast_(message, title, timeoutSeconds) {
  getSpreadsheet_().toast(message, title || APP_CONFIG.menuTitle, timeoutSeconds || 5);
}

function normalizeText_(value) {
  if (value === null || value === undefined) {
    return "";
  }
  return String(value).trim();
}

function isBlankValue_(value) {
  if (value === null || value === undefined || value === "") {
    return true;
  }
  if (value instanceof Date) {
    return false;
  }
  return normalizeText_(value) === "";
}

function isTruthyCheckboxValue_(value) {
  if (value === true) {
    return true;
  }
  if (typeof value === "number") {
    return value === 1;
  }

  const text = normalizeText_(value).toUpperCase();
  return ["TRUE", "Y", "YES", "1", "예", "O"].indexOf(text) !== -1;
}

function toMidnightDate_(value) {
  if (!value) {
    return null;
  }

  const date = value instanceof Date ? new Date(value) : new Date(value);
  if (isNaN(date.getTime())) {
    return null;
  }

  date.setHours(0, 0, 0, 0);
  return date;
}

function formatDateKey_(date, pattern) {
  const normalized = toMidnightDate_(date);
  if (!normalized) {
    return "";
  }
  return Utilities.formatDate(normalized, APP_CONFIG.timezone, pattern || "yyyy-MM-dd");
}

function sanitizeIdPart_(value, fallbackValue) {
  const text = normalizeText_(value)
    .replace(/[\\/:*?"<>|]/g, "")
    .replace(/\s+/g, "");
  return text || fallbackValue || "";
}

function styleHeaderRow_(sheet, rowNumber, columnCount) {
  styleCustomHeaderRow_(sheet, rowNumber, 1, columnCount);
}

function styleCustomHeaderRow_(sheet, rowNumber, startColumn, columnCount) {
  sheet.getRange(rowNumber, startColumn, 1, columnCount)
    .setFontWeight("bold")
    .setBackground("#d9ead3")
    .setHorizontalAlignment("center");
}

function ensureFilter_(sheet) {
  const filter = sheet.getFilter();
  if (filter) {
    filter.remove();
  }

  const lastRow = sheet.getLastRow();
  const lastColumn = sheet.getLastColumn();
  if (lastRow >= 1 && lastColumn >= 1) {
    sheet.getRange(1, 1, lastRow, lastColumn).createFilter();
  }
}

function dedupeStrings_(values) {
  const seen = {};
  const result = [];

  values.forEach(function(value) {
    const text = normalizeText_(value);
    if (!text || seen[text]) {
      return;
    }
    seen[text] = true;
    result.push(text);
  });

  return result;
}

function getDataRows_(sheet) {
  if (!sheet || sheet.getLastRow() < 2 || sheet.getLastColumn() < 1) {
    return [];
  }
  return sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).getValues();
}

function quoteSheetNameForFormula_(sheetName) {
  return "'" + String(sheetName).replace(/'/g, "''") + "'";
}
