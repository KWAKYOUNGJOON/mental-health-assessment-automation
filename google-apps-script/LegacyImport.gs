function openLegacyImportDialog() {
  const html = HtmlService.createHtmlOutputFromFile("LegacyImportDialog")
    .setWidth(540)
    .setHeight(620);

  SpreadsheetApp.getUi().showModalDialog(html, "과거 실적 파일 불러오기");
}

function importLegacyRawInputTsv(payload) {
  if (!payload || typeof payload.content !== "string") {
    throw new Error("불러올 파일 내용이 비어 있습니다.");
  }

  const parsed = parseLegacyImportRows_(payload.content);
  return applyLegacyImportRows_(parsed.rows, payload, parsed.sanitizedBirthRows);
}

function importLegacyRawInputRows(payload) {
  if (!payload || !payload.rows || !payload.rows.length) {
    throw new Error("불러올 행 데이터가 비어 있습니다.");
  }

  const rows = normalizeLegacyImportRows_(payload.rows);
  const sanitization = sanitizeLegacyImportBirthRows_(rows);
  validateLegacyImportRows_(sanitization.rows);
  return applyLegacyImportRows_(sanitization.rows, payload, sanitization.sanitizedBirthRows);
}

function applyLegacyImportRows_(rows, payload, sanitizedBirthRows) {
  const importMode = normalizeLegacyImportMode_(payload.mode);
  const fileName = normalizeText_(payload.fileName) || "legacy_import.tsv";
  const rawSetup = ensureRawInputSheetForSetup_();
  const rawSheet = rawSetup.sheet;
  const columnCount = APP_CONFIG.rawInputTemplate.detailHeaders.length;

  let startRow = APP_CONFIG.sourceHeaderRows + 1;
  let clearedRowCount = 0;

  const lock = LockService.getDocumentLock();
  lock.waitLock(30000);

  try {
    assignLegacyImportBatchIds_(rows);

    if (importMode === "replace") {
      clearedRowCount = Math.max(rawSheet.getLastRow() - APP_CONFIG.sourceHeaderRows, 0);
      clearRawInputDataRows_(rawSheet, columnCount);
      startRow = APP_CONFIG.sourceHeaderRows + 1;
    } else {
      startRow = getNextRawInputAppendRow_(rawSheet);
    }

    ensureSheetSize_(rawSheet, startRow + rows.length - 1, columnCount);
    clearLegacyImportTargetValidations_(rawSheet, startRow, rows.length, columnCount);
    rawSheet.getRange(startRow, 1, rows.length, columnCount).setValues(rows);
    applyLegacyImportFormatting_(rawSheet, startRow, rows.length);
  } finally {
    lock.releaseLock();
  }

  const rebuildSummary = rebuildDatabase(false) || {};
  const toastMessage =
    "과거 실적 " + rows.length + "행을 " +
    (importMode === "replace" ? "덮어쓰기" : "이어붙이기") +
    "로 불러왔습니다.";
  showToast_(toastMessage, APP_CONFIG.menuTitle, 8);

  return {
    fileName: fileName,
    mode: importMode,
    importedRowCount: rows.length,
    clearedRowCount: clearedRowCount,
    startRow: startRow,
    rawSheetName: rawSheet.getName(),
    sanitizedBirthRows: sanitizedBirthRows || [],
    rebuildSummary: rebuildSummary
  };
}

function normalizeLegacyImportMode_(mode) {
  return normalizeText_(mode).toLowerCase() === "replace" ? "replace" : "append";
}

function parseLegacyImportRows_(content) {
  const normalizedContent = normalizeLegacyImportContent_(content);
  if (!normalizedContent) {
    throw new Error("TSV 파일 내용이 비어 있습니다.");
  }

  let rows = Utilities.parseCsv(normalizedContent, "\t");
  rows = stripLegacyImportHeaderRows_(rows);
  rows = rows.filter(hasMeaningfulLegacyImportRow_);
  rows = normalizeLegacyImportRows_(rows);
  const sanitization = sanitizeLegacyImportBirthRows_(rows);
  validateLegacyImportRows_(sanitization.rows);

  if (!sanitization.rows.length) {
    throw new Error("불러올 데이터 행이 없습니다. 'A3붙여넣기용.tsv' 파일을 선택했는지 확인해 주세요.");
  }

  return {
    rows: sanitization.rows,
    sanitizedBirthRows: sanitization.sanitizedBirthRows
  };
}

function normalizeLegacyImportContent_(content) {
  return String(content || "")
    .replace(/^\uFEFF/, "")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .trim();
}

function stripLegacyImportHeaderRows_(rows) {
  if (!rows || !rows.length) {
    return [];
  }

  const firstRow = rows[0] || [];
  const secondRow = rows[1] || [];
  const firstCell = normalizeText_(firstRow[0]);
  const secondCell = normalizeText_(firstRow[1]);

  if (firstCell === "공통" && normalizeText_(secondRow[0]) === "번호") {
    return rows.slice(2);
  }

  if (firstCell === "번호" && secondCell === "날짜") {
    return rows.slice(1);
  }

  return rows;
}

function hasMeaningfulLegacyImportRow_(row) {
  return (row || []).some(function(cell) {
    return normalizeText_(cell) !== "";
  });
}

function normalizeLegacyImportRows_(rows) {
  const expectedColumns = APP_CONFIG.rawInputTemplate.detailHeaders.length;
  const checkboxColumns = {
    16: true,
    17: true,
    18: true,
    19: true,
    20: true
  };

  return rows.map(function(row, rowIndex) {
    const normalizedRow = (row || []).slice();

    if (normalizedRow.length > expectedColumns) {
      const overflow = normalizedRow.slice(expectedColumns).some(function(value) {
        return normalizeText_(value) !== "";
      });
      if (overflow) {
        throw new Error((rowIndex + 1) + "번째 행의 열 수가 예상보다 많습니다. TSV 파일 형식을 확인해 주세요.");
      }
      normalizedRow.length = expectedColumns;
    }

    while (normalizedRow.length < expectedColumns) {
      normalizedRow.push("");
    }

    return normalizedRow.map(function(cellValue, columnIndex) {
      if (checkboxColumns[columnIndex]) {
        return isTruthyCheckboxValue_(cellValue);
      }
      return normalizeText_(cellValue);
    });
  });
}

function validateLegacyImportRows_(rows) {
  const invalidDateRows = [];

  rows.forEach(function(row, index) {
    const logicalRow = index + 1;
    const dateValue = normalizeText_(row[1]);
    const nameValue = normalizeText_(row[3]);

    if (!dateValue || !nameValue) {
      return;
    }

    if (!normalizeDateValue(dateValue)) {
      invalidDateRows.push(logicalRow);
    }
  });

  if (invalidDateRows.length) {
    throw new Error("날짜 형식을 읽을 수 없는 행이 있습니다: " + invalidDateRows.slice(0, 10).join(", "));
  }
}

function sanitizeLegacyImportBirthRows_(rows) {
  const sanitizedBirthRows = [];

  rows.forEach(function(row, index) {
    const originalBirth = normalizeText_(row[4]);
    const sanitizedBirth = normalizeLegacyBirthValue_(originalBirth);
    if (originalBirth && sanitizedBirth !== originalBirth) {
      sanitizedBirthRows.push(index + 1);
    }
    row[4] = sanitizedBirth;
  });

  return {
    rows: rows,
    sanitizedBirthRows: sanitizedBirthRows
  };
}

function normalizeLegacyBirthValue_(value) {
  const text = normalizeText_(value);
  if (!text) {
    return "";
  }

  if (isStrictBirthInputValue_(text)) {
    return text;
  }

  const birthInfo = normalizeBirthValue(text);
  if (birthInfo.warning) {
    return "";
  }

  const digits = normalizeText_(birthInfo.birth_normalized).replace(/[^\d]/g, "");
  if (digits.length === 6 && isStrictBirthInputValue_(digits)) {
    return digits;
  }

  return "";
}

function assignLegacyImportBatchIds_(rows) {
  const batchKey = Utilities.formatDate(new Date(), APP_CONFIG.timezone, "yyMMddHHmmss");
  rows.forEach(function(row, index) {
    row[0] = "IMP-" + batchKey + "-" + Utilities.formatString("%04d", index + 1);
  });
}

function clearRawInputDataRows_(sheet, columnCount) {
  const maxDataRows = Math.max(sheet.getMaxRows() - APP_CONFIG.sourceHeaderRows, 0);
  if (!maxDataRows) {
    return;
  }

  const range = sheet.getRange(APP_CONFIG.sourceHeaderRows + 1, 1, maxDataRows, columnCount);
  range.clearContent();
  range.clearNote();
}

function getNextRawInputAppendRow_(sheet) {
  return Math.max(sheet.getLastRow() + 1, APP_CONFIG.sourceHeaderRows + 1);
}

function clearLegacyImportTargetValidations_(sheet, startRow, rowCount, columnCount) {
  if (!rowCount || !columnCount) {
    return;
  }

  const targetRange = sheet.getRange(startRow, 1, rowCount, columnCount);
  targetRange.clearDataValidations();
  targetRange.clearNote();
}

function applyLegacyImportFormatting_(sheet, startRow, rowCount) {
  if (!rowCount) {
    return;
  }

  const template = APP_CONFIG.rawInputTemplate;
  const headerMap = buildCompositeHeaderMap(template.groupHeaders, template.detailHeaders);

  sheet.getRange(startRow, 2, rowCount, 1).setNumberFormat("yyyy-mm-dd");

  template.checkboxFields.forEach(function(compositeKey) {
    const columnIndex = headerMap[compositeKey];
    if (columnIndex === undefined) {
      return;
    }

    sheet.getRange(startRow, columnIndex + 1, rowCount, 1).insertCheckboxes();
  });

  applyLegacyImportBirthValidation_(sheet, startRow, rowCount, headerMap);
}

function applyLegacyImportBirthValidation_(sheet, startRow, rowCount, headerMap) {
  const birthColumnIndex = headerMap["공통_생년월일"];
  if (birthColumnIndex === undefined || !rowCount) {
    return;
  }

  const birthColumnNumber = birthColumnIndex + 1;
  const targetRange = sheet.getRange(startRow, birthColumnNumber, rowCount, 1);
  const firstCellA1 = sheet.getRange(startRow, birthColumnNumber).getA1Notation();
  const validationRule = SpreadsheetApp.newDataValidation()
    .requireFormulaSatisfied(buildBirthValidationFormula_(firstCellA1))
    .setAllowInvalid(false)
    .setHelpText(getBirthInputGuidanceMessage_())
    .build();

  targetRange.setNumberFormat("@");
  targetRange.setDataValidation(validationRule);
  sheet.getRange(2, birthColumnNumber).setNote(getBirthInputGuidanceMessage_());
}
