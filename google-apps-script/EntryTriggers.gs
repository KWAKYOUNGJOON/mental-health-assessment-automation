function onOpen() {
  addCustomMenu();

  try {
    bootstrapCounselingAiOnOpen_();
  } catch (error) {
    console.error("AI 상담기록 자동 준비 실패:", error);
  }

  SpreadsheetApp.getActiveSpreadsheet().toast("AI 상담기록 메뉴가 생성되었습니다.");
}

function onInstall(e) {
  onOpen(e);
}

function addCustomMenu() {
  SpreadsheetApp.getUi()
    .createMenu(APP_CONFIG.menuTitle)
    .addItem("Gemini 수식 사용 안내", "showGeminiFormulaGuide")
    .addItem("AI 상담기록 시트 준비", "setupCounselingAiSheet")
    .addItem("선택 행 AI입력텍스트 수식 입력", "generateCounselingRecordsForSelection")
    .addItem("미처리 행 AI입력텍스트 수식 입력", "generatePendingCounselingRecords")
    .addSeparator()
    .addItem("척도검사 시트 준비", "setupScaleScreeningSyncSheets")
    .addItem("현재 시트를 척도검사 DB로 지정", "setScaleScreeningTargetToCurrentSpreadsheet")
    .addItem("척도검사 토큰 저장", "promptScaleScreeningSyncToken")
    .addItem("척도검사 연동 상태 보기", "showScaleScreeningSyncStatus")
    .addToUi();
}

function clearErrorLog() {
  writeErrorLog([]);
  formatSheets();
  showToast_("오류로그를 비웠습니다.");
}

/**
 * 원본 시트에서 수정된 행만 즉시 다시 파싱해서 파생 시트로 반영하는 실시간 동기화 트리거
 */
function syncOnEdit(e) {
  if (!e || !e.range) return;

  const sheet = e.range.getSheet();
  const rawNames = [
    APP_CONFIG.sheetNames.rawInput,
    getSettingValue_("raw_input_sheet_name", APP_CONFIG.sheetNames.rawInput)
  ].concat(APP_CONFIG.rawSheetFallbackNames);
  const isRawSheet = rawNames.indexOf(sheet.getName()) !== -1;
  const isDataRow = e.range.getLastRow() > APP_CONFIG.sourceHeaderRows;

  if (!isRawSheet || !isDataRow) {
    return;
  }

  try {
    if (rejectInvalidBirthEdit_(e)) {
      return;
    }

    if (e.range.getNumRows() === 1) {
      syncEditedRow(e);
    } else {
      rebuildDatabase(false);
    }
  } catch (err) {
    console.error("실시간 반영 실패:", err);
    try {
      rebuildDatabase(false);
    } catch (rebuildError) {
      console.error("실시간 전체 재구성 복구 실패:", rebuildError);
    }
  }
}

/**
 * 실시간 동기화를 위한 설치형 onEdit 트리거를 자동으로 설정합니다.
 */
function setupTrigger() {
  const functionName = "syncOnEdit";
  const triggers = ScriptApp.getProjectTriggers();

  ensureRawInputBirthValidation_(getSheetMap().rawInput);

  const isTriggered = triggers.some(function(trigger) {
    return trigger.getHandlerFunction() === functionName;
  });

  if (isTriggered) {
    SpreadsheetApp.getUi().alert("이미 실시간 동기화 트리거가 설정되어 있습니다.");
    return;
  }

  ScriptApp.newTrigger(functionName)
    .forSpreadsheet(SpreadsheetApp.getActive())
    .onEdit()
    .create();

  SpreadsheetApp.getUi().alert("실시간 동기화 트리거 설정을 완료했습니다. 이제 원본데이터 수정 시 자동으로 반영됩니다.");
}

/**
 * 메뉴에서 수동으로 실행할 때 호출하는 헬퍼 함수
 */
function runManualRebuild() {
  rebuildDatabase(true);
}

function setupInitialSheets() {
  const ui = typeof SpreadsheetApp !== "undefined" ? SpreadsheetApp.getUi() : null;
  const lock = LockService.getDocumentLock();
  lock.waitLock(30000);

  try {
    const spreadsheet = getSpreadsheet_();
    spreadsheet.setSpreadsheetTimeZone(APP_CONFIG.timezone);

    const rawSetup = ensureRawInputSheetForSetup_();
    const sheetMap = getSheetMap();
    const seededSheets = ensureManagedOutputSheetsForSetup_(sheetMap);

    ensureRawInputBirthValidation_(rawSetup.sheet);

    refreshDashboard(true);
    formatSheets();
    sheetMap.rawInput.activate();

    const summaryLines = [
      "초기 셋업을 완료했습니다.",
      "원본 입력 시트: " + rawSetup.sheet.getName() + (rawSetup.seededTemplate ? " (템플릿 생성)" : ""),
      seededSheets.length
        ? "초기화된 출력 시트: " + seededSheets.join(", ")
        : "기존 출력 시트는 그대로 유지했습니다.",
      "원본 시트 3행부터 데이터를 붙여 넣은 뒤 '전체 재구성 (동기화)'를 실행하세요."
    ];

    showToast_("초기 셋업을 완료했습니다.", APP_CONFIG.menuTitle, 5);
    if (ui) {
      ui.alert("초기 셋업 완료", summaryLines.join("\n"), ui.ButtonSet.OK);
    }
  } catch (error) {
    if (ui) {
      ui.alert("초기 셋업 오류", error.message, ui.ButtonSet.OK);
    }
    throw error;
  } finally {
    lock.releaseLock();
  }
}

function rejectInvalidBirthEdit_(e) {
  const sheet = e.range.getSheet();
  const lastColumn = sheet.getLastColumn();
  if (lastColumn < 1 || sheet.getLastRow() < APP_CONFIG.sourceHeaderRows) {
    return false;
  }

  const headerRows = sheet.getRange(1, 1, APP_CONFIG.sourceHeaderRows, lastColumn).getDisplayValues();
  const headerMap = buildCompositeHeaderMap(headerRows[0], headerRows[1]);
  const birthColumnIndex = headerMap["공통_생년월일"];
  if (birthColumnIndex === undefined) {
    return false;
  }

  ensureRawInputBirthValidation_(sheet, headerMap);

  const birthColumnNumber = birthColumnIndex + 1;
  if (e.range.getColumn() > birthColumnNumber || e.range.getLastColumn() < birthColumnNumber) {
    return false;
  }

  const invalidCells = [];
  const firstRow = Math.max(e.range.getRow(), APP_CONFIG.sourceHeaderRows + 1);
  const lastRow = e.range.getLastRow();

  for (let rowNumber = firstRow; rowNumber <= lastRow; rowNumber += 1) {
    const cell = sheet.getRange(rowNumber, birthColumnNumber);
    const displayValue = cell.getDisplayValue();

    if (isStrictBirthInputValue_(displayValue)) {
      cell.setNote("");
      continue;
    }

    invalidCells.push(cell);
  }

  if (!invalidCells.length) {
    return false;
  }

  const isSingleBirthCellEdit =
    e.range.getNumRows() === 1 &&
    e.range.getNumColumns() === 1 &&
    e.range.getColumn() === birthColumnNumber;

  if (isSingleBirthCellEdit && e.oldValue !== undefined) {
    invalidCells[0].setValue(e.oldValue);
  } else {
    invalidCells.forEach(function(cell) {
      cell.clearContent();
    });
  }

  const message = getBirthInputGuidanceMessage_();
  invalidCells.forEach(function(cell) {
    cell.setNote(message);
  });
  invalidCells[0].activate();

  notifyBirthInputRejected_(message, invalidCells);
  return true;
}

function notifyBirthInputRejected_(message, invalidCells) {
  const rowLabel = invalidCells.map(function(cell) {
    return cell.getRow();
  }).join(", ");
  const detailMessage = message + "\n잘못된 입력은 저장하지 않았습니다.\n행: " + rowLabel;

  showToast_(message, APP_CONFIG.menuTitle, 5);

  try {
    SpreadsheetApp.getUi().alert("생년월일 입력 오류", detailMessage, SpreadsheetApp.getUi().ButtonSet.OK);
  } catch (error) {
    console.error("생년월일 경고창 표시 실패:", error);
  }
}
