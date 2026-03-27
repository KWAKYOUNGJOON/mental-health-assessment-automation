function showGeminiFormulaGuide() {
  const ui = SpreadsheetApp.getUi();
  ui.alert(
    "Gemini 수식 사용 안내",
    [
      "이제 AI입력텍스트는 API 키 없이 Google Sheets의 =GEMINI() 수식으로 준비됩니다.",
      "1. 원본 시트에서 행을 선택한 뒤 '선택 행 AI입력텍스트 수식 입력' 또는 '미처리 행 AI입력텍스트 수식 입력'을 실행합니다.",
      "2. AI상담기록 시트의 AI입력텍스트 셀을 선택합니다.",
      "3. Google Sheets에서 'Generate and Insert'를 눌러 최종 텍스트를 반영합니다.",
      "4. Google 공식 도움말 기준으로 AI 함수는 사용자의 Generate and Insert 동작이 필요합니다."
    ].join("\n\n"),
    ui.ButtonSet.OK
  );
}

function promptAndSaveOpenAiApiKey() {
  showGeminiFormulaGuide();
}

function setupCounselingAiSheet() {
  const spreadsheet = getSpreadsheet_();
  const activeSheet = spreadsheet.getActiveSheet();
  const outputSheet = getCounselingAiSheet_();
  const wasBlank = isSheetEffectivelyBlank_(outputSheet);
  const activeSheetName = activeSheet ? activeSheet.getName() : "";
  const isEligibleActiveSheet =
    !!activeSheetName &&
    activeSheetName !== outputSheet.getName() &&
    activeSheetName !== APP_CONFIG.sheetNames.settings &&
    APP_CONFIG.counselingAi.hiddenLegacySheetNames.indexOf(activeSheetName) === -1;

  if (!getSettingValue_("counseling_ai_source_sheet_name", "")) {
    const defaultSourceSheet = isEligibleActiveSheet
      ? activeSheet
      : findPreferredCounselingAiSourceSheet_(spreadsheet);
    setSettingValue_(
      "counseling_ai_source_sheet_name",
      defaultSourceSheet.getName(),
      "AI 상담기록 원본 시트 이름"
    );
  }

  try {
    hideLegacyPerformanceSheets_();
    const readySheet = ensureCounselingAiSheetReady_();
    readySheet.activate();
    showToast_(
      wasBlank ? "AI 상담기록 시트를 만들었습니다." : "AI 상담기록 시트를 준비했습니다.",
      APP_CONFIG.menuTitle,
      5
    );
  } catch (error) {
    SpreadsheetApp.getUi().alert("AI 상담기록 시트 준비 오류", error.message, SpreadsheetApp.getUi().ButtonSet.OK);
    throw error;
  }
}

function bootstrapCounselingAiOnOpen_() {
  const spreadsheet = getSpreadsheet_();
  const activeSheet = spreadsheet.getActiveSheet();
  const activeSheetName = activeSheet ? activeSheet.getName() : "";

  try {
    hideLegacyPerformanceSheets_();
    const modelName = getCounselingAiModelName_();
    const outputSheet = getCounselingAiSheet_();
    const isEligibleActiveSheet =
      !!activeSheetName &&
      activeSheetName !== outputSheet.getName() &&
      activeSheetName !== APP_CONFIG.sheetNames.settings &&
      APP_CONFIG.counselingAi.hiddenLegacySheetNames.indexOf(activeSheetName) === -1;

    if (!getSettingValue_("counseling_ai_source_sheet_name", "")) {
      const defaultSourceSheet = isEligibleActiveSheet
        ? spreadsheet.getSheetByName(activeSheetName)
        : findPreferredCounselingAiSourceSheet_(spreadsheet);

      if (defaultSourceSheet && defaultSourceSheet.getName() !== outputSheet.getName()) {
        setSettingValue_(
          "counseling_ai_source_sheet_name",
          defaultSourceSheet.getName(),
          "AI 상담기록 원본 시트 이름"
        );
      }
    }

    ensureCounselingAiSheetReady_();
    return {
      modelName: modelName,
      outputSheetName: outputSheet.getName()
    };
  } finally {
    if (activeSheetName) {
      const sheetToRestore = spreadsheet.getSheetByName(activeSheetName);
      if (sheetToRestore) {
        sheetToRestore.activate();
      }
    }
  }
}

function syncCounselingAiAutomationState() {
  const bootstrapState = bootstrapCounselingAiOnOpen_() || {};
  return {
    ok: true,
    modelName: bootstrapState.modelName || getCounselingAiModelName_(),
    outputSheetName: bootstrapState.outputSheetName || getCounselingAiOutputSheetName_(),
    usesGeminiFormula: true
  };
}

function generateCounselingRecordsForSelection() {
  const spreadsheet = getSpreadsheet_();
  const activeSheet = spreadsheet.getActiveSheet();
  const outputSheetName = getCounselingAiOutputSheetName_();

  if (!activeSheet || activeSheet.getName() === outputSheetName) {
    SpreadsheetApp.getUi().alert("원본 시트에서 생성할 행을 먼저 선택하세요.");
    return;
  }

  const sourceSpec = resolveCounselingAiSourceSpec_({ preferActiveSheet: true });
  if (activeSheet.getName() !== sourceSpec.sheet.getName()) {
    SpreadsheetApp.getUi().alert("원본 시트에서 생성할 행을 먼저 선택하세요.");
    return;
  }

  const rowNumbers = collectSelectedRowNumbers_(activeSheet, sourceSpec.headerRows);
  if (!rowNumbers.length) {
    SpreadsheetApp.getUi().alert("헤더 아래의 데이터 행을 하나 이상 선택하세요.");
    return;
  }

  try {
    runCounselingRecordGeneration_(sourceSpec, rowNumbers, { interactive: true, force: true });
  } catch (error) {
    SpreadsheetApp.getUi().alert("AI입력텍스트 수식 입력 오류", error.message, SpreadsheetApp.getUi().ButtonSet.OK);
    throw error;
  }
}

function generatePendingCounselingRecords() {
  const sourceSpec = resolveCounselingAiSourceSpec_({ preferActiveSheet: false });
  const rowNumbers = collectPendingCounselingRowNumbers_(sourceSpec);

  if (!rowNumbers.length) {
    showToast_("생성할 미처리 행이 없습니다.", APP_CONFIG.menuTitle, 5);
    return;
  }

  try {
    runCounselingRecordGeneration_(sourceSpec, rowNumbers, { interactive: true, force: false });
  } catch (error) {
    SpreadsheetApp.getUi().alert("AI입력텍스트 수식 입력 오류", error.message, SpreadsheetApp.getUi().ButtonSet.OK);
    throw error;
  }
}

function runCounselingRecordGeneration_(sourceSpec, rowNumbers, options) {
  const lock = LockService.getDocumentLock();
  lock.waitLock(30000);

  try {
    const outputSheet = ensureCounselingAiSheetReady_();
    const statusMap = getCounselingAiStatusMap_(outputSheet);
    const skipCompleted = getBooleanSetting_("counseling_ai_skip_completed", true);
    const modelName = getCounselingAiModelName_();
    const summary = {
      requested: rowNumbers.length,
      completed: 0,
      skipped: 0,
      failed: 0
    };

    rowNumbers.forEach(function(rowNumber) {
      const recordId = getCounselingAiRecordId_(sourceSpec.sheet.getName(), rowNumber);
      if (!options.force && skipCompleted && isHandledCounselingStatus_(statusMap[recordId])) {
        summary.skipped += 1;
        return;
      }

      const sourceRecord = buildCounselingSourceRecord_(sourceSpec, rowNumber);
      if (!sourceRecord || !sourceRecord.geminiPromptText) {
        summary.skipped += 1;
        return;
      }

      try {
        const outputRowNumber = upsertCounselingAiOutputRow_(
          outputSheet,
          buildCounselingAiOutputRow_(sourceRecord, createEmptyCounselingAiResult_(), "수식입력", "", modelName)
        );
        applyCounselingAiPromptFormulaToRow_(outputSheet, outputRowNumber, sourceRecord);
        summary.completed += 1;
      } catch (error) {
        upsertCounselingAiOutputRow_(
          outputSheet,
          buildCounselingAiOutputRow_(
            sourceRecord,
            createEmptyCounselingAiResult_(),
            "오류",
            error.message,
            modelName
          )
        );
        summary.failed += 1;
      }
    });

    formatCounselingAiSheet_(outputSheet);
    showToast_(
      "AI입력텍스트 수식 입력 완료: 완료 " + summary.completed +
        "건, 건너뜀 " + summary.skipped + "건, 오류 " + summary.failed + "건",
      APP_CONFIG.menuTitle,
      8
    );

    if (options && options.interactive) {
      SpreadsheetApp.getUi().alert(
        "AI입력텍스트 수식 입력 결과",
        [
          "원본 시트: " + sourceSpec.sheet.getName(),
          "요청 행 수: " + summary.requested + "건",
          "수식 입력 완료: " + summary.completed + "건",
          "건너뜀: " + summary.skipped + "건",
          "오류: " + summary.failed + "건",
          "",
          "다음 단계: AI상담기록 시트의 AI입력텍스트 셀을 선택한 뒤 Generate and Insert를 눌러 주세요."
        ].join("\n"),
        SpreadsheetApp.getUi().ButtonSet.OK
      );
    }

    return summary;
  } finally {
    lock.releaseLock();
  }
}

function isHandledCounselingStatus_(statusText) {
  const normalizedStatus = normalizeText_(statusText);
  if (!normalizedStatus) {
    return false;
  }

  return normalizedStatus.indexOf("오류") !== 0;
}

function resolveCounselingAiSourceSpec_(options) {
  const spreadsheet = getSpreadsheet_();
  const outputSheetName = getCounselingAiOutputSheetName_();
  const configuredSourceName = getSettingValue_("counseling_ai_source_sheet_name", "");
  const activeSheet = spreadsheet.getActiveSheet();
  let sourceSheet = null;

  if (
    options &&
    options.preferActiveSheet &&
    activeSheet &&
    activeSheet.getName() !== outputSheetName &&
    activeSheet.getName() !== APP_CONFIG.sheetNames.settings &&
    APP_CONFIG.counselingAi.hiddenLegacySheetNames.indexOf(activeSheet.getName()) === -1
  ) {
    sourceSheet = activeSheet;
  }

  if (!sourceSheet && configuredSourceName) {
    sourceSheet = spreadsheet.getSheetByName(configuredSourceName);
  }

  if (
    !sourceSheet &&
    activeSheet &&
    activeSheet.getName() !== outputSheetName &&
    activeSheet.getName() !== APP_CONFIG.sheetNames.settings &&
    APP_CONFIG.counselingAi.hiddenLegacySheetNames.indexOf(activeSheet.getName()) === -1
  ) {
    sourceSheet = activeSheet;
  }

  if (!sourceSheet) {
    sourceSheet = findPreferredCounselingAiSourceSheet_(spreadsheet);
  }

  const headerRows = resolveCounselingHeaderRowCount_(sourceSheet);
  const headerInfo = buildCounselingHeaderInfo_(sourceSheet, headerRows);

  return {
    sheet: sourceSheet,
    headerRows: headerRows,
    headerKeys: headerInfo.headerKeys,
    headerMap: headerInfo.headerMap
  };
}

function resolveCounselingHeaderRowCount_(sheet) {
  const configuredValue = parseInt(
    getSettingValue_("counseling_ai_header_rows", ""),
    10
  );
  if (configuredValue >= 1) {
    return configuredValue;
  }

  if (sheet && sheet.getName() === APP_CONFIG.sheetNames.rawInput) {
    return 2;
  }

  if (!sheet || sheet.getLastRow() < 2 || sheet.getLastColumn() < 1) {
    return 1;
  }

  const firstRow = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getDisplayValues()[0];
  const secondRow = sheet.getRange(2, 1, 1, sheet.getLastColumn()).getDisplayValues()[0];
  const groupedKeywords = ["공통", "병원", "시설", "서비스", "공적서비스"];
  const firstHasGroupedKeyword = firstRow.some(function(value) {
    return groupedKeywords.indexOf(normalizeText_(value)) !== -1;
  });
  const firstFilled = countNonBlankValues_(firstRow);
  const secondFilled = countNonBlankValues_(secondRow);

  return firstHasGroupedKeyword && secondFilled >= firstFilled ? 2 : 1;
}

function buildCounselingHeaderInfo_(sheet, headerRows) {
  const lastColumn = sheet.getLastColumn();
  if (lastColumn < 1) {
    throw new Error("원본 시트에 헤더가 없습니다.");
  }

  if (headerRows === 2) {
    const groupRow = sheet.getRange(1, 1, 1, lastColumn).getDisplayValues()[0];
    const detailRow = sheet.getRange(2, 1, 1, lastColumn).getDisplayValues()[0];
    const headerKeys = buildCompositeHeaderKeys_(groupRow, detailRow);
    return {
      headerKeys: headerKeys,
      headerMap: getHeaderMap(headerKeys)
    };
  }

  const headerKeys = sheet.getRange(1, 1, 1, lastColumn).getDisplayValues()[0].map(function(value) {
    return normalizeText_(value);
  });
  return {
    headerKeys: headerKeys,
    headerMap: getHeaderMap(headerKeys)
  };
}

function buildCompositeHeaderKeys_(groupRow, headerRow) {
  const headerKeys = [];
  const duplicateCounts = {};
  let currentGroup = "공통";

  headerRow.forEach(function(header, index) {
    const groupValue = normalizeText_(groupRow[index]);
    const headerValue = normalizeText_(header);

    if (groupValue) {
      currentGroup = groupValue;
    }

    if (!headerValue) {
      headerKeys.push("");
      return;
    }

    const compositeKey = (currentGroup || "공통") + "_" + headerValue;
    let resolvedKey = compositeKey;

    if (duplicateCounts[compositeKey] !== undefined) {
      duplicateCounts[compositeKey] += 1;
      resolvedKey = compositeKey + "__" + duplicateCounts[compositeKey];
    } else {
      duplicateCounts[compositeKey] = 1;
    }

    headerKeys.push(resolvedKey);
  });

  return headerKeys;
}

function collectSelectedRowNumbers_(sheet, headerRows) {
  const rangeList = sheet.getActiveRangeList();
  const ranges = rangeList ? rangeList.getRanges() : (sheet.getActiveRange() ? [sheet.getActiveRange()] : []);
  const rowMap = {};

  ranges.forEach(function(range) {
    for (let rowNumber = range.getRow(); rowNumber <= range.getLastRow(); rowNumber += 1) {
      if (rowNumber > headerRows) {
        rowMap[rowNumber] = true;
      }
    }
  });

  return Object.keys(rowMap).map(function(rowNumber) {
    return Number(rowNumber);
  }).sort(function(left, right) {
    return left - right;
  });
}

function collectPendingCounselingRowNumbers_(sourceSpec) {
  const sheet = sourceSpec.sheet;
  const outputSheet = ensureCounselingAiSheetReady_();
  const statusMap = getCounselingAiStatusMap_(outputSheet);
  const skipCompleted = getBooleanSetting_("counseling_ai_skip_completed", true);
  const batchSize = getCounselingAiBatchSize_();
  const rowNumbers = [];

  for (let rowNumber = sourceSpec.headerRows + 1; rowNumber <= sheet.getLastRow(); rowNumber += 1) {
    const recordId = getCounselingAiRecordId_(sheet.getName(), rowNumber);
    if (skipCompleted && isHandledCounselingStatus_(statusMap[recordId])) {
      continue;
    }

    const sourceRecord = buildCounselingSourceRecord_(sourceSpec, rowNumber);
    if (!sourceRecord || !sourceRecord.geminiPromptText) {
      continue;
    }

    rowNumbers.push(rowNumber);
    if (rowNumbers.length >= batchSize) {
      break;
    }
  }

  return rowNumbers;
}

function buildCounselingSourceRecord_(sourceSpec, rowNumber) {
  const lastColumn = sourceSpec.sheet.getLastColumn();
  if (rowNumber <= sourceSpec.headerRows || lastColumn < 1 || rowNumber > sourceSpec.sheet.getLastRow()) {
    return null;
  }

  const rowValues = sourceSpec.sheet.getRange(rowNumber, 1, 1, lastColumn).getDisplayValues()[0];
  if (!countNonBlankValues_(rowValues)) {
    return null;
  }

  const headerKeys = sourceSpec.headerKeys;
  const record = {
    recordId: getCounselingAiRecordId_(sourceSpec.sheet.getName(), rowNumber),
    sourceSheetName: sourceSpec.sheet.getName(),
    rowNumber: rowNumber,
    clientName: getSourceFieldByAliases_(headerKeys, rowValues, APP_CONFIG.counselingAi.nameAliases),
    sessionDateText: normalizeCounselingDateText_(
      getSourceFieldByAliases_(headerKeys, rowValues, APP_CONFIG.counselingAi.dateAliases)
    ),
    workerName: getSourceFieldByAliases_(headerKeys, rowValues, APP_CONFIG.counselingAi.workerAliases),
    sourceConsultationType: getSourceFieldByAliases_(headerKeys, rowValues, APP_CONFIG.counselingAi.typeAliases),
    primaryNarrative: getSourceFieldByAliases_(headerKeys, rowValues, APP_CONFIG.counselingAi.noteAliases),
    providedServices: collectSourceFieldValuesByAliases_(headerKeys, rowValues, APP_CONFIG.counselingAi.serviceAliases)
  };

  record.providedServicesText = record.providedServices.join(", ");

  if (!countNonBlankValues_([
    record.primaryNarrative,
    record.providedServicesText
  ])) {
    return null;
  }

  record.maskedPrimaryNarrative = truncateCounselingPromptText_(
    maskSensitiveText_(record.primaryNarrative, record)
  );
  record.maskedServicesText = truncateCounselingPromptText_(
    maskSensitiveText_(record.providedServicesText, record)
  );
  record.geminiPromptText = buildCounselingPromptText_(record);
  return record;
}

function buildCounselingPromptText_(record) {
  return [
    "당신은 다시서기 노숙인 지원센터 정신건강팀에서 일하는 정신건강 사회복지사입니다.",
    "아래 자료만 근거로 일일상담기록용 AI입력텍스트를 작성하세요.",
    "반드시 아래 4개 항목만, 항목명 그대로 출력하세요.",
    "1. 내방 및 방문사유 :",
    "2. 현재 정신과적 증상의 특이점 및 변화내용 :",
    "3. 개입내용 :",
    "4. 향후 계획 및 전달사항 :",
    "",
    "작성 기준:",
    "- 사실만 작성하고 추측하지 마세요.",
    "- 원본시트명, 행번호, 상담일자, 상담유형은 근거로 사용하지 마세요.",
    "- 상담내용은 1번과 2번의 핵심 근거로 사용하세요.",
    "- 제공서비스는 3번 개입내용의 핵심 근거로 사용하세요.",
    "- 2번은 추정 진단명 또는 확인된 진단명이 원본에 근거 있게 보이면 간략히 적고, 그렇지 않으면 최근 정신 및 알코올 증상의 특이사항만 간략히 요약하세요.",
    "- 2번에 쓸 근거가 부족하면 항목명 뒤를 비워 두세요.",
    "- 4번은 향후 계획 및 전달사항 근거가 없으면 항목명 뒤를 비워 두세요.",
    "- 소스링크, 설명, 따옴표, 마크다운, 서론, 결론 없이 4개 항목만 출력하세요.",
    "",
    "상담내용:",
    normalizeText_(record.maskedPrimaryNarrative) || "(없음)",
    "",
    "제공서비스:",
    normalizeText_(record.maskedServicesText) || "(없음)"
  ].join("\n");
}

function shouldExcludeHeaderFromAi_(headerKey) {
  const normalizedHeader = stripDuplicateHeaderSuffix_(normalizeText_(headerKey));
  if (!normalizedHeader) {
    return true;
  }

  const lastToken = normalizedHeader.split("_").pop();
  if (APP_CONFIG.counselingAi.piiExactHeaderKeywords.indexOf(lastToken) !== -1) {
    return true;
  }

  return APP_CONFIG.counselingAi.piiPartialHeaderKeywords.some(function(keyword) {
    return normalizedHeader.indexOf(keyword) !== -1;
  });
}

function getSourceFieldByAliases_(headerKeys, rowValues, aliases) {
  const headerKey = findHeaderKeyByAliases_(headerKeys, aliases);
  if (!headerKey) {
    return "";
  }

  const index = headerKeys.indexOf(headerKey);
  return index === -1 ? "" : normalizeText_(rowValues[index]);
}

function findHeaderKeyByAliases_(headerKeys, aliases) {
  const cleanedAliases = (aliases || []).map(function(alias) {
    return normalizeText_(alias).toLowerCase();
  }).filter(function(alias) {
    return !!alias;
  });

  for (let index = 0; index < headerKeys.length; index += 1) {
    const headerKey = stripDuplicateHeaderSuffix_(normalizeText_(headerKeys[index]));
    if (!headerKey) {
      continue;
    }

    const lastToken = headerKey.split("_").pop().toLowerCase();
    if (cleanedAliases.indexOf(lastToken) !== -1) {
      return headerKeys[index];
    }
  }

  for (let index = 0; index < headerKeys.length; index += 1) {
    const headerKey = stripDuplicateHeaderSuffix_(normalizeText_(headerKeys[index]));
    if (!headerKey) {
      continue;
    }

    const loweredHeaderKey = headerKey.toLowerCase();
    const matched = cleanedAliases.some(function(alias) {
      return loweredHeaderKey.indexOf(alias) !== -1;
    });
    if (matched) {
      return headerKeys[index];
    }
  }

  return "";
}

function collectSourceFieldValuesByAliases_(headerKeys, rowValues, aliases) {
  const values = [];
  const cleanedAliases = (aliases || []).map(function(alias) {
    return normalizeText_(alias).toLowerCase();
  }).filter(function(alias) {
    return !!alias;
  });

  headerKeys.forEach(function(headerKey, index) {
    const normalizedHeader = stripDuplicateHeaderSuffix_(normalizeText_(headerKey));
    const value = normalizeText_(rowValues[index]);
    if (!normalizedHeader || !value) {
      return;
    }

    if (["FALSE", "N", "NO"].indexOf(value.toUpperCase()) !== -1) {
      return;
    }

    const lastToken = normalizedHeader.split("_").pop().toLowerCase();
    const loweredHeader = normalizedHeader.toLowerCase();
    const matched = cleanedAliases.some(function(alias) {
      return lastToken === alias || lastToken.indexOf(alias) !== -1 || loweredHeader.indexOf(alias) !== -1;
    });

    if (!matched || values.indexOf(value) !== -1) {
      return;
    }

    values.push(value);
  });

  return values;
}

function stripDuplicateHeaderSuffix_(headerKey) {
  return normalizeText_(headerKey).replace(/__\d+$/, "");
}

function normalizeCounselingDateText_(value) {
  const normalizedDate = normalizeDateValue(value);
  if (normalizedDate) {
    return formatDateKey_(normalizedDate, "yyyy-MM-dd");
  }
  return normalizeText_(value);
}

function maskSensitiveText_(text, sourceRecord) {
  let masked = normalizeText_(text);
  if (!masked) {
    return "";
  }

  if (sourceRecord && sourceRecord.clientName) {
    masked = masked.split(sourceRecord.clientName).join("[대상자]");
  }
  if (sourceRecord && sourceRecord.workerName) {
    masked = masked.split(sourceRecord.workerName).join("[담당자]");
  }

  return masked
    .replace(/\b01\d[- ]?\d{3,4}[- ]?\d{4}\b/g, "[연락처]")
    .replace(/\b\d{2,3}[- ]?\d{3,4}[- ]?\d{4}\b/g, "[전화번호]")
    .replace(/\b\d{6}[- ]?[1-4]\d{6}\b/g, "[주민번호]")
    .replace(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi, "[이메일]");
}

function truncateCounselingPromptText_(text) {
  const normalizedText = normalizeText_(text);
  if (normalizedText.length <= APP_CONFIG.counselingAi.maxContextCharacters) {
    return normalizedText;
  }

  return normalizedText.substring(0, APP_CONFIG.counselingAi.maxContextCharacters) + "\n[후략]";
}

function requestCounselingRecordFromGemini_(apiKey, modelName, sourceRecord) {
  const endpoint = "https://generativelanguage.googleapis.com/v1beta/models/" + encodeURIComponent(modelName) + ":generateContent";
  const recordFormat = getCounselingAiRecordFormat_();
  const fullPrompt = [
    buildCounselingSystemPrompt_(recordFormat),
    "",
    "다음 비식별화된 상담 입력만 바탕으로 상담기록을 작성하세요.",
    "",
    sourceRecord.geminiPromptText
  ].join("\n");
  const payload = {
    contents: [
      {
        role: "user",
        parts: [
          { text: fullPrompt }
        ]
      }
    ],
    generationConfig: {
      temperature: 0.2,
      responseMimeType: "application/json",
      responseJsonSchema: buildCounselingResponseJsonSchema_()
    }
  };

  for (let attempt = 1; attempt <= APP_CONFIG.counselingAi.maxRetryAttempts; attempt += 1) {
    const response = UrlFetchApp.fetch(endpoint, {
      method: "post",
      contentType: "application/json",
      headers: {
        "x-goog-api-key": apiKey
      },
      payload: JSON.stringify(payload),
      muteHttpExceptions: true
    });

    const statusCode = response.getResponseCode();
    const bodyText = response.getContentText();
    let parsedBody = {};

    try {
      parsedBody = bodyText ? JSON.parse(bodyText) : {};
    } catch (error) {
      if (statusCode < 200 || statusCode >= 300) {
        throw new Error("Gemini API 오류 응답을 해석할 수 없습니다. HTTP " + statusCode);
      }
    }

    if (statusCode >= 200 && statusCode < 300) {
      const contentParts = ((((parsedBody.candidates || [])[0] || {}).content || {}).parts || []);
      const content = contentParts.map(function(part) {
        return normalizeText_(part && part.text);
      }).filter(function(text) {
        return !!text;
      }).join("\n");
      if (!content) {
        throw new Error("Gemini 응답 본문이 비어 있습니다.");
      }

      const result = extractCounselingJson_(content);
      result.risk_level = normalizeText_(result.risk_level) || APP_CONFIG.counselingAi.defaultRiskLevel;
      if (["정보부족", "낮음", "보통", "높음", "긴급"].indexOf(result.risk_level) === -1) {
        result.risk_level = APP_CONFIG.counselingAi.defaultRiskLevel;
      }

      return result;
    }

    const apiMessage = parsedBody && parsedBody.error ? parsedBody.error.message : "";
    const resolvedMessage = apiMessage || ("Gemini API 호출 실패: HTTP " + statusCode);
    if (attempt < APP_CONFIG.counselingAi.maxRetryAttempts && shouldRetryGeminiRequest_(statusCode, resolvedMessage)) {
      Utilities.sleep(resolveGeminiRetryDelayMs_(resolvedMessage, attempt));
      continue;
    }

    throw new Error(resolvedMessage);
  }

  throw new Error("Gemini API 호출 재시도 한도를 초과했습니다.");
}

function extractCounselingJson_(content) {
  const rawText = normalizeText_(content);
  const fencedMatch = rawText.match(/```(?:json)?\s*([\s\S]*?)\s*```/i);
  const candidateText = fencedMatch ? fencedMatch[1].trim() : rawText;
  const startIndex = candidateText.indexOf("{");
  const endIndex = candidateText.lastIndexOf("}");

  if (startIndex === -1 || endIndex === -1 || endIndex <= startIndex) {
    throw new Error("AI 응답에서 JSON 객체를 찾을 수 없습니다.");
  }

  return JSON.parse(candidateText.substring(startIndex, endIndex + 1));
}

function buildCounselingResponseJsonSchema_() {
  return {
    type: "object",
    properties: {
      consultation_type: { type: "string" },
      risk_level: {
        type: "string",
        enum: ["정보부족", "낮음", "보통", "높음", "긴급"]
      },
      chief_complaint: { type: "string" },
      summary: { type: "string" },
      counseling_record: { type: "string" },
      follow_up_action: { type: "string" },
      keywords: {
        type: "array",
        items: { type: "string" }
      }
    },
    required: [
      "consultation_type",
      "risk_level",
      "chief_complaint",
      "summary",
      "counseling_record",
      "follow_up_action",
      "keywords"
    ]
  };
}

function buildCounselingSystemPrompt_(recordFormat) {
  const formatGuide = recordFormat === "사례정리보고서"
    ? [
        "출력 상담기록 본문은 반드시 아래 9개 번호 구조를 유지하세요.",
        "1. 주호소(본인 호소 주요문제, 담당자가 판단한 주요문제)",
        "2. 개인력(가족병력, 수급자격)",
        "3. 병력(정신과, 신체질환, 정신과 입원력. 임상적 추정진단은 근거를 반드시 함께 기입)",
        "4. 노숙력",
        "5. MSE(상세하고 전문적으로, 원본 근거 범위 내에서만 작성)",
        "6. 자살력",
        "7. 물질력",
        "8. 법적문제",
        "9. 개입계획"
      ].join("\n")
    : [
        "출력 상담기록 본문은 반드시 아래 4개 번호 구조를 유지하세요.",
        "1. 내방 및 방문사유 :",
        "2. 현재 정신과적 증상의 특이점 및 변화내용 :",
        "3. 개입내용 :",
        "4. 향후 계획 및 전달사항 :"
      ].join("\n");

  return [
    "당신은 다시서기 노숙인 지원센터 정신건강팀에서 일하는 정신건강 사회복지사입니다.",
    "제공된 상담 입력만 근거로 상담기록을 작성하세요. 외부 출처 검색, 최신 정보 인용, 링크 인용은 하지 마세요.",
    "항상 사실만 진실하게 작성하고, 추측하거나 조작하지 마세요.",
    "원본에 없는 정보는 추가하지 말고, 불확실하면 '이것은 확인할 수 없습니다' 또는 '원본 기준 확인 필요'라고 명시하세요.",
    "날짜나 기간이 원본에 있으면 그대로 기록하고, 없으면 임의 날짜를 만들지 마세요.",
    "직접 식별 정보는 새로 만들지 말고 '대상자', '보호자', '유관기관' 같은 일반 표현을 사용하세요.",
    "상담내용과 제공서비스만 상담의 주 내용으로 사용하세요. 원본시트명, 행번호, 상담일자, 유형은 출력 근거로 사용하지 마세요.",
    "2. 현재 정신과적 증상의 특이점 및 변화내용에는 추정 진단명 또는 확인된 진단명, 최근 정신 및 알코올 증상의 특이사항을 간략히 요약하세요.",
    "근거가 부족하면 진단명을 단정하지 말고 관찰 가능한 증상 변화만 적거나 빈칸으로 두세요.",
    "4. 향후 계획 및 전달사항은 원본 근거가 없으면 빈칸으로 두세요.",
    formatGuide,
    "출력은 반드시 JSON 객체 1개만 반환하세요.",
    "JSON 스키마:",
    "{",
    '  "consultation_type": "",',
    '  "risk_level": "정보부족|낮음|보통|높음|긴급",',
    '  "chief_complaint": "",',
    '  "summary": "",',
    '  "counseling_record": "",',
    '  "follow_up_action": "",',
    '  "keywords": ["", "", ""]',
    "}",
    "chief_complaint는 주호소를 한 줄로 요약하세요.",
    "summary는 상담내용과 제공서비스 중심의 핵심 요약을 한 줄로 작성하세요.",
    "follow_up_action은 향후 계획 및 전달사항만 한 줄로 정리하세요.",
    "keywords는 원본에 실제로 등장하거나 직접 추론 가능한 핵심어만 3~5개 이내로 작성하세요."
  ].join("\n");
}

function getCounselingAiRecordFormat_() {
  const configuredValue = normalizeText_(
    getSettingValue_("counseling_ai_record_format", APP_CONFIG.counselingAi.defaultRecordFormat)
  );

  if (APP_CONFIG.counselingAi.supportedRecordFormats.indexOf(configuredValue) !== -1) {
    return configuredValue;
  }

  return APP_CONFIG.counselingAi.defaultRecordFormat;
}

function getCounselingAiModelName_() {
  const configuredValue = normalizeText_(
    getSettingValue_("counseling_ai_model", APP_CONFIG.counselingAi.defaultModel)
  );

  if (
    !configuredValue ||
    /^gpt/i.test(configuredValue) ||
    /^gemini[- ]/i.test(configuredValue) ||
    /api/i.test(configuredValue)
  ) {
    setSettingValue_(
      "counseling_ai_model",
      APP_CONFIG.counselingAi.defaultModel,
      "AI입력텍스트 생성 방식 표기용 값입니다."
    );
    return APP_CONFIG.counselingAi.defaultModel;
  }

  return configuredValue;
}

function findPreferredCounselingAiSourceSheet_(spreadsheet) {
  const targetSpreadsheet = spreadsheet || getSpreadsheet_();
  const excludedNames = [
    getCounselingAiOutputSheetName_(),
    APP_CONFIG.sheetNames.settings
  ].concat(APP_CONFIG.counselingAi.hiddenLegacySheetNames);

  const candidateSheets = targetSpreadsheet.getSheets().filter(function(sheet) {
    return excludedNames.indexOf(sheet.getName()) === -1;
  });

  for (let index = 0; index < APP_CONFIG.counselingAi.preferredSourceSheetNames.length; index += 1) {
    const preferredName = APP_CONFIG.counselingAi.preferredSourceSheetNames[index];
    const preferredSheet = targetSpreadsheet.getSheetByName(preferredName);
    if (preferredSheet && excludedNames.indexOf(preferredSheet.getName()) === -1) {
      return preferredSheet;
    }
  }

  for (let index = 0; index < candidateSheets.length; index += 1) {
    const sheetName = candidateSheets[index].getName();
    const matchedKeyword = APP_CONFIG.counselingAi.preferredSourceSheetKeywords.some(function(keyword) {
      return sheetName.indexOf(keyword) !== -1;
    });
    if (matchedKeyword) {
      return candidateSheets[index];
    }
  }

  return resolveRawInputSheet_();
}

function hideLegacyPerformanceSheets_() {
  const spreadsheet = getSpreadsheet_();
  const activeSheet = spreadsheet.getActiveSheet();
  const activeSheetName = activeSheet ? activeSheet.getName() : "";

  APP_CONFIG.counselingAi.hiddenLegacySheetNames.forEach(function(sheetName) {
    const sheet = spreadsheet.getSheetByName(sheetName);
    if (!sheet || sheetName === activeSheetName || sheet.isSheetHidden()) {
      return;
    }

    sheet.hideSheet();
  });
}

function createEmptyCounselingAiResult_() {
  return {
    consultation_type: "",
    risk_level: APP_CONFIG.counselingAi.defaultRiskLevel,
    chief_complaint: "",
    summary: "",
    counseling_record: "",
    follow_up_action: "",
    keywords: []
  };
}

function buildCounselingAiOutputRow_(sourceRecord, aiResult, status, errorMessage, modelName) {
  const normalizedErrorMessage = normalizeText_(errorMessage);
  const statusText = status === "오류" && normalizedErrorMessage
    ? "오류: " + normalizedErrorMessage
    : status;

  return [
    sourceRecord.sessionDateText,
    sourceRecord.clientName,
    normalizeText_(aiResult.consultation_type) || sourceRecord.sourceConsultationType || "",
    "",
    statusText,
    normalizeText_(aiResult.counseling_record),
    normalizeSingleLineText_(aiResult.follow_up_action),
    normalizeSingleLineText_(aiResult.chief_complaint),
    normalizeSingleLineText_(aiResult.summary),
    joinCounselingKeywords_(aiResult.keywords),
    modelName,
    normalizedErrorMessage,
    sourceRecord.recordId,
    sourceRecord.sourceSheetName,
    sourceRecord.rowNumber,
    new Date(),
    sourceRecord.workerName,
    normalizeText_(aiResult.risk_level) || APP_CONFIG.counselingAi.defaultRiskLevel,
    normalizeText_(sourceRecord.maskedPrimaryNarrative),
    normalizeText_(sourceRecord.maskedServicesText),
    normalizeText_(sourceRecord.geminiPromptText)
  ];
}

function normalizeSingleLineText_(value) {
  return normalizeText_(value).replace(/\s+/g, " ").trim();
}

function joinCounselingKeywords_(keywords) {
  if (!Array.isArray(keywords)) {
    return normalizeSingleLineText_(keywords);
  }

  return keywords.map(function(keyword) {
    return normalizeSingleLineText_(keyword);
  }).filter(function(keyword) {
    return !!keyword;
  }).join(", ");
}

function getCounselingAiSheet_() {
  return getOrCreateSheet_(getCounselingAiOutputSheetName_());
}

function getCounselingAiOutputSheetName_() {
  return getSettingValue_("counseling_ai_output_sheet_name", APP_CONFIG.sheetNames.counselingAi);
}

function ensureCounselingAiSheetReady_() {
  const sheet = getCounselingAiSheet_();
  const expectedHeaders = APP_CONFIG.headers.counselingAi;
  const hasExpectedHeaders = hasMatchingHeaderRows_(sheet, [expectedHeaders]);

  if (!hasExpectedHeaders) {
    if (canRebuildCounselingAiSheet_(sheet, expectedHeaders)) {
      rebuildCounselingAiSheet_(sheet, expectedHeaders);
    } else {
      throw new Error("AI 상담기록 출력 시트 헤더가 예상과 다릅니다. 빈 시트를 지정하거나 A:U 헤더를 확인하세요.");
    }
  }
  formatCounselingAiSheet_(sheet);
  return sheet;
}

function canRebuildCounselingAiSheet_(sheet, expectedHeaders) {
  if (!sheet) {
    return false;
  }

  if (sheet.getLastRow() <= 1) {
    return true;
  }

  if (sheet.getLastColumn() < 1) {
    return true;
  }

  const currentHeaders = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getDisplayValues()[0]
    .map(function(value) {
      return normalizeText_(value);
    })
    .filter(function(value) {
      return !!value;
    });

  if (!currentHeaders.length) {
    return true;
  }

  const currentHeaderMap = {};
  currentHeaders.forEach(function(header) {
    currentHeaderMap[header] = true;
  });

  const matchedHeaders = expectedHeaders.filter(function(header) {
    return !!currentHeaderMap[header];
  });

  if (matchedHeaders.length === expectedHeaders.length) {
    return true;
  }

  const visibleAnchorHeaders = ["상담일자", "대상자명", "상담유형", "AI입력텍스트", "처리상태"];
  const matchedAnchorCount = visibleAnchorHeaders.filter(function(header) {
    return !!currentHeaderMap[header];
  }).length;

  if (matchedAnchorCount >= 3 && matchedHeaders.length >= 6) {
    return true;
  }

  if (matchedHeaders.length < 4) {
    return false;
  }

  const foreignHeaderCount = currentHeaders.filter(function(header) {
    return expectedHeaders.indexOf(header) === -1;
  }).length;

  return foreignHeaderCount <= Math.max(3, Math.floor(currentHeaders.length / 3));
}

function rebuildCounselingAiSheet_(sheet, expectedHeaders) {
  const lastRow = sheet.getLastRow();
  const lastColumn = Math.max(sheet.getLastColumn(), expectedHeaders.length);
  let dataRows = [];

  if (lastRow > 1 && sheet.getLastColumn() > 0) {
    const currentHeaders = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getDisplayValues()[0]
      .map(function(value) {
        return normalizeText_(value);
      });
    const currentValues = sheet.getRange(2, 1, lastRow - 1, sheet.getLastColumn()).getDisplayValues();
    dataRows = currentValues.map(function(row) {
      return expectedHeaders.map(function(header) {
        const sourceIndex = currentHeaders.indexOf(header);
        return sourceIndex === -1 ? "" : row[sourceIndex];
      });
    });
  }

  clearSheet_(sheet);
  ensureSheetSize_(sheet, Math.max(dataRows.length + 1, 2), lastColumn);
  sheet.getRange(1, 1, 1, expectedHeaders.length).setValues([expectedHeaders]);
  styleHeaderRow_(sheet, 1, expectedHeaders.length);

  if (dataRows.length) {
    sheet.getRange(2, 1, dataRows.length, expectedHeaders.length).setValues(dataRows);
  }
}

function getCounselingAiRecordId_(sheetName, rowNumber) {
  return sanitizeIdPart_(sheetName, "sheet") + ":" + String(rowNumber);
}

function getCounselingAiColumnIndexes_() {
  const headerMap = getHeaderMap(APP_CONFIG.headers.counselingAi);
  return {
    sessionDate: headerMap["상담일자"] + 1,
    clientName: headerMap["대상자명"] + 1,
    consultationType: headerMap["상담유형"] + 1,
    promptText: headerMap["AI입력텍스트"] + 1,
    status: headerMap["처리상태"] + 1,
    recordText: headerMap["상담기록"] + 1,
    followUp: headerMap["후속조치"] + 1,
    chiefComplaint: headerMap["주호소"] + 1,
    summary: headerMap["핵심요약"] + 1,
    keywords: headerMap["키워드"] + 1,
    modelName: headerMap["사용모델"] + 1,
    errorMessage: headerMap["오류메시지"] + 1,
    recordId: headerMap["기록ID"] + 1,
    sourceSheet: headerMap["원본시트"] + 1,
    sourceRowNumber: headerMap["원본행번호"] + 1,
    processedAt: headerMap["처리일시"] + 1,
    workerName: headerMap["담당자"] + 1,
    riskLevel: headerMap["위험도"] + 1,
    sourceNarrative: headerMap["원본상담내용"] + 1,
    sourceServices: headerMap["원본제공서비스"] + 1,
    geminiPrompt: headerMap["Gemini프롬프트"] + 1
  };
}

function getCounselingAiStatusMap_(sheet) {
  const map = {};
  if (!sheet || sheet.getLastRow() < 2) {
    return map;
  }

  const columnIndexes = getCounselingAiColumnIndexes_();
  const lastColumn = sheet.getLastColumn();
  const values = sheet.getRange(2, 1, sheet.getLastRow() - 1, lastColumn).getDisplayValues();
  values.forEach(function(row) {
    const recordId = normalizeText_(row[columnIndexes.recordId - 1]);
    if (!recordId) {
      return;
    }
    map[recordId] = normalizeText_(row[columnIndexes.status - 1]);
  });

  return map;
}

function upsertCounselingAiOutputRow_(sheet, rowValues) {
  const columnIndexes = getCounselingAiColumnIndexes_();
  const recordId = normalizeText_(rowValues[columnIndexes.recordId - 1]);
  const existingRowNumbers = findSheetRowNumbersByKeys_(sheet, columnIndexes.recordId, [recordId]);

  if (existingRowNumbers.length) {
    sheet.getRange(existingRowNumbers[0], 1, 1, rowValues.length).setValues([rowValues]);
    if (existingRowNumbers.length > 1) {
      deleteSheetRowGroups_(sheet, existingRowNumbers.slice(1));
    }
    return existingRowNumbers[0];
  }

  const startRow = Math.max(sheet.getLastRow(), 1) + 1;
  sheet.getRange(startRow, 1, 1, rowValues.length).setValues([rowValues]);
  return startRow;
}

function formatCounselingAiSheet_(sheet) {
  if (!sheet || sheet.getLastColumn() === 0) {
    return;
  }

  const columnIndexes = getCounselingAiColumnIndexes_();
  sheet.setFrozenRows(1);
  ensureFilter_(sheet);
  sheet.getRange(1, columnIndexes.processedAt, sheet.getMaxRows(), 1).setNumberFormat("yyyy-mm-dd hh:mm:ss");
  sheet.getRange(1, columnIndexes.sessionDate, sheet.getMaxRows(), 1).setNumberFormat("@");
  sheet.getRange("A:U").setVerticalAlignment("top");
  sheet.getRange(1, columnIndexes.promptText, sheet.getMaxRows(), 1).setWrap(true);
  sheet.getRange(1, columnIndexes.recordText, sheet.getMaxRows(), 1).setWrap(true);
  sheet.getRange(1, columnIndexes.promptText).setNote(
    "이 열은 =GEMINI() 수식으로 준비됩니다. 셀을 선택한 뒤 Generate and Insert를 눌러 최종 텍스트를 반영하세요."
  );
  sheet.hideColumns(columnIndexes.recordId, 9);
  applyColumnWidths_(sheet, [
    110, 110, 100, 420, 240, 380, 220, 180, 220,
    160, 180, 220, 140, 120, 90, 150, 110, 90, 260, 220, 380
  ]);
}

function applyCounselingAiPromptFormulaToRow_(sheet, rowNumber, sourceRecord) {
  const columnIndexes = getCounselingAiColumnIndexes_();
  const promptFormula = buildCounselingAiPromptFormula_(rowNumber, columnIndexes);
  const promptCell = sheet.getRange(rowNumber, columnIndexes.promptText);
  promptCell.setFormula(promptFormula);
  promptCell.setNote(
    [
      "Google Sheets Gemini 수식이 입력된 셀입니다.",
      "셀을 선택한 뒤 Generate and Insert를 눌러야 최종 텍스트가 반영됩니다."
    ].join("\n")
  );
  sheet.getRange(rowNumber, columnIndexes.status).setValue("수식입력");
  sheet.getRange(rowNumber, columnIndexes.errorMessage).setValue("");
  sheet.getRange(rowNumber, columnIndexes.modelName).setValue(getCounselingAiModelName_());
}

function buildCounselingAiPromptFormula_(rowNumber, columnIndexes) {
  const promptRef = "$" + getColumnA1Letter_(columnIndexes.geminiPrompt) + rowNumber;
  return "=GEMINI(" + promptRef + ")";
}

function getCounselingAiBatchSize_() {
  const value = parseInt(getSettingValue_("counseling_ai_batch_size", "10"), 10);
  if (!value || value < 1) {
    return 10;
  }
  return Math.min(value, 50);
}

function getCounselingAiInterRequestDelayMs_() {
  return APP_CONFIG.counselingAi.interRequestDelayMs;
}

function shouldRetryGeminiRequest_(statusCode, message) {
  const normalizedMessage = normalizeText_(message).toLowerCase();
  return statusCode === 429 ||
    normalizedMessage.indexOf("quota exceeded") !== -1 ||
    normalizedMessage.indexOf("please retry in") !== -1 ||
    normalizedMessage.indexOf("rate limit") !== -1;
}

function resolveGeminiRetryDelayMs_(message, attempt) {
  const retryMatch = normalizeText_(message).match(/please retry in\s*([0-9.]+)s/i);
  if (retryMatch) {
    const retrySeconds = parseFloat(retryMatch[1]);
    if (!isNaN(retrySeconds) && retrySeconds > 0) {
      return Math.max(
        Math.ceil(retrySeconds * 1000) + 500,
        APP_CONFIG.counselingAi.minRetryDelayMs
      );
    }
  }

  return Math.min(
    APP_CONFIG.counselingAi.minRetryDelayMs * Math.pow(2, Math.max(attempt - 1, 0)),
    60000
  );
}

function getGeminiApiKey_() {
  return normalizeText_(PropertiesService.getScriptProperties().getProperty("GEMINI_API_KEY"));
}

function countNonBlankValues_(values) {
  return (values || []).filter(function(value) {
    return !!normalizeText_(value);
  }).length;
}
