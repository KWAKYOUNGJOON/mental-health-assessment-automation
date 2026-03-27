function rebuildDatabase(isManual) {
  const lock = LockService.getDocumentLock();
  if (isManual) {
    lock.waitLock(30000);
  } else {
    // 실시간 동기화 시에는 1초만 대기하고 이미 잠겨있으면 건너뜀 (다음 편집 시 처리되므로)
    if (!lock.tryLock(1000)) return;
  }

  try {
    const sheetMap = getSheetMap();
    const rawSheet = sheetMap.rawInput;
    const dashboardLookupValue = sheetMap.dashboard.getRange(APP_CONFIG.dashboard.lookupCell).getDisplayValue();
    const rawRange = rawSheet.getDataRange();
    const values = rawRange.getValues();
    const displayValues = rawRange.getDisplayValues();

    if (values.length < APP_CONFIG.sourceHeaderRows) {
      throw new Error("원본 시트에는 최소 1행 그룹 헤더와 2행 실제 헤더가 필요합니다.");
    }

    const headerMap = buildCompositeHeaderMap(displayValues[0], displayValues[1]);
    validateRequiredHeaders_(headerMap);

    const parsed = parseRawRows(values, displayValues, headerMap);

    clearTargetSheets();
    writeBatchData(APP_CONFIG.sheetNames.base, APP_CONFIG.headers.base, parsed.baseRows);
    writeBatchData(APP_CONFIG.sheetNames.hospital, APP_CONFIG.headers.hospital, parsed.hospitalRows);
    writeBatchData(APP_CONFIG.sheetNames.facility, APP_CONFIG.headers.facility, parsed.facilityRows);
    writeBatchData(APP_CONFIG.sheetNames.service, APP_CONFIG.headers.service, parsed.serviceRows);
    writeBatchData(APP_CONFIG.sheetNames.publicService, APP_CONFIG.headers.publicService, parsed.publicServiceRows);
    writeErrorLog(parsed.errorRows);

    refreshDashboard(true, dashboardLookupValue);
    formatSheets();

    if (isManual) {
      showToast_(
        "전체 재구성이 완료되었습니다. base " + parsed.baseRows.length +
          "건, 서비스 " + parsed.serviceRows.length + "건",
        APP_CONFIG.menuTitle,
        8
      );
    }

    return {
      baseRecords: parsed.baseRows.length,
      hospitalRecords: parsed.hospitalRows.length,
      facilityRecords: parsed.facilityRows.length,
      serviceRecords: parsed.serviceRows.length,
      publicServiceRecords: parsed.publicServiceRows.length,
      errorRows: parsed.errorRows.length
    };
  } catch (error) {
    if (isManual && typeof SpreadsheetApp !== 'undefined') {
      SpreadsheetApp.getUi().alert("데이터 자동화 오류", error.message + "\n\n(README.md의 헤더 구조 안내를 참고해주세요.)", SpreadsheetApp.getUi().ButtonSet.OK);
    } else {
      console.error("실시간 동기화 오류:", error);
    }
    throw error;
  } finally {
    lock.releaseLock();
  }
}

function validateRequiredHeaders_(headerMap) {
  const missingHeaders = APP_CONFIG.requiredSourceHeaders.filter(function(key) {
    return headerMap[key] === undefined;
  });

  if (missingHeaders.length) {
    throw new Error("원본 시트 헤더가 누락되었습니다: " + missingHeaders.join(", "));
  }
}

function syncEditedRow(e) {
  const lock = LockService.getDocumentLock();
  lock.waitLock(10000);

  try {
    const range = e.range;
    const rawSheet = range.getSheet();
    const firstRow = Math.max(range.getRow(), APP_CONFIG.sourceHeaderRows + 1);
    const lastRow = Math.max(range.getLastRow(), APP_CONFIG.sourceHeaderRows + 1);
    const rawRange = rawSheet.getDataRange();
    const values = rawRange.getValues();
    const displayValues = rawRange.getDisplayValues();

    if (values.length < APP_CONFIG.sourceHeaderRows) {
      return;
    }

    const headerMap = buildCompositeHeaderMap(displayValues[0], displayValues[1]);
    validateRequiredHeaders_(headerMap);

    const sheetMap = getSheetMap();
    const recordIdCounts = buildRecordIdCountMap_(values, displayValues, headerMap);
    const monthlyDeltaMap = {};

    for (let rowNumber = firstRow; rowNumber <= lastRow; rowNumber += 1) {
      const row = values[rowNumber - 1] || [];
      const displayRow = displayValues[rowNumber - 1] || [];
      const parsedRow = parseSingleRawRowForSync_(row, displayRow, headerMap, rowNumber, recordIdCounts, e);
      const existingMonthlySourceRows = collectExistingMonthlySourceRows_(sheetMap, parsedRow.recordIdsToClear);

      accumulateMonthlyDeltaMap_(monthlyDeltaMap, existingMonthlySourceRows, -1);
      accumulateMonthlyDeltaMap_(monthlyDeltaMap, parsedRow, 1);
      applyParsedRowToManagedSheets_(sheetMap, parsedRow);
    }

    applyMonthlyDeltaMap_(sheetMap.monthly, monthlyDeltaMap);

    return {
      baseRecords: Math.max(sheetMap.base.getLastRow() - 1, 0),
      hospitalRecords: Math.max(sheetMap.hospital.getLastRow() - 1, 0),
      facilityRecords: Math.max(sheetMap.facility.getLastRow() - 1, 0),
      serviceRecords: Math.max(sheetMap.service.getLastRow() - 1, 0),
      publicServiceRecords: Math.max(sheetMap.publicService.getLastRow() - 1, 0),
      errorRows: Math.max(sheetMap.error.getLastRow() - 1, 0)
    };
  } finally {
    lock.releaseLock();
  }
}

function buildRecordIdCountMap_(values, displayValues, headerMap) {
  const counts = {};

  for (let rowIndex = APP_CONFIG.sourceHeaderRows; rowIndex < values.length; rowIndex += 1) {
    const row = values[rowIndex];
    const displayRow = displayValues[rowIndex];
    if (!isMeaningfulRow(row, displayRow, headerMap)) {
      continue;
    }

    const rowNumber = rowIndex + 1;
    const recordId = generateRecordId(row, displayRow, headerMap, rowNumber);
    counts[recordId] = (counts[recordId] || 0) + 1;
  }

  return counts;
}

function parseSingleRawRowForSync_(row, displayRow, headerMap, rowNumber, recordIdCounts, e) {
  const result = {
    recordIdsToClear: [],
    baseRows: [],
    hospitalRows: [],
    facilityRows: [],
    serviceRows: [],
    publicServiceRows: [],
    errorRows: []
  };
  const initialRecordId = generateRecordId(row, displayRow, headerMap, rowNumber);
  result.recordIdsToClear = buildRecordIdsToClearForSync_(row, displayRow, headerMap, rowNumber, e, initialRecordId);

  if (!isMeaningfulRow(row, displayRow, headerMap)) {
    return result;
  }

  const validation = validateRow(row, displayRow, headerMap, rowNumber);
  const recordId = generateRecordId(row, displayRow, headerMap, rowNumber, validation);
  const skipInvalidDetails = getBooleanSetting_("skip_invalid_detail_rows", true);
  const now = new Date();

  result.recordIdsToClear = buildRecordIdsToClearForSync_(row, displayRow, headerMap, rowNumber, e, recordId);

  if ((recordIdCounts[recordId] || 0) > 1) {
    validation.errors.push({
      type: "DUPLICATE_RECORD_ID",
      message: "생성된 record_id가 중복되었습니다.",
      status: "OPEN"
    });
    validation.isValid = false;
  }

  result.baseRows.push(normalizeBaseRecord(row, displayRow, headerMap, rowNumber, recordId, validation, now));

  validation.errors.forEach(function(error) {
    result.errorRows.push(buildErrorLogRow_(rowNumber, recordId, validation, error, now));
  });

  if (!validation.isValid && skipInvalidDetails) {
    return result;
  }

  const hospitalRow = normalizeHospitalRecord(row, displayRow, headerMap, recordId, rowNumber, validation, now);
  if (hospitalRow) {
    result.hospitalRows.push(hospitalRow);
  }

  const facilityRow = normalizeFacilityRecord(row, displayRow, headerMap, recordId, rowNumber, validation, now);
  if (facilityRow) {
    result.facilityRows.push(facilityRow);
  }

  const normalizedServiceRows = normalizeServiceRecords(
    row,
    displayRow,
    headerMap,
    recordId,
    rowNumber,
    validation,
    now
  );
  if (normalizedServiceRows.serviceRows.length) {
    Array.prototype.push.apply(result.serviceRows, normalizedServiceRows.serviceRows);
  }
  if (normalizedServiceRows.publicServiceRows.length) {
    Array.prototype.push.apply(result.publicServiceRows, normalizedServiceRows.publicServiceRows);
  }

  return result;
}

function buildRecordIdsToClearForSync_(row, displayRow, headerMap, rowNumber, e, currentRecordId) {
  const recordIds = dedupeStrings_([currentRecordId, String(rowNumber)]);
  const numberColumnIndex = headerMap["공통_번호"];

  if (
    e &&
    e.range &&
    numberColumnIndex !== undefined &&
    e.range.getNumRows() === 1 &&
    e.range.getNumColumns() === 1 &&
    e.range.getColumn() === numberColumnIndex + 1
  ) {
    const oldValue = normalizeText_(e.oldValue);
    if (oldValue) {
      recordIds.push(oldValue);
    }
  }

  return dedupeStrings_(recordIds);
}

function mergeRowsForSync_(existingRows, newRows, recordIdsToClear, keyIndex) {
  const deleteMap = {};
  dedupeStrings_(recordIdsToClear || []).forEach(function(recordId) {
    deleteMap[recordId] = true;
  });

  const mergedRows = (existingRows || []).filter(function(row) {
    const rowKey = normalizeText_(row[keyIndex]);
    return !deleteMap[rowKey];
  });

  if (newRows && newRows.length) {
    Array.prototype.push.apply(mergedRows, newRows);
  }

  return mergedRows;
}

function collectExistingMonthlySourceRows_(sheetMap, recordIdsToClear) {
  return {
    baseRows: findMatchingRowsByKeysInSheet_(sheetMap.base, 1, recordIdsToClear),
    hospitalRows: findMatchingRowsByKeysInSheet_(sheetMap.hospital, 1, recordIdsToClear),
    facilityRows: findMatchingRowsByKeysInSheet_(sheetMap.facility, 1, recordIdsToClear),
    serviceRows: findMatchingRowsByKeysInSheet_(sheetMap.service, 2, recordIdsToClear),
    publicServiceRows: findMatchingRowsByKeysInSheet_(sheetMap.publicService, 2, recordIdsToClear)
  };
}

function applyParsedRowToManagedSheets_(sheetMap, parsedRow) {
  upsertRowsInSheet_(
    sheetMap.base,
    APP_CONFIG.headers.base,
    parsedRow.baseRows,
    parsedRow.recordIdsToClear,
    1,
    APP_CONFIG.sheetNames.base
  );
  upsertRowsInSheet_(
    sheetMap.hospital,
    APP_CONFIG.headers.hospital,
    parsedRow.hospitalRows,
    parsedRow.recordIdsToClear,
    1,
    APP_CONFIG.sheetNames.hospital
  );
  upsertRowsInSheet_(
    sheetMap.facility,
    APP_CONFIG.headers.facility,
    parsedRow.facilityRows,
    parsedRow.recordIdsToClear,
    1,
    APP_CONFIG.sheetNames.facility
  );
  upsertRowsInSheet_(
    sheetMap.service,
    APP_CONFIG.headers.service,
    parsedRow.serviceRows,
    parsedRow.recordIdsToClear,
    2,
    APP_CONFIG.sheetNames.service
  );
  upsertRowsInSheet_(
    sheetMap.publicService,
    APP_CONFIG.headers.publicService,
    parsedRow.publicServiceRows,
    parsedRow.recordIdsToClear,
    2,
    APP_CONFIG.sheetNames.publicService
  );
  upsertRowsInSheet_(
    sheetMap.error,
    APP_CONFIG.headers.error,
    parsedRow.errorRows,
    parsedRow.recordIdsToClear,
    2,
    APP_CONFIG.sheetNames.error
  );
}

function upsertRowsInSheet_(sheet, headers, newRows, recordIdsToClear, keyColumnNumber, sheetName) {
  ensureSheetHeaderForSync_(sheet, headers);
  deleteRowsByKeysFromSheet_(sheet, keyColumnNumber, recordIdsToClear);
  appendRowsForSync_(sheet, headers, newRows, sheetName);
}

function ensureSheetHeaderForSync_(sheet, headers) {
  if (sheet.getLastRow() > 0 && sheet.getLastColumn() > 0) {
    return;
  }

  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  styleHeaderRow_(sheet, 1, headers.length);
}

function deleteRowsByKeysFromSheet_(sheet, keyColumnNumber, recordIdsToClear) {
  if (sheet.getLastRow() < 2) {
    return;
  }

  const rowNumbersToDelete = findSheetRowNumbersByKeys_(sheet, keyColumnNumber, recordIdsToClear);
  if (!rowNumbersToDelete.length) {
    return;
  }

  deleteSheetRowGroups_(sheet, rowNumbersToDelete);
}

function deleteSheetRowGroups_(sheet, rowNumbers) {
  const sortedRows = rowNumbers.slice().sort(function(left, right) {
    return left - right;
  });
  const groups = [];
  let groupStart = sortedRows[0];
  let groupEnd = sortedRows[0];

  for (let index = 1; index < sortedRows.length; index += 1) {
    const rowNumber = sortedRows[index];
    if (rowNumber === groupEnd + 1) {
      groupEnd = rowNumber;
      continue;
    }

    groups.push([groupStart, groupEnd]);
    groupStart = rowNumber;
    groupEnd = rowNumber;
  }
  groups.push([groupStart, groupEnd]);

  for (let index = groups.length - 1; index >= 0; index -= 1) {
    const group = groups[index];
    sheet.deleteRows(group[0], group[1] - group[0] + 1);
  }
}

function appendRowsForSync_(sheet, headers, rows, sheetName) {
  if (!rows || !rows.length) {
    return;
  }

  const startRow = Math.max(sheet.getLastRow(), 1) + 1;
  sheet.getRange(startRow, 1, rows.length, headers.length).setValues(rows);
  formatSyncSheetAfterAppend_(sheet, sheetName, startRow, rows);
}

function formatSyncSheetAfterAppend_(sheet, sheetName, startRow, rows) {
  const rowCount = rows.length;

  sheet.setFrozenRows(1);
  if (!sheet.getFilter() && sheet.getLastRow() >= 1 && sheet.getLastColumn() >= 1) {
    sheet.getRange(1, 1, sheet.getLastRow(), sheet.getLastColumn()).createFilter();
  }

  if (sheetName === APP_CONFIG.sheetNames.base) {
    sheet.getRange("B:B").setNumberFormat("yyyy-mm-dd");
    sheet.getRange(startRow, 9, rowCount, 1).insertCheckboxes();
    sheet.getRange(startRow, 9, rowCount, 1).setValues(rows.map(function(row) {
      return [row[8] === true];
    }));
    return;
  }

  if (sheetName === APP_CONFIG.sheetNames.hospital || sheetName === APP_CONFIG.sheetNames.facility) {
    sheet.getRange("B:B").setNumberFormat("yyyy-mm-dd");
    return;
  }

  if (sheetName === APP_CONFIG.sheetNames.service || sheetName === APP_CONFIG.sheetNames.publicService) {
    sheet.getRange("C:C").setNumberFormat("yyyy-mm-dd");
    return;
  }

  if (sheetName === APP_CONFIG.sheetNames.error) {
    sheet.getRange("A:A").setNumberFormat("yyyy-mm-dd hh:mm:ss");
  }
}

function findMatchingRowsByKeysInSheet_(sheet, keyColumnNumber, keys) {
  const rowNumbers = findSheetRowNumbersByKeys_(sheet, keyColumnNumber, keys);
  if (!rowNumbers.length) {
    return [];
  }

  const lastColumn = sheet.getLastColumn();
  return rowNumbers.map(function(rowNumber) {
    return sheet.getRange(rowNumber, 1, 1, lastColumn).getValues()[0];
  });
}

function findSheetRowNumbersByKeys_(sheet, keyColumnNumber, keys) {
  const normalizedKeys = dedupeStrings_(keys || []);
  if (!normalizedKeys.length || sheet.getLastRow() < 2) {
    return [];
  }

  const keyRange = sheet.getRange(2, keyColumnNumber, sheet.getLastRow() - 1, 1);
  const rowNumberMap = {};

  normalizedKeys.forEach(function(key) {
    if (!key) {
      return;
    }

    const matches = keyRange.createTextFinder(key)
      .matchEntireCell(true)
      .findAll();
    matches.forEach(function(cell) {
      rowNumberMap[cell.getRow()] = true;
    });
  });

  return Object.keys(rowNumberMap).map(function(rowNumber) {
    return Number(rowNumber);
  }).sort(function(left, right) {
    return left - right;
  });
}

function accumulateMonthlyDeltaMap_(deltaMap, sourceRows, multiplier) {
  if (!sourceRows) {
    return;
  }

  (sourceRows.baseRows || []).forEach(function(row) {
    if (row[8] !== true) {
      return;
    }
    addMonthlyMetricDelta_(deltaMap, getMonthKeyForSummary_(row[1]), 1, multiplier);
  });

  (sourceRows.hospitalRows || []).forEach(function(row) {
    addMonthlyMetricDelta_(deltaMap, getMonthKeyForSummary_(row[1]), 2, multiplier);
  });

  (sourceRows.facilityRows || []).forEach(function(row) {
    addMonthlyMetricDelta_(deltaMap, getMonthKeyForSummary_(row[1]), 3, multiplier);
  });

  (sourceRows.serviceRows || []).forEach(function(row) {
    const monthKey = getMonthKeyForSummary_(row[2]);
    addMonthlyMetricDelta_(deltaMap, monthKey, 11, multiplier);

    const serviceType = normalizeText_(row[7]);
    const serviceColumnMap = {
      "주거지원": 4,
      "물품제공": 5,
      "방문상담": 6,
      "외래진료": 7,
      "투약관리": 8,
      "기타": 10
    };
    addMonthlyMetricDelta_(deltaMap, monthKey, serviceColumnMap[serviceType] || 10, multiplier);
  });

  (sourceRows.publicServiceRows || []).forEach(function(row) {
    const monthKey = getMonthKeyForSummary_(row[2]);
    addMonthlyMetricDelta_(deltaMap, monthKey, 9, multiplier);
    addMonthlyMetricDelta_(deltaMap, monthKey, 11, multiplier);
  });
}

function addMonthlyMetricDelta_(deltaMap, monthKey, metricIndex, deltaValue) {
  if (!monthKey || !metricIndex || !deltaValue) {
    return;
  }

  if (!deltaMap[monthKey]) {
    deltaMap[monthKey] = createEmptyMonthlySummaryRow_(monthKey);
  }
  deltaMap[monthKey][metricIndex] += deltaValue;
}

function applyMonthlyDeltaMap_(monthlySheet, deltaMap) {
  const months = Object.keys(deltaMap || {});
  if (!months.length) {
    return;
  }

  const rowMap = {};
  getDataRows_(monthlySheet).forEach(function(row) {
    const monthKey = normalizeText_(row[0]);
    if (!monthKey) {
      return;
    }
    rowMap[monthKey] = normalizeMonthlySummaryRow_(row);
  });

  months.forEach(function(monthKey) {
    const currentRow = rowMap[monthKey] || createEmptyMonthlySummaryRow_(monthKey);
    for (let index = 1; index < currentRow.length; index += 1) {
      currentRow[index] += deltaMap[monthKey][index];
      if (currentRow[index] === -0) {
        currentRow[index] = 0;
      }
    }

    if (hasNonZeroMonthlyMetrics_(currentRow)) {
      rowMap[monthKey] = currentRow;
    } else {
      delete rowMap[monthKey];
    }
  });

  const summaryRows = Object.keys(rowMap).sort().map(function(monthKey) {
    return rowMap[monthKey];
  });
  writeBatchData(monthlySheet, APP_CONFIG.headers.monthly, summaryRows);
  formatMonthlySheet_(monthlySheet);
}

function createEmptyMonthlySummaryRow_(monthKey) {
  return [monthKey, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
}

function normalizeMonthlySummaryRow_(row) {
  const normalizedRow = createEmptyMonthlySummaryRow_(normalizeText_(row[0]));
  for (let index = 1; index < normalizedRow.length; index += 1) {
    normalizedRow[index] = Number(row[index]) || 0;
  }
  return normalizedRow;
}

function hasNonZeroMonthlyMetrics_(row) {
  for (let index = 1; index < row.length; index += 1) {
    if ((Number(row[index]) || 0) !== 0) {
      return true;
    }
  }
  return false;
}

function getMonthKeyForSummary_(value) {
  const date = toMidnightDate_(value);
  if (!date) {
    return "";
  }
  return Utilities.formatDate(date, APP_CONFIG.timezone, "yyyy-MM");
}

function parseRawRows(values, displayValues, headerMap) {
  const baseRows = [];
  const hospitalRows = [];
  const facilityRows = [];
  const serviceRows = [];
  const publicServiceRows = [];
  const errorRows = [];
  const seenRecordIds = {};
  const skipInvalidDetails = getBooleanSetting_("skip_invalid_detail_rows", true);
  const now = new Date();

  for (let rowIndex = APP_CONFIG.sourceHeaderRows; rowIndex < values.length; rowIndex += 1) {
    const row = values[rowIndex];
    const displayRow = displayValues[rowIndex];
    const rowNumber = rowIndex + 1;

    if (!isMeaningfulRow(row, displayRow, headerMap)) {
      continue;
    }

    const validation = validateRow(row, displayRow, headerMap, rowNumber);
    const recordId = generateRecordId(row, displayRow, headerMap, rowNumber, validation);

    if (seenRecordIds[recordId]) {
      validation.errors.push({
        type: "DUPLICATE_RECORD_ID",
        message: "생성된 record_id가 중복되었습니다.",
        status: "OPEN"
      });
      validation.isValid = false;
    } else {
      seenRecordIds[recordId] = true;
    }

    baseRows.push(normalizeBaseRecord(row, displayRow, headerMap, rowNumber, recordId, validation, now));

    validation.errors.forEach(function(error) {
      errorRows.push(buildErrorLogRow_(rowNumber, recordId, validation, error, now));
    });

    if (!validation.isValid && skipInvalidDetails) {
      continue;
    }

    const hospitalRow = normalizeHospitalRecord(row, displayRow, headerMap, recordId, rowNumber, validation, now);
    if (hospitalRow) {
      hospitalRows.push(hospitalRow);
    }

    const facilityRow = normalizeFacilityRecord(row, displayRow, headerMap, recordId, rowNumber, validation, now);
    if (facilityRow) {
      facilityRows.push(facilityRow);
    }

    const normalizedServiceRows = normalizeServiceRecords(
      row,
      displayRow,
      headerMap,
      recordId,
      rowNumber,
      validation,
      now
    );
    if (normalizedServiceRows.serviceRows.length) {
      Array.prototype.push.apply(serviceRows, normalizedServiceRows.serviceRows);
    }
    if (normalizedServiceRows.publicServiceRows.length) {
      Array.prototype.push.apply(publicServiceRows, normalizedServiceRows.publicServiceRows);
    }
  }

  return {
    baseRows: baseRows,
    hospitalRows: hospitalRows,
    facilityRows: facilityRows,
    serviceRows: serviceRows,
    publicServiceRows: publicServiceRows,
    errorRows: errorRows
  };
}

function generateRecordId(row, displayRow, headerMap, rowNumber, validation) {
  const originalNumber = normalizeText_(
    getDisplayFieldValue_(displayRow, headerMap, "공통_번호") || getFieldValue_(row, headerMap, "공통_번호")
  );

  return originalNumber ? originalNumber : String(rowNumber);
}

function normalizeBaseRecord(row, displayRow, headerMap, rowNumber, recordId, validation, now) {
  return [
    recordId,
    validation.normalizedDate || validation.rawDate || "",
    normalizeText_(getDisplayFieldValue_(displayRow, headerMap, "공통_담당자") || getFieldValue_(row, headerMap, "공통_담당자")),
    validation.rawName,
    validation.birthInfo.birth_raw,
    validation.birthInfo.birth_normalized,
    normalizeText_(getDisplayFieldValue_(displayRow, headerMap, "공통_성별") || getFieldValue_(row, headerMap, "공통_성별")),
    normalizeText_(getDisplayFieldValue_(displayRow, headerMap, "공통_상담유형") || getFieldValue_(row, headerMap, "공통_상담유형")),
    validation.isValid
  ];
}

function extractCommonPrefix_(row, displayRow, headerMap, validation) {
  return [
    validation.normalizedDate || validation.rawDate || "",
    normalizeText_(getDisplayFieldValue_(displayRow, headerMap, "공통_담당자") || getFieldValue_(row, headerMap, "공통_담당자")),
    validation.rawName,
    validation.birthInfo.birth_normalized,
    normalizeText_(getDisplayFieldValue_(displayRow, headerMap, "공통_성별") || getFieldValue_(row, headerMap, "공통_성별"))
  ];
}

function normalizeHospitalRecord(row, displayRow, headerMap, recordId, rowNumber, validation, now) {
  const hospitalizationType = normalizeText_(
    getDisplayFieldValue_(displayRow, headerMap, "병원_입원유형") || getFieldValue_(row, headerMap, "병원_입원유형")
  );
  const hospitalName = normalizeText_(
    getDisplayFieldValue_(displayRow, headerMap, "병원_병원명") || getFieldValue_(row, headerMap, "병원_병원명")
  );
  const hospitalizationStatus = normalizeText_(
    getDisplayFieldValue_(displayRow, headerMap, "병원_입원여부") || getFieldValue_(row, headerMap, "병원_입원여부")
  );
  const postDischargeAction = normalizeText_(
    getDisplayFieldValue_(displayRow, headerMap, "병원_퇴원후연계") || getFieldValue_(row, headerMap, "병원_퇴원후연계")
  );

  if (!hospitalizationType && !hospitalName && !hospitalizationStatus && !postDischargeAction) {
    return null;
  }

  const commonData = extractCommonPrefix_(row, displayRow, headerMap, validation);
  return [
    recordId,
    ...commonData,
    hospitalizationType,
    hospitalName,
    hospitalizationStatus,
    postDischargeAction
  ];
}

function normalizeFacilityRecord(row, displayRow, headerMap, recordId, rowNumber, validation, now) {
  const facilityType = normalizeText_(
    getDisplayFieldValue_(displayRow, headerMap, "시설_입소유형") || getFieldValue_(row, headerMap, "시설_입소유형")
  );
  const facilityName = normalizeText_(
    getDisplayFieldValue_(displayRow, headerMap, "시설_시설명") || getFieldValue_(row, headerMap, "시설_시설명")
  );
  const facilityStatus = normalizeText_(
    getDisplayFieldValue_(displayRow, headerMap, "시설_입퇴소여부") || getFieldValue_(row, headerMap, "시설_입퇴소여부")
  );
  const postExitAction = normalizeText_(
    getDisplayFieldValue_(displayRow, headerMap, "시설_퇴소후조치") || getFieldValue_(row, headerMap, "시설_퇴소후조치")
  );

  if (!facilityType && !facilityName && !facilityStatus && !postExitAction) {
    return null;
  }

  const commonData = extractCommonPrefix_(row, displayRow, headerMap, validation);
  return [
    recordId,
    ...commonData,
    facilityType,
    facilityName,
    facilityStatus,
    postExitAction
  ];
}

function normalizeServiceRecords(row, displayRow, headerMap, recordId, rowNumber, validation, now) {
  const normalizedRows = [];
  const normalizedPublicRows = [];
  const commonData = extractCommonPrefix_(row, displayRow, headerMap, validation);
  const relatedOrganization = normalizeText_(
    getDisplayFieldValue_(displayRow, headerMap, "서비스_병원명") || getFieldValue_(row, headerMap, "서비스_병원명")
  );
  let sequence = 1;

  APP_CONFIG.serviceFieldMap.forEach(function(serviceSpec) {
    if (!isTruthyCheckboxValue_(getFieldValue_(row, headerMap, serviceSpec.key))) {
      return;
    }

    normalizedRows.push([
      generateServiceId(recordId, serviceSpec.type, sequence),
      recordId,
      ...commonData,
      serviceSpec.type,
      "",
      relatedOrganization
    ]);
    sequence += 1;
  });

  // 공적서비스 별도 분리
  const supportItem = normalizeText_(getDisplayFieldValue_(displayRow, headerMap, "공적서비스_지원항목") || getFieldValue_(row, headerMap, "공적서비스_지원항목"));
  const applicantOrg = normalizeText_(getDisplayFieldValue_(displayRow, headerMap, "공적서비스_신청기관") || getFieldValue_(row, headerMap, "공적서비스_신청기관"));
  const supportDate = normalizeText_(getDisplayFieldValue_(displayRow, headerMap, "공적서비스_지원시기") || getFieldValue_(row, headerMap, "공적서비스_지원시기"));

  if (supportItem || applicantOrg || supportDate) {
    normalizedPublicRows.push([
      generateServiceId(recordId, "공적서비스", sequence),
      recordId,
      ...commonData,
      supportItem,
      applicantOrg,
      supportDate
    ]);
    sequence += 1;
  }

  const serviceDetail = normalizeText_(
    getDisplayFieldValue_(displayRow, headerMap, "서비스_기타") || getFieldValue_(row, headerMap, "서비스_기타")
  );
  if (serviceDetail) {
    normalizedRows.push([
      generateServiceId(recordId, "기타", sequence),
      recordId,
      ...commonData,
      "기타",
      serviceDetail,
      relatedOrganization
    ]);
  }

  return {
    serviceRows: normalizedRows,
    publicServiceRows: normalizedPublicRows
  };
}

function generateServiceId(recordId, serviceType, seq) {
  return [
    recordId,
    seq
  ].join("_");
}

function clearTargetSheets() {
  [
    APP_CONFIG.sheetNames.base,
    APP_CONFIG.sheetNames.hospital,
    APP_CONFIG.sheetNames.facility,
    APP_CONFIG.sheetNames.service,
    APP_CONFIG.sheetNames.publicService,
    APP_CONFIG.sheetNames.monthly,
    APP_CONFIG.sheetNames.error,
    APP_CONFIG.sheetNames.dashboard
  ].forEach(function(sheetName) {
    clearSheet_(getOrCreateSheet_(sheetName));
  });
}

function writeBatchData(sheetName, headers, rows) {
  const sheet = typeof sheetName === "string" ? getOrCreateSheet_(sheetName) : sheetName;

  clearSheet_(sheet);
  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  styleHeaderRow_(sheet, 1, headers.length);

  if (rows.length) {
    sheet.getRange(2, 1, rows.length, headers.length).setValues(rows);
  }
}

function writeErrorLog(errorRows) {
  writeBatchData(APP_CONFIG.sheetNames.error, APP_CONFIG.headers.error, errorRows);
}

function buildErrorLogRow_(sourceRow, recordId, validation, error, now) {
  return [
    now,
    recordId,
    error.type,
    error.message,
    validation.rawName,
    validation.rawDate,
    error.status
  ];
}

function formatSheets() {
  const sheetMap = getSheetMap();

  formatBaseSheet_(sheetMap.base);
  formatHospitalSheet_(sheetMap.hospital);
  formatFacilitySheet_(sheetMap.facility);
  formatServiceSheet_(sheetMap.service);
  formatPublicServiceSheet_(sheetMap.publicService);
  formatCounselingAiSheet_(sheetMap.counselingAi);
  formatMonthlySheet_(sheetMap.monthly);
  formatErrorSheet_(sheetMap.error);
  formatDashboardSheet_(sheetMap.dashboard);
}

function formatBaseSheet_(sheet) {
  if (sheet.getLastColumn() === 0) {
    return;
  }

  sheet.setFrozenRows(1);
  ensureFilter_(sheet);
  sheet.getRange("B:B").setNumberFormat("yyyy-mm-dd");
  if (sheet.getLastRow() > 1) {
    sheet.getRange(2, 9, sheet.getLastRow() - 1, 1).insertCheckboxes();
  }

  applyColumnWidths_(sheet, [100, 110, 100, 100, 110, 130, 80, 100, 80]);
}

function formatHospitalSheet_(sheet) {
  if (sheet.getLastColumn() === 0) {
    return;
  }

  sheet.setFrozenRows(1);
  ensureFilter_(sheet);
  sheet.getRange("B:B").setNumberFormat("yyyy-mm-dd");
  applyColumnWidths_(sheet, [100, 100, 80, 80, 100, 60, 110, 180, 100, 180]);
}

function formatFacilitySheet_(sheet) {
  if (sheet.getLastColumn() === 0) {
    return;
  }

  sheet.setFrozenRows(1);
  ensureFilter_(sheet);
  sheet.getRange("B:B").setNumberFormat("yyyy-mm-dd");
  applyColumnWidths_(sheet, [100, 100, 80, 80, 100, 60, 110, 180, 100, 180]);
}

function formatServiceSheet_(sheet) {
  if (sheet.getLastColumn() === 0) {
    return;
  }

  sheet.setFrozenRows(1);
  ensureFilter_(sheet);
  sheet.getRange("C:C").setNumberFormat("yyyy-mm-dd");
  applyColumnWidths_(sheet, [120, 100, 100, 80, 80, 100, 60, 110, 180, 180]);
}

function formatPublicServiceSheet_(sheet) {
  if (sheet.getLastColumn() === 0) {
    return;
  }

  sheet.setFrozenRows(1);
  ensureFilter_(sheet);
  sheet.getRange("C:C").setNumberFormat("yyyy-mm-dd");
  applyColumnWidths_(sheet, [120, 100, 100, 80, 80, 100, 60, 150, 150, 120]);
}

function formatMonthlySheet_(sheet) {
  if (sheet.getLastColumn() === 0) {
    return;
  }

  sheet.setFrozenRows(1);
  ensureFilter_(sheet);
  applyColumnWidths_(sheet, [100, 80, 80, 80, 80, 80, 80, 80, 80, 90, 90, 100]);
}

function formatErrorSheet_(sheet) {
  if (sheet.getLastColumn() === 0) {
    return;
  }

  sheet.setFrozenRows(1);
  ensureFilter_(sheet);
  sheet.getRange("A:A").setNumberFormat("yyyy-mm-dd hh:mm:ss");
  applyColumnWidths_(sheet, [150, 220, 140, 360, 100, 110, 90]);
}

function applyColumnWidths_(sheet, widths) {
  widths.forEach(function(width, index) {
    sheet.setColumnWidth(index + 1, width);
  });
}
