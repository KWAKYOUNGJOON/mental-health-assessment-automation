function refreshDashboard(silent, initialLookupValue) {
  const sheetMap = getSheetMap();
  const sheet = sheetMap.dashboard;
  const previousLookupValue = initialLookupValue !== undefined
    ? initialLookupValue
    : sheet.getRange(APP_CONFIG.dashboard.lookupCell).getDisplayValue();

  resetDashboardSheet_(sheet);

  const baseRows = getDataRows_(sheetMap.base);
  const hospitalRows = getDataRows_(sheetMap.hospital);
  const facilityRows = getDataRows_(sheetMap.facility);
  const serviceRows = getDataRows_(sheetMap.service);
  const publicServiceRows = getDataRows_(sheetMap.publicService);
  const errorRows = getDataRows_(sheetMap.error);
  const validBaseRows = baseRows.filter(function(row) {
    return row[8] === true;
  });

  refreshMonthlySheet_(sheetMap.monthly, baseRows, hospitalRows, facilityRows, serviceRows, publicServiceRows);
  const monthlyRows = getDataRows_(sheetMap.monthly);
  const dashboardModel = buildDashboardModel_(
    baseRows,
    validBaseRows,
    hospitalRows,
    facilityRows,
    serviceRows,
    publicServiceRows,
    errorRows,
    monthlyRows
  );

  renderDashboardHeader_(sheet);
  renderDashboardCards_(sheet, dashboardModel);
  renderMonthlyOverviewSection_(sheet, dashboardModel, monthlyRows);
  renderOperationalSection_(sheet, dashboardModel, validBaseRows, serviceRows, errorRows);
  renderLookupSection_(sheet, previousLookupValue, baseRows);
  formatDashboardSheet_(sheet);

  if (!silent) {
    showToast_("대시보드를 갱신했습니다.");
  }
}

function resetDashboardSheet_(sheet) {
  ensureSheetSize_(sheet, 110, 24);

  const allRange = sheet.getRange(1, 1, sheet.getMaxRows(), sheet.getMaxColumns());
  allRange.breakApart();

  sheet.getCharts().forEach(function(chart) {
    sheet.removeChart(chart);
  });

  clearSheet_(sheet);
  allRange.clearDataValidations();
}

function buildDashboardModel_(
  baseRows,
  validBaseRows,
  hospitalRows,
  facilityRows,
  serviceRows,
  publicServiceRows,
  errorRows,
  monthlyRows
) {
  const latestMonthRow = getLatestMonthlyRow_(monthlyRows);
  const previousMonthRow = getPreviousMonthlyRow_(monthlyRows, latestMonthRow ? latestMonthRow[0] : "");
  const invalidBaseCount = Math.max(baseRows.length - validBaseRows.length, 0);
  const openErrorCount = errorRows.filter(function(row) {
    return normalizeText_(row[6]).toUpperCase() !== "CLOSED";
  }).length;

  return {
    totalBase: baseRows.length,
    validBase: validBaseRows.length,
    invalidBase: invalidBaseCount,
    hospitalCount: hospitalRows.length,
    facilityCount: facilityRows.length,
    serviceCount: serviceRows.length,
    publicServiceCount: publicServiceRows.length,
    totalServiceCount: serviceRows.length + publicServiceRows.length,
    errorCount: errorRows.length,
    openErrorCount: openErrorCount,
    uniqueNameCount: countUniqueValues_(baseRows, 3),
    uniqueWorkerCount: countUniqueValues_(validBaseRows, 2),
    recentSevenDayCount: countRowsWithinDays_(validBaseRows, 1, 7),
    latestMonthRow: latestMonthRow,
    previousMonthRow: previousMonthRow
  };
}

function renderDashboardHeader_(sheet) {
  sheet.getRange("A1:L1").merge()
    .setValue("정신건강팀 실적 대시보드")
    .setFontSize(15)
    .setFontWeight("bold")
    .setFontColor("#1f1f1f")
    .setBackground("#dce6f2")
    .setHorizontalAlignment("left")
    .setVerticalAlignment("middle");

  sheet.getRange("A2:L2").merge()
    .setValue(
      "마지막 갱신: " +
      Utilities.formatDate(new Date(), APP_CONFIG.timezone, "yyyy-MM-dd HH:mm:ss") +
      " | 실시간 입력은 기록 시트와 월간실적에 반영되고, 대시보드는 전체 갱신 시 최신화됩니다."
    )
    .setFontSize(9)
    .setFontColor("#5f6368")
    .setBackground("#f5f9ff")
    .setHorizontalAlignment("left")
    .setVerticalAlignment("middle");

  sheet.setFrozenRows(2);
}

function renderDashboardCards_(sheet, model) {
  const cardSpecs = [
    {
      row: 4,
      col: 1,
      title: "전체 기본정보",
      value: model.totalBase,
      subtitle: "유효 " + model.validBase + "건 / 미검증 " + model.invalidBase + "건",
      background: "#e8f0fe",
      accent: "#1a73e8"
    },
    {
      row: 4,
      col: 4,
      title: "병원 연계",
      value: model.hospitalCount,
      subtitle: "병원기록 시트 기준",
      background: "#e6f4ea",
      accent: "#188038"
    },
    {
      row: 4,
      col: 7,
      title: "시설 연계",
      value: model.facilityCount,
      subtitle: "시설기록 시트 기준",
      background: "#fff4e5",
      accent: "#b06000"
    },
    {
      row: 4,
      col: 10,
      title: "일반 서비스",
      value: model.serviceCount,
      subtitle: "서비스기록 시트 기준",
      background: "#eaf7f6",
      accent: "#00796b"
    },
    {
      row: 7,
      col: 1,
      title: "공적 서비스",
      value: model.publicServiceCount,
      subtitle: "공적서비스 시트 기준",
      background: "#fce8e6",
      accent: "#c5221f"
    },
    {
      row: 7,
      col: 4,
      title: "최근 7일 접수",
      value: model.recentSevenDayCount,
      subtitle: "유효 기본정보 기준",
      background: "#f3e8fd",
      accent: "#7b1fa2"
    },
    {
      row: 7,
      col: 7,
      title: "등록 대상자 수",
      value: model.uniqueNameCount,
      subtitle: "중복 제외 이름 기준",
      background: "#fff8e1",
      accent: "#8d6e63"
    },
    {
      row: 7,
      col: 10,
      title: "오류 로그",
      value: model.errorCount,
      subtitle: "OPEN " + model.openErrorCount + "건",
      background: "#fdecea",
      accent: "#b3261e"
    }
  ];

  cardSpecs.forEach(function(spec) {
    writeDashboardCard_(
      sheet,
      spec.row,
      spec.col,
      spec.title,
      spec.value,
      spec.subtitle,
      spec.background,
      spec.accent
    );
  });
}

function writeDashboardCard_(sheet, startRow, startColumn, title, value, subtitle, background, accent) {
  const area = sheet.getRange(startRow, startColumn, 3, 3);
  area
    .setBackground(background)
    .setBorder(true, true, true, true, false, false, accent, SpreadsheetApp.BorderStyle.SOLID_MEDIUM);

  sheet.getRange(startRow, startColumn, 1, 3).merge()
    .setValue(title)
    .setFontWeight("bold")
    .setFontColor(accent)
    .setHorizontalAlignment("left")
    .setVerticalAlignment("middle");

  sheet.getRange(startRow + 1, startColumn, 1, 3).merge()
    .setValue(formatDashboardNumber_(value))
    .setFontSize(20)
    .setFontWeight("bold")
    .setFontColor("#202124")
    .setHorizontalAlignment("center")
    .setVerticalAlignment("middle");

  sheet.getRange(startRow + 2, startColumn, 1, 3).merge()
    .setValue(subtitle)
    .setFontSize(9)
    .setFontColor("#5f6368")
    .setWrap(true)
    .setHorizontalAlignment("left")
    .setVerticalAlignment("middle");
}

function renderMonthlyOverviewSection_(sheet, model, monthlyRows) {
  writeDashboardSectionTitle_(sheet, 10, "월간 추이");

  const monthlyOverviewRows = buildRecentMonthlyOverviewRows_(monthlyRows, 6);
  writeSummaryTable_(
    sheet,
    11,
    1,
    ["월", "접수", "병원", "시설", "공적", "총서비스"],
    monthlyOverviewRows
  );

  renderCurrentMonthFocusCard_(sheet, 11, 8, model);
  renderQualityMemoCard_(sheet, 15, 8, model);
}

function renderCurrentMonthFocusCard_(sheet, startRow, startColumn, model) {
  const latestRow = model.latestMonthRow;
  const previousRow = model.previousMonthRow;
  const monthLabel = latestRow ? latestRow[0] : "데이터 없음";
  const receiptDelta = latestRow
    ? buildDeltaText_("접수", latestRow[1], previousRow ? previousRow[1] : null)
    : "접수 데이터 없음";
  const serviceDelta = latestRow
    ? buildDeltaText_("총서비스", latestRow[11], previousRow ? previousRow[11] : null)
    : "서비스 데이터 없음";

  sheet.getRange(startRow, startColumn, 1, 5).merge()
    .setValue("최근 월 포커스")
    .setFontWeight("bold")
    .setBackground("#e8f0fe")
    .setFontColor("#174ea6")
    .setHorizontalAlignment("left");

  sheet.getRange(startRow + 1, startColumn, 1, 5).merge()
    .setValue(monthLabel)
    .setFontSize(16)
    .setFontWeight("bold")
    .setBackground("#f8fbff")
    .setHorizontalAlignment("center")
    .setVerticalAlignment("middle");

  sheet.getRange(startRow + 2, startColumn, 2, 5).merge()
    .setValue(buildCurrentMonthFocusText_(latestRow, receiptDelta, serviceDelta))
    .setBackground("#f8fbff")
    .setWrap(true)
    .setVerticalAlignment("middle")
    .setHorizontalAlignment("left");

  sheet.getRange(startRow, startColumn, 4, 5)
    .setBorder(true, true, true, true, false, false, "#174ea6", SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
}

function buildCurrentMonthFocusText_(latestRow, receiptDelta, serviceDelta) {
  if (!latestRow) {
    return "월간실적 데이터가 아직 없습니다.\n전체 재구성을 실행하면 최근 월 요약이 표시됩니다.";
  }

  return [
    "접수 " + formatDashboardNumber_(latestRow[1]) + "건 / 병원 " + formatDashboardNumber_(latestRow[2]) + "건 / 시설 " + formatDashboardNumber_(latestRow[3]) + "건",
    "일반서비스 " + formatDashboardNumber_(Math.max((Number(latestRow[11]) || 0) - (Number(latestRow[9]) || 0), 0)) + "건 / 공적서비스 " + formatDashboardNumber_(latestRow[9]) + "건",
    receiptDelta + " | " + serviceDelta
  ].join("\n");
}

function renderQualityMemoCard_(sheet, startRow, startColumn, model) {
  sheet.getRange(startRow, startColumn, 1, 5).merge()
    .setValue("운영 메모")
    .setFontWeight("bold")
    .setBackground("#fef7e0")
    .setFontColor("#8d5a00")
    .setHorizontalAlignment("left");

  sheet.getRange(startRow + 1, startColumn, 2, 5).merge()
    .setValue([
      "담당자 " + formatDashboardNumber_(model.uniqueWorkerCount) + "명 / 대상자 " + formatDashboardNumber_(model.uniqueNameCount) + "명",
      "미검증 기본정보 " + formatDashboardNumber_(model.invalidBase) + "건 / OPEN 오류 " + formatDashboardNumber_(model.openErrorCount) + "건"
    ].join("\n"))
    .setBackground("#fffdf5")
    .setWrap(true)
    .setHorizontalAlignment("left")
    .setVerticalAlignment("middle");

  sheet.getRange(startRow, startColumn, 3, 5)
    .setBorder(true, true, true, true, false, false, "#b06000", SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
}

function renderOperationalSection_(sheet, model, validBaseRows, serviceRows, errorRows) {
  writeDashboardSectionTitle_(sheet, 18, "운영 요약");

  const workerRows = buildTopRowsWithShare_(validBaseRows, 2, model.validBase, 6);
  const serviceTypeRows = buildServiceTypeRows_(serviceRows, model.serviceCount, 6);
  const recentErrorRows = buildRecentErrorRows_(errorRows, 5);

  writeSummaryTable_(sheet, 19, 1, ["담당자", "접수", "비중"], workerRows);
  writeSummaryTable_(sheet, 19, 5, ["서비스", "건수", "비중"], serviceTypeRows);
  writeSummaryTable_(sheet, 19, 9, ["오류유형", "이름", "입력일", "상태"], recentErrorRows);
}

function renderLookupSection_(sheet, lookupValue, baseRows) {
  writeDashboardSectionTitle_(sheet, 27, "이름 조회");

  sheet.getRange("A28").setValue("조회 이름").setFontWeight("bold");
  sheet.getRange(APP_CONFIG.dashboard.lookupCell)
    .setValue(lookupValue || "")
    .setBackground("#fff2cc")
    .setBorder(true, true, true, true, false, false)
    .setNote("이름을 직접 입력하거나 목록에서 선택하면 각 기록 시트 조회 결과가 아래에 표시됩니다.");

  sheet.getRange("D28:H28").merge()
    .setValue("기본정보, 병원기록, 시설기록, 서비스기록, 공적서비스를 한 번에 조회합니다.")
    .setFontSize(9)
    .setFontColor("#5f6368")
    .setHorizontalAlignment("left");

  setLookupValidation_(sheet, APP_CONFIG.dashboard.lookupCell, baseRows);

  const lookupCellRef = toAbsoluteA1Ref_(APP_CONFIG.dashboard.lookupCell);
  let row = 30;
  const blockGap = 12;

  writeLookupBlock_(sheet, row, 1, "기본정보", APP_CONFIG.sheetNames.base, APP_CONFIG.headers.base, 4, lookupCellRef);
  row += blockGap;

  writeLookupBlock_(sheet, row, 1, "병원기록", APP_CONFIG.sheetNames.hospital, APP_CONFIG.headers.hospital, 4, lookupCellRef);
  row += blockGap;

  writeLookupBlock_(sheet, row, 1, "시설기록", APP_CONFIG.sheetNames.facility, APP_CONFIG.headers.facility, 4, lookupCellRef);
  row += blockGap;

  writeLookupBlock_(sheet, row, 1, "서비스기록", APP_CONFIG.sheetNames.service, APP_CONFIG.headers.service, 5, lookupCellRef);
  row += blockGap;

  writeLookupBlock_(sheet, row, 1, "공적서비스", APP_CONFIG.sheetNames.publicService, APP_CONFIG.headers.publicService, 5, lookupCellRef);
}

function setLookupValidation_(sheet, lookupCellA1, baseRows) {
  const uniqueNames = dedupeStrings_((baseRows || []).map(function(row) {
    return row[3];
  })).sort();
  const helperColumn = 24;

  ensureSheetSize_(sheet, Math.max(sheet.getMaxRows(), uniqueNames.length + 2), helperColumn);
  sheet.getRange(1, helperColumn, sheet.getMaxRows(), 1).clearContent();

  if (!uniqueNames.length) {
    sheet.getRange(lookupCellA1).clearDataValidations();
    sheet.hideColumns(helperColumn);
    return;
  }

  sheet.getRange(1, helperColumn).setValue("__lookup_names");
  sheet.getRange(2, helperColumn, uniqueNames.length, 1).setValues(uniqueNames.map(function(name) {
    return [name];
  }));

  const validation = SpreadsheetApp.newDataValidation()
    .requireValueInRange(sheet.getRange(2, helperColumn, uniqueNames.length, 1), true)
    .setAllowInvalid(true)
    .build();

  sheet.getRange(lookupCellA1).setDataValidation(validation);
  sheet.hideColumns(helperColumn);
}

function writeDashboardSectionTitle_(sheet, row, title) {
  sheet.getRange(row, 1, 1, 12).merge()
    .setValue(title)
    .setFontSize(11)
    .setFontWeight("bold")
    .setBackground("#d9e2f3")
    .setFontColor("#1f1f1f")
    .setHorizontalAlignment("left")
    .setVerticalAlignment("middle");
}

function writeSummaryTable_(sheet, startRow, startColumn, headers, rows) {
  const width = headers.length;
  const values = rows && rows.length ? rows : [buildEmptyDataRow_(width)];

  sheet.getRange(startRow, startColumn, 1, width).setValues([headers]);
  styleCustomHeaderRow_(sheet, startRow, startColumn, width);

  sheet.getRange(startRow + 1, startColumn, values.length, width).setValues(values);
  sheet.getRange(startRow, startColumn, values.length + 1, width)
    .setBorder(true, true, true, true, true, true, "#d0d7de", SpreadsheetApp.BorderStyle.SOLID);
}

function buildRecentMonthlyOverviewRows_(monthlyRows, limit) {
  return (monthlyRows || [])
    .slice()
    .sort(function(left, right) {
      return normalizeText_(right[0]).localeCompare(normalizeText_(left[0]));
    })
    .slice(0, limit || 6)
    .map(function(row) {
      return [
        normalizeText_(row[0]),
        Number(row[1]) || 0,
        Number(row[2]) || 0,
        Number(row[3]) || 0,
        Number(row[9]) || 0,
        Number(row[11]) || 0
      ];
    });
}

function buildTopRowsWithShare_(rows, keyIndex, totalCount, limit) {
  return buildCountRows_(rows, keyIndex)
    .slice(0, limit || 6)
    .map(function(item) {
      return [
        item[0],
        item[1],
        buildShareLabel_(item[1], totalCount)
      ];
    });
}

function buildServiceTypeRows_(serviceRows, totalCount, limit) {
  return buildCountRows_(serviceRows, 7)
    .slice(0, limit || 6)
    .map(function(item) {
      return [
        item[0],
        item[1],
        buildShareLabel_(item[1], totalCount)
      ];
    });
}

function buildRecentErrorRows_(errorRows, limit) {
  return (errorRows || [])
    .slice()
    .sort(function(left, right) {
      return getComparableTime_(right[0]) - getComparableTime_(left[0]);
    })
    .slice(0, limit || 5)
    .map(function(row) {
      return [
        normalizeText_(row[2]) || "-",
        normalizeText_(row[4]) || "-",
        formatDashboardDateTime_(row[0]),
        normalizeText_(row[6]) || "OPEN"
      ];
    });
}

function getLatestMonthlyRow_(monthlyRows) {
  if (!monthlyRows || !monthlyRows.length) {
    return null;
  }

  return monthlyRows.slice().sort(function(left, right) {
    return normalizeText_(right[0]).localeCompare(normalizeText_(left[0]));
  })[0];
}

function getPreviousMonthlyRow_(monthlyRows, monthKey) {
  if (!monthlyRows || monthlyRows.length < 2 || !monthKey) {
    return null;
  }

  const sortedRows = monthlyRows.slice().sort(function(left, right) {
    return normalizeText_(right[0]).localeCompare(normalizeText_(left[0]));
  });

  for (let index = 0; index < sortedRows.length; index += 1) {
    if (normalizeText_(sortedRows[index][0]) === monthKey) {
      return sortedRows[index + 1] || null;
    }
  }

  return null;
}

function buildDeltaText_(label, currentValue, previousValue) {
  if (previousValue === null || previousValue === undefined) {
    return label + " 전월 데이터 없음";
  }

  const diff = (Number(currentValue) || 0) - (Number(previousValue) || 0);
  const diffText = diff > 0 ? "+" + diff : String(diff);
  return label + " 전월 대비 " + diffText;
}

function countUniqueValues_(rows, keyIndex) {
  return dedupeStrings_((rows || []).map(function(row) {
    return row[keyIndex];
  })).length;
}

function countRowsWithinDays_(rows, dateIndex, dayCount) {
  const today = toMidnightDate_(new Date());
  if (!today) {
    return 0;
  }

  const start = new Date(today);
  start.setDate(start.getDate() - Math.max((dayCount || 1) - 1, 0));

  return (rows || []).filter(function(row) {
    const date = toMidnightDate_(row[dateIndex]);
    return date && date.getTime() >= start.getTime() && date.getTime() <= today.getTime();
  }).length;
}

function buildShareLabel_(count, totalCount) {
  const total = Number(totalCount) || 0;
  if (!total) {
    return "0.0%";
  }

  return ((Number(count) || 0) * 100 / total).toFixed(1) + "%";
}

function getComparableTime_(value) {
  if (value instanceof Date && !isNaN(value.getTime())) {
    return value.getTime();
  }

  const parsed = new Date(value);
  if (isNaN(parsed.getTime())) {
    return 0;
  }

  return parsed.getTime();
}

function formatDashboardDateTime_(value) {
  const time = getComparableTime_(value);
  if (!time) {
    return "-";
  }

  return Utilities.formatDate(new Date(time), APP_CONFIG.timezone, "MM-dd HH:mm");
}

function formatDashboardNumber_(value) {
  return Math.round(Number(value) || 0).toLocaleString("ko-KR");
}

function buildCountRows_(rows, keyIndex) {
  const counts = {};

  (rows || []).forEach(function(row) {
    const key = normalizeText_(row[keyIndex]);
    if (!key) {
      return;
    }
    counts[key] = (counts[key] || 0) + 1;
  });

  return Object.keys(counts).sort(function(left, right) {
    if (counts[right] !== counts[left]) {
      return counts[right] - counts[left];
    }
    return left.localeCompare(right);
  }).map(function(key) {
    return [key, counts[key]];
  });
}

function buildEmptyDataRow_(width) {
  const row = new Array(width).fill("");
  row[0] = "데이터 없음";
  return row;
}

function writeLookupBlock_(sheet, startRow, startColumn, title, sourceSheetName, headers, nameColumnNumber, lookupCellRef) {
  const width = headers.length;
  const sourceRange = "A2:" + getColumnA1Letter_(width);
  const filterRange = getColumnA1Letter_(nameColumnNumber) + "2:" + getColumnA1Letter_(nameColumnNumber);

  sheet.getRange(startRow, startColumn, 1, width).merge()
    .setValue(title)
    .setFontWeight("bold")
    .setBackground("#f3f6fc")
    .setHorizontalAlignment("left")
    .setVerticalAlignment("middle");

  sheet.getRange(startRow + 1, startColumn, 1, width).setValues([headers]);
  styleCustomHeaderRow_(sheet, startRow + 1, startColumn, width);

  const quotedSourceSheet = quoteSheetNameForFormula_(sourceSheetName);
  const formula =
    "=IF(" + lookupCellRef + "=\"\",\"\",IFERROR(" +
    "FILTER(" + quotedSourceSheet + "!" + sourceRange + "," +
    quotedSourceSheet + "!" + filterRange + "=" + lookupCellRef + ")," +
    buildFormulaFallbackArray_(width) +
    "))";

  sheet.getRange(startRow + 2, startColumn).setFormula(formula);
}

function buildFormulaFallbackArray_(width) {
  const values = ["\"조회 결과 없음\""];
  for (let index = 1; index < width; index += 1) {
    values.push("\"\"");
  }
  return "{" + values.join(",") + "}";
}

function getColumnA1Letter_(columnNumber) {
  let dividend = Number(columnNumber) || 1;
  let columnLabel = "";

  while (dividend > 0) {
    const modulo = (dividend - 1) % 26;
    columnLabel = String.fromCharCode(65 + modulo) + columnLabel;
    dividend = Math.floor((dividend - modulo) / 26);
  }

  return columnLabel;
}

function toAbsoluteA1Ref_(a1Notation) {
  const match = String(a1Notation || "").match(/^([A-Za-z]+)(\d+)$/);
  if (!match) {
    return "$B$28";
  }

  return "$" + match[1].toUpperCase() + "$" + match[2];
}

function formatDashboardSheet_(sheet) {
  const widths = [130, 120, 95, 130, 120, 95, 130, 120, 120, 120, 120, 120];
  widths.forEach(function(width, index) {
    sheet.setColumnWidth(index + 1, width);
  });

  sheet.setRowHeight(1, 30);
  sheet.setRowHeight(2, 22);
  for (let row = 4; row <= 9; row += 1) {
    sheet.setRowHeight(row, row % 3 === 2 ? 34 : 24);
  }

  sheet.getRange("A:L")
    .setVerticalAlignment("middle")
    .setWrap(true);

  if (sheet.getMaxColumns() >= 24) {
    sheet.hideColumns(24);
  }
}

function refreshMonthlySheet_(monthlySheet, baseRows, hospitalRows, facilityRows, serviceRows, publicServiceRows) {
  const monthData = {};

  function ensureMonth_(monthKey) {
    if (!monthKey) {
      return null;
    }

    if (!monthData[monthKey]) {
      monthData[monthKey] = {
        base: 0,
        hospital: 0,
        facility: 0,
        services: {
          "주거지원": 0,
          "물품제공": 0,
          "방문상담": 0,
          "외래진료": 0,
          "약물관리": 0,
          "공적서비스": 0,
          "기타": 0
        },
        totalService: 0
      };
    }

    return monthData[monthKey];
  }

  function getMonthKey_(value) {
    const date = toMidnightDate_(value);
    if (!date) {
      return "";
    }
    return Utilities.formatDate(date, APP_CONFIG.timezone, "yyyy-MM");
  }

  const baseMonthByRecordId = {};

  (baseRows || []).forEach(function(row) {
    if (row[8] !== true) {
      return;
    }

    const monthKey = getMonthKey_(row[1]);
    const bucket = ensureMonth_(monthKey);
    if (!bucket) {
      return;
    }

    baseMonthByRecordId[normalizeText_(row[0])] = monthKey;
    bucket.base += 1;
  });

  (hospitalRows || []).forEach(function(row) {
    const monthKey = baseMonthByRecordId[normalizeText_(row[0])] || getMonthKey_(row[1]);
    const bucket = ensureMonth_(monthKey);
    if (bucket) {
      bucket.hospital += 1;
    }
  });

  (facilityRows || []).forEach(function(row) {
    const monthKey = baseMonthByRecordId[normalizeText_(row[0])] || getMonthKey_(row[1]);
    const bucket = ensureMonth_(monthKey);
    if (bucket) {
      bucket.facility += 1;
    }
  });

  (serviceRows || []).forEach(function(row) {
    const monthKey = getMonthKey_(row[2]);
    const bucket = ensureMonth_(monthKey);
    if (!bucket) {
      return;
    }

    bucket.totalService += 1;

    const serviceType = normalizeText_(row[7]);
    if (bucket.services[serviceType] !== undefined) {
      bucket.services[serviceType] += 1;
    } else {
      bucket.services["기타"] += 1;
    }
  });

  (publicServiceRows || []).forEach(function(row) {
    const monthKey = getMonthKey_(row[2]);
    const bucket = ensureMonth_(monthKey);
    if (!bucket) {
      return;
    }

    bucket.services["공적서비스"] += 1;
    bucket.totalService += 1;
  });

  const summaryRows = Object.keys(monthData).sort().map(function(monthKey) {
    const bucket = monthData[monthKey];
    return [
      monthKey,
      bucket.base,
      bucket.hospital,
      bucket.facility,
      bucket.services["주거지원"],
      bucket.services["물품제공"],
      bucket.services["방문상담"],
      bucket.services["외래진료"],
      bucket.services["약물관리"],
      bucket.services["공적서비스"],
      bucket.services["기타"],
      bucket.totalService
    ];
  });

  writeBatchData(monthlySheet, APP_CONFIG.headers.monthly, summaryRows);
  formatMonthlySheet_(monthlySheet);
}
