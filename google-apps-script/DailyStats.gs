function generateDailyStatsSheets() {
  const ui = typeof SpreadsheetApp !== 'undefined' ? SpreadsheetApp.getUi() : null;
  const lock = LockService.getDocumentLock();
  lock.waitLock(30000);

  try {
    const sheetMap = getSheetMap();
    const rawSheet = sheetMap.rawInput;
    const rawRange = rawSheet.getDataRange();
    const values = rawRange.getValues();
    const displayValues = rawRange.getDisplayValues();

    if (values.length < APP_CONFIG.sourceHeaderRows) {
      throw new Error("원본 시트에는 최소 1행 그룹 헤더와 2행 실제 헤더가 필요합니다.");
    }

    const headerMap = buildCompositeHeaderMap(displayValues[0], displayValues[1]);
    validateRequiredHeaders_(headerMap);
    const parsed = parseRawRows(values, displayValues, headerMap);

    const statsData = aggregateDailyStats_(
      parsed.baseRows,
      parsed.hospitalRows,
      parsed.facilityRows,
      parsed.serviceRows
    );

    const months = Object.keys(statsData);
    if (months.length === 0) {
      if (ui) ui.alert("알림", "통계 데이터가 생성되지 않았습니다. (조건에 맞는 데이터가 없거나 유효성 체크 실패)", ui.ButtonSet.OK);
      return;
    }

    renderAllMonthlySheets_(statsData);

    if (ui) {
      showToast_("일일실적 시트 생성을 완료했습니다. (" + months.join(", ") + ")", APP_CONFIG.menuTitle, 5);
    }
  } catch (error) {
    if (ui) {
      ui.alert("일일실적 생성 오류", error.message, ui.ButtonSet.OK);
    }
    throw error;
  } finally {
    lock.releaseLock();
  }
}

function aggregateDailyStats_(baseRows, hospitalRows, facilityRows, serviceRows) {
  // stats[YYYY-MM][YYYY-MM-DD][WorkerName][Metric] = Count
  const stats = {};
  const recordIndexMap = {}; // recordId -> { dateStr, worker }

  function initStatBucket_(dateStr, worker) {
    if (!dateStr || !worker) return;
    const d = new Date(dateStr);
    if (isNaN(d.getTime())) return;
    
    const yearMonth = Utilities.formatDate(d, APP_CONFIG.timezone, "yyyy년 M월");
    const fullDate = Utilities.formatDate(d, APP_CONFIG.timezone, "yyyy-MM-dd");

    if (!stats[yearMonth]) stats[yearMonth] = {};
    if (!stats[yearMonth][fullDate]) stats[yearMonth][fullDate] = {};
    if (!stats[yearMonth][fullDate][worker]) {
      stats[yearMonth][fullDate][worker] = {};
      Object.keys(APP_CONFIG.dailyStats.mappingRules).forEach(function(metric) {
        stats[yearMonth][fullDate][worker][metric] = 0;
      });
    }
    return { yearMonth: yearMonth, fullDate: fullDate, worker: worker };
  }

  function evaluateRule_(cellValue, rule) {
    if (cellValue === undefined || cellValue === null || cellValue === "") return false;
    const textVal = String(cellValue).trim();
    
    if (rule.val) {
      if (Array.isArray(rule.val)) {
        return rule.val.indexOf(textVal) !== -1;
      }
      return textVal === rule.val;
    }
    if (rule.contains) {
      return textVal.indexOf(rule.contains) !== -1;
    }
    if (rule.notEmpty) {
      return textVal.length > 0;
    }
    return false;
  }

  const baseHeaders = APP_CONFIG.headers.base;
  const idxBaseValid = baseHeaders.indexOf("유효성");
  const idxBaseDate = baseHeaders.indexOf("날짜");
  const idxBaseWorker = baseHeaders.indexOf("담당자");

  baseRows.forEach(function(row) {
    if (row[idxBaseValid] !== true) return;
    const dateStr = row[idxBaseDate];
    const worker = row[idxBaseWorker];
    recordIndexMap[row[0]] = { dateStr: dateStr, worker: worker };

    const loc = initStatBucket_(dateStr, worker);
    if (!loc) return;

    Object.keys(APP_CONFIG.dailyStats.mappingRules).forEach(function(metric) {
      const rule = APP_CONFIG.dailyStats.mappingRules[metric];
      if (rule.sheet === "base") {
        const val = row[baseHeaders.indexOf(rule.col)];
        if (evaluateRule_(val, rule)) {
          stats[loc.yearMonth][loc.fullDate][loc.worker][metric] += 1;
        }
      }
    });
  });

  const hospHeaders = APP_CONFIG.headers.hospital;
  hospitalRows.forEach(function(row) {
    const parent = recordIndexMap[row[0]];
    if (!parent) return;
    const loc = initStatBucket_(parent.dateStr, parent.worker);
    if (!loc) return;

    Object.keys(APP_CONFIG.dailyStats.mappingRules).forEach(function(metric) {
      const rule = APP_CONFIG.dailyStats.mappingRules[metric];
      if (rule.sheet === "hospital") {
        const val = row[hospHeaders.indexOf(rule.col)];
        if (evaluateRule_(val, rule)) {
          stats[loc.yearMonth][loc.fullDate][loc.worker][metric] += 1;
        }
      }
    });
  });

  const facHeaders = APP_CONFIG.headers.facility;
  facilityRows.forEach(function(row) {
    const parent = recordIndexMap[row[0]];
    if (!parent) return;
    const loc = initStatBucket_(parent.dateStr, parent.worker);
    if (!loc) return;

    Object.keys(APP_CONFIG.dailyStats.mappingRules).forEach(function(metric) {
      const rule = APP_CONFIG.dailyStats.mappingRules[metric];
      if (rule.sheet === "facility") {
        const val = row[facHeaders.indexOf(rule.col)];
        if (evaluateRule_(val, rule)) {
          stats[loc.yearMonth][loc.fullDate][loc.worker][metric] += 1;
        }
      }
    });
  });

  const svcHeaders = APP_CONFIG.headers.service;
  const idxSvcDate = svcHeaders.indexOf("날짜");
  const idxSvcWorker = svcHeaders.indexOf("담당자");

  serviceRows.forEach(function(row) {
    const loc = initStatBucket_(row[idxSvcDate], row[idxSvcWorker]);
    if (!loc) return;

    Object.keys(APP_CONFIG.dailyStats.mappingRules).forEach(function(metric) {
      const rule = APP_CONFIG.dailyStats.mappingRules[metric];
      if (rule.sheet === "service") {
        const val = row[svcHeaders.indexOf(rule.col)];
        if (evaluateRule_(val, rule)) {
          stats[loc.yearMonth][loc.fullDate][loc.worker][metric] += 1;
        }
      }
    });
  });

  return stats;
}

function renderAllMonthlySheets_(statsData) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const months = Object.keys(statsData).sort();

  months.forEach(function(yearMonth) {
    const sheetName = APP_CONFIG.dailyStats.sheetPrefix + yearMonth;
    let sheet = ss.getSheetByName(sheetName);
    if (!sheet) {
      sheet = ss.insertSheet(sheetName);
    } else {
      sheet.clear();
      sheet.clearFormats();
    }
    renderSingleMonthlySheet_(sheet, yearMonth, statsData[yearMonth]);
  });
}

function renderSingleMonthlySheet_(sheet, yearMonth, monthData) {
  // 3-row headers matching JSON dump exactly
  const header1 = ["", "", "상담(거리 및 내방)", "", "", "", "조치", "", "", "", "", "", "조치후 사례관리", "", "", "", "", "", "", "", ""];
  const header2 = ["날짜", "이름", "정신상담", "알콜상담", "기타상담", "소계", "자의입원", "응급입원", "보호진단", "시설입소\n(노숙영역)", "시설입소\n(타복지영역)", "지원주택", "주거지원", "물품제공", "기타\n(귀가,외래)", "외래진료", "병원방문", "주거지방문", "투약관리", "기타\n(척도,내방)", "합계"];
  
  sheet.getRange(1, 1, 1, header1.length).setValues([header1]).setBackground("#eaeaea").setFontWeight("bold").setHorizontalAlignment("center");
  sheet.getRange(2, 1, 1, header2.length).setValues([header2]).setBackground("#f3f3f3").setFontWeight("bold").setHorizontalAlignment("center");
  sheet.setFrozenRows(2);
  
  // Merge Header groups
  sheet.getRange(1, 1, 2, 1).merge(); // 날짜
  sheet.getRange(1, 2, 2, 1).merge(); // 이름
  sheet.getRange(1, 3, 1, 4).merge(); // 상담
  sheet.getRange(1, 7, 1, 6).merge(); // 조치
  sheet.getRange(1, 13, 1, 9).merge(); // 조치후 사례관리

  let currentRow = 3;
  const staffList = APP_CONFIG.dailyStats.staffNames;
  const metricKeys = Object.keys(APP_CONFIG.dailyStats.mappingRules);
  const sortedDates = Object.keys(monthData).sort();

  const totalMonthlyAggregate = new Array(21).fill(0);

  sortedDates.forEach(function(dateStr) {
    const startRowForDate = currentRow;
    const dateData = monthData[dateStr];
    const dailyTotal = new Array(21).fill(0);
    dailyTotal[1] = "일일누계";

    staffList.forEach(function(staff) {
      const row = new Array(21).fill(0);
      row[0] = dateStr;
      row[1] = staff;
      
      const counts = dateData[staff] || {};
      
      row[2] = counts["정신상담"] || 0;
      row[3] = counts["알콜상담"] || 0;
      row[4] = counts["기타상담"] || 0;
      row[5] = row[2] + row[3] + row[4]; // 소계
      
      row[6] = counts["자의입원"] || 0;
      row[7] = counts["응급입원"] || 0;
      row[8] = counts["보호진단"] || 0;
      row[9] = counts["시설입소(노숙영역)"] || 0;
      row[10] = counts["시설입소(타복지영역)"] || 0;
      row[11] = counts["지원주택"] || 0;
      row[12] = counts["주거지원"] || 0;
      row[13] = counts["물품제공"] || 0;
      row[14] = counts["기타(귀가,외래등)"] || 0;
      row[15] = counts["외래진료"] || 0;
      row[16] = counts["병원방문"] || 0;
      row[17] = counts["주거지방문"] || 0;
      row[18] = counts["투약관리"] || 0;
      row[19] = counts["기타(척도, 내방)"] || 0;
      
      let sum = 0;
      for (let i = 2; i <= 19; i++) {
        if (i !== 5) sum += row[i]; // exclude subtotal
      }
      row[20] = sum;

      // Accumulate daily total
      for (let i = 2; i <= 20; i++) dailyTotal[i] += row[i];
      
      sheet.getRange(currentRow, 1, 1, 21).setValues([row]);
      currentRow++;
    });

    // Write daily total row
    sheet.getRange(currentRow, 1, 1, 21).setValues([dailyTotal]).setBackground("#fff2cc").setFontWeight("bold");
    
    // Accumulate monthly total
    for (let i = 2; i <= 20; i++) totalMonthlyAggregate[i] += dailyTotal[i];

    // Merge Date Col
    sheet.getRange(startRowForDate, 1, staffList.length + 1, 1).merge().setVerticalAlignment("middle");
    currentRow++;
  });

  totalMonthlyAggregate[0] = "";
  totalMonthlyAggregate[1] = "총누계";
  sheet.getRange(currentRow, 1, 1, 21).setValues([totalMonthlyAggregate]).setBackground("#fce5cd").setFontWeight("bold");
  
  sheet.getRange(1, 1, currentRow, 21).setBorder(true, true, true, true, true, true);
  
  // Format numeric columns to clear 0s to empty strings for cleaner view? 
  // User might prefer "0" or blank. We will leave 0 as it mimics standard pivot.
}
