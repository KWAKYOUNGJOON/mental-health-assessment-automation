function getHeaderMap(headers) {
  const map = {};

  headers.forEach(function(header, index) {
    const headerName = normalizeText_(header);
    if (headerName && map[headerName] === undefined) {
      map[headerName] = index;
    }
  });

  return map;
}

function buildCompositeHeaderMap(groupRow, headerRow) {
  const compositeMap = {};
  const duplicateCounts = {};
  let currentGroup = "공통";

  headerRow.forEach(function(header, index) {
    const groupValue = normalizeText_(groupRow[index]);
    const headerValue = normalizeText_(header);

    if (groupValue) {
      currentGroup = groupValue;
    }
    if (!headerValue) {
      return;
    }

    const compositeKey = (currentGroup || "공통") + "_" + headerValue;
    let resolvedKey = compositeKey;

    if (compositeMap[resolvedKey] !== undefined) {
      duplicateCounts[compositeKey] = (duplicateCounts[compositeKey] || 1) + 1;
      resolvedKey = compositeKey + "__" + duplicateCounts[compositeKey];
    } else {
      duplicateCounts[compositeKey] = 1;
    }

    compositeMap[resolvedKey] = index;
  });

  return compositeMap;
}

function getFieldValue_(row, headerMap, compositeKey) {
  const columnIndex = headerMap[compositeKey];
  return columnIndex === undefined ? "" : row[columnIndex];
}

function getDisplayFieldValue_(row, headerMap, compositeKey) {
  const columnIndex = headerMap[compositeKey];
  return columnIndex === undefined ? "" : row[columnIndex];
}

function isMeaningfulRow(row, displayRow, headerMap) {
  return APP_CONFIG.meaningfulKeys.some(function(compositeKey) {
    if (compositeKey !== "서비스_기타" && compositeKey.indexOf("서비스_") === 0) {
      return isTruthyCheckboxValue_(getFieldValue_(row, headerMap, compositeKey));
    }

    return (
      !isBlankValue_(getFieldValue_(row, headerMap, compositeKey)) ||
      !isBlankValue_(getDisplayFieldValue_(displayRow, headerMap, compositeKey))
    );
  });
}

function validateRow(row, displayRow, headerMap, rowNumber) {
  const rawName = normalizeText_(
    getDisplayFieldValue_(displayRow, headerMap, "공통_이름") ||
    getFieldValue_(row, headerMap, "공통_이름")
  );
  const rawDateValue = getFieldValue_(row, headerMap, "공통_날짜");
  const rawDateText = normalizeText_(
    getDisplayFieldValue_(displayRow, headerMap, "공통_날짜") || rawDateValue
  );
  const normalizedDate = normalizeDateValue(rawDateValue);
  const birthInfo = normalizeBirthValue(getFieldValue_(row, headerMap, "공통_생년월일"));

  const errors = [];
  if (isBlankValue_(rawDateValue) && !rawDateText) {
    errors.push({
      type: "MISSING_REQUIRED_FIELD",
      message: "날짜가 비어 있습니다.",
      status: "OPEN"
    });
  } else if (!normalizedDate) {
    errors.push({
      type: "INVALID_DATE",
      message: "날짜 형식을 해석할 수 없습니다.",
      status: "OPEN"
    });
  }

  if (!rawName) {
    errors.push({
      type: "MISSING_REQUIRED_FIELD",
      message: "이름이 비어 있습니다.",
      status: "OPEN"
    });
  }

  if (birthInfo.warning) {
    errors.push({
      type: "INVALID_BIRTH",
      message: birthInfo.warning,
      status: "WARN"
    });
  }

  return {
    rowNumber: rowNumber,
    isValid: errors.filter(function(error) {
      return error.status === "OPEN";
    }).length === 0,
    errors: errors,
    normalizedDate: normalizedDate,
    birthInfo: birthInfo,
    rawName: rawName,
    rawDate: rawDateText
  };
}

function normalizeDateValue(value) {
  if (value instanceof Date && !isNaN(value.getTime())) {
    return toMidnightDate_(value);
  }

  if (typeof value === "number" && isFinite(value)) {
    if (value > 20000 && value < 80000) {
      const utcMillis = Math.round((value - 25569) * 86400 * 1000);
      const tempDate = new Date(utcMillis);
      return new Date(tempDate.getUTCFullYear(), tempDate.getUTCMonth(), tempDate.getUTCDate());
    }

    if (value > 19000101 && value < 21000101) {
      return parseCompactDate_(String(Math.round(value)));
    }
  }

  const text = normalizeText_(value);
  if (!text) {
    return null;
  }

  const compactDigits = text.replace(/[^\d]/g, "");
  if (/^\d{8}$/.test(compactDigits)) {
    return parseCompactDate_(compactDigits);
  }

  const normalized = text.replace(/[./]/g, "-");
  const match = normalized.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
  if (!match) {
    return null;
  }

  return buildDate_(match[1], match[2], match[3]);
}

function normalizeBirthValue(value) {
  if (value === null || value === undefined || value === "") {
    return {
      birth_raw: "",
      birth_normalized: "",
      warning: ""
    };
  }

  if (value instanceof Date && !isNaN(value.getTime())) {
    const formattedRaw = formatDateKey_(value, "yyMMdd");
    const formattedNormalized = formatDateKey_(value, "yy-MM-dd");
    return {
      birth_raw: formattedRaw,
      birth_normalized: formattedNormalized,
      warning: ""
    };
  }

  let rawText = typeof value === "number" ? String(Math.trunc(value)) : normalizeText_(value);
  rawText = rawText.replace(/\.0+$/, "");

  if (!rawText) {
    return {
      birth_raw: "",
      birth_normalized: "",
      warning: ""
    };
  }

  if (isStrictBirthInputValue_(rawText)) {
    return {
      birth_raw: rawText,
      birth_normalized: rawText.replace(/^(\d{2})(\d{2})(\d{2})$/, "$1-$2-$3"),
      warning: ""
    };
  }

  const digits = rawText.replace(/[^\d]/g, "");
  if (
    /^\d{6}$/.test(digits) &&
    isValidBirthYymmdd_(digits.substring(0, 2), digits.substring(2, 4), digits.substring(4, 6))
  ) {
    return {
      birth_raw: rawText,
      birth_normalized: digits.replace(/^(\d{2})(\d{2})(\d{2})$/, "$1-$2-$3"),
      warning: ""
    };
  }

  if (/^\d{8}$/.test(digits)) {
    const parsedBirthDate = parseCompactDate_(digits);
    if (parsedBirthDate) {
      return {
        birth_raw: rawText,
        birth_normalized: formatDateKey_(parsedBirthDate, "yy-MM-dd"),
        warning: ""
      };
    }
  }

  const parsedDate = normalizeDateValue(rawText);
  if (parsedDate) {
    return {
      birth_raw: rawText,
      birth_normalized: formatDateKey_(parsedDate, "yy-MM-dd"),
      warning: ""
    };
  }

  return {
    birth_raw: rawText,
    birth_normalized: rawText,
    warning: getBirthInputGuidanceMessage_()
  };
}

function isStrictBirthInputValue_(value) {
  const text = normalizeText_(value);
  if (!text) {
    return true;
  }

  const match = text.match(/^(\d{2})(\d{2})(\d{2})$/);
  if (!match) {
    return false;
  }

  return isValidBirthYymmdd_(match[1], match[2], match[3]);
}

function isValidBirthYymmdd_(year, month, day) {
  const parsedYear = 2000 + parseInt(year, 10);
  const parsedMonth = parseInt(month, 10);
  const parsedDay = parseInt(day, 10);
  if (!parsedYear || !parsedMonth || !parsedDay) {
    return false;
  }

  const date = new Date(parsedYear, parsedMonth - 1, parsedDay);
  return (
    date.getFullYear() === parsedYear &&
    date.getMonth() === parsedMonth - 1 &&
    date.getDate() === parsedDay
  );
}

function getBirthInputGuidanceMessage_() {
  return "생년월일은 yymmdd 형식으로 입력하세요. 예: 900131";
}

function parseCompactDate_(rawText) {
  const match = normalizeText_(rawText).match(/^(\d{4})(\d{2})(\d{2})$/);
  if (!match) {
    return null;
  }
  return buildDate_(match[1], match[2], match[3]);
}

function buildDate_(year, month, day) {
  const parsedYear = parseInt(year, 10);
  const parsedMonth = parseInt(month, 10);
  const parsedDay = parseInt(day, 10);
  const date = new Date(parsedYear, parsedMonth - 1, parsedDay);

  if (
    date.getFullYear() !== parsedYear ||
    date.getMonth() !== parsedMonth - 1 ||
    date.getDate() !== parsedDay
  ) {
    return null;
  }

  date.setHours(0, 0, 0, 0);
  return date;
}
