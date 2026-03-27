(() => {
  "use strict";

  const ROLE = {
    SOCIAL_WORKER: "social_worker",
    TEAM_LEAD: "team_lead",
    CORP_REPORTER: "corp_reporter"
  };

  const TABLE_META = {
    files: "업로드 이력",
    parse_errors: "파싱 오류",
    daily_activities: "월별 일일실적",
    hospitalizations: "입원현황",
    facility_placements: "시설입소현황",
    counseling_records: "상담현황",
    referral_log: "연계대장",
    case_management: "사례관리",
    deaths: "사망자현황",
    non_psych_hospitalizations: "정신과외 입원",
    hospital_visits: "병원방문교육",
    community_resources: "지역사회자원동원",
    meetings_education: "회의교육참여",
    post_action_cases: "조치후 사례관리",
    long_term_inpatients: "장기입원환자",
    backups: "백업 이력"
  };

  const YEAR_TABLES = [
    "daily_activities",
    "hospitalizations",
    "facility_placements",
    "counseling_records",
    "referral_log",
    "case_management",
    "deaths",
    "non_psych_hospitalizations",
    "hospital_visits",
    "community_resources",
    "meetings_education",
    "post_action_cases",
    "long_term_inpatients"
  ];

  const EXPORT_TABLES = [
    "files",
    "parse_errors",
    ...YEAR_TABLES
  ];

  const ERROR_CELL_REGEX = /^#(REF!|DIV\/0!|VALUE!|N\/A|NAME\?|NUM!|NULL!)/i;

  const db = new Dexie("mh_team_performance_db");
  db.version(1).stores({
    files: "++id, file_name, year_hint, imported_at",
    parse_errors: "++id, created_at, file_name, sheet, code",
    daily_activities: "++id, year, month, date, worker, [date+worker]",
    hospitalizations: "++id, year, month, admission_date, name, hospital, status, [admission_date+name+hospital]",
    facility_placements: "++id, year, month, date, name, facility_type, status",
    counseling_records: "++id, year, month, counseling_date, worker, type",
    referral_log: "++id, year, month, date, receiver",
    case_management: "++id, year, type, worker, status, name",
    deaths: "++id, year, death_date, name, worker",
    non_psych_hospitalizations: "++id, year, date, name, worker, hospital",
    hospital_visits: "++id, year, counseling_date, name, worker",
    community_resources: "++id, year, date, name, worker, org",
    meetings_education: "++id, year, date, category",
    post_action_cases: "++id, year, month",
    long_term_inpatients: "++id, year, is_longterm, admission_date, name, hospital, status",
    backups: "++id, created_at, date_key"
  });

  const state = {
    parserConfig: null,
    role: ROLE.SOCIAL_WORKER,
    lastReport: null
  };

  const ui = {};

  document.addEventListener("DOMContentLoaded", init);

  async function init() {
    cacheUi();
    bindEvents();
    buildMonths();
    await loadParserConfig();
    await refreshSelectedFiles([]);
    await refreshErrorTable();
    await refreshSummary();
    await refreshYearOptions();
    await refreshPreviewTableOptions();
    await runAutoBackupIfNeeded();
  }

  function cacheUi() {
    ui.tabButtons = Array.from(document.querySelectorAll(".tab-btn"));
    ui.tabPanels = {
      upload: document.getElementById("tab-upload"),
      data: document.getElementById("tab-data"),
      report: document.getElementById("tab-report")
    };
    ui.roleSelect = document.getElementById("roleSelect");
    ui.backupBtn = document.getElementById("backupBtn");
    ui.clearDbBtn = document.getElementById("clearDbBtn");
    ui.excelFiles = document.getElementById("excelFiles");
    ui.importBtn = document.getElementById("importBtn");
    ui.selectedFiles = document.getElementById("selectedFiles");
    ui.importStatus = document.getElementById("importStatus");
    ui.refreshErrorsBtn = document.getElementById("refreshErrorsBtn");
    ui.errorTableBody = document.querySelector("#errorTable tbody");
    ui.refreshDataBtn = document.getElementById("refreshDataBtn");
    ui.summaryCards = document.getElementById("summaryCards");
    ui.previewTableSelect = document.getElementById("previewTableSelect");
    ui.previewLimit = document.getElementById("previewLimit");
    ui.loadPreviewBtn = document.getElementById("loadPreviewBtn");
    ui.previewTableHead = document.querySelector("#previewTable thead");
    ui.previewTableBody = document.querySelector("#previewTable tbody");
    ui.reportYear = document.getElementById("reportYear");
    ui.periodType = document.getElementById("periodType");
    ui.reportMonth = document.getElementById("reportMonth");
    ui.reportQuarter = document.getElementById("reportQuarter");
    ui.monthWrap = document.getElementById("monthWrap");
    ui.quarterWrap = document.getElementById("quarterWrap");
    ui.reportType = document.getElementById("reportType");
    ui.generateReportBtn = document.getElementById("generateReportBtn");
    ui.reportOutput = document.getElementById("reportOutput");
    ui.exportCsvBtn = document.getElementById("exportCsvBtn");
    ui.printPdfBtn = document.getElementById("printPdfBtn");
  }

  function bindEvents() {
    ui.tabButtons.forEach((btn) =>
      btn.addEventListener("click", () => activateTab(btn.dataset.tab))
    );

    ui.roleSelect.addEventListener("change", async (event) => {
      state.role = event.target.value;
      await loadPreviewRows();
      if (state.lastReport) renderReport(state.lastReport);
    });

    ui.excelFiles.addEventListener("change", async () => {
      await refreshSelectedFiles(Array.from(ui.excelFiles.files || []));
    });

    ui.importBtn.addEventListener("click", onImportClick);
    ui.refreshErrorsBtn.addEventListener("click", refreshErrorTable);
    ui.refreshDataBtn.addEventListener("click", refreshSummary);
    ui.loadPreviewBtn.addEventListener("click", loadPreviewRows);
    ui.periodType.addEventListener("change", onPeriodTypeChange);
    ui.generateReportBtn.addEventListener("click", onGenerateReport);
    ui.exportCsvBtn.addEventListener("click", onExportCsv);
    ui.printPdfBtn.addEventListener("click", onPrintReport);
    ui.backupBtn.addEventListener("click", onBackupDownload);
    ui.clearDbBtn.addEventListener("click", onClearDb);
  }

  async function loadParserConfig() {
    try {
      const response = await fetch("./parser-config.json");
      if (!response.ok) throw new Error("config load failed");
      state.parserConfig = await response.json();
    } catch (error) {
      state.parserConfig = null;
      setStatus("파서 설정 파일을 읽지 못했습니다. 기본 규칙으로 진행합니다.", "warn");
      console.warn(error);
    }
  }

  function activateTab(tab) {
    ui.tabButtons.forEach((btn) =>
      btn.classList.toggle("active", btn.dataset.tab === tab)
    );
    Object.entries(ui.tabPanels).forEach(([key, panel]) =>
      panel.classList.toggle("active", key === tab)
    );
  }

  function buildMonths() {
    ui.reportMonth.innerHTML = "";
    for (let month = 1; month <= 12; month += 1) {
      const option = document.createElement("option");
      option.value = String(month);
      option.textContent = `${month}월`;
      ui.reportMonth.appendChild(option);
    }
  }

  function onPeriodTypeChange() {
    const type = ui.periodType.value;
    ui.monthWrap.classList.toggle("hidden", type !== "monthly");
    ui.quarterWrap.classList.toggle("hidden", type !== "quarterly");
  }

  async function refreshSelectedFiles(files) {
    ui.selectedFiles.innerHTML = "";
    if (!files.length) {
      const item = document.createElement("li");
      item.textContent = "선택된 파일이 없습니다.";
      ui.selectedFiles.appendChild(item);
      return;
    }
    files.forEach((file) => {
      const item = document.createElement("li");
      item.textContent = `${file.name} (${formatNumber(file.size)} bytes)`;
      ui.selectedFiles.appendChild(item);
    });
  }

  function getImportMode() {
    const checked = document.querySelector('input[name="importMode"]:checked');
    return checked ? checked.value : "merge";
  }

  async function onImportClick() {
    const files = Array.from(ui.excelFiles.files || []);
    if (!files.length) {
      setStatus("업로드할 엑셀 파일을 먼저 선택해주세요.", "warn");
      return;
    }

    const mode = getImportMode();
    ui.importBtn.disabled = true;
    const startedAt = performance.now();

    try {
      let totalRows = 0;
      let totalErrors = 0;
      const fileSummaries = [];

      for (const file of files) {
        setStatus(`[${file.name}] 파싱 중...`);
        const parseContext = await parseWorkbookFile(file);
        const saveResult = await saveParseContext(parseContext, mode);
        totalRows += saveResult.insertedRows;
        totalErrors += parseContext.errors.length;
        fileSummaries.push({
          file: file.name,
          rows: saveResult.insertedRows,
          errors: parseContext.errors.length
        });
      }

      const sec = ((performance.now() - startedAt) / 1000).toFixed(2);
      setStatus(
        [
          `업로드 완료`,
          `- 모드: ${mode === "overwrite" ? "연도 덮어쓰기" : "병합"}`,
          `- 파일 수: ${files.length}`,
          `- 적재 행수: ${formatNumber(totalRows)}`,
          `- 오류 건수: ${formatNumber(totalErrors)}`,
          `- 처리 시간: ${sec}초`,
          "",
          ...fileSummaries.map(
            (item) => `• ${item.file}: ${formatNumber(item.rows)}행 / 오류 ${formatNumber(item.errors)}건`
          )
        ].join("\n"),
        "ok"
      );
    } catch (error) {
      console.error(error);
      setStatus(`업로드 실패: ${error.message}`, "danger");
    } finally {
      ui.importBtn.disabled = false;
      await refreshErrorTable();
      await refreshSummary();
      await refreshYearOptions();
      await loadPreviewRows();
    }
  }

  async function parseWorkbookFile(file) {
    const arrayBuffer = await file.arrayBuffer();
    const workbook = XLSX.read(arrayBuffer, {
      type: "array",
      cellDates: false,
      raw: true
    });

    const context = createParseContext(file.name);
    const sheetNames = workbook.SheetNames || [];

    for (const sheetName of sheetNames) {
      const ws = workbook.Sheets[sheetName];
      const kind = classifySheet(sheetName);
      if (!ws || kind === "skip") continue;
      try {
        if (kind === "daily_activities") parseDailyActivitiesSheet(context, sheetName, ws);
        if (kind === "hospitalizations") parseHospitalizationSheet(context, sheetName, ws);
        if (kind === "facility_placements") parseFacilitySheet(context, sheetName, ws);
        if (kind === "counseling_records") parseCounselingSheet(context, sheetName, ws);
        if (kind === "referral_log") parseReferralSheet(context, sheetName, ws);
        if (kind === "case_housing") parseCaseHousingSheet(context, sheetName, ws);
        if (kind === "case_longterm") parseCaseLongTermSheet(context, sheetName, ws);
        if (kind === "case_intensive") parseCaseIntensiveSheet(context, sheetName, ws);
        if (kind === "deaths") parseDeathsSheet(context, sheetName, ws);
        if (kind === "non_psych_hospitalizations")
          parseNonPsychHospitalizationSheet(context, sheetName, ws);
        if (kind === "hospital_visits") parseHospitalVisitSheet(context, sheetName, ws);
        if (kind === "community_resources")
          parseCommunityResourcesSheet(context, sheetName, ws);
        if (kind === "meetings_education") parseMeetingEducationSheet(context, sheetName, ws);
        if (kind === "post_action_cases") parsePostActionSheet(context, sheetName, ws);
        if (kind === "long_term_inpatients")
          parseLongTermInpatientsSheet(context, sheetName, ws);
      } catch (error) {
        pushError(context, {
          sheet: sheetName,
          code: "SHEET_PARSE_FAILURE",
          message: `시트 파싱 실패: ${error.message}`
        });
      }
    }
    return context;
  }

  function createParseContext(fileName) {
    const rowsByTable = {};
    Object.keys(TABLE_META)
      .filter((table) => !["files", "parse_errors", "backups"].includes(table))
      .forEach((table) => {
        rowsByTable[table] = [];
      });
    return {
      fileName,
      importedAt: new Date().toISOString(),
      yearHints: new Set(),
      rowsByTable,
      errors: []
    };
  }

  function classifySheet(sheetName) {
    const config = state.parserConfig || {};
    const detect = config.sheet_detection || {};
    const name = String(sheetName || "").trim();

    const excluded = detect.excluded_summary_sheets || [];
    if (excluded.includes(name)) return "skip";

    const dailyPattern = new RegExp(detect.daily_activities_pattern || "^\\d{4}년\\d+월$");
    if (dailyPattern.test(name)) return "daily_activities";

    const hospitalizationSheets = detect.hospitalization_sheets || [
      "입원현황",
      "전체입원현황",
      "전체입원현황(15년부터)"
    ];
    if (hospitalizationSheets.includes(name)) return "hospitalizations";

    if (name === (detect.facility_sheet || "시설입소현황")) return "facility_placements";

    const counselingPattern = new RegExp(detect.counseling_pattern || "^상담현황\\(");
    if (counselingPattern.test(name)) return "counseling_records";

    if (name === (detect.referral_sheet || "연계대장")) return "referral_log";
    if (name === "사례관리현황표(주거지원 및 사례관리)") return "case_housing";
    if (name === "사례관리현황표(장기모니터링)") return "case_longterm";
    if (name === "2026집중사례관리리스트" || name === "집중사례관리 명단")
      return "case_intensive";

    const other = detect.other_sheets || {};
    if (name === (other.deaths || "사망자현황")) return "deaths";
    if (name === (other.non_psych_hospitalizations || "정신과외 입원"))
      return "non_psych_hospitalizations";
    if (name === (other.hospital_visits || "병원방문교육")) return "hospital_visits";
    if (name === (other.community_resources || "지역사회자원동원"))
      return "community_resources";
    if (name === (other.meetings_education || "회의교육참여")) return "meetings_education";
    if (name === (other.post_action_cases || "조치후 사례관리")) return "post_action_cases";
    if (name === (other.long_term_inpatients || "장기입원환자"))
      return "long_term_inpatients";

    return "skip";
  }

  function setStatus(message, level = "normal") {
    ui.importStatus.textContent = message;
    ui.importStatus.classList.remove("text-ok", "text-warn", "text-danger");
    if (level === "ok") ui.importStatus.classList.add("text-ok");
    if (level === "warn") ui.importStatus.classList.add("text-warn");
    if (level === "danger") ui.importStatus.classList.add("text-danger");
  }

  function pushError(context, payload) {
    context.errors.push({
      sheet: payload.sheet || null,
      row: payload.row || null,
      column: payload.column || null,
      code: payload.code || "UNKNOWN",
      message: payload.message || ""
    });
  }

  function readCell(context, sheetName, row, rowIndex, colIndex) {
    if (colIndex === -1 || colIndex === null || colIndex === undefined) return null;
    const value = row[colIndex];
    if (typeof value === "string" && ERROR_CELL_REGEX.test(value.trim())) {
      pushError(context, {
        sheet: sheetName,
        row: rowIndex + 1,
        column: colName(colIndex),
        code: "FORMULA_ERROR",
        message: `수식 오류값 발견 (${value.trim()})`
      });
      return null;
    }
    return value;
  }

  function sheetToMatrixWithMerges(ws) {
    const matrix = XLSX.utils.sheet_to_json(ws, {
      header: 1,
      raw: true,
      defval: null,
      blankrows: false
    });
    const merges = ws["!merges"] || [];
    merges.forEach((merge) => {
      const base =
        matrix[merge.s.r] && matrix[merge.s.r][merge.s.c] !== undefined
          ? matrix[merge.s.r][merge.s.c]
          : null;
      for (let r = merge.s.r; r <= merge.e.r; r += 1) {
        if (!matrix[r]) matrix[r] = [];
        for (let c = merge.s.c; c <= merge.e.c; c += 1) {
          if (matrix[r][c] === null || matrix[r][c] === undefined || matrix[r][c] === "") {
            matrix[r][c] = base;
          }
        }
      }
    });
    return matrix;
  }

  function extractYearMonthFromSheet(sheetName) {
    const match = String(sheetName || "").match(/(\d{4})년\s*(\d{1,2})월/);
    if (!match) return null;
    return { year: Number(match[1]), month: Number(match[2]) };
  }

  function extractYearFromText(text) {
    const match = String(text || "").match(/(20\d{2})/);
    return match ? Number(match[1]) : null;
  }

  function inferYearFromFileName(fileName) {
    return extractYearFromText(fileName);
  }

  function parseExcelDate(value, fallbackYear, fallbackMonth) {
    if (value === null || value === undefined || value === "") return null;
    if (typeof value === "number" && Number.isFinite(value)) {
      if (value > 10000 && XLSX.SSF && XLSX.SSF.parse_date_code) {
        const parsed = XLSX.SSF.parse_date_code(value);
        if (parsed && parsed.y && parsed.m && parsed.d) {
          return `${parsed.y}-${pad2(parsed.m)}-${pad2(parsed.d)}`;
        }
      }
      if (value > 19000000 && value < 21000101) {
        const s = String(Math.trunc(value));
        return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
      }
    }
    const text = toText(value).replace(/\./g, "-").replace(/\//g, "-").replace(/\s+/g, "");
    if (!text) return null;
    let m = text.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
    if (m) return `${m[1]}-${pad2(Number(m[2]))}-${pad2(Number(m[3]))}`;
    m = text.match(/^(\d{4})(\d{2})(\d{2})$/);
    if (m) return `${m[1]}-${m[2]}-${m[3]}`;
    m = text.match(/^(\d{1,2})-(\d{1,2})$/);
    if (m && fallbackYear) return `${fallbackYear}-${pad2(Number(m[1]))}-${pad2(Number(m[2]))}`;
    if (/^\d{1,2}$/.test(text) && fallbackYear && fallbackMonth) {
      return `${fallbackYear}-${pad2(fallbackMonth)}-${pad2(Number(text))}`;
    }
    return null;
  }

  function sanitizeDobValue(value) {
    if (value === null || value === undefined || value === "") return null;
    if (typeof value === "number" && Number.isFinite(value)) {
      const n = Math.trunc(value);
      if (n > 19000000 && n < 21000101) {
        const s = String(n);
        return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
      }
      if (n > 0 && n < 1000000) return String(n).padStart(6, "0");
      return String(n);
    }
    let text = String(value).trim().replace(/\s+/g, "");
    if (!text) return null;
    if (/^\d{13}$/.test(text)) return `${text.slice(0, 6)}-${text.slice(6, 7)}******`;
    if (/^\d{6}-\d{7}$/.test(text)) return `${text.slice(0, 6)}-${text.slice(7, 8)}******`;
    return text;
  }

  function deriveYear(dateText, fallbackYear) {
    if (dateText && /^\d{4}-\d{2}-\d{2}$/.test(dateText)) return Number(dateText.slice(0, 4));
    return fallbackYear || null;
  }

  function deriveMonth(dateText, fallbackMonth) {
    if (dateText && /^\d{4}-\d{2}-\d{2}$/.test(dateText)) return Number(dateText.slice(5, 7));
    return fallbackMonth || null;
  }

  function normalizeStayDays(value, status) {
    if (value === null || value === undefined || !Number.isFinite(value)) return null;
    if (value < 0 && toText(status).includes("입원중")) return null;
    return Math.trunc(value);
  }

  function normalizeHeader(value) {
    if (value === null || value === undefined) return "";
    return String(value)
      .toLowerCase()
      .replace(/\s+/g, "")
      .replace(/[(){}\[\]→\-_:.,]/g, "");
  }

  function findHeaderRow(matrix, mustContainTexts) {
    const targets = mustContainTexts.map(normalizeHeader);
    const maxRows = Math.min(matrix.length, 40);
    for (let r = 0; r < maxRows; r += 1) {
      const rowText = (matrix[r] || [])
        .map((v) => normalizeHeader(v))
        .filter(Boolean)
        .join("|");
      if (!rowText) continue;
      if (targets.every((t) => rowText.includes(t))) return r;
    }
    return -1;
  }

  function findCol(headerRow, candidates) {
    if (!Array.isArray(headerRow)) return -1;
    const normalized = headerRow.map(normalizeHeader);
    for (const candidate of candidates) {
      const key = normalizeHeader(candidate);
      const exact = normalized.findIndex((v) => v === key);
      if (exact >= 0) return exact;
    }
    for (const candidate of candidates) {
      const key = normalizeHeader(candidate);
      const include = normalized.findIndex((v) => v.includes(key));
      if (include >= 0) return include;
    }
    return -1;
  }

  function toText(value) {
    if (value === null || value === undefined) return "";
    return String(value).trim();
  }

  function toNumber(value) {
    if (value === null || value === undefined || value === "") return null;
    if (typeof value === "number" && Number.isFinite(value)) return value;
    const normalized = String(value).replace(/,/g, "").trim();
    if (!normalized) return null;
    const num = Number(normalized);
    return Number.isFinite(num) ? num : null;
  }

  function toInteger(value) {
    const num = toNumber(value);
    return num === null ? null : Math.trunc(num);
  }

  function pad2(value) {
    return String(value).padStart(2, "0");
  }

  function colName(colIndex) {
    if (colIndex === null || colIndex === undefined || colIndex < 0) return "";
    let col = "";
    let n = colIndex + 1;
    while (n > 0) {
      const mod = (n - 1) % 26;
      col = String.fromCharCode(65 + mod) + col;
      n = Math.floor((n - mod) / 26);
    }
    return col;
  }

  function getSkipKeywords() {
    const config = state.parserConfig || {};
    const rules = config.parsing_rules || {};
    return rules.skip_keywords || ["일일누계", "총누계", "누계", "합계"];
  }

  function normalizeMetricDefaults(metrics) {
    const normalized = {};
    Object.entries(metrics).forEach(([key, value]) => {
      normalized[key] = value || 0;
    });
    normalized.action_total =
      normalized.voluntary_admission +
      normalized.emergency_admission +
      normalized.protective_diagnosis +
      normalized.facility_homeless +
      normalized.facility_other +
      normalized.supported_housing +
      normalized.housing_support +
      normalized.supplies_support +
      normalized.action_other;
    normalized.post_action_total =
      normalized.outpatient +
      normalized.hospital_visit +
      normalized.home_visit +
      normalized.medication +
      normalized.post_other;
    return normalized;
  }

  function parseDailyActivitiesSheet(context, sheetName, ws) {
    const matrix = sheetToMatrixWithMerges(ws);
    const yearMonth = extractYearMonthFromSheet(sheetName);
    if (!yearMonth) return;
    context.yearHints.add(yearMonth.year);

    let currentDate = null;
    const dedupe = new Set();
    const skipKeywords = getSkipKeywords();

    for (let rowIndex = 3; rowIndex < matrix.length; rowIndex += 1) {
      const row = matrix[rowIndex] || [];
      const dateRaw = readCell(context, sheetName, row, rowIndex, 0);
      const worker = toText(readCell(context, sheetName, row, rowIndex, 1));

      if (dateRaw !== null && dateRaw !== undefined && String(dateRaw).trim() !== "") {
        const parsed = parseExcelDate(dateRaw, yearMonth.year, yearMonth.month);
        if (parsed) currentDate = parsed;
        else {
          pushError(context, {
            sheet: sheetName,
            row: rowIndex + 1,
            column: "A",
            code: "INVALID_DATE",
            message: `날짜 파싱 실패: ${String(dateRaw)}`
          });
        }
      }

      if (!worker) continue;
      if (skipKeywords.some((key) => worker.includes(key))) continue;

      const metrics = normalizeMetricDefaults({
        mental_counseling: toNumber(readCell(context, sheetName, row, rowIndex, 2)),
        alcohol_counseling: toNumber(readCell(context, sheetName, row, rowIndex, 3)),
        other_counseling: toNumber(readCell(context, sheetName, row, rowIndex, 4)),
        counseling_total: toNumber(readCell(context, sheetName, row, rowIndex, 5)),
        voluntary_admission: toNumber(readCell(context, sheetName, row, rowIndex, 6)),
        emergency_admission: toNumber(readCell(context, sheetName, row, rowIndex, 7)),
        protective_diagnosis: toNumber(readCell(context, sheetName, row, rowIndex, 8)),
        facility_homeless: toNumber(readCell(context, sheetName, row, rowIndex, 9)),
        facility_other: toNumber(readCell(context, sheetName, row, rowIndex, 10)),
        supported_housing: toNumber(readCell(context, sheetName, row, rowIndex, 11)),
        housing_support: toNumber(readCell(context, sheetName, row, rowIndex, 12)),
        supplies_support: toNumber(readCell(context, sheetName, row, rowIndex, 13)),
        action_other: toNumber(readCell(context, sheetName, row, rowIndex, 14)),
        outpatient: toNumber(readCell(context, sheetName, row, rowIndex, 15)),
        hospital_visit: toNumber(readCell(context, sheetName, row, rowIndex, 16)),
        home_visit: toNumber(readCell(context, sheetName, row, rowIndex, 17)),
        medication: toNumber(readCell(context, sheetName, row, rowIndex, 18)),
        post_other: toNumber(readCell(context, sheetName, row, rowIndex, 19)),
        total: toNumber(readCell(context, sheetName, row, rowIndex, 20))
      });

      const hasMetric = Object.values(metrics).some((v) => v !== null && v !== 0);
      if (!hasMetric && !currentDate) continue;

      const key = `${currentDate || "unknown"}|${worker}`;
      if (dedupe.has(key)) continue;
      dedupe.add(key);

      context.rowsByTable.daily_activities.push({
        year: yearMonth.year,
        month: yearMonth.month,
        date: currentDate || `${yearMonth.year}-${pad2(yearMonth.month)}-01`,
        worker,
        ...metrics,
        source_sheet: sheetName
      });
    }
  }

  function parseHospitalizationSheet(context, sheetName, ws) {
    const matrix = sheetToMatrixWithMerges(ws);
    const headerRow = findHeaderRow(matrix, ["입원날짜", "대상자명", "병원"]);
    if (headerRow < 0) return;

    const header = matrix[headerRow] || [];
    const yearHint = inferYearFromFileName(context.fileName);
    if (yearHint) context.yearHints.add(yearHint);
    const idx = {
      seq: findCol(header, ["연번"]),
      month: findCol(header, ["월"]),
      admissionDate: findCol(header, ["입원날짜"]),
      gender: findCol(header, ["성별"]),
      name: findCol(header, ["대상자명", "이름"]),
      dob: findCol(header, ["생년월일", "주민번호"]),
      type: findCol(header, ["입원형태"]),
      diseaseType: findCol(header, ["질환명", "질환구분"]),
      hospital: findCol(header, ["병원명"]),
      worker: findCol(header, ["담당자"]),
      caseWorker: findCol(header, ["사례관리담당자"]),
      diagnosis: findCol(header, ["진단명"]),
      status: findCol(header, ["입원여부"]),
      stayDays: findCol(header, ["입원기간"]),
      dischargeDate: findCol(header, ["퇴원일"]),
      afterCare: findCol(header, ["퇴원 후 연계", "퇴원후연계"]),
      note: findCol(header, ["비고"])
    };

    const dedupe = new Set();
    for (let rowIndex = headerRow + 1; rowIndex < matrix.length; rowIndex += 1) {
      const row = matrix[rowIndex] || [];
      const seq = toNumber(readCell(context, sheetName, row, rowIndex, idx.seq));
      const name = toText(readCell(context, sheetName, row, rowIndex, idx.name));
      const admissionRaw = readCell(context, sheetName, row, rowIndex, idx.admissionDate);
      const hospital = toText(readCell(context, sheetName, row, rowIndex, idx.hospital));
      if (!seq && !name && !admissionRaw && !hospital) continue;
      if (!name && seq) continue;

      const admissionDate = parseExcelDate(admissionRaw, yearHint, null);
      const rowYear = deriveYear(admissionDate, yearHint);
      if (rowYear) context.yearHints.add(rowYear);
      const status = toText(readCell(context, sheetName, row, rowIndex, idx.status));
      const stayDays = normalizeStayDays(
        toNumber(readCell(context, sheetName, row, rowIndex, idx.stayDays)),
        status
      );

      const key = `${admissionDate || "unknown"}|${name}|${hospital || ""}`;
      if (dedupe.has(key)) continue;
      dedupe.add(key);

      context.rowsByTable.hospitalizations.push({
        year: rowYear,
        seq: toInteger(seq),
        month:
          toInteger(readCell(context, sheetName, row, rowIndex, idx.month)) ||
          deriveMonth(admissionDate, null),
        admission_date: admissionDate,
        gender: toText(readCell(context, sheetName, row, rowIndex, idx.gender)),
        name,
        dob: sanitizeDobValue(readCell(context, sheetName, row, rowIndex, idx.dob)),
        type: toText(readCell(context, sheetName, row, rowIndex, idx.type)),
        disease_type: toText(readCell(context, sheetName, row, rowIndex, idx.diseaseType)),
        hospital,
        worker: toText(readCell(context, sheetName, row, rowIndex, idx.worker)),
        case_worker: toText(readCell(context, sheetName, row, rowIndex, idx.caseWorker)),
        diagnosis: toText(readCell(context, sheetName, row, rowIndex, idx.diagnosis)),
        status,
        stay_days: stayDays,
        discharge_date: parseExcelDate(
          readCell(context, sheetName, row, rowIndex, idx.dischargeDate),
          rowYear,
          null
        ),
        after_care: toText(readCell(context, sheetName, row, rowIndex, idx.afterCare)),
        note: toText(readCell(context, sheetName, row, rowIndex, idx.note)),
        source_sheet: sheetName
      });
    }
  }

  function parseFacilitySheet(context, sheetName, ws) {
    const matrix = sheetToMatrixWithMerges(ws);
    const headerRow = findHeaderRow(matrix, ["연번", "대상자", "입퇴소"]);
    if (headerRow < 0) return;

    const header = matrix[headerRow] || [];
    const yearHint = inferYearFromFileName(context.fileName);
    if (yearHint) context.yearHints.add(yearHint);

    const idx = {
      seq: findCol(header, ["연번"]),
      month: findCol(header, ["월"]),
      date: findCol(header, ["날짜"]),
      name: findCol(header, ["대상자명", "이름"]),
      dob: findCol(header, ["주민번호", "생년월일"]),
      facility: findCol(header, ["시설명", "시설", "입소처"]),
      diseaseType: findCol(header, ["질환명"]),
      worker: findCol(header, ["담당자"]),
      diagnosis: findCol(header, ["진단명"]),
      facilityType: findCol(header, ["종류"]),
      status: findCol(header, ["입퇴소여부", "상태"]),
      period: findCol(header, ["입소기간"]),
      dischargeDate: findCol(header, ["퇴소일"]),
      afterCare: findCol(header, ["퇴소후조치", "퇴소후 조치"]),
      confirmDate: findCol(header, ["입소확인일"]),
      note: findCol(header, ["비고"])
    };

    const dedupe = new Set();
    for (let rowIndex = headerRow + 1; rowIndex < matrix.length; rowIndex += 1) {
      const row = matrix[rowIndex] || [];
      const seq = toNumber(readCell(context, sheetName, row, rowIndex, idx.seq));
      const name = toText(readCell(context, sheetName, row, rowIndex, idx.name));
      const dateRaw = readCell(context, sheetName, row, rowIndex, idx.date);
      if (!seq && !name && !dateRaw) continue;
      if (!name && seq) continue;

      const date = parseExcelDate(dateRaw, yearHint, null);
      const rowYear = deriveYear(date, yearHint);
      if (rowYear) context.yearHints.add(rowYear);

      const facility = toText(readCell(context, sheetName, row, rowIndex, idx.facility));
      const key = `${date || "unknown"}|${name}|${facility}`;
      if (dedupe.has(key)) continue;
      dedupe.add(key);

      context.rowsByTable.facility_placements.push({
        year: rowYear,
        seq: toInteger(seq),
        month:
          toInteger(readCell(context, sheetName, row, rowIndex, idx.month)) ||
          deriveMonth(date, null),
        date,
        name,
        dob: sanitizeDobValue(readCell(context, sheetName, row, rowIndex, idx.dob)),
        facility,
        disease_type: toText(readCell(context, sheetName, row, rowIndex, idx.diseaseType)),
        worker: toText(readCell(context, sheetName, row, rowIndex, idx.worker)),
        diagnosis: toText(readCell(context, sheetName, row, rowIndex, idx.diagnosis)),
        facility_type: toText(readCell(context, sheetName, row, rowIndex, idx.facilityType)),
        status: toText(readCell(context, sheetName, row, rowIndex, idx.status)),
        period: toText(readCell(context, sheetName, row, rowIndex, idx.period)),
        discharge_date: parseExcelDate(
          readCell(context, sheetName, row, rowIndex, idx.dischargeDate),
          rowYear,
          null
        ),
        after_care: toText(readCell(context, sheetName, row, rowIndex, idx.afterCare)),
        confirm_date: parseExcelDate(
          readCell(context, sheetName, row, rowIndex, idx.confirmDate),
          rowYear,
          null
        ),
        note: toText(readCell(context, sheetName, row, rowIndex, idx.note)),
        source_sheet: sheetName
      });
    }
  }

  function parseCounselingSheet(context, sheetName, ws) {
    const matrix = sheetToMatrixWithMerges(ws);
    const headerRow = findHeaderRow(matrix, ["성명", "상담일"]);
    if (headerRow < 0) return;

    const header = matrix[headerRow] || [];
    const yearHint = extractYearFromText(sheetName) || inferYearFromFileName(context.fileName);
    if (yearHint) context.yearHints.add(yearHint);

    const idx = {
      seq: findCol(header, ["연번", "번호"]),
      name: findCol(header, ["성명", "이름"]),
      dob: findCol(header, ["생년월일"]),
      gender: findCol(header, ["성별"]),
      counselingDate: findCol(header, ["상담일"]),
      type: findCol(header, ["상담유형"]),
      method: findCol(header, ["상담방법"]),
      worker: findCol(header, ["담당자"]),
      referralPath: findCol(header, ["노숙인방문", "내방경위", "연계경로"]),
      age: findCol(header, ["나이"]),
      ageGroup: findCol(header, ["연령대"]),
      month: findCol(header, ["상담월"])
    };

    for (let rowIndex = headerRow + 1; rowIndex < matrix.length; rowIndex += 1) {
      const row = matrix[rowIndex] || [];
      const name = toText(readCell(context, sheetName, row, rowIndex, idx.name));
      const counselingRaw = readCell(context, sheetName, row, rowIndex, idx.counselingDate);
      const counselingDate = parseExcelDate(counselingRaw, yearHint, null);
      if (!name && !counselingRaw) continue;
      if (!name) continue;

      const rowYear = deriveYear(counselingDate, yearHint);
      if (rowYear) context.yearHints.add(rowYear);

      context.rowsByTable.counseling_records.push({
        year: rowYear,
        seq: toInteger(readCell(context, sheetName, row, rowIndex, idx.seq)),
        name,
        dob: sanitizeDobValue(readCell(context, sheetName, row, rowIndex, idx.dob)),
        gender: toText(readCell(context, sheetName, row, rowIndex, idx.gender)),
        counseling_date: counselingDate,
        type: toText(readCell(context, sheetName, row, rowIndex, idx.type)),
        method: toText(readCell(context, sheetName, row, rowIndex, idx.method)),
        worker: toText(readCell(context, sheetName, row, rowIndex, idx.worker)),
        referral_path: toText(readCell(context, sheetName, row, rowIndex, idx.referralPath)),
        age: toInteger(readCell(context, sheetName, row, rowIndex, idx.age)),
        age_group: toText(readCell(context, sheetName, row, rowIndex, idx.ageGroup)),
        month:
          toInteger(readCell(context, sheetName, row, rowIndex, idx.month)) ||
          deriveMonth(counselingDate, null),
        source_sheet: sheetName
      });
    }
  }

  function parseReferralSheet(context, sheetName, ws) {
    const matrix = sheetToMatrixWithMerges(ws);
    const headerRow = findHeaderRow(matrix, ["연번", "내방경위", "개입내용"]);
    if (headerRow < 0) return;

    const header = matrix[headerRow] || [];
    const yearHint = inferYearFromFileName(context.fileName);
    if (yearHint) context.yearHints.add(yearHint);

    const idx = {
      seq: findCol(header, ["연번"]),
      month: findCol(header, ["월"]),
      week: findCol(header, ["주"]),
      date: findCol(header, ["날짜"]),
      visitType: findCol(header, ["내방경위"]),
      referrerOrg: findCol(header, ["의뢰처"]),
      referrer: findCol(header, ["의뢰자"]),
      receiver: findCol(header, ["접수자"]),
      clientType: findCol(header, ["구분"]),
      name: findCol(header, ["이름"]),
      dob: findCol(header, ["생년월일"]),
      gender: findCol(header, ["성별"]),
      disease: findCol(header, ["질환구분"]),
      requestType: findCol(header, ["요청사항"]),
      interventionYn: findCol(header, ["개입여부"]),
      result: findCol(header, ["개입결과"]),
      content: findCol(header, ["개입내용"])
    };

    for (let rowIndex = headerRow + 1; rowIndex < matrix.length; rowIndex += 1) {
      const row = matrix[rowIndex] || [];
      const seq = toNumber(readCell(context, sheetName, row, rowIndex, idx.seq));
      const name = toText(readCell(context, sheetName, row, rowIndex, idx.name));
      const dateRaw = readCell(context, sheetName, row, rowIndex, idx.date);
      if (!seq && !name && !dateRaw) continue;
      if (!name && seq) continue;

      const date = parseExcelDate(dateRaw, yearHint, null);
      const rowYear = deriveYear(date, yearHint);
      if (rowYear) context.yearHints.add(rowYear);

      context.rowsByTable.referral_log.push({
        year: rowYear,
        seq: toInteger(seq),
        month:
          toInteger(readCell(context, sheetName, row, rowIndex, idx.month)) ||
          deriveMonth(date, null),
        week: toInteger(readCell(context, sheetName, row, rowIndex, idx.week)),
        date,
        visit_type: toText(readCell(context, sheetName, row, rowIndex, idx.visitType)),
        referrer_org: toText(readCell(context, sheetName, row, rowIndex, idx.referrerOrg)),
        referrer: toText(readCell(context, sheetName, row, rowIndex, idx.referrer)),
        receiver: toText(readCell(context, sheetName, row, rowIndex, idx.receiver)),
        client_type: toText(readCell(context, sheetName, row, rowIndex, idx.clientType)),
        name,
        dob: sanitizeDobValue(readCell(context, sheetName, row, rowIndex, idx.dob)),
        gender: toText(readCell(context, sheetName, row, rowIndex, idx.gender)),
        disease: toText(readCell(context, sheetName, row, rowIndex, idx.disease)),
        request_type: toText(readCell(context, sheetName, row, rowIndex, idx.requestType)),
        intervention_yn: toText(readCell(context, sheetName, row, rowIndex, idx.interventionYn)),
        result: toText(readCell(context, sheetName, row, rowIndex, idx.result)),
        content: toText(readCell(context, sheetName, row, rowIndex, idx.content)),
        source_sheet: sheetName
      });
    }
  }

  function parseCaseHousingSheet(context, sheetName, ws) {
    const matrix = sheetToMatrixWithMerges(ws);
    const headerRow = findHeaderRow(matrix, ["사례개시일", "종결여부", "이름"]);
    if (headerRow < 0) return;
    const header = matrix[headerRow] || [];
    const yearHint = inferYearFromFileName(context.fileName);
    if (yearHint) context.yearHints.add(yearHint);

    const idx = {
      seq: findCol(header, ["구분", "연번", "번호"]),
      caseStart: findCol(header, ["사례개시일"]),
      housingEnd: findCol(header, ["주거지원 종료일", "주거지원종료일"]),
      caseEnd: findCol(header, ["사례관리 종결예정일", "사례관리종결예정일"]),
      status: findCol(header, ["종결여부"]),
      name: findCol(header, ["이름"]),
      dob: findCol(header, ["생년월일"]),
      worker: findCol(header, ["사례관리자"]),
      address: findCol(header, ["거주지"]),
      category: findCol(header, ["분류"]),
      suicideHistory: findCol(header, ["자살시도 경험", "자살시도경험"]),
      diagnosis: findCol(header, ["진단명"]),
      benefitYn: findCol(header, ["수급"]),
      memo: findCol(header, ["메모"])
    };

    for (let rowIndex = headerRow + 1; rowIndex < matrix.length; rowIndex += 1) {
      const row = matrix[rowIndex] || [];
      const name = toText(readCell(context, sheetName, row, rowIndex, idx.name));
      const status = toText(readCell(context, sheetName, row, rowIndex, idx.status));
      const caseStart = parseExcelDate(
        readCell(context, sheetName, row, rowIndex, idx.caseStart),
        yearHint,
        null
      );
      if (!name && !status && !caseStart) continue;
      if (!name) continue;

      const rowYear = deriveYear(caseStart, yearHint);
      if (rowYear) context.yearHints.add(rowYear);

      context.rowsByTable.case_management.push({
        year: rowYear,
        seq: toInteger(readCell(context, sheetName, row, rowIndex, idx.seq)),
        type: "주거지원",
        case_start: caseStart,
        housing_end: parseExcelDate(
          readCell(context, sheetName, row, rowIndex, idx.housingEnd),
          rowYear,
          null
        ),
        case_end: parseExcelDate(
          readCell(context, sheetName, row, rowIndex, idx.caseEnd),
          rowYear,
          null
        ),
        status,
        name,
        dob: sanitizeDobValue(readCell(context, sheetName, row, rowIndex, idx.dob)),
        worker: toText(readCell(context, sheetName, row, rowIndex, idx.worker)),
        address: toText(readCell(context, sheetName, row, rowIndex, idx.address)),
        category: toText(readCell(context, sheetName, row, rowIndex, idx.category)),
        suicide_history: toText(readCell(context, sheetName, row, rowIndex, idx.suicideHistory)),
        diagnosis: toText(readCell(context, sheetName, row, rowIndex, idx.diagnosis)),
        benefit_yn: toText(readCell(context, sheetName, row, rowIndex, idx.benefitYn)),
        memo: toText(readCell(context, sheetName, row, rowIndex, idx.memo)),
        source_sheet: sheetName
      });
    }
  }

  function parseCaseLongTermSheet(context, sheetName, ws) {
    const matrix = sheetToMatrixWithMerges(ws);
    const headerRow = findHeaderRow(matrix, ["사례개시일", "종결여부", "이름"]);
    if (headerRow < 0) return;
    const header = matrix[headerRow] || [];
    const yearHint = inferYearFromFileName(context.fileName);
    if (yearHint) context.yearHints.add(yearHint);

    const idx = {
      seq: findCol(header, ["구분", "연번", "번호"]),
      caseStart: findCol(header, ["사례개시일"]),
      housingEnd: findCol(header, ["주거지원 종결일", "주거지원종결일"]),
      caseEnd: findCol(header, ["사례관리 종결예정일", "사례관리종결예정일"]),
      status: findCol(header, ["종결여부"]),
      name: findCol(header, ["이름"]),
      dob: findCol(header, ["생년월일"]),
      worker: findCol(header, ["사례관리자"]),
      address: findCol(header, ["거주지"]),
      category: findCol(header, ["분류"]),
      suicideHistory: findCol(header, ["자살시도 경험", "자살시도경험"]),
      diagnosis: findCol(header, ["진단명"]),
      benefitYn: findCol(header, ["수급"]),
      memo: findCol(header, ["메모"])
    };

    for (let rowIndex = headerRow + 1; rowIndex < matrix.length; rowIndex += 1) {
      const row = matrix[rowIndex] || [];
      const name = toText(readCell(context, sheetName, row, rowIndex, idx.name));
      const status = toText(readCell(context, sheetName, row, rowIndex, idx.status));
      const caseStart = parseExcelDate(
        readCell(context, sheetName, row, rowIndex, idx.caseStart),
        yearHint,
        null
      );
      if (!name && !status && !caseStart) continue;
      if (!name) continue;

      const rowYear = deriveYear(caseStart, yearHint);
      if (rowYear) context.yearHints.add(rowYear);

      context.rowsByTable.case_management.push({
        year: rowYear,
        seq: toInteger(readCell(context, sheetName, row, rowIndex, idx.seq)),
        type: "장기모니터링",
        case_start: caseStart,
        housing_end: parseExcelDate(
          readCell(context, sheetName, row, rowIndex, idx.housingEnd),
          rowYear,
          null
        ),
        case_end: parseExcelDate(
          readCell(context, sheetName, row, rowIndex, idx.caseEnd),
          rowYear,
          null
        ),
        status,
        name,
        dob: sanitizeDobValue(readCell(context, sheetName, row, rowIndex, idx.dob)),
        worker: toText(readCell(context, sheetName, row, rowIndex, idx.worker)),
        address: toText(readCell(context, sheetName, row, rowIndex, idx.address)),
        category: toText(readCell(context, sheetName, row, rowIndex, idx.category)),
        suicide_history: toText(readCell(context, sheetName, row, rowIndex, idx.suicideHistory)),
        diagnosis: toText(readCell(context, sheetName, row, rowIndex, idx.diagnosis)),
        benefit_yn: toText(readCell(context, sheetName, row, rowIndex, idx.benefitYn)),
        memo: toText(readCell(context, sheetName, row, rowIndex, idx.memo)),
        source_sheet: sheetName
      });
    }
  }

  function parseCaseIntensiveSheet(context, sheetName, ws) {
    const matrix = sheetToMatrixWithMerges(ws);
    const headerRow = findHeaderRow(matrix, ["순번", "성명", "담당자", "최근 3개월"]);
    if (headerRow < 0) return;
    const header = matrix[headerRow] || [];
    const yearHint = extractYearFromText(sheetName) || inferYearFromFileName(context.fileName);
    if (yearHint) context.yearHints.add(yearHint);

    const idx = {
      seq: findCol(header, ["순번", "연번"]),
      name: findCol(header, ["성명", "이름"]),
      dob: findCol(header, ["생년월일", "생년 월일"]),
      worker: findCol(header, ["담당자"]),
      address: findCol(header, ["주요 노숙지역", "주요노숙지역"]),
      risk: findCol(header, ["위험내용"]),
      status: findCol(header, ["현재상태"]),
      category: findCol(header, ["개입방향"]),
      memo: findCol(header, ["최근 3개월 조치 및 지원내용", "최근3개월조치및지원내용"])
    };

    for (let rowIndex = headerRow + 1; rowIndex < matrix.length; rowIndex += 1) {
      const row = matrix[rowIndex] || [];
      const seq = toNumber(readCell(context, sheetName, row, rowIndex, idx.seq));
      const name = toText(readCell(context, sheetName, row, rowIndex, idx.name));
      if (!seq && !name) continue;
      if (!name) continue;

      context.rowsByTable.case_management.push({
        year: yearHint,
        seq: toInteger(seq),
        type: "집중사례관리",
        case_start: null,
        housing_end: null,
        case_end: null,
        status: toText(readCell(context, sheetName, row, rowIndex, idx.status)),
        name,
        dob: sanitizeDobValue(readCell(context, sheetName, row, rowIndex, idx.dob)),
        worker: toText(readCell(context, sheetName, row, rowIndex, idx.worker)),
        address: toText(readCell(context, sheetName, row, rowIndex, idx.address)),
        category: toText(readCell(context, sheetName, row, rowIndex, idx.category)),
        suicide_history: null,
        diagnosis: toText(readCell(context, sheetName, row, rowIndex, idx.risk)),
        benefit_yn: null,
        memo: toText(readCell(context, sheetName, row, rowIndex, idx.memo)),
        source_sheet: sheetName
      });
    }
  }

  function parseDeathsSheet(context, sheetName, ws) {
    const matrix = sheetToMatrixWithMerges(ws);
    const headerRow = findHeaderRow(matrix, ["연번", "성명", "사망일자"]);
    if (headerRow < 0) return;
    const header = matrix[headerRow] || [];
    const yearHint = inferYearFromFileName(context.fileName);
    if (yearHint) context.yearHints.add(yearHint);

    const idx = {
      seq: findCol(header, ["연번"]),
      gender: findCol(header, ["성별"]),
      name: findCol(header, ["성명", "이름"]),
      dob: findCol(header, ["생년월일"]),
      deathDate: findCol(header, ["사망일자"]),
      place: findCol(header, ["사망장소"]),
      detailPlace: findCol(header, ["상세사망장소"]),
      cause: findCol(header, ["사망원인"]),
      detailCause: findCol(header, ["상세사망원인"]),
      worker: findCol(header, ["담당자"]),
      followUp: findCol(header, ["후속조치"])
    };

    for (let rowIndex = headerRow + 1; rowIndex < matrix.length; rowIndex += 1) {
      const row = matrix[rowIndex] || [];
      const seq = toNumber(readCell(context, sheetName, row, rowIndex, idx.seq));
      const name = toText(readCell(context, sheetName, row, rowIndex, idx.name));
      if (!seq && !name) continue;
      if (!name) continue;
      const deathDate = parseExcelDate(
        readCell(context, sheetName, row, rowIndex, idx.deathDate),
        yearHint,
        null
      );
      const rowYear = deriveYear(deathDate, yearHint);
      if (rowYear) context.yearHints.add(rowYear);
      context.rowsByTable.deaths.push({
        year: rowYear,
        seq: toInteger(seq),
        gender: toText(readCell(context, sheetName, row, rowIndex, idx.gender)),
        name,
        dob: sanitizeDobValue(readCell(context, sheetName, row, rowIndex, idx.dob)),
        death_date: deathDate,
        place: toText(readCell(context, sheetName, row, rowIndex, idx.place)),
        detail_place: toText(readCell(context, sheetName, row, rowIndex, idx.detailPlace)),
        cause: toText(readCell(context, sheetName, row, rowIndex, idx.cause)),
        detail_cause: toText(readCell(context, sheetName, row, rowIndex, idx.detailCause)),
        worker: toText(readCell(context, sheetName, row, rowIndex, idx.worker)),
        follow_up: toText(readCell(context, sheetName, row, rowIndex, idx.followUp)),
        source_sheet: sheetName
      });
    }
  }

  function parseNonPsychHospitalizationSheet(context, sheetName, ws) {
    const matrix = sheetToMatrixWithMerges(ws);
    const headerRow = findHeaderRow(matrix, ["연번", "날짜", "병원"]);
    if (headerRow < 0) return;
    const header = matrix[headerRow] || [];
    const yearHint = inferYearFromFileName(context.fileName);
    if (yearHint) context.yearHints.add(yearHint);

    const idx = {
      seq: findCol(header, ["연번"]),
      date: findCol(header, ["날짜"]),
      name: findCol(header, ["이름"]),
      dob: findCol(header, ["생년월일", "주민번호"]),
      hospital: findCol(header, ["병원"]),
      department: findCol(header, ["과목"]),
      worker: findCol(header, ["담당"]),
      status: findCol(header, ["현재상황"]),
      note: findCol(header, ["비고"])
    };

    for (let rowIndex = headerRow + 1; rowIndex < matrix.length; rowIndex += 1) {
      const row = matrix[rowIndex] || [];
      const seq = toNumber(readCell(context, sheetName, row, rowIndex, idx.seq));
      const name = toText(readCell(context, sheetName, row, rowIndex, idx.name));
      if (!seq && !name) continue;
      if (!name) continue;
      const date = parseExcelDate(readCell(context, sheetName, row, rowIndex, idx.date), yearHint, null);
      const rowYear = deriveYear(date, yearHint);
      if (rowYear) context.yearHints.add(rowYear);
      context.rowsByTable.non_psych_hospitalizations.push({
        year: rowYear,
        seq: toInteger(seq),
        date,
        name,
        dob: sanitizeDobValue(readCell(context, sheetName, row, rowIndex, idx.dob)),
        hospital: toText(readCell(context, sheetName, row, rowIndex, idx.hospital)),
        department: toText(readCell(context, sheetName, row, rowIndex, idx.department)),
        worker: toText(readCell(context, sheetName, row, rowIndex, idx.worker)),
        status: toText(readCell(context, sheetName, row, rowIndex, idx.status)),
        note: toText(readCell(context, sheetName, row, rowIndex, idx.note)),
        source_sheet: sheetName
      });
    }
  }

  function parseHospitalVisitSheet(context, sheetName, ws) {
    const matrix = sheetToMatrixWithMerges(ws);
    const headerRow = findHeaderRow(matrix, ["상담일", "입원일", "대상자명"]);
    if (headerRow < 0) return;
    const header = matrix[headerRow] || [];
    const yearHint = inferYearFromFileName(context.fileName);
    if (yearHint) context.yearHints.add(yearHint);
    const idx = {
      seq: findCol(header, ["연번", "번호"]),
      counselingDate: findCol(header, ["상담일"]),
      admissionDate: findCol(header, ["입원일"]),
      name: findCol(header, ["대상자명"]),
      dob: findCol(header, ["주민번호", "생년월일"]),
      type: findCol(header, ["입원형태"]),
      diseaseType: findCol(header, ["질환분류"]),
      hospital: findCol(header, ["입원병원"]),
      worker: findCol(header, ["담당자"]),
      counselor: findCol(header, ["상담자"]),
      diagnosis: findCol(header, ["병명"]),
      need: findCol(header, ["욕구"]),
      specialNote: findCol(header, ["특이사항"]),
      afterCare: findCol(header, ["상담후 조치", "상담후조치"]),
      actionDate: findCol(header, ["조치일"]),
      note: findCol(header, ["비고"])
    };
    for (let rowIndex = headerRow + 1; rowIndex < matrix.length; rowIndex += 1) {
      const row = matrix[rowIndex] || [];
      const seq = toNumber(readCell(context, sheetName, row, rowIndex, idx.seq));
      const name = toText(readCell(context, sheetName, row, rowIndex, idx.name));
      if (!seq && !name) continue;
      if (!name) continue;
      const counselingDate = parseExcelDate(
        readCell(context, sheetName, row, rowIndex, idx.counselingDate),
        yearHint,
        null
      );
      const rowYear = deriveYear(counselingDate, yearHint);
      if (rowYear) context.yearHints.add(rowYear);
      context.rowsByTable.hospital_visits.push({
        year: rowYear,
        seq: toInteger(seq),
        counseling_date: counselingDate,
        admission_date: parseExcelDate(
          readCell(context, sheetName, row, rowIndex, idx.admissionDate),
          rowYear,
          null
        ),
        name,
        dob: sanitizeDobValue(readCell(context, sheetName, row, rowIndex, idx.dob)),
        type: toText(readCell(context, sheetName, row, rowIndex, idx.type)),
        disease_type: toText(readCell(context, sheetName, row, rowIndex, idx.diseaseType)),
        hospital: toText(readCell(context, sheetName, row, rowIndex, idx.hospital)),
        worker: toText(readCell(context, sheetName, row, rowIndex, idx.worker)),
        counselor: toText(readCell(context, sheetName, row, rowIndex, idx.counselor)),
        diagnosis: toText(readCell(context, sheetName, row, rowIndex, idx.diagnosis)),
        need: toText(readCell(context, sheetName, row, rowIndex, idx.need)),
        special_note: toText(readCell(context, sheetName, row, rowIndex, idx.specialNote)),
        after_care: toText(readCell(context, sheetName, row, rowIndex, idx.afterCare)),
        action_date: parseExcelDate(
          readCell(context, sheetName, row, rowIndex, idx.actionDate),
          rowYear,
          null
        ),
        note: toText(readCell(context, sheetName, row, rowIndex, idx.note)),
        source_sheet: sheetName
      });
    }
  }

  function parseCommunityResourcesSheet(context, sheetName, ws) {
    const matrix = sheetToMatrixWithMerges(ws);
    const headerRow = findHeaderRow(matrix, ["번호", "날짜", "연계센터"]);
    if (headerRow < 0) return;
    const header = matrix[headerRow] || [];
    const yearHint = inferYearFromFileName(context.fileName);
    if (yearHint) context.yearHints.add(yearHint);
    const idx = {
      seq: findCol(header, ["번호", "연번"]),
      date: findCol(header, ["날짜"]),
      name: findCol(header, ["대상자명", "이름"]),
      gender: findCol(header, ["성별"]),
      dob: findCol(header, ["생년월일"]),
      worker: findCol(header, ["담당자"]),
      org: findCol(header, ["연계센터", "기관"]),
      reason: findCol(header, ["사유"]),
      note: findCol(header, ["비고"]),
      result: findCol(header, ["결과"]),
      failReason: findCol(header, ["실패사유"])
    };
    for (let rowIndex = headerRow + 1; rowIndex < matrix.length; rowIndex += 1) {
      const row = matrix[rowIndex] || [];
      const seq = toNumber(readCell(context, sheetName, row, rowIndex, idx.seq));
      const name = toText(readCell(context, sheetName, row, rowIndex, idx.name));
      if (!seq && !name) continue;
      if (!name) continue;
      const date = parseExcelDate(readCell(context, sheetName, row, rowIndex, idx.date), yearHint, null);
      const rowYear = deriveYear(date, yearHint);
      if (rowYear) context.yearHints.add(rowYear);
      context.rowsByTable.community_resources.push({
        year: rowYear,
        seq: toInteger(seq),
        date,
        name,
        gender: toText(readCell(context, sheetName, row, rowIndex, idx.gender)),
        dob: sanitizeDobValue(readCell(context, sheetName, row, rowIndex, idx.dob)),
        worker: toText(readCell(context, sheetName, row, rowIndex, idx.worker)),
        org: toText(readCell(context, sheetName, row, rowIndex, idx.org)),
        reason: toText(readCell(context, sheetName, row, rowIndex, idx.reason)),
        note: toText(readCell(context, sheetName, row, rowIndex, idx.note)),
        result: toText(readCell(context, sheetName, row, rowIndex, idx.result)),
        fail_reason: toText(readCell(context, sheetName, row, rowIndex, idx.failReason)),
        source_sheet: sheetName
      });
    }
  }

  function parseMeetingEducationSheet(context, sheetName, ws) {
    const matrix = sheetToMatrixWithMerges(ws);
    const headerRow = findHeaderRow(matrix, ["날짜", "구분", "내용"]);
    if (headerRow < 0) return;
    const header = matrix[headerRow] || [];
    const yearHint = inferYearFromFileName(context.fileName);
    if (yearHint) context.yearHints.add(yearHint);
    const idx = {
      seq: findCol(header, ["연번", "번호"]),
      date: findCol(header, ["날짜"]),
      category: findCol(header, ["구분"]),
      content: findCol(header, ["내용"]),
      place: findCol(header, ["장소"]),
      instructor: findCol(header, ["강사"]),
      participants: findCol(header, ["참여자"])
    };
    for (let rowIndex = headerRow + 1; rowIndex < matrix.length; rowIndex += 1) {
      const row = matrix[rowIndex] || [];
      const seq = toNumber(readCell(context, sheetName, row, rowIndex, idx.seq));
      const dateRaw = readCell(context, sheetName, row, rowIndex, idx.date);
      const content = toText(readCell(context, sheetName, row, rowIndex, idx.content));
      if (!seq && !dateRaw && !content) continue;
      const date = parseExcelDate(dateRaw, yearHint, null);
      const rowYear = deriveYear(date, yearHint);
      if (rowYear) context.yearHints.add(rowYear);
      context.rowsByTable.meetings_education.push({
        year: rowYear,
        seq: toInteger(seq),
        date,
        category: toText(readCell(context, sheetName, row, rowIndex, idx.category)),
        content,
        place: toText(readCell(context, sheetName, row, rowIndex, idx.place)),
        instructor: toText(readCell(context, sheetName, row, rowIndex, idx.instructor)),
        participants: toText(readCell(context, sheetName, row, rowIndex, idx.participants)),
        source_sheet: sheetName
      });
    }
  }

  function parsePostActionSheet(context, sheetName, ws) {
    const matrix = sheetToMatrixWithMerges(ws);
    const headerRow = findHeaderRow(matrix, ["월", "주민등록재발급", "수급"]);
    if (headerRow < 0) return;
    const yearHint = inferYearFromFileName(context.fileName);
    if (yearHint) context.yearHints.add(yearHint);
    for (let rowIndex = headerRow + 1; rowIndex < matrix.length; rowIndex += 1) {
      const row = matrix[rowIndex] || [];
      const month = toInteger(readCell(context, sheetName, row, rowIndex, 0));
      if (!month || month < 1 || month > 12) continue;
      context.rowsByTable.post_action_cases.push({
        year: yearHint,
        month,
        id_reissue: toNumber(readCell(context, sheetName, row, rowIndex, 1)) || 0,
        id_restore: toNumber(readCell(context, sheetName, row, rowIndex, 2)) || 0,
        benefit: toNumber(readCell(context, sheetName, row, rowIndex, 3)) || 0,
        other: toNumber(readCell(context, sheetName, row, rowIndex, 4)) || 0,
        return_home: toNumber(readCell(context, sheetName, row, rowIndex, 5)) || 0,
        total: toNumber(readCell(context, sheetName, row, rowIndex, 6)) || 0,
        source_sheet: sheetName
      });
    }
  }

  function parseLongTermInpatientsSheet(context, sheetName, ws) {
    const matrix = sheetToMatrixWithMerges(ws);
    const headerRow = findHeaderRow(matrix, ["연번", "장기입원여부", "입원일"]);
    if (headerRow < 0) return;
    const header = matrix[headerRow] || [];
    const yearHint = inferYearFromFileName(context.fileName);
    if (yearHint) context.yearHints.add(yearHint);
    const idx = {
      seq: findCol(header, ["연번"]),
      isLongterm: findCol(header, ["장기입원여부"]),
      category: findCol(header, ["구분"]),
      name: findCol(header, ["이름"]),
      dob: findCol(header, ["생년월일"]),
      admissionDate: findCol(header, ["입원일"]),
      hospital: findCol(header, ["병원명"]),
      type: findCol(header, ["입원형태"]),
      status: findCol(header, ["입원여부"]),
      dischargeDate: findCol(header, ["퇴원일"]),
      confirmDate: findCol(header, ["재원확인일"])
    };
    for (let rowIndex = headerRow + 1; rowIndex < matrix.length; rowIndex += 1) {
      const row = matrix[rowIndex] || [];
      const seq = toNumber(readCell(context, sheetName, row, rowIndex, idx.seq));
      const name = toText(readCell(context, sheetName, row, rowIndex, idx.name));
      if (!seq && !name) continue;
      if (!name) continue;
      const admissionDate = parseExcelDate(
        readCell(context, sheetName, row, rowIndex, idx.admissionDate),
        yearHint,
        null
      );
      const rowYear = deriveYear(admissionDate, yearHint);
      if (rowYear) context.yearHints.add(rowYear);
      context.rowsByTable.long_term_inpatients.push({
        year: rowYear,
        seq: toInteger(seq),
        is_longterm: toText(readCell(context, sheetName, row, rowIndex, idx.isLongterm)),
        category: toText(readCell(context, sheetName, row, rowIndex, idx.category)),
        name,
        dob: sanitizeDobValue(readCell(context, sheetName, row, rowIndex, idx.dob)),
        admission_date: admissionDate,
        hospital: toText(readCell(context, sheetName, row, rowIndex, idx.hospital)),
        type: toText(readCell(context, sheetName, row, rowIndex, idx.type)),
        status: toText(readCell(context, sheetName, row, rowIndex, idx.status)),
        discharge_date: parseExcelDate(
          readCell(context, sheetName, row, rowIndex, idx.dischargeDate),
          rowYear,
          null
        ),
        confirm_date: parseExcelDate(
          readCell(context, sheetName, row, rowIndex, idx.confirmDate),
          rowYear,
          null
        ),
        source_sheet: sheetName
      });
    }
  }

  async function saveParseContext(context, mode) {
    const allTables = Object.keys(context.rowsByTable);
    const years = Array.from(context.yearHints).filter(Boolean);
    let insertedRows = 0;

    await db.transaction(
      "rw",
      db.files,
      db.parse_errors,
      ...allTables.map((t) => db[t]),
      async () => {
        if (mode === "overwrite" && years.length) {
          for (const year of years) {
            for (const table of YEAR_TABLES) {
              await db[table].where("year").equals(year).delete();
            }
          }
        }

        for (const table of allTables) {
          const rows = context.rowsByTable[table];
          if (!rows.length) continue;
          const filtered = await filterDuplicatesForMerge(table, rows, context, mode);
          if (filtered.length) {
            await db[table].bulkAdd(filtered);
            insertedRows += filtered.length;
          }
        }

        if (context.errors.length) {
          await db.parse_errors.bulkAdd(
            context.errors.map((error) => ({
              ...error,
              created_at: context.importedAt,
              file_name: context.fileName
            }))
          );
        }

        await db.files.add({
          file_name: context.fileName,
          year_hint: years.join(","),
          imported_at: context.importedAt,
          inserted_rows: insertedRows,
          error_count: context.errors.length
        });
      }
    );

    return { insertedRows };
  }

  async function filterDuplicatesForMerge(table, rows, context, mode) {
    if (mode !== "merge") return rows;
    if (table === "hospitalizations") {
      const out = [];
      for (const row of rows) {
        if (!row.admission_date || !row.name || !row.hospital) {
          out.push(row);
          continue;
        }
        const exists = await db.hospitalizations
          .where("[admission_date+name+hospital]")
          .equals([row.admission_date, row.name, row.hospital])
          .first();
        if (exists) {
          pushError(context, {
            sheet: row.source_sheet || "",
            code: "DUPLICATE_MERGE",
            message: `중복 입원건 병합 제외: ${row.admission_date}/${row.name}/${row.hospital}`
          });
          continue;
        }
        out.push(row);
      }
      return out;
    }
    if (table === "daily_activities") {
      const out = [];
      for (const row of rows) {
        if (!row.date || !row.worker) {
          out.push(row);
          continue;
        }
        const exists = await db.daily_activities.where("[date+worker]").equals([row.date, row.worker]).first();
        if (exists) continue;
        out.push(row);
      }
      return out;
    }
    return rows;
  }

  async function refreshErrorTable() {
    const rows = await db.parse_errors.orderBy("id").reverse().limit(200).toArray();
    ui.errorTableBody.innerHTML = "";
    if (!rows.length) {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td colspan="7">오류가 없습니다.</td>`;
      ui.errorTableBody.appendChild(tr);
      return;
    }
    rows.forEach((row) => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${escapeHtml(formatDateTime(row.created_at))}</td>
        <td>${escapeHtml(row.file_name || "")}</td>
        <td>${escapeHtml(row.sheet || "")}</td>
        <td>${escapeHtml(String(row.row || ""))}</td>
        <td>${escapeHtml(String(row.column || ""))}</td>
        <td>${escapeHtml(row.code || "")}</td>
        <td>${escapeHtml(row.message || "")}</td>
      `;
      ui.errorTableBody.appendChild(tr);
    });
  }

  async function refreshSummary() {
    ui.summaryCards.innerHTML = "";
    const tables = Object.keys(TABLE_META).filter((table) => table !== "backups");
    const counts = await Promise.all(tables.map((table) => db[table].count()));
    tables.forEach((table, index) => {
      const card = document.createElement("article");
      card.className = "summary-card";
      card.innerHTML = `
        <h3>${escapeHtml(TABLE_META[table])}</h3>
        <p>${formatNumber(counts[index])}</p>
      `;
      ui.summaryCards.appendChild(card);
    });
  }

  async function refreshPreviewTableOptions() {
    ui.previewTableSelect.innerHTML = "";
    const options = YEAR_TABLES.concat(["files", "parse_errors"]);
    options.forEach((table) => {
      const option = document.createElement("option");
      option.value = table;
      option.textContent = TABLE_META[table] || table;
      ui.previewTableSelect.appendChild(option);
    });
  }

  async function loadPreviewRows() {
    const table = ui.previewTableSelect.value || "daily_activities";
    const limit = Number(ui.previewLimit.value || 50);
    const rows = await db[table].orderBy("id").reverse().limit(limit).toArray();
    ui.previewTableHead.innerHTML = "";
    ui.previewTableBody.innerHTML = "";
    if (!rows.length) {
      ui.previewTableHead.innerHTML = "<tr><th>데이터 없음</th></tr>";
      ui.previewTableBody.innerHTML = "<tr><td>선택한 테이블의 데이터가 없습니다.</td></tr>";
      return;
    }
    const columns = collectColumns(rows, 20);
    const htr = document.createElement("tr");
    columns.forEach((column) => {
      const th = document.createElement("th");
      th.textContent = column;
      htr.appendChild(th);
    });
    ui.previewTableHead.appendChild(htr);
    rows.forEach((row) => {
      const tr = document.createElement("tr");
      columns.forEach((column) => {
        const td = document.createElement("td");
        td.textContent = formatCellForRole(column, row[column]);
        tr.appendChild(td);
      });
      ui.previewTableBody.appendChild(tr);
    });
  }

  async function refreshYearOptions() {
    const years = await collectAvailableYears();
    ui.reportYear.innerHTML = "";
    if (!years.length) {
      const option = document.createElement("option");
      option.value = "";
      option.textContent = "연도 없음";
      ui.reportYear.appendChild(option);
      return;
    }
    years.forEach((year) => {
      const option = document.createElement("option");
      option.value = String(year);
      option.textContent = `${year}년`;
      ui.reportYear.appendChild(option);
    });
  }

  async function collectAvailableYears() {
    const set = new Set();
    for (const table of YEAR_TABLES) {
      const keys = await db[table].orderBy("year").uniqueKeys();
      keys.forEach((key) => {
        if (key && /^\d{4}$/.test(String(key))) set.add(Number(key));
      });
    }
    return Array.from(set).sort((a, b) => a - b);
  }

  async function onGenerateReport() {
    const year = Number(ui.reportYear.value);
    const periodType = ui.periodType.value;
    const month = Number(ui.reportMonth.value);
    const quarter = Number(ui.reportQuarter.value);
    const reportType = ui.reportType.value;
    if (!year) {
      ui.reportOutput.textContent = "보고서 생성 가능한 연도 데이터가 없습니다.";
      return;
    }
    let report = null;
    if (reportType === "monthly_corporate") {
      report = await buildMonthlyCorporateReport(year, month);
    } else if (reportType === "quarterly_integrated") {
      report = await buildQuarterlyIntegratedReport(year, periodType, quarter, month);
    } else if (reportType === "admission_facility") {
      report = await buildAdmissionFacilityReport(year, periodType, quarter, month);
    } else if (reportType === "counseling_stats") {
      report = await buildCounselingReport(year, periodType, quarter, month);
    } else if (reportType === "case_management") {
      report = await buildCaseManagementReport(year, periodType, quarter, month);
    } else if (reportType === "other_management") {
      report = await buildOtherManagementReport(year, periodType, quarter, month);
    }
    state.lastReport = report;
    renderReport(report);
  }

  function onExportCsv() {
    if (!state.lastReport) {
      alert("먼저 보고서를 생성해주세요.");
      return;
    }
    const rows = state.lastReport.rowsForExport || [];
    if (!rows.length) {
      alert("내보낼 데이터가 없습니다.");
      return;
    }
    const csv = toCsv(rows);
    const fileName = `${(state.lastReport.title || "report").replace(/[\\/:*?\"<>|]/g, "_")}.csv`;
    downloadBlob(fileName, csv, "text/csv;charset=utf-8;");
  }

  function onPrintReport() {
    if (!state.lastReport) {
      alert("먼저 보고서를 생성해주세요.");
      return;
    }
    const printWindow = window.open("", "_blank", "width=1100,height=900");
    if (!printWindow) return;
    printWindow.document.write(`
      <!doctype html>
      <html lang="ko">
        <head>
          <meta charset="utf-8" />
          <title>${escapeHtml(state.lastReport.title || "보고서")}</title>
          <style>
            body { font-family: 'Noto Sans KR', 'Malgun Gothic', sans-serif; padding: 20px; color: #111; }
            h3 { margin-bottom: 4px; }
            table { width: 100%; border-collapse: collapse; margin-bottom: 16px; }
            td, th { border: 1px solid #cfd7e6; padding: 6px 8px; font-size: 12px; }
            .kpi-row { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 16px; }
            .kpi { border: 1px solid #d7dfea; border-radius: 8px; padding: 8px 10px; min-width: 150px; }
            .name { font-size: 11px; color: #555; }
            .value { font-size: 18px; font-weight: 800; }
          </style>
        </head>
        <body>${ui.reportOutput.innerHTML}</body>
      </html>
    `);
    printWindow.document.close();
    printWindow.focus();
    printWindow.print();
  }

  async function onBackupDownload() {
    const payload = await exportAllTables();
    const text = JSON.stringify(payload, null, 2);
    const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
    downloadBlob(`mh_team_backup_${stamp}.json`, text, "application/json;charset=utf-8;");
  }

  async function onClearDb() {
    const ok = window.confirm(
      "정말 DB를 초기화할까요?\n모든 파싱 데이터와 오류 이력이 삭제됩니다."
    );
    if (!ok) return;
    await db.transaction("rw", ...Object.keys(TABLE_META).map((t) => db[t]), async () => {
      for (const table of Object.keys(TABLE_META)) {
        await db[table].clear();
      }
    });
    setStatus("DB 초기화 완료", "ok");
    await refreshSummary();
    await refreshErrorTable();
    await refreshYearOptions();
    await loadPreviewRows();
  }

  async function runAutoBackupIfNeeded() {
    const today = new Date().toISOString().slice(0, 10);
    const last = localStorage.getItem("mh_auto_backup_date");
    if (last === today) return;
    const count = await db.daily_activities.count();
    if (!count) return;
    const snapshot = await exportAllTables();
    await db.backups.add({
      created_at: new Date().toISOString(),
      date_key: today,
      payload: JSON.stringify(snapshot)
    });
    const backups = await db.backups.orderBy("created_at").toArray();
    if (backups.length > 30) {
      const old = backups.slice(0, backups.length - 30);
      await db.backups.bulkDelete(old.map((b) => b.id));
    }
    localStorage.setItem("mh_auto_backup_date", today);
  }

  async function exportAllTables() {
    const payload = {
      exported_at: new Date().toISOString(),
      version: "1.0",
      data: {}
    };
    for (const table of EXPORT_TABLES) {
      payload.data[table] = await db[table].toArray();
    }
    return payload;
  }

  function aggregateDailyMetrics(rows) {
    const base = normalizeMetricDefaults({
      mental_counseling: 0,
      alcohol_counseling: 0,
      other_counseling: 0,
      counseling_total: 0,
      voluntary_admission: 0,
      emergency_admission: 0,
      protective_diagnosis: 0,
      facility_homeless: 0,
      facility_other: 0,
      supported_housing: 0,
      housing_support: 0,
      supplies_support: 0,
      action_other: 0,
      outpatient: 0,
      hospital_visit: 0,
      home_visit: 0,
      medication: 0,
      post_other: 0,
      total: 0
    });
    rows.forEach((row) => {
      Object.keys(base).forEach((key) => {
        if (key === "action_total" || key === "post_action_total") return;
        base[key] += toNumber(row[key]) || 0;
      });
    });
    return normalizeMetricDefaults(base);
  }

  function resolveMonths(periodType, quarter, month) {
    if (periodType === "monthly") return [month];
    if (periodType === "quarterly") {
      if (quarter === 1) return [1, 2, 3];
      if (quarter === 2) return [4, 5, 6];
      if (quarter === 3) return [7, 8, 9];
      if (quarter === 4) return [10, 11, 12];
    }
    return [];
  }

  function periodLabel(periodType, quarter, month) {
    if (periodType === "monthly") return `${month}월`;
    if (periodType === "quarterly") return `${quarter}분기`;
    return "연간";
  }

  function groupCount(rows, selector) {
    const out = {};
    rows.forEach((row) => {
      const key = selector(row);
      out[key] = (out[key] || 0) + 1;
    });
    return out;
  }

  function monthFilterByDate(dateText, months) {
    if (!months.length) return true;
    if (!dateText || typeof dateText !== "string") return true;
    const m = Number(dateText.slice(5, 7));
    if (!m) return true;
    return months.includes(m);
  }

  async function buildMonthlyCorporateReport(year, month) {
    const activities = await db.daily_activities
      .where("year")
      .equals(year)
      .and((row) => row.month === month)
      .toArray();
    const postAction = await db.post_action_cases
      .where("year")
      .equals(year)
      .and((row) => row.month === month)
      .toArray();
    const summary = aggregateDailyMetrics(activities);
    const post = postAction[0] || {
      id_reissue: 0,
      id_restore: 0,
      benefit: 0,
      other: 0,
      return_home: 0,
      total: 0
    };
    return {
      title: `${year}년 ${month}월 법인 월보고`,
      subtitle: "월별 활동실적 자동 집계",
      kpis: [
        { name: "정신상담", value: summary.mental_counseling },
        { name: "알코올상담", value: summary.alcohol_counseling },
        { name: "기타상담", value: summary.other_counseling },
        { name: "상담소계", value: summary.counseling_total },
        { name: "조치 합계", value: summary.action_total },
        { name: "조치후 사례관리", value: summary.post_action_total },
        { name: "기초상담 총합", value: summary.total }
      ],
      sections: [
        {
          title: "상담/조치/조치후 사례관리",
          rows: [
            ["정신상담", summary.mental_counseling],
            ["알코올상담", summary.alcohol_counseling],
            ["기타상담", summary.other_counseling],
            ["상담 소계", summary.counseling_total],
            ["자의입원", summary.voluntary_admission],
            ["응급입원", summary.emergency_admission],
            ["보호진단", summary.protective_diagnosis],
            ["시설입소(노숙)", summary.facility_homeless],
            ["시설입소(타복지)", summary.facility_other],
            ["지원주택", summary.supported_housing],
            ["주거지원", summary.housing_support],
            ["물품제공", summary.supplies_support],
            ["기타조치", summary.action_other],
            ["외래진료", summary.outpatient],
            ["병원방문", summary.hospital_visit],
            ["주거지방문", summary.home_visit],
            ["투약관리", summary.medication],
            ["기타(조치후)", summary.post_other],
            ["월 합계", summary.total]
          ]
        },
        {
          title: "조치후 사례관리(집계시트)",
          rows: [
            ["주민등록재발급", post.id_reissue || 0],
            ["주민등록복원", post.id_restore || 0],
            ["수급", post.benefit || 0],
            ["기타", post.other || 0],
            ["귀가", post.return_home || 0],
            ["합계", post.total || 0]
          ]
        }
      ],
      rowsForExport: [
        {
          year,
          month,
          counseling_total: summary.counseling_total,
          action_total: summary.action_total,
          post_action_total: summary.post_action_total,
          total: summary.total
        }
      ]
    };
  }

  async function buildQuarterlyIntegratedReport(year, periodType, quarter, month) {
    const months = resolveMonths(periodType, quarter, month);
    const activities = await db.daily_activities
      .where("year")
      .equals(year)
      .and((row) => !months.length || months.includes(row.month))
      .toArray();
    const referral = await db.referral_log
      .where("year")
      .equals(year)
      .and((row) => !months.length || months.includes(row.month))
      .toArray();
    const resources = await db.community_resources
      .where("year")
      .equals(year)
      .and((row) => monthFilterByDate(row.date, months))
      .toArray();

    const summary = aggregateDailyMetrics(activities);
    const prevActivities = await db.daily_activities
      .where("year")
      .equals(year - 1)
      .and((row) => !months.length || months.includes(row.month))
      .toArray();
    const prev = aggregateDailyMetrics(prevActivities);
    const delta = prev.total > 0 ? (((summary.total - prev.total) / prev.total) * 100).toFixed(1) : "N/A";

    return {
      title: `${year}년 ${periodLabel(periodType, quarter, month)} 통합 활동실적`,
      subtitle: "상담/조치/사례관리/연계/자원동원",
      kpis: [
        { name: "상담 합계", value: summary.counseling_total },
        { name: "조치 합계", value: summary.action_total },
        { name: "조치후 사례관리", value: summary.post_action_total },
        { name: "연계 건수", value: referral.length },
        { name: "자원동원 건수", value: resources.length },
        { name: "전년 동기 대비", value: `${delta}%` }
      ],
      sections: [
        {
          title: "기간 집계",
          rows: [
            ["상담 소계", summary.counseling_total],
            ["조치 소계", summary.action_total],
            ["조치후 사례관리 소계", summary.post_action_total],
            ["연계대장 건수", referral.length],
            ["자원동원 건수", resources.length]
          ]
        }
      ],
      rowsForExport: [
        {
          year,
          period: periodLabel(periodType, quarter, month),
          counseling_total: summary.counseling_total,
          action_total: summary.action_total,
          post_action_total: summary.post_action_total,
          referral_count: referral.length,
          resource_count: resources.length,
          yoy_delta_percent: delta
        }
      ]
    };
  }

  async function buildAdmissionFacilityReport(year, periodType, quarter, month) {
    const months = resolveMonths(periodType, quarter, month);
    const admissions = await db.hospitalizations
      .where("year")
      .equals(year)
      .and((row) => !months.length || months.includes(row.month))
      .toArray();
    const facilities = await db.facility_placements
      .where("year")
      .equals(year)
      .and((row) => !months.length || months.includes(row.month))
      .toArray();
    const longterm = await db.long_term_inpatients.where("year").equals(year).toArray();
    const current = admissions.filter((row) => toText(row.status).includes("입원중"));
    const byType = groupCount(admissions, (row) => row.type || "미분류");
    const byHospital = groupCount(current, (row) => row.hospital || "미상");
    const facilityByType = groupCount(facilities, (row) => row.facility_type || "미분류");
    const longtermIn = longterm.filter(
      (row) => toText(row.is_longterm).toUpperCase() === "O" && toText(row.status).includes("입원중")
    );
    return {
      title: `${year}년 입원·시설입소 현황`,
      subtitle: `${periodLabel(periodType, quarter, month)} 기준`,
      kpis: [
        { name: "입원 총건수", value: admissions.length },
        { name: "현재 입원중", value: current.length },
        { name: "시설입소 총건수", value: facilities.length },
        { name: "장기입원(입원중)", value: longtermIn.length }
      ],
      sections: [
        { title: "입원 유형별", rows: Object.entries(byType).map(([k, v]) => [k, v]) },
        { title: "입원 병원별", rows: Object.entries(byHospital).map(([k, v]) => [k, v]) },
        { title: "시설 유형별", rows: Object.entries(facilityByType).map(([k, v]) => [k, v]) }
      ],
      rowsForExport: [
        {
          year,
          period: periodLabel(periodType, quarter, month),
          admissions: admissions.length,
          current_admissions: current.length,
          facilities: facilities.length,
          longterm_current: longtermIn.length
        }
      ]
    };
  }

  async function buildCounselingReport(year, periodType, quarter, month) {
    const months = resolveMonths(periodType, quarter, month);
    const counseling = await db.counseling_records
      .where("year")
      .equals(year)
      .and((row) => !months.length || months.includes(row.month))
      .toArray();
    const activities = await db.daily_activities
      .where("year")
      .equals(year)
      .and((row) => !months.length || months.includes(row.month))
      .toArray();
    const byType = groupCount(counseling, (row) => row.type || "미분류");
    const byMethod = groupCount(counseling, (row) => row.method || "미분류");
    const byWorker = groupCount(counseling, (row) => row.worker || "미상");
    const byAgeGroup = groupCount(counseling, (row) => row.age_group || "미분류");
    const fallback = aggregateDailyMetrics(activities);
    return {
      title: `${year}년 상담 통계`,
      subtitle: `${periodLabel(periodType, quarter, month)} 기준`,
      kpis: [
        { name: "상담 레코드", value: counseling.length },
        { name: "상담 총합(대체 포함)", value: counseling.length || fallback.counseling_total },
        { name: "담당자 수", value: Object.keys(byWorker).length },
        { name: "상담유형 수", value: Object.keys(byType).length }
      ],
      sections: [
        { title: "상담유형별", rows: Object.entries(byType).map(([k, v]) => [k, v]) },
        { title: "상담방법별", rows: Object.entries(byMethod).map(([k, v]) => [k, v]) },
        { title: "담당자별", rows: Object.entries(byWorker).map(([k, v]) => [k, v]) },
        { title: "연령대별", rows: Object.entries(byAgeGroup).map(([k, v]) => [k, v]) }
      ],
      rowsForExport: [
        {
          year,
          period: periodLabel(periodType, quarter, month),
          counseling_records: counseling.length,
          fallback_total: fallback.counseling_total
        }
      ]
    };
  }

  async function buildCaseManagementReport(year, periodType, quarter, month) {
    const months = resolveMonths(periodType, quarter, month);
    const cases = await db.case_management.where("year").equals(year).toArray();
    const filtered = cases.filter((row) => {
      if (!months.length) return true;
      if (!row.case_start) return true;
      const m = Number(String(row.case_start).slice(5, 7));
      return months.includes(m);
    });
    const byType = groupCount(cases, (row) => row.type || "미분류");
    const byStatus = groupCount(cases, (row) => row.status || "미분류");
    const byWorker = groupCount(cases, (row) => row.worker || "미상");
    return {
      title: `${year}년 사례관리 현황`,
      subtitle: `${periodLabel(periodType, quarter, month)} 기준`,
      kpis: [
        { name: "사례 수(전체)", value: cases.length },
        { name: "사례 수(기간 필터)", value: filtered.length },
        { name: "담당자 수", value: Object.keys(byWorker).length },
        { name: "유형 수", value: Object.keys(byType).length }
      ],
      sections: [
        { title: "사례 유형별", rows: Object.entries(byType).map(([k, v]) => [k, v]) },
        { title: "상태별", rows: Object.entries(byStatus).map(([k, v]) => [k, v]) },
        { title: "담당자별", rows: Object.entries(byWorker).map(([k, v]) => [k, v]) }
      ],
      rowsForExport: [
        {
          year,
          period: periodLabel(periodType, quarter, month),
          total_cases: cases.length,
          filtered_cases: filtered.length,
          workers: Object.keys(byWorker).length
        }
      ]
    };
  }

  async function buildOtherManagementReport(year, periodType, quarter, month) {
    const months = resolveMonths(periodType, quarter, month);
    const deaths = await db.deaths
      .where("year")
      .equals(year)
      .and((row) => monthFilterByDate(row.death_date, months))
      .toArray();
    const hospitalVisits = await db.hospital_visits
      .where("year")
      .equals(year)
      .and((row) => monthFilterByDate(row.counseling_date, months))
      .toArray();
    const resources = await db.community_resources
      .where("year")
      .equals(year)
      .and((row) => monthFilterByDate(row.date, months))
      .toArray();
    const meetings = await db.meetings_education
      .where("year")
      .equals(year)
      .and((row) => monthFilterByDate(row.date, months))
      .toArray();
    const deathByCause = groupCount(deaths, (row) => row.cause || "미상");
    const resourceByOrg = groupCount(resources, (row) => row.org || "미상");
    return {
      title: `${year}년 기타 관리 보고`,
      subtitle: `${periodLabel(periodType, quarter, month)} 기준`,
      kpis: [
        { name: "사망자 건수", value: deaths.length },
        { name: "병원방문교육", value: hospitalVisits.length },
        { name: "자원동원", value: resources.length },
        { name: "회의·교육", value: meetings.length }
      ],
      sections: [
        { title: "사망원인별", rows: Object.entries(deathByCause).map(([k, v]) => [k, v]) },
        { title: "자원동원 기관별", rows: Object.entries(resourceByOrg).map(([k, v]) => [k, v]) }
      ],
      rowsForExport: [
        {
          year,
          period: periodLabel(periodType, quarter, month),
          deaths: deaths.length,
          hospital_visits: hospitalVisits.length,
          community_resources: resources.length,
          meetings_education: meetings.length
        }
      ]
    };
  }

  function renderReport(report) {
    if (!report) {
      ui.reportOutput.textContent = "보고서 데이터가 없습니다.";
      return;
    }
    const kpiHtml = (report.kpis || [])
      .map(
        (kpi) =>
          `<div class="kpi"><div class="name">${escapeHtml(kpi.name)}</div><div class="value">${escapeHtml(
            String(kpi.value)
          )}</div></div>`
      )
      .join("");
    const sectionHtml = (report.sections || [])
      .map((section) => {
        const rows = (section.rows || [])
          .map((cells) => `<tr>${cells.map((c) => `<td>${escapeHtml(String(c ?? ""))}</td>`).join("")}</tr>`)
          .join("");
        return `
          <h4>${escapeHtml(section.title || "")}</h4>
          <div class="table-wrap">
            <table><tbody>${rows || "<tr><td>데이터 없음</td></tr>"}</tbody></table>
          </div>
        `;
      })
      .join("");
    ui.reportOutput.innerHTML = `
      <h3>${escapeHtml(report.title || "")}</h3>
      <p>${escapeHtml(report.subtitle || "")}</p>
      <div class="kpi-row">${kpiHtml}</div>
      ${sectionHtml}
    `;
  }

  function collectColumns(rows, maxColumns = 20) {
    const set = new Set();
    rows.forEach((row) => {
      Object.keys(row).forEach((key) => {
        if (set.size < maxColumns) set.add(key);
      });
    });
    return Array.from(set);
  }

  function formatCellForRole(column, value) {
    if (value === null || value === undefined) return "";
    const key = String(column).toLowerCase();
    const text = String(value);
    const isName = key === "name";
    const isDob = key === "dob";
    const isDiagnosis = key.includes("diagnosis") || key === "disease";
    const isSensitiveText = key === "address" || key === "memo" || key === "content" || key === "note";
    if (state.role === ROLE.CORP_REPORTER) {
      if (isName) return maskName(text);
      if (isDob) return maskDob(text);
      if (isDiagnosis || isSensitiveText) return "비공개";
      return text;
    }
    if (state.role === ROLE.TEAM_LEAD) {
      if (isDob) return maskDob(text);
      return text;
    }
    return text;
  }

  function maskName(name) {
    const text = String(name || "");
    if (text.length <= 1) return "*";
    if (text.length === 2) return `${text[0]}*`;
    return `${text[0]}*${text.slice(-1)}`;
  }

  function maskDob(value) {
    const text = String(value || "");
    if (/^\d{6}-\d\*{6}$/.test(text)) return text;
    if (/^\d{6}-\d{7}$/.test(text)) return `${text.slice(0, 7)}******`;
    if (/^\d{13}$/.test(text)) return `${text.slice(0, 7)}******`;
    if (/^\d{6}-\d$/.test(text)) return `${text}******`;
    if (/^\d{6}$/.test(text)) return `${text.slice(0, 4)}**`;
    if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return `${text.slice(0, 4)}-**-**`;
    return "******";
  }

  function formatDateTime(iso) {
    if (!iso) return "";
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return String(iso);
    return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())} ${pad2(
      d.getHours()
    )}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}`;
  }

  function formatNumber(value) {
    const num = Number(value || 0);
    return Number.isFinite(num) ? num.toLocaleString("ko-KR") : "0";
  }

  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function toCsv(rows) {
    if (!rows.length) return "";
    const headers = Object.keys(rows[0]);
    const lines = [headers.join(",")];
    rows.forEach((row) => {
      const line = headers
        .map((h) => {
          const raw = row[h] === null || row[h] === undefined ? "" : String(row[h]);
          return `"${raw.replace(/"/g, '""')}"`;
        })
        .join(",");
      lines.push(line);
    });
    return lines.join("\n");
  }

  function downloadBlob(fileName, text, mimeType) {
    const blob = new Blob([text], { type: mimeType });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = fileName;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  }
})();
