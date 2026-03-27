const SCALE_WEBAPP_CONFIG = {
  appName: "MindMap 다시서기",
  subtitle: "노숙인 정신건강 척도 관리 웹앱",
  sourceApp: "mindmap-dashiseogi-apps-script-webapp",
  publicBaseUrl: "",
  defaultHistoryLimit: 20,
  masterHashPropertyKey: "scale_webapp_master_hash",
  settings: {
    organizationName: "다시서기종합지원센터",
    teamName: "정신건강팀",
    contactNote: ""
  }
};

const SCALE_SYNC_CONFIG = {
  propertyKeys: {
    spreadsheetId: "scale_screening_target_spreadsheet_id",
    token: "scale_screening_sync_token"
  },
  defaults: {
    spreadsheetId: ""
  },
  legacySpreadsheetIds: [
    "129w-AhjiLg2fxGqssFeC4vcQJv9hG89M4SAxXXEKrCU"
  ],
  sheetNames: {
    records: "척도검사기록",
    answers: "척도문항응답",
    questionnaires: "척도마스터",
    fields: "척도문항마스터",
    options: "척도선택지마스터",
    workerView: "실무자보기",
    riskView: "고위험군보기",
    dashboard: "척도대시보드",
    settings: "척도설정"
  },
  headers: {
    records: [
      "record_id",
      "exported_at",
      "sync_scope",
      "source_app",
      "organization_name",
      "team_name",
      "contact_note",
      "record_created_at",
      "session_date",
      "questionnaire_id",
      "questionnaire_title",
      "questionnaire_short_title",
      "score_text",
      "band_text",
      "worker_name",
      "client_label",
      "birth_date",
      "gender",
      "age_group",
      "progress_summary",
      "progress_percent",
      "progress_answered",
      "progress_total",
      "signature_present",
      "session_note",
      "highlights",
      "flags",
      "respondent_summary",
      "breakdown_summary",
      "record_json"
    ],
    answers: [
      "detail_key",
      "record_id",
      "exported_at",
      "session_date",
      "questionnaire_id",
      "questionnaire_title",
      "worker_name",
      "client_label",
      "birth_date",
      "is_subquestion",
      "parent_question_id",
      "question_id",
      "question_number",
      "question_text",
      "answer_label",
      "score",
      "raw_json"
    ],
    questionnaires: [
      "questionnaire_id",
      "self_seq",
      "title",
      "short_title",
      "recommended_age",
      "question_count",
      "respondent_field_count",
      "question_prompt",
      "intro_text",
      "source_reference_page",
      "source_institution",
      "source_citation",
      "scoring_type",
      "scoring_json",
      "extraction_notes_json",
      "questionnaire_json"
    ],
    fields: [
      "field_key",
      "questionnaire_id",
      "field_scope",
      "parent_field_id",
      "field_id",
      "field_number",
      "field_label",
      "field_text",
      "field_type",
      "is_required",
      "option_count",
      "field_json"
    ],
    options: [
      "option_key",
      "questionnaire_id",
      "field_scope",
      "parent_field_id",
      "field_id",
      "option_order",
      "option_value",
      "option_label",
      "option_score",
      "option_json"
    ]
  },
  headerLabels: {
    records: [
      "기록고유값",
      "전송시각",
      "동기화범위",
      "전송앱",
      "기관명",
      "팀명",
      "비고",
      "기록생성시각",
      "검사일",
      "척도고유값",
      "척도명",
      "척도약칭",
      "점수표시",
      "결과구간",
      "담당자",
      "대상자",
      "생년월일",
      "성별",
      "연령대",
      "응답진행률",
      "응답진행률(%)",
      "응답완료항목수",
      "전체항목수",
      "서명여부",
      "비고",
      "핵심요약",
      "주의표시",
      "응답자요약",
      "응답상세요약",
      "원본기록"
    ],
    answers: [
      "상세고유값",
      "기록고유값",
      "전송시각",
      "검사일",
      "척도고유값",
      "척도명",
      "담당자",
      "대상자",
      "생년월일",
      "하위문항여부",
      "상위문항고유값",
      "문항고유값",
      "문항번호",
      "문항내용",
      "응답내용",
      "점수",
      "원본문항"
    ],
    questionnaires: [
      "척도고유값",
      "척도순번",
      "척도명",
      "척도약칭",
      "권장연령",
      "문항수",
      "응답자항목수",
      "문항안내문",
      "소개문구",
      "출처페이지",
      "출처기관",
      "출처문헌",
      "채점유형",
      "채점설정",
      "추출메모",
      "원본척도"
    ],
    fields: [
      "문항고유키",
      "척도고유값",
      "문항영역",
      "상위문항고유값",
      "문항고유값",
      "문항번호",
      "문항라벨",
      "문항내용",
      "문항유형",
      "필수여부",
      "선택지수",
      "원본문항"
    ],
    options: [
      "선택지고유키",
      "척도고유값",
      "문항영역",
      "상위문항고유값",
      "문항고유값",
      "선택지순번",
      "선택값",
      "선택지라벨",
      "선택지점수",
      "원본선택지"
    ]
  },
  viewHeaders: {
    workerView: [
      "검사일",
      "대상자",
      "생년월일",
      "척도",
      "점수값",
      "점수표시",
      "결과구간",
      "담당자",
      "비고",
      "경고여부",
      "기록고유값"
    ],
    riskView: [
      "검사일",
      "대상자",
      "생년월일",
      "척도",
      "점수표시",
      "결과구간",
      "담당자",
      "경고내용",
      "기록고유값"
    ]
  },
  dashboard: {
    clientNameCell: "B3",
    birthDateCell: "E3",
    trendStartCell: "N9",
    helperNameCell: "T2"
  },
  settingsRows: [
    ["항목", "값", "설명"],
    ["기록 시트", "척도검사기록", "원본 검사 결과가 저장되는 시트"],
    ["문항응답 시트", "척도문항응답", "문항별 응답 원본 시트"],
    ["실무자 보기 시트", "실무자보기", "실무자가 결과를 조회하는 시트"],
    ["고위험군 보기 시트", "고위험군보기", "고위험 결과만 모아보는 시트"],
    ["대시보드 시트", "척도대시보드", "대상자 검색 및 점수 변화 그래프 시트"],
    ["척도 마스터 시트", "척도마스터", "척도 메타데이터 시트"],
    ["문항 마스터 시트", "척도문항마스터", "문항 정의 시트"],
    ["선택지 마스터 시트", "척도선택지마스터", "선택지 정의 시트"],
    ["검색 사용법", "대상자명을 입력", "척도대시보드 B3에 대상자명을 입력하면 비교표와 그래프가 갱신됩니다."],
    ["생년월일 필터", "선택 입력", "동명이인 구분이 필요할 때 척도대시보드 E3에 생년월일을 입력합니다."]
  ]
};

const SCALE_AUTH_CONFIG = {
  propertyKeys: {
    accounts: "mindmap_webapp_auth_accounts_v1",
    sessions: "mindmap_webapp_auth_sessions_v1",
    requests: "mindmap_webapp_signup_requests_v1",
    pepper: "mindmap_webapp_auth_pepper_v1"
  },
  sessionTtlHours: 24 * 7,
  hashVersion: "sha256_iter_v1",
  hashIterations: 2048,
  defaultAccounts: []
};

const SCALE_AUTH_CACHE_CONFIG = {
  keys: {
    accounts: "mindmap_webapp_auth_accounts_cache_v1",
    sessions: "mindmap_webapp_auth_sessions_cache_v1",
    requests: "mindmap_webapp_signup_requests_cache_v1",
    webAppUrl: "mindmap_webapp_service_url_cache_v1",
    statusSummary: "mindmap_webapp_status_summary_cache_v1"
  },
  ttlSeconds: {
    accounts: 300,
    sessions: 300,
    requests: 300,
    webAppUrl: 21600,
    statusSummary: 60
  }
};

let questionnaireBundleCache_ = null;
let scaleWebappAccountsCache_ = null;
let scaleWebappSessionsCache_ = null;
let scaleSignupRequestsCache_ = null;
let scaleWebAppUrlCache_ = "";

function doGet(e) {
  const pageMode = normalizeText_(e && e.parameter && (e.parameter.page || e.parameter.view)) || "login";
  if (e && e.parameter && e.parameter.ping === "1") {
    return ContentService.createTextOutput("ok");
  }

  if (e && e.parameter && e.parameter.format === "json") {
    return createJsonOutput_(getScaleWebappBootstrap(e.parameter.sessionToken || e.parameter.token || ""));
  }

  const templateName = pageMode === "signup"
    ? "Signup"
    : pageMode === "app"
      ? "Index"
      : "LoginV4";
  const template = HtmlService.createTemplateFromFile(templateName);
  template.bootstrapJson = JSON.stringify(buildTemplateBootstrap_(pageMode));

  return template.evaluate()
    .setTitle(
      pageMode === "signup"
        ? `${SCALE_WEBAPP_CONFIG.appName} | 가입 신청`
        : pageMode === "app"
          ? SCALE_WEBAPP_CONFIG.appName
          : `${SCALE_WEBAPP_CONFIG.appName} | 로그인`
    )
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL)
    .addMetaTag("viewport", "width=device-width, initial-scale=1");
}

function doPost(e) {
  const lock = LockService.getScriptLock();
  let hasLock = false;

  try {
    lock.waitLock(30000);
    hasLock = true;

    const rawPayload = parseJsonBody_(e);
    const payload = parsePayload_(rawPayload);
    validateToken_(payload.token);
    const result = upsertPayload_(payload);
    invalidateScaleStatusSummaryCache_();

    return createJsonOutput_(Object.assign({ ok: true }, result));
  } catch (error) {
    console.error("scale sync failed", error);
    return createJsonOutput_({
      ok: false,
      error: error.message
    });
  } finally {
    if (hasLock) {
      lock.releaseLock();
    }
  }
}

function include(filename) {
  return HtmlService.createHtmlOutputFromFile(filename).getContent();
}

function getDashiseogiCiDataUri() {
  return "data:image/png;base64,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";
}

function getDashiseogiCiBase64Url() {
  return getDashiseogiCiDataUri()
    .replace(/^data:image\/png;base64,/, "")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function loginScaleWebapp(username, password) {
  console.time("loginScaleWebapp");
  const normalizedUsername = normalizeText_(username);
  const normalizedPassword = normalizeText_(password);
  if (!normalizedUsername || !normalizedPassword) {
    throw new Error("아이디와 비밀번호를 입력해주세요.");
  }

  const lock = LockService.getScriptLock();
  let hasLock = false;
  try {
    lock.waitLock(30000);
    hasLock = true;

    const accounts = ensureScaleWebappAccounts_();
    const account = accounts[normalizedUsername];
    if (!account || account.active === false || !verifyScaleWebappPassword_(account, normalizedPassword)) {
      throw new Error("로그인 정보를 확인해주세요.");
    }

    const normalizedAccount = normalizeScaleWebappAccount_(account, normalizedUsername);
    const nowIso = new Date().toISOString();
    normalizedAccount.lastLoginAt = nowIso;
    normalizedAccount.updatedAt = nowIso;
    accounts[normalizedUsername] = normalizedAccount;
    saveScaleWebappAccounts_(accounts);

    const sessionContext = createScaleWebappSessionLocked_(normalizedAccount);
    return buildAuthenticatedBootstrap_(sessionContext, { includeStatus: false });
  } finally {
    if (hasLock) {
      lock.releaseLock();
    }
    console.timeEnd("loginScaleWebapp");
  }
}

function logoutScaleWebapp(sessionToken) {
  invalidateScaleWebappSession_(sessionToken);
  return buildPublicBootstrap_();
}

function submitScaleSignupRequest(request) {
  const source = request && typeof request === "object" ? request : {};
  const password = normalizeText_(source.password);
  const passwordConfirm = normalizeText_(source.passwordConfirm);
  if (!password) {
    throw new Error("비밀번호를 입력해주세요.");
  }
  if (!passwordConfirm) {
    throw new Error("비밀번호 확인을 입력해주세요.");
  }
  if (password !== passwordConfirm) {
    throw new Error("비밀번호와 비밀번호 확인이 일치하지 않습니다.");
  }

  const normalized = normalizeScaleSignupRequest_(request);
  const lock = LockService.getScriptLock();
  let hasLock = false;
  try {
    lock.waitLock(30000);
    hasLock = true;
    const accounts = ensureScaleWebappAccounts_();
    if (accounts[normalized.username]) {
      throw new Error("이미 사용 중인 아이디입니다.");
    }

    const requests = getScaleSignupRequests_();
    const pendingDuplicate = requests.some(function(item) {
      return item
        && normalizeText_(item.username) === normalized.username
        && normalizeText_(item.status) === "pending";
    });

    if (pendingDuplicate) {
      throw new Error("이미 접수된 가입신청이 있습니다.");
    }

    requests.unshift(Object.assign({}, normalized, {
      status: "pending",
      reviewedBy: "",
      reviewedAt: "",
      reviewNote: "",
      linkedUsername: ""
    }));
    saveScaleSignupRequests_(requests);

    return {
      ok: true,
      message: "가입신청이 접수되었습니다. 관리자 승인 후 로그인할 수 있습니다.",
      request: sanitizeScaleSignupRequestForClient_(normalized)
    };
  } finally {
    if (hasLock) {
      lock.releaseLock();
    }
  }
}

function getScaleWebappBootstrap(sessionToken) {
  const normalizedToken = normalizeText_(sessionToken);
  const sessionContext = getSessionContext_(normalizedToken);
  const bootstrap = buildBootstrapPayload_(sessionContext, { includeStatus: false });

  if (normalizedToken && !sessionContext) {
    bootstrap.sessionExpired = true;
    bootstrap.loginHint = "세션이 만료되었습니다. 다시 로그인해주세요.";
  }

  return bootstrap;
}

function getScaleWebappStatus(sessionToken) {
  console.time("getScaleWebappStatus");
  try {
    const sessionContext = requireScaleWebappSession_(sessionToken);
    return buildStatusPayload_(sessionContext);
  } finally {
    console.timeEnd("getScaleWebappStatus");
  }
}

function prepareScaleWebapp(sessionToken) {
  const sessionContext = requireScaleWebappSession_(sessionToken);
  ensureManagedSheets_();
  const masterSync = syncQuestionnaireMaster_();
  return Object.assign(
    {
      ok: true,
      authenticated: true,
      masterSync: masterSync
    },
    buildStatusPayload_(sessionContext)
  );
}

function saveScaleRecord(sessionToken, record) {
  const sessionContext = requireScaleWebappSession_(sessionToken);
  const normalizedRecord = normalizeRecord_(record, sessionContext);
  ensureManagedSheets_();
  const masterSync = syncQuestionnaireMaster_();

  const payload = {
    sentAt: new Date().toISOString(),
    syncScope: "apps_script_webapp",
    source: SCALE_WEBAPP_CONFIG.sourceApp,
    appSettings: {
      organizationName: SCALE_WEBAPP_CONFIG.settings.organizationName,
      teamName: SCALE_WEBAPP_CONFIG.settings.teamName,
      contactNote: SCALE_WEBAPP_CONFIG.settings.contactNote
    },
    records: [normalizedRecord]
  };

  const result = upsertPayload_(payload);
  invalidateScaleStatusSummaryCache_();

  return {
    ok: true,
    authenticated: true,
    record: normalizedRecord,
    masterSync: masterSync,
    syncResult: result,
    recentRecords: listRecentScaleRecordsInternal_(SCALE_WEBAPP_CONFIG.defaultHistoryLimit),
    status: buildStatusPayload_(sessionContext)
  };
}

function listRecentScaleRecords(sessionToken, limit) {
  requireScaleWebappSession_(sessionToken);
  return {
    ok: true,
    authenticated: true,
    records: listRecentScaleRecordsInternal_(limit)
  };
}

function listScaleWebappAccounts(sessionToken) {
  requireAdminScaleWebappSession_(sessionToken);
    return {
      ok: true,
      authenticated: true,
      accounts: listScaleWebappAccountsInternal_(),
      status: buildStatusPayload_(requireAdminScaleWebappSession_(sessionToken)),
    adminData: buildAdminDashboardPayload_()
  };
}

function saveScaleWebappAccount(sessionToken, account) {
  const sessionContext = requireAdminScaleWebappSession_(sessionToken);
  const normalized = normalizeScaleWebappAccount_(account, account && account.username);
  const originalUsername = normalizeText_(account && (account.originalUsername || account.previousUsername || account.sourceUsername || account.username));
  const providedSourceRequestId = normalizeText_(account && account.sourceRequestId);

  if (!normalized.username) {
    throw new Error("아이디를 입력해주세요.");
  }
  if (!normalized.passwordHash && !originalUsername && !providedSourceRequestId) {
    throw new Error("비밀번호를 입력해주세요.");
  }

  const lock = LockService.getScriptLock();
  let hasLock = false;
  try {
    lock.waitLock(30000);
    hasLock = true;
    const accounts = ensureScaleWebappAccounts_();
    const existing = accounts[normalized.username];
    const sourceRequestId = normalizeText_(normalized.sourceRequestId);
    const sourceRequest = sourceRequestId ? getScaleSignupRequestRecord_(sourceRequestId) : null;
    const sourcePasswordFields = sourceRequest ? extractScaleWebappPasswordFields_(sourceRequest) : createEmptyScalePasswordFields_();

    if (!originalUsername && existing) {
      throw new Error("이미 사용 중인 아이디입니다.");
    }
    if (originalUsername && originalUsername !== normalized.username && existing) {
      throw new Error("변경하려는 아이디가 이미 사용 중입니다.");
    }

    if (originalUsername && originalUsername !== normalized.username && accounts[originalUsername]) {
      delete accounts[originalUsername];
    }

    const nextAccount = Object.assign(
      {},
      existing || {},
      normalized,
      {
        username: normalized.username,
        displayName: normalized.displayName || normalized.username,
        role: normalized.role || "user",
        active: normalized.active !== false,
        passwordHash: normalized.passwordHash || normalizeText_(existing && existing.passwordHash) || sourcePasswordFields.passwordHash,
        passwordSalt: normalized.passwordSalt || normalizeText_(existing && existing.passwordSalt) || sourcePasswordFields.passwordSalt,
        passwordVersion: normalized.passwordVersion || normalizeText_(existing && existing.passwordVersion) || sourcePasswordFields.passwordVersion || SCALE_AUTH_CONFIG.hashVersion,
        passwordUpdatedAt: normalized.passwordUpdatedAt || normalizeText_(existing && existing.passwordUpdatedAt) || sourcePasswordFields.passwordUpdatedAt || new Date().toISOString(),
        createdAt: (existing && existing.createdAt) || normalized.createdAt || new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        sourceRequestId: sourceRequestId || normalizeText_(existing && existing.sourceRequestId)
      }
    );

    if (!nextAccount.passwordHash || !nextAccount.passwordSalt) {
      throw new Error("비밀번호를 입력해주세요.");
    }
    assertAdminAccountSafety_(accounts, originalUsername, existing, nextAccount);

    accounts[nextAccount.username] = normalizeScaleWebappAccount_(nextAccount, nextAccount.username);
    saveScaleWebappAccounts_(accounts);

    if (sourceRequestId) {
      markScaleSignupRequestReviewed_(sourceRequestId, "approved", sessionContext.username, "계정 등록 완료", nextAccount.username);
    }

    return {
      ok: true,
      authenticated: true,
      account: sanitizeScaleWebappAccountForClient_(accounts[nextAccount.username], nextAccount.username),
      accounts: listScaleWebappAccountsInternal_(),
      status: buildStatusPayload_(sessionContext),
      adminData: buildAdminDashboardPayload_()
    };
  } finally {
    if (hasLock) {
      lock.releaseLock();
    }
  }
}

function setScaleWebappAccountActive(sessionToken, username, active) {
  const sessionContext = requireAdminScaleWebappSession_(sessionToken);
  const normalizedUsername = normalizeText_(username);
  if (!normalizedUsername) {
    throw new Error("아이디를 입력해주세요.");
  }

  const lock = LockService.getScriptLock();
  let hasLock = false;
  try {
    lock.waitLock(30000);
    hasLock = true;
    const accounts = ensureScaleWebappAccounts_();
    const account = accounts[normalizedUsername];
    if (!account) {
      throw new Error("계정을 찾을 수 없습니다.");
    }
    if (normalizedUsername === normalizeText_(sessionContext.username) && active === false) {
      throw new Error("현재 로그인한 관리자 계정은 비활성화할 수 없습니다.");
    }
    if (normalizeText_(account.role) === "admin" && account.active !== false && active === false && countActiveAdminAccounts_(accounts) <= 1) {
      throw new Error("마지막 활성 관리자 계정은 비활성화할 수 없습니다.");
    }
    account.active = active !== false;
    account.updatedAt = new Date().toISOString();
    accounts[normalizedUsername] = normalizeScaleWebappAccount_(account, normalizedUsername);
    saveScaleWebappAccounts_(accounts);
    return {
      ok: true,
      authenticated: true,
      account: sanitizeScaleWebappAccountForClient_(accounts[normalizedUsername], normalizedUsername),
      status: buildStatusPayload_(requireAdminScaleWebappSession_(sessionToken)),
      adminData: buildAdminDashboardPayload_()
    };
  } finally {
    if (hasLock) {
      lock.releaseLock();
    }
  }
}

function deleteScaleWebappAccount(sessionToken, username) {
  const sessionContext = requireAdminScaleWebappSession_(sessionToken);
  const normalizedUsername = normalizeText_(username);
  if (!normalizedUsername) {
    throw new Error("아이디를 입력해주세요.");
  }

  const lock = LockService.getScriptLock();
  let hasLock = false;
  try {
    lock.waitLock(30000);
    hasLock = true;
    const accounts = ensureScaleWebappAccounts_();
    if (!accounts[normalizedUsername]) {
      throw new Error("계정을 찾을 수 없습니다.");
    }
    if (normalizedUsername === normalizeText_(sessionContext.username)) {
      throw new Error("현재 로그인한 계정은 삭제할 수 없습니다.");
    }
    if (normalizeText_(accounts[normalizedUsername].role) === "admin" && accounts[normalizedUsername].active !== false && countActiveAdminAccounts_(accounts) <= 1) {
      throw new Error("마지막 활성 관리자 계정은 삭제할 수 없습니다.");
    }
    delete accounts[normalizedUsername];
    saveScaleWebappAccounts_(accounts);
    return {
      ok: true,
      authenticated: true,
      deletedUsername: normalizedUsername,
      status: buildStatusPayload_(requireAdminScaleWebappSession_(sessionToken)),
      adminData: buildAdminDashboardPayload_()
    };
  } finally {
    if (hasLock) {
      lock.releaseLock();
    }
  }
}

function listScaleSignupRequests(sessionToken) {
  requireAdminScaleWebappSession_(sessionToken);
  return {
    ok: true,
    authenticated: true,
    requests: listScaleSignupRequestsInternal_(),
    status: buildStatusPayload_(requireAdminScaleWebappSession_(sessionToken)),
    adminData: buildAdminDashboardPayload_()
  };
}

function rejectScaleSignupRequest(sessionToken, requestId, reviewNote) {
  const sessionContext = requireAdminScaleWebappSession_(sessionToken);
  const normalizedRequestId = normalizeText_(requestId);
  if (!normalizedRequestId) {
    throw new Error("요청 고유값이 필요합니다.");
  }

  const lock = LockService.getScriptLock();
  let hasLock = false;
  try {
    lock.waitLock(30000);
    hasLock = true;
    const requests = getScaleSignupRequests_();
    const index = requests.findIndex(function(item) {
      return item && item.requestId === normalizedRequestId;
    });
    if (index < 0) {
      throw new Error("가입신청을 찾을 수 없습니다.");
    }
    requests[index].status = "rejected";
    requests[index].reviewedBy = sessionContext.username;
    requests[index].reviewedAt = new Date().toISOString();
    requests[index].reviewNote = normalizeText_(reviewNote) || "관리자가 반려했습니다.";
    saveScaleSignupRequests_(requests);
    return {
      ok: true,
      authenticated: true,
      request: structuredCloneSafe_(requests[index]),
      status: buildStatusPayload_(sessionContext),
      adminData: buildAdminDashboardPayload_()
    };
  } finally {
    if (hasLock) {
      lock.releaseLock();
    }
  }
}

function syncScaleQuestionnaireMaster(sessionToken) {
  const sessionContext = requireScaleWebappSession_(sessionToken);
  ensureManagedSheets_();
  const masterSync = syncQuestionnaireMaster_(true);
  invalidateScaleStatusSummaryCache_();
  return {
    ok: true,
    authenticated: true,
    masterSync: masterSync,
    status: buildStatusPayload_(sessionContext)
  };
}

function setupScaleScreeningSheets(sessionToken) {
  requireScaleWebappSession_(sessionToken);
  ensureManagedSheets_();
  const workspace = rebuildScaleAnalyticsWorkspace_();
  invalidateScaleStatusSummaryCache_();
  return {
    ok: true,
    authenticated: true,
    message: "척도검사 저장용 시트와 분석 시트를 준비했습니다.",
    targetSpreadsheetId: getTargetSpreadsheetId_(),
    workspace: workspace
  };
}

function setTargetSpreadsheetId(sessionToken, spreadsheetId) {
  requireScaleWebappSession_(sessionToken);
  const normalized = normalizeText_(spreadsheetId);
  if (!normalized) {
    throw new Error("spreadsheetId를 입력해주세요.");
  }

  PropertiesService.getScriptProperties().setProperty(
    SCALE_SYNC_CONFIG.propertyKeys.spreadsheetId,
    normalized
  );
  invalidateScaleStatusSummaryCache_();

  return {
    ok: true,
    authenticated: true,
    targetSpreadsheetId: normalized
  };
}

function clearTargetSpreadsheetId(sessionToken) {
  requireScaleWebappSession_(sessionToken);
  PropertiesService.getScriptProperties().deleteProperty(SCALE_SYNC_CONFIG.propertyKeys.spreadsheetId);
  invalidateScaleStatusSummaryCache_();
  return {
    ok: true,
    authenticated: true,
    targetSpreadsheetId: getTargetSpreadsheetId_()
  };
}

function setSyncToken(sessionToken, token) {
  requireScaleWebappSession_(sessionToken);
  const normalized = normalizeText_(token);
  if (!normalized) {
    throw new Error("token을 입력해주세요.");
  }

  PropertiesService.getScriptProperties().setProperty(
    SCALE_SYNC_CONFIG.propertyKeys.token,
    normalized
  );
  invalidateScaleStatusSummaryCache_();

  return {
    ok: true,
    authenticated: true,
    tokenConfigured: true
  };
}

function clearSyncToken(sessionToken) {
  requireScaleWebappSession_(sessionToken);
  PropertiesService.getScriptProperties().deleteProperty(SCALE_SYNC_CONFIG.propertyKeys.token);
  invalidateScaleStatusSummaryCache_();
  return {
    ok: true,
    authenticated: true,
    tokenConfigured: false
  };
}

function getScaleSyncStatus(sessionToken) {
  const sessionContext = requireScaleWebappSession_(sessionToken);
  return Object.assign({
    ok: true,
    authenticated: true
  }, buildStatusPayload_(sessionContext));
}

function buildTemplateBootstrap_(pageMode) {
  const baseUrl = getScaleWebAppUrl_();
  return Object.assign(
    {},
    buildPublicBootstrap_(),
    {
      pageMode: normalizeText_(pageMode) || "login",
      webAppUrl: baseUrl,
      loginUrl: buildScaleWebAppPageUrl_(baseUrl, "login"),
      signupUrl: buildScaleWebAppPageUrl_(baseUrl, "signup"),
      appUrl: buildScaleWebAppPageUrl_(baseUrl, "app")
    }
  );
}

function buildPublicBootstrap_() {
  const baseUrl = getScaleWebAppUrl_();
  const hasAccounts = hasScaleWebappAccounts_();
  return {
    ok: true,
    authenticated: false,
    sessionToken: "",
    currentUser: {
      email: "",
      domain: "",
      displayName: ""
    },
    appName: SCALE_WEBAPP_CONFIG.appName,
    subtitle: SCALE_WEBAPP_CONFIG.subtitle,
    settings: structuredCloneSafe_(SCALE_WEBAPP_CONFIG.settings),
    webAppUrl: baseUrl,
    loginUrl: buildScaleWebAppPageUrl_(baseUrl, "login"),
    signupUrl: buildScaleWebAppPageUrl_(baseUrl, "signup"),
    appUrl: buildScaleWebAppPageUrl_(baseUrl, "app"),
    loginHint: hasAccounts
      ? "가입한 계정으로 로그인해주세요. 계정이 없으면 가입신청을 접수하세요."
      : "관리자 초기 계정 설정이 필요합니다. 스크립트 관리자에게 문의하세요.",
    signupEnabled: hasAccounts,
    signupHint: hasAccounts
      ? "가입신청 후 관리자 승인 뒤 로그인할 수 있습니다."
      : "초기 관리자 계정이 설정된 뒤 가입신청을 사용할 수 있습니다.",
    status: null
  };
}

function buildAuthenticatedBootstrap_(sessionContext, options) {
  const baseUrl = getScaleWebAppUrl_();
  const includeStatus = !(options && options.includeStatus === false);
  return {
    ok: true,
    authenticated: true,
    sessionToken: sessionContext ? sessionContext.token : "",
    currentUser: getCurrentUserContext_(sessionContext),
    appName: SCALE_WEBAPP_CONFIG.appName,
    subtitle: SCALE_WEBAPP_CONFIG.subtitle,
    settings: structuredCloneSafe_(SCALE_WEBAPP_CONFIG.settings),
    webAppUrl: baseUrl,
    loginUrl: buildScaleWebAppPageUrl_(baseUrl, "login"),
    signupUrl: buildScaleWebAppPageUrl_(baseUrl, "signup"),
    appUrl: buildScaleWebAppPageUrl_(baseUrl, "app"),
    loginHint: "로그인 상태가 유지됩니다.",
    status: includeStatus ? buildStatusPayload_(sessionContext) : null
  };
}

function buildBootstrapPayload_(sessionContext, options) {
  return sessionContext ? buildAuthenticatedBootstrap_(sessionContext, options) : buildPublicBootstrap_();
}

function buildStatusPayload_(sessionContext) {
  const currentUser = getCurrentUserContext_(sessionContext);
  const summary = getScaleStatusSummary_();
  const payload = {
    ok: true,
    authenticated: true,
    currentUser: currentUser,
    targetSpreadsheetId: summary.targetSpreadsheetId,
    targetSpreadsheetName: summary.targetSpreadsheetName,
    tokenConfigured: summary.tokenConfigured,
    sheets: summary.sheets,
    recentRecords: listRecentScaleRecordsInternal_(SCALE_WEBAPP_CONFIG.defaultHistoryLimit)
  };

  if (normalizeText_(currentUser.role) === "admin") {
    payload.adminData = buildAdminDashboardPayload_();
  }

  return payload;
}

function buildAdminDashboardPayload_() {
  const accounts = listScaleWebappAccountsInternal_();
  const requests = listScaleSignupRequestsInternal_();
  return {
    accounts: accounts,
    requests: requests,
    accountStats: {
      total: accounts.length,
      active: accounts.filter(function(account) { return account.active !== false; }).length,
      admins: accounts.filter(function(account) { return normalizeText_(account.role) === "admin" && account.active !== false; }).length
    },
    requestStats: {
      total: requests.length,
      pending: requests.filter(function(request) { return normalizeText_(request.status) === "pending"; }).length,
      approved: requests.filter(function(request) { return normalizeText_(request.status) === "approved"; }).length,
      rejected: requests.filter(function(request) { return normalizeText_(request.status) === "rejected"; }).length
    }
  };
}

function getCurrentUserContext_(sessionContext) {
  const context = sessionContext && typeof sessionContext === "object" ? sessionContext : null;
  return {
    email: "",
    domain: "",
    displayName: context && context.displayName ? context.displayName : "",
    username: context && context.username ? context.username : "",
    role: context && context.role ? context.role : ""
  };
}

function getScaleWebAppUrl_() {
  const configuredUrl = normalizeText_(SCALE_WEBAPP_CONFIG.publicBaseUrl);
  if (configuredUrl) {
    scaleWebAppUrlCache_ = configuredUrl;
    return configuredUrl;
  }

  if (scaleWebAppUrlCache_) {
    return scaleWebAppUrlCache_;
  }

  const cacheKey = SCALE_AUTH_CACHE_CONFIG.keys.webAppUrl;
  const cached = getScriptCacheText_(cacheKey);
  if (cached) {
    scaleWebAppUrlCache_ = cached;
    return cached;
  }

  try {
    const serviceUrl = normalizeText_(ScriptApp.getService().getUrl());
    scaleWebAppUrlCache_ = serviceUrl;
    if (serviceUrl) {
      putScriptCacheText_(cacheKey, serviceUrl, SCALE_AUTH_CACHE_CONFIG.ttlSeconds.webAppUrl);
    }
    return serviceUrl;
  } catch (error) {
    return "";
  }
}

function buildScaleWebAppPageUrl_(baseUrl, pageMode) {
  const normalizedBase = normalizeText_(baseUrl);
  if (!normalizedBase) {
    return "";
  }

  const url = normalizedBase;
  const page = normalizeText_(pageMode);
  if (!page || page === "login") {
    return url;
  }

  return url + (url.indexOf("?") >= 0 ? "&" : "?") + "page=" + encodeURIComponent(page);
}

function normalizeRecord_(record, sessionContext) {
  if (!record || typeof record !== "object") {
    throw new Error("저장할 검사 결과가 없습니다.");
  }

  const bundle = getQuestionnaireBundle_();
  const questionnaire = bundle.questionnaires[record.questionnaireId];
  if (!questionnaire) {
    throw new Error("척도 정보를 찾을 수 없습니다.");
  }

  const currentUser = getCurrentUserContext_(sessionContext);
  const respondent = record.respondent && typeof record.respondent === "object"
    ? structuredCloneSafe_(record.respondent)
    : {};
  const answers = record.answers && typeof record.answers === "object"
    ? structuredCloneSafe_(record.answers)
    : {};

  const meta = Object.assign(
    {
      sessionDate: "",
      workerName: "",
      clientLabel: "",
      birthDate: "",
      sessionNote: ""
    },
    record.meta || {}
  );

  if (!normalizeText_(meta.workerName) && currentUser.displayName) {
    meta.workerName = currentUser.displayName;
  }

  const normalized = {
    id: normalizeText_(record.id) || Utilities.getUuid(),
    questionnaireId: questionnaire.id,
    questionnaireTitle: questionnaire.title,
    shortTitle: questionnaire.shortTitle || questionnaire.title,
    createdAt: normalizeText_(record.createdAt) || new Date().toISOString(),
    meta: {
      sessionDate: normalizeText_(meta.sessionDate),
      workerName: normalizeText_(meta.workerName),
      clientLabel: normalizeText_(meta.clientLabel),
      birthDate: normalizeText_(meta.birthDate),
      sessionNote: normalizeText_(meta.sessionNote)
    },
    respondent: respondent,
    respondentDisplay: Array.isArray(record.respondentDisplay) && record.respondentDisplay.length
      ? structuredCloneSafe_(record.respondentDisplay)
      : buildRespondentDisplay_(questionnaire, respondent),
    answers: answers,
    breakdown: Array.isArray(record.breakdown) && record.breakdown.length
      ? structuredCloneSafe_(record.breakdown)
      : buildAnswerBreakdown_(questionnaire, answers),
    progress: normalizeProgress_(record.progress),
    evaluation: structuredCloneSafe_(record.evaluation || {}),
    bundleId: normalizeText_(record.bundleId),
    owner: currentUser
  };

  if (!normalized.meta.sessionDate) {
    throw new Error("검사일이 없습니다.");
  }

  return normalized;
}

function normalizeProgress_(progress) {
  const value = progress && typeof progress === "object" ? progress : {};
  return {
    answered: toNumberOrZero_(value.answered),
    total: toNumberOrZero_(value.total),
    percent: toNumberOrZero_(value.percent),
    respondentAnswered: toNumberOrZero_(value.respondentAnswered),
    respondentTotal: toNumberOrZero_(value.respondentTotal),
    questionAnswered: toNumberOrZero_(value.questionAnswered),
    questionTotal: toNumberOrZero_(value.questionTotal)
  };
}

function toNumberOrZero_(value) {
  return Number.isFinite(Number(value)) ? Number(value) : 0;
}

function buildRespondentDisplay_(questionnaire, respondent) {
  return (questionnaire.respondentFields || []).map(function(field) {
    return {
      label: field.label,
      value: optionLabel_(field.options || [], respondent[field.id]) || "미응답"
    };
  });
}

function buildAnswerBreakdown_(questionnaire, responses) {
  return (questionnaire.questions || []).map(function(question) {
    const answer = responses[question.id];
    return {
      id: question.id,
      number: question.number,
      text: question.text,
      answerLabel: optionLabel_(question.options || [], answer && answer.value) || "미응답",
      score: answer && answer.score !== undefined ? answer.score : null,
      subAnswers: (question.subQuestions || []).map(function(subQuestion) {
        const subAnswer = responses[subQuestion.id];
        if (!subAnswer) {
          return null;
        }
        return {
          number: subQuestion.number,
          text: subQuestion.label || subQuestion.text || "",
          answerLabel: optionLabel_(subQuestion.options || [], subAnswer.value) || "미응답",
          score: subAnswer.score
        };
      }).filter(Boolean)
    };
  });
}

function optionLabel_(options, value) {
  const matched = (options || []).filter(function(option) {
    return String(option.value) === String(value);
  })[0];

  return matched ? matched.label : "";
}

function syncQuestionnaireMaster_(force) {
  const rawBundle = getQuestionnaireBundleRaw_();
  const digest = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, rawBundle);
  const nextHash = Utilities.base64EncodeWebSafe(digest);
  const properties = PropertiesService.getScriptProperties();
  const currentHash = normalizeText_(properties.getProperty(SCALE_WEBAPP_CONFIG.masterHashPropertyKey));

  if (!force && currentHash && currentHash === nextHash) {
    return {
      ok: true,
      skipped: true,
      hash: nextHash
    };
  }

  const bundle = getQuestionnaireBundle_();
  const result = upsertQuestionnaires_({
    questionnaires: Object.keys(bundle.questionnaires || {}).map(function(key) {
      return bundle.questionnaires[key];
    })
  });

  properties.setProperty(SCALE_WEBAPP_CONFIG.masterHashPropertyKey, nextHash);

  return Object.assign(
    {
      ok: true,
      skipped: false,
      hash: nextHash
    },
    result
  );
}

function listRecentScaleRecordsInternal_(limit) {
  const safeLimit = Math.max(1, Math.min(Number(limit) || SCALE_WEBAPP_CONFIG.defaultHistoryLimit, 100));
  const sheet = getOrCreateSheet_(SCALE_SYNC_CONFIG.sheetNames.records);
  const lastRow = sheet.getLastRow();

  if (lastRow <= 1) {
    return [];
  }

  const availableRows = lastRow - 1;
  const takeRows = Math.min(availableRows, Math.max(safeLimit * 4, safeLimit));
  const startRow = lastRow - takeRows + 1;
  const values = sheet.getRange(
    startRow,
    1,
    takeRows,
    Math.max(sheet.getLastColumn(), SCALE_SYNC_CONFIG.headers.records.length)
  ).getDisplayValues();

  const headerIndex = buildHeaderIndexMap_(SCALE_SYNC_CONFIG.headers.records);
  const records = values
    .map(function(row) {
      return buildRecentRecordSummaryFromRow_(row, headerIndex);
    })
    .filter(Boolean);

  return records
    .sort(function(left, right) {
      return new Date(right.createdAt || 0).getTime() - new Date(left.createdAt || 0).getTime();
    })
    .slice(0, safeLimit);
}

function getQuestionnaireBundle_() {
  if (questionnaireBundleCache_) {
    return questionnaireBundleCache_;
  }

  const raw = getQuestionnaireBundleRaw_();
  const jsonText = extractQuestionnaireBundleJson_(raw);

  questionnaireBundleCache_ = JSON.parse(jsonText);
  return questionnaireBundleCache_;
}

function extractQuestionnaireBundleJson_(raw) {
  const text = normalizeText_(raw);
  if (!text) {
    throw new Error("척도 번들이 비어 있습니다.");
  }

  const scriptMatch = text.match(/<script[^>]*>([\s\S]*?)<\/script>/i);
  const body = scriptMatch ? scriptMatch[1] : text;
  const assignmentMatch = body.match(/window\.__SCALE_SCREENING_BUNDLE__\s*=\s*([\s\S]*?);?\s*$/i);
  const candidate = assignmentMatch ? assignmentMatch[1] : body;
  const trimmed = candidate
    .replace(/^\s*window\.__SCALE_SCREENING_BUNDLE__\s*=\s*/i, "")
    .replace(/^\s*<script[^>]*>\s*/i, "")
    .replace(/\s*<\/script>\s*$/i, "")
    .replace(/;\s*$/,"")
    .trim();

  const objectMatch = trimmed.match(/\{[\s\S]*\}$/);
  if (objectMatch) {
    return objectMatch[0];
  }

  return trimmed;
}

function getQuestionnaireBundleRaw_() {
  return HtmlService.createHtmlOutputFromFile("QuestionnaireBundle")
    .getContent()
    .replace(/^\uFEFF/, "")
    .trim();
}

function authenticateScaleWebappUser_(username, password) {
  const normalizedUsername = normalizeText_(username);
  const normalizedPassword = normalizeText_(password);

  if (!normalizedUsername || !normalizedPassword) {
    throw new Error("아이디와 비밀번호를 입력해주세요.");
  }

  const accounts = ensureScaleWebappAccounts_();
  const account = accounts[normalizedUsername];

  if (!account || account.active === false || !verifyScaleWebappPassword_(account, normalizedPassword)) {
    throw new Error("로그인 정보를 확인해주세요.");
  }

  return normalizeScaleWebappAccount_(account, normalizedUsername);
}

function ensureScaleWebappAccounts_() {
  if (scaleWebappAccountsCache_) {
    return scaleWebappAccountsCache_;
  }

  const properties = PropertiesService.getScriptProperties();
  const cacheKey = SCALE_AUTH_CACHE_CONFIG.keys.accounts;
  const cached = getScriptCacheJson_(cacheKey);
  const raw = cached
    ? JSON.stringify(cached)
    : normalizeText_(properties.getProperty(SCALE_AUTH_CONFIG.propertyKeys.accounts));
  let accounts = {};

  if (cached && typeof cached === "object") {
    accounts = cached;
  } else if (raw) {
    try {
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === "object") {
        accounts = parsed;
      }
    } catch (error) {
      accounts = {};
    }
  }

  let changed = false;
  SCALE_AUTH_CONFIG.defaultAccounts.forEach(function(seed) {
    const key = normalizeText_(seed.username);
    if (!key) {
      return;
    }

    const normalized = normalizeScaleWebappAccount_(accounts[key] || seed, key);
    if (JSON.stringify(accounts[key]) !== JSON.stringify(normalized)) {
      accounts[key] = normalized;
      changed = true;
    }
  });

  if (!raw || changed) {
    properties.setProperty(SCALE_AUTH_CONFIG.propertyKeys.accounts, JSON.stringify(accounts));
  }

  scaleWebappAccountsCache_ = accounts;
  putScriptCacheJson_(cacheKey, accounts, SCALE_AUTH_CACHE_CONFIG.ttlSeconds.accounts);
  return scaleWebappAccountsCache_;
}

function normalizeScaleWebappAccount_(account, fallbackUsername) {
  const source = account && typeof account === "object" ? account : {};
  const username = normalizeText_(source.username || fallbackUsername);
  const role = normalizeText_(source.role) === "admin" ? "admin" : "user";
  const password = normalizeText_(source.password);
  const passwordFields = password
    ? buildScaleWebappPasswordFields_(password)
    : extractScaleWebappPasswordFields_(source);

  return {
    username: username,
    password: "",
    passwordHash: passwordFields.passwordHash,
    passwordSalt: passwordFields.passwordSalt,
    passwordVersion: passwordFields.passwordVersion,
    passwordUpdatedAt: passwordFields.passwordUpdatedAt,
    displayName: normalizeText_(source.displayName) || username,
    role: role,
    active: source.active !== false,
    contact: normalizeText_(source.contact),
    affiliation: normalizeText_(source.affiliation),
    notes: normalizeText_(source.notes),
    sourceRequestId: normalizeText_(source.sourceRequestId),
    lastLoginAt: normalizeText_(source.lastLoginAt),
    createdAt: normalizeText_(source.createdAt) || new Date().toISOString(),
    updatedAt: normalizeText_(source.updatedAt) || new Date().toISOString()
  };
}

function touchScaleWebappAccountLogin_(username) {
  const normalizedUsername = normalizeText_(username);
  if (!normalizedUsername) {
    return;
  }

  const lock = LockService.getScriptLock();
  let hasLock = false;
  try {
    lock.waitLock(30000);
    hasLock = true;
    const accounts = ensureScaleWebappAccounts_();
    const account = accounts[normalizedUsername];
    if (!account) {
      return;
    }
    account.lastLoginAt = new Date().toISOString();
    account.updatedAt = new Date().toISOString();
    accounts[normalizedUsername] = normalizeScaleWebappAccount_(account, normalizedUsername);
    saveScaleWebappAccounts_(accounts);
  } finally {
    if (hasLock) {
      lock.releaseLock();
    }
  }
}

function requireAdminScaleWebappSession_(sessionToken) {
  const sessionContext = requireScaleWebappSession_(sessionToken);
  if (normalizeText_(sessionContext.role) !== "admin") {
    throw new Error("관리자 권한이 필요합니다.");
  }
  return sessionContext;
}

function getScaleSignupRequests_() {
  if (scaleSignupRequestsCache_) {
    return scaleSignupRequestsCache_;
  }

  const cacheKey = SCALE_AUTH_CACHE_CONFIG.keys.requests;
  const cached = getScriptCacheJson_(cacheKey);
  if (Array.isArray(cached)) {
    scaleSignupRequestsCache_ = cached;
    return scaleSignupRequestsCache_;
  }

  const properties = PropertiesService.getScriptProperties();
  const raw = normalizeText_(properties.getProperty(SCALE_AUTH_CONFIG.propertyKeys.requests));
  if (!raw) {
    return [];
  }

  try {
    const parsed = JSON.parse(raw);
    scaleSignupRequestsCache_ = Array.isArray(parsed) ? parsed : [];
    putScriptCacheJson_(cacheKey, scaleSignupRequestsCache_, SCALE_AUTH_CACHE_CONFIG.ttlSeconds.requests);
    return scaleSignupRequestsCache_;
  } catch (error) {
    return [];
  }
}

function saveScaleSignupRequests_(requests) {
  const cleaned = (requests || [])
    .map(function(request) {
      return normalizeScaleSignupRequest_(request);
    })
    .filter(Boolean)
    .sort(function(left, right) {
      return new Date(right.createdAt || 0).getTime() - new Date(left.createdAt || 0).getTime();
    });

  PropertiesService.getScriptProperties().setProperty(
    SCALE_AUTH_CONFIG.propertyKeys.requests,
    JSON.stringify(cleaned)
  );

  scaleSignupRequestsCache_ = cleaned;
  putScriptCacheJson_(SCALE_AUTH_CACHE_CONFIG.keys.requests, cleaned, SCALE_AUTH_CACHE_CONFIG.ttlSeconds.requests);
  return cleaned;
}

function listScaleSignupRequestsInternal_() {
  return getScaleSignupRequests_()
    .map(function(request) {
      return sanitizeScaleSignupRequestForClient_(request);
    })
    .sort(function(left, right) {
      return new Date(right.createdAt || 0).getTime() - new Date(left.createdAt || 0).getTime();
    });
}

function normalizeScaleSignupRequest_(request) {
  const source = request && typeof request === "object" ? request : {};
  const requestId = normalizeText_(source.requestId) || Utilities.getUuid();
  const createdAt = normalizeText_(source.createdAt) || new Date().toISOString();
  const rawStatus = normalizeText_(source.status);
  const status = ["pending", "approved", "rejected"].indexOf(rawStatus) >= 0 ? rawStatus : "pending";
  const displayName = normalizeText_(source.displayName || source.name);
  const username = normalizeText_(source.username);

  if (!displayName) {
    throw new Error("이름을 입력해주세요.");
  }
  if (!username) {
    throw new Error("아이디를 입력해주세요.");
  }
  const password = normalizeText_(source.password);
  const passwordFields = password
    ? buildScaleWebappPasswordFields_(password)
    : extractScaleWebappPasswordFields_(source);

  return {
    requestId: requestId,
    createdAt: createdAt,
    updatedAt: normalizeText_(source.updatedAt) || createdAt,
    status: status,
    displayName: displayName,
    username: username,
    contact: "",
    affiliation: "다시서기",
    reason: "",
    note: normalizeText_(source.note),
    passwordHash: passwordFields.passwordHash,
    passwordSalt: passwordFields.passwordSalt,
    passwordVersion: passwordFields.passwordVersion,
    passwordUpdatedAt: passwordFields.passwordUpdatedAt,
    reviewedBy: normalizeText_(source.reviewedBy),
    reviewedAt: normalizeText_(source.reviewedAt),
    reviewNote: normalizeText_(source.reviewNote),
    linkedUsername: normalizeText_(source.linkedUsername)
  };
}

function markScaleSignupRequestReviewed_(requestId, status, reviewedBy, reviewNote, linkedUsername) {
  const normalizedRequestId = normalizeText_(requestId);
  if (!normalizedRequestId) {
    return null;
  }

  const requests = getScaleSignupRequests_();
  const index = requests.findIndex(function(item) {
    return item && item.requestId === normalizedRequestId;
  });

  if (index < 0) {
    return null;
  }

  requests[index].status = normalizeText_(status) || "approved";
  requests[index].reviewedBy = normalizeText_(reviewedBy);
  requests[index].reviewedAt = new Date().toISOString();
  requests[index].reviewNote = normalizeText_(reviewNote);
  requests[index].linkedUsername = normalizeText_(linkedUsername) || normalizeText_(requests[index].linkedUsername);
  requests[index].passwordHash = "";
  requests[index].passwordSalt = "";
  requests[index].passwordVersion = "";
  requests[index].passwordUpdatedAt = "";
  requests[index].updatedAt = new Date().toISOString();
  saveScaleSignupRequests_(requests);
  return sanitizeScaleSignupRequestForClient_(requests[index]);
}

function saveScaleWebappAccounts_(accounts) {
  const cleaned = {};
  Object.keys(accounts || {}).forEach(function(username) {
    const normalized = normalizeScaleWebappAccount_(accounts[username], username);
    if (normalized.username) {
      cleaned[normalized.username] = normalized;
    }
  });

  PropertiesService.getScriptProperties().setProperty(
    SCALE_AUTH_CONFIG.propertyKeys.accounts,
    JSON.stringify(cleaned)
  );

  scaleWebappAccountsCache_ = cleaned;
  putScriptCacheJson_(SCALE_AUTH_CACHE_CONFIG.keys.accounts, cleaned, SCALE_AUTH_CACHE_CONFIG.ttlSeconds.accounts);
  return cleaned;
}

function listScaleWebappAccountsInternal_() {
  const accounts = ensureScaleWebappAccounts_();
  return Object.keys(accounts)
    .sort()
    .map(function(username) {
      return sanitizeScaleWebappAccountForClient_(accounts[username], username);
    });
}

function sanitizeScaleSignupRequestForClient_(request) {
  const normalized = normalizeScaleSignupRequest_(request);
  return {
    requestId: normalized.requestId,
    createdAt: normalized.createdAt,
    updatedAt: normalized.updatedAt,
    status: normalized.status,
    displayName: normalized.displayName,
    username: normalized.username,
    contact: normalized.contact,
    affiliation: normalized.affiliation,
    reason: normalized.reason,
    note: normalized.note,
    reviewedBy: normalized.reviewedBy,
    reviewedAt: normalized.reviewedAt,
    reviewNote: normalized.reviewNote,
    linkedUsername: normalized.linkedUsername
  };
}

function createEmptyScalePasswordFields_() {
  return {
    passwordHash: "",
    passwordSalt: "",
    passwordVersion: "",
    passwordUpdatedAt: ""
  };
}

function extractScaleWebappPasswordFields_(source) {
  return {
    passwordHash: normalizeText_(source && source.passwordHash),
    passwordSalt: normalizeText_(source && source.passwordSalt),
    passwordVersion: normalizeText_(source && source.passwordVersion),
    passwordUpdatedAt: normalizeText_(source && source.passwordUpdatedAt)
  };
}

function buildScaleWebappPasswordFields_(password) {
  const normalizedPassword = normalizeText_(password);
  if (!normalizedPassword) {
    return createEmptyScalePasswordFields_();
  }

  const salt = Utilities.getUuid().replace(/-/g, "");
  return {
    passwordHash: hashScaleWebappPassword_(normalizedPassword, salt),
    passwordSalt: salt,
    passwordVersion: SCALE_AUTH_CONFIG.hashVersion,
    passwordUpdatedAt: new Date().toISOString()
  };
}

function hashScaleWebappPassword_(password, salt) {
  const normalizedPassword = normalizeText_(password);
  const normalizedSalt = normalizeText_(salt);
  if (!normalizedPassword || !normalizedSalt) {
    return "";
  }

  let token = normalizedSalt + "::" + normalizedPassword + "::" + getScaleAuthPepper_();
  for (let index = 0; index < SCALE_AUTH_CONFIG.hashIterations; index += 1) {
    const digest = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, token, Utilities.Charset.UTF_8);
    token = Utilities.base64EncodeWebSafe(digest);
  }
  return token;
}

function generateScaleAuthPepper_() {
  const seed = [
    Utilities.getUuid(),
    Utilities.getUuid(),
    Utilities.getUuid(),
    Utilities.getUuid()
  ].join("::");
  const digest = Utilities.computeDigest(
    Utilities.DigestAlgorithm.SHA_256,
    seed,
    Utilities.Charset.UTF_8
  );
  return Utilities.base64EncodeWebSafe(digest);
}

function getScaleAuthPepper_() {
  const properties = PropertiesService.getScriptProperties();
  const propertyKey = SCALE_AUTH_CONFIG.propertyKeys.pepper;
  let pepper = normalizeText_(properties.getProperty(propertyKey));
  if (pepper) {
    return pepper;
  }

  const lock = LockService.getScriptLock();
  lock.waitLock(30 * 1000);
  try {
    pepper = normalizeText_(properties.getProperty(propertyKey));
    if (pepper) {
      return pepper;
    }

    pepper = generateScaleAuthPepper_();
    properties.setProperty(propertyKey, pepper);
    return pepper;
  } finally {
    lock.releaseLock();
  }
}

function verifyScaleWebappPassword_(account, password) {
  const normalizedAccount = normalizeScaleWebappAccount_(account, account && account.username);
  const normalizedPassword = normalizeText_(password);
  if (!normalizedAccount.passwordHash || !normalizedAccount.passwordSalt || !normalizedPassword) {
    return false;
  }
  return normalizedAccount.passwordHash === hashScaleWebappPassword_(normalizedPassword, normalizedAccount.passwordSalt);
}

function hasScaleWebappAccounts_() {
  return Object.keys(ensureScaleWebappAccounts_()).length > 0;
}

function getScaleSignupRequestRecord_(requestId) {
  const normalizedRequestId = normalizeText_(requestId);
  if (!normalizedRequestId) {
    return null;
  }

  const requests = getScaleSignupRequests_();
  const matched = requests.filter(function(item) {
    return item && item.requestId === normalizedRequestId;
  })[0];

  return matched ? normalizeScaleSignupRequest_(matched) : null;
}

function countActiveAdminAccounts_(accounts) {
  return Object.keys(accounts || {}).reduce(function(total, username) {
    const account = normalizeScaleWebappAccount_(accounts[username], username);
    if (account.username && account.active !== false && account.role === "admin") {
      return total + 1;
    }
    return total;
  }, 0);
}

function assertAdminAccountSafety_(accounts, originalUsername, existingAccount, nextAccount) {
  const normalizedExisting = existingAccount ? normalizeScaleWebappAccount_(existingAccount, originalUsername || existingAccount.username) : null;
  const normalizedNext = normalizeScaleWebappAccount_(nextAccount, nextAccount && nextAccount.username);
  const activeAdmins = countActiveAdminAccounts_(accounts);

  if (activeAdmins === 0 && !(normalizedNext.role === "admin" && normalizedNext.active !== false)) {
    throw new Error("초기 계정은 관리자 권한으로 등록해야 합니다.");
  }

  if (!normalizedExisting) {
    return;
  }

  const wasActiveAdmin = normalizedExisting.role === "admin" && normalizedExisting.active !== false;
  const willBeActiveAdmin = normalizedNext.role === "admin" && normalizedNext.active !== false;
  if (wasActiveAdmin && !willBeActiveAdmin && activeAdmins <= 1) {
    throw new Error("마지막 활성 관리자 계정은 일반 계정으로 변경하거나 비활성화할 수 없습니다.");
  }
}

function setupInitialScaleWebappAdmin(username, password, displayName) {
  const normalizedUsername = normalizeText_(username);
  const normalizedPassword = normalizeText_(password);
  const normalizedDisplayName = normalizeText_(displayName) || normalizedUsername;

  if (!normalizedUsername || !normalizedPassword) {
    throw new Error("관리자 아이디와 비밀번호를 모두 입력해주세요.");
  }

  const lock = LockService.getScriptLock();
  lock.waitLock(30000);
  try {
    const accounts = ensureScaleWebappAccounts_();
    accounts[normalizedUsername] = normalizeScaleWebappAccount_({
      username: normalizedUsername,
      password: normalizedPassword,
      displayName: normalizedDisplayName,
      role: "admin",
      active: true,
      affiliation: "다시서기",
      notes: "초기 관리자 계정"
    }, normalizedUsername);
    saveScaleWebappAccounts_(accounts);
    return sanitizeScaleWebappAccountForClient_(accounts[normalizedUsername], normalizedUsername);
  } finally {
    lock.releaseLock();
  }
}

function sanitizeScaleWebappAccountForClient_(account, fallbackUsername) {
  const normalized = normalizeScaleWebappAccount_(account, fallbackUsername);
  return {
    username: normalized.username,
    displayName: normalized.displayName,
    role: normalized.role,
    active: normalized.active,
    contact: normalized.contact,
    affiliation: normalized.affiliation,
    notes: normalized.notes,
    sourceRequestId: normalized.sourceRequestId,
    lastLoginAt: normalized.lastLoginAt,
    createdAt: normalized.createdAt,
    updatedAt: normalized.updatedAt
  };
}

function getScaleWebappSessions_() {
  if (scaleWebappSessionsCache_) {
    return scaleWebappSessionsCache_;
  }

  const cacheKey = SCALE_AUTH_CACHE_CONFIG.keys.sessions;
  const cached = getScriptCacheJson_(cacheKey);
  if (cached && typeof cached === "object") {
    scaleWebappSessionsCache_ = cached;
    return scaleWebappSessionsCache_;
  }

  const properties = PropertiesService.getScriptProperties();
  const raw = normalizeText_(properties.getProperty(SCALE_AUTH_CONFIG.propertyKeys.sessions));

  if (!raw) {
    return {};
  }

  try {
    const parsed = JSON.parse(raw);
    scaleWebappSessionsCache_ = parsed && typeof parsed === "object" ? parsed : {};
    putScriptCacheJson_(cacheKey, scaleWebappSessionsCache_, SCALE_AUTH_CACHE_CONFIG.ttlSeconds.sessions);
    return scaleWebappSessionsCache_;
  } catch (error) {
    return {};
  }
}

function saveScaleWebappSessions_(sessions) {
  const cleaned = pruneScaleWebappSessions_(sessions || {});
  PropertiesService.getScriptProperties().setProperty(
    SCALE_AUTH_CONFIG.propertyKeys.sessions,
    JSON.stringify(cleaned)
  );
  scaleWebappSessionsCache_ = cleaned;
  putScriptCacheJson_(SCALE_AUTH_CACHE_CONFIG.keys.sessions, cleaned, SCALE_AUTH_CACHE_CONFIG.ttlSeconds.sessions);
  return cleaned;
}

function pruneScaleWebappSessions_(sessions) {
  const cleaned = {};
  const now = Date.now();

  Object.keys(sessions || {}).forEach(function(token) {
    const session = sessions[token];
    if (!session || typeof session !== "object") {
      return;
    }

    const expiresAt = new Date(session.expiresAt || "").getTime();
    if (!Number.isFinite(expiresAt) || expiresAt <= now) {
      return;
    }

    cleaned[token] = session;
  });

  return cleaned;
}

function createScaleWebappSession_(account) {
  const lock = LockService.getScriptLock();
  lock.waitLock(30000);
  try {
    return createScaleWebappSessionLocked_(account);
  } finally {
    lock.releaseLock();
  }
}

function createScaleWebappSessionLocked_(account) {
  const normalizedAccount = normalizeScaleWebappAccount_(account, account && account.username);
  const token = Utilities.getUuid().replace(/-/g, "") + Utilities.getUuid().replace(/-/g, "");
  const createdAt = new Date();
  const expiresAt = new Date(createdAt.getTime() + SCALE_AUTH_CONFIG.sessionTtlHours * 60 * 60 * 1000);
  const session = {
    token: token,
    username: normalizedAccount.username,
    displayName: normalizedAccount.displayName,
    role: normalizedAccount.role,
    createdAt: createdAt.toISOString(),
    expiresAt: expiresAt.toISOString()
  };

  const sessions = getScaleWebappSessions_();
  sessions[token] = session;
  saveScaleWebappSessions_(sessions);

  return session;
}

function invalidateScaleWebappSession_(sessionToken) {
  const normalizedToken = normalizeText_(sessionToken);
  if (!normalizedToken) {
    return;
  }

  const lock = LockService.getScriptLock();
  lock.waitLock(30000);
  try {
    const sessions = getScaleWebappSessions_();
    delete sessions[normalizedToken];
    saveScaleWebappSessions_(sessions);
  } finally {
    lock.releaseLock();
  }
}

function getSessionContext_(sessionToken) {
  const normalizedToken = normalizeText_(sessionToken);
  if (!normalizedToken) {
    return null;
  }

  const sessions = getScaleWebappSessions_();
  const session = sessions[normalizedToken];
  if (!session || typeof session !== "object") {
    return null;
  }

  const expiresAt = new Date(session.expiresAt || "").getTime();
  if (!Number.isFinite(expiresAt) || expiresAt <= Date.now()) {
    delete sessions[normalizedToken];
    saveScaleWebappSessions_(sessions);
    return null;
  }

  const accounts = ensureScaleWebappAccounts_();
  const account = accounts[normalizeText_(session.username)];
  if (!account || account.active === false) {
    delete sessions[normalizedToken];
    saveScaleWebappSessions_(sessions);
    return null;
  }

  return {
    token: normalizedToken,
    username: account.username,
    displayName: account.displayName || account.username,
    role: account.role || "user"
  };
}

function requireScaleWebappSession_(sessionToken) {
  const normalizedToken = normalizeText_(sessionToken);
  const sessionContext = getSessionContext_(normalizedToken);
  if (!sessionContext) {
    throw new Error(normalizedToken ? "세션이 만료되었습니다. 다시 로그인해주세요." : "로그인이 필요합니다.");
  }
  return sessionContext;
}

function getScriptCache_() {
  try {
    return CacheService.getScriptCache();
  } catch (error) {
    return null;
  }
}

function getScriptCacheText_(key) {
  const normalizedKey = normalizeText_(key);
  if (!normalizedKey) {
    return "";
  }

  const cache = getScriptCache_();
  if (!cache) {
    return "";
  }

  try {
    return normalizeText_(cache.get(normalizedKey));
  } catch (error) {
    return "";
  }
}

function putScriptCacheText_(key, value, ttlSeconds) {
  const normalizedKey = normalizeText_(key);
  const normalizedValue = normalizeText_(value);
  if (!normalizedKey || !normalizedValue) {
    return;
  }

  const cache = getScriptCache_();
  if (!cache) {
    return;
  }

  try {
    cache.put(normalizedKey, normalizedValue, Number(ttlSeconds) || 300);
  } catch (error) {
    console.warn("script cache put skipped", normalizedKey, error && error.message);
  }
}

function getScriptCacheJson_(key) {
  const text = getScriptCacheText_(key);
  if (!text) {
    return null;
  }

  try {
    return JSON.parse(text);
  } catch (error) {
    return null;
  }
}

function putScriptCacheJson_(key, value, ttlSeconds) {
  if (value === undefined) {
    return;
  }

  try {
    putScriptCacheText_(key, JSON.stringify(value), ttlSeconds);
  } catch (error) {
    console.warn("script cache json put skipped", key, error && error.message);
  }
}

function pickPublicUserContext_(sessionContext) {
  const context = sessionContext && typeof sessionContext === "object" ? sessionContext : {};
  return {
    email: "",
    domain: "",
    displayName: normalizeText_(context.displayName),
    username: normalizeText_(context.username),
    role: normalizeText_(context.role)
  };
}

function parseJsonBody_(e) {
  if (!e || !e.postData || !e.postData.contents) {
    throw new Error("요청 본문이 비어 있습니다.");
  }

  try {
    return JSON.parse(e.postData.contents);
  } catch (error) {
    throw new Error("JSON 본문을 해석할 수 없습니다.");
  }
}

function parsePayload_(payload) {
  if (!payload || typeof payload !== "object") {
    throw new Error("JSON 본문을 해석할 수 없습니다.");
  }

  if (payload && (payload.repairExistingRecords === true || payload.syncScope === "repair_existing_records")) {
    return payload;
  }

  const hasRecords = payload && Array.isArray(payload.records) && payload.records.length;
  const hasQuestionnaires = payload && Array.isArray(payload.questionnaires) && payload.questionnaires.length;

  if (!hasRecords && !hasQuestionnaires) {
    throw new Error("전송할 검사 기록 또는 척도 마스터가 없습니다.");
  }

  return payload;
}

function validateToken_(token) {
  const configuredToken = getSyncToken_();
  if (!configuredToken) {
    return;
  }

  if (normalizeText_(token) !== configuredToken) {
    throw new Error("동기화 토큰이 일치하지 않습니다.");
  }
}

function upsertPayload_(payload) {
  ensureManagedSheets_();

  const result = {
    targetSpreadsheetId: getTargetSpreadsheetId_(),
    targetSpreadsheetName: getTargetSpreadsheet_().getName(),
    recordsInserted: 0,
    recordsUpdated: 0,
    answersInserted: 0,
    answersUpdated: 0,
    questionnairesInserted: 0,
    questionnairesUpdated: 0,
    fieldsInserted: 0,
    fieldsUpdated: 0,
    optionsInserted: 0,
    optionsUpdated: 0
  };

  if (payload.repairExistingRecords === true || payload.syncScope === "repair_existing_records") {
    mergeCounts_(result, repairExistingRecords_());
  }

  if (Array.isArray(payload.records) && payload.records.length) {
    mergeCounts_(result, upsertRecords_(payload));
  }

  if (Array.isArray(payload.questionnaires) && payload.questionnaires.length) {
    mergeCounts_(result, upsertQuestionnaires_(payload));
  }

  return result;
}

function ensureManagedSheets_() {
  ensureSheet_(SCALE_SYNC_CONFIG.sheetNames.records, SCALE_SYNC_CONFIG.headers.records, SCALE_SYNC_CONFIG.headerLabels.records);
  ensureSheet_(SCALE_SYNC_CONFIG.sheetNames.answers, SCALE_SYNC_CONFIG.headers.answers, SCALE_SYNC_CONFIG.headerLabels.answers);
  ensureSheet_(SCALE_SYNC_CONFIG.sheetNames.questionnaires, SCALE_SYNC_CONFIG.headers.questionnaires, SCALE_SYNC_CONFIG.headerLabels.questionnaires);
  ensureSheet_(SCALE_SYNC_CONFIG.sheetNames.fields, SCALE_SYNC_CONFIG.headers.fields, SCALE_SYNC_CONFIG.headerLabels.fields);
  ensureSheet_(SCALE_SYNC_CONFIG.sheetNames.options, SCALE_SYNC_CONFIG.headers.options, SCALE_SYNC_CONFIG.headerLabels.options);
  ensureScaleAnalyticsSheets_();
}

function ensureSheet_(sheetName, headers, displayHeaders) {
  const sheet = getOrCreateSheet_(sheetName);
  const normalizedHeaders = headers.map(function(header) {
    return normalizeText_(header);
  });
  const visibleHeaders = (displayHeaders || headers).map(function(header) {
    return normalizeText_(header);
  });

  ensureSheetSize_(sheet, 2, normalizedHeaders.length);
  sheet.getRange(1, 1, 1, normalizedHeaders.length).setValues([visibleHeaders]);

  formatHeader_(sheet, normalizedHeaders.length);
  return sheet;
}

function ensureScaleAnalyticsSheets_() {
  const spreadsheet = getTargetSpreadsheet_();
  const requiredNames = [
    SCALE_SYNC_CONFIG.sheetNames.workerView,
    SCALE_SYNC_CONFIG.sheetNames.riskView,
    SCALE_SYNC_CONFIG.sheetNames.dashboard,
    SCALE_SYNC_CONFIG.sheetNames.settings
  ];
  const hasAllSheets = requiredNames.every(function(sheetName) {
    return spreadsheet.getSheetByName(sheetName);
  });

  if (!hasAllSheets) {
    rebuildScaleAnalyticsWorkspace_();
  }
}

function rebuildScaleAnalyticsWorkspace_() {
  const workerViewSheet = getOrCreateSheet_(SCALE_SYNC_CONFIG.sheetNames.workerView);
  const riskViewSheet = getOrCreateSheet_(SCALE_SYNC_CONFIG.sheetNames.riskView);
  const dashboardSheet = getOrCreateSheet_(SCALE_SYNC_CONFIG.sheetNames.dashboard);
  const settingsSheet = getOrCreateSheet_(SCALE_SYNC_CONFIG.sheetNames.settings);

  buildWorkerViewSheet_(workerViewSheet);
  buildRiskViewSheet_(riskViewSheet);
  buildAnalyticsSettingsSheet_(settingsSheet);
  buildAnalyticsDashboardSheet_(dashboardSheet);

  return {
    workerViewSheetName: workerViewSheet.getName(),
    riskViewSheetName: riskViewSheet.getName(),
    dashboardSheetName: dashboardSheet.getName(),
    settingsSheetName: settingsSheet.getName()
  };
}

function buildWorkerViewSheet_(sheet) {
  const raw = "'" + SCALE_SYNC_CONFIG.sheetNames.records + "'";
  const formula = "=IFERROR(SORT(FILTER({"
    + raw + "!I2:I,"
    + raw + "!P2:P,"
    + raw + "!Q2:Q,"
    + raw + "!K2:K,"
    + "IF(" + raw + "!M2:M=\"\",,IFERROR(VALUE(REGEXEXTRACT(" + raw + "!M2:M,\"-?\\d+(?:\\.\\d+)?\")),)),"
    + raw + "!M2:M,"
    + raw + "!N2:N,"
    + raw + "!O2:O,"
    + raw + "!Y2:Y,"
    + raw + "!AA2:AA,"
    + raw + "!A2:A"
    + "},"
    + raw + "!A2:A<>\"\"),1,FALSE),\"\")";

  sheet.clear();
  ensureSheetSize_(sheet, 200, SCALE_SYNC_CONFIG.viewHeaders.workerView.length);
  sheet.getRange(1, 1, 1, SCALE_SYNC_CONFIG.viewHeaders.workerView.length)
    .setValues([SCALE_SYNC_CONFIG.viewHeaders.workerView]);
  sheet.getRange(2, 1).setFormula(formula);
  sheet.setFrozenRows(1);
  formatHeader_(sheet, SCALE_SYNC_CONFIG.viewHeaders.workerView.length);
}

function buildRiskViewSheet_(sheet) {
  const worker = "'" + SCALE_SYNC_CONFIG.sheetNames.workerView + "'";
  const formula = "=IFERROR(SORT(FILTER({"
    + worker + "!A2:A,"
    + worker + "!B2:B,"
    + worker + "!C2:C,"
    + worker + "!D2:D,"
    + worker + "!F2:F,"
    + worker + "!G2:G,"
    + worker + "!H2:H,"
    + worker + "!J2:J,"
    + worker + "!K2:K"
    + "},"
    + worker + "!K2:K<>\"\","
    + "(( " + worker + "!J2:J<>\"\" )+REGEXMATCH(" + worker + "!G2:G,\"^(A|B|C)\"))>0"
    + "),1,FALSE),\"\")";

  sheet.clear();
  ensureSheetSize_(sheet, 200, SCALE_SYNC_CONFIG.viewHeaders.riskView.length);
  sheet.getRange(1, 1, 1, SCALE_SYNC_CONFIG.viewHeaders.riskView.length)
    .setValues([SCALE_SYNC_CONFIG.viewHeaders.riskView]);
  sheet.getRange(2, 1).setFormula(formula);
  sheet.setFrozenRows(1);
  formatHeader_(sheet, SCALE_SYNC_CONFIG.viewHeaders.riskView.length);
}

function buildAnalyticsSettingsSheet_(sheet) {
  sheet.clear();
  ensureSheetSize_(sheet, SCALE_SYNC_CONFIG.settingsRows.length + 8, 3);
  sheet.getRange(1, 1, SCALE_SYNC_CONFIG.settingsRows.length, 3)
    .setValues(SCALE_SYNC_CONFIG.settingsRows);
  sheet.setFrozenRows(1);
  formatHeader_(sheet, 3);
}

function buildAnalyticsDashboardSheet_(sheet) {
  const worker = "'" + SCALE_SYNC_CONFIG.sheetNames.workerView + "'";
  const detailFormula = "=IF($B$3=\"\",\"\",IFERROR(SORT(FILTER({"
    + worker + "!A2:A,"
    + worker + "!D2:D,"
    + worker + "!E2:E,"
    + worker + "!F2:F,"
    + worker + "!G2:G,"
    + worker + "!H2:H,"
    + worker + "!I2:I,"
    + worker + "!J2:J,"
    + worker + "!K2:K"
    + "},"
    + worker + "!B2:B=$B$3,"
    + "IF($E$3=\"\"," + worker + "!A2:A<>\"\","
    + worker + "!C2:C=TEXT($E$3,\"yyyy-mm-dd\"))"
    + "),1,TRUE),\"검색 결과가 없습니다.\"))";
  const trendFormula = "=IF($B$3=\"\",\"\",IFERROR(QUERY(FILTER({"
    + worker + "!A2:A,"
    + worker + "!D2:D,"
    + worker + "!E2:E,"
    + worker + "!B2:B,"
    + worker + "!C2:C"
    + "},"
    + worker + "!B2:B=$B$3,"
    + "IF($E$3=\"\"," + worker + "!A2:A<>\"\","
    + worker + "!C2:C=TEXT($E$3,\"yyyy-mm-dd\"))"
    + "),\"select Col1, max(Col3) where Col3 is not null group by Col1 pivot Col2 label Col1 '검사일', max(Col3) ''\",0),\"\"))";
  const namesFormula = "=ARRAYFORMULA(SORT(UNIQUE(FILTER(" + worker + "!B2:B," + worker + "!B2:B<>\"\"))))";
  const helperCell = SCALE_SYNC_CONFIG.dashboard.helperNameCell;
  const helperColumnLetter = helperCell.replace(/[0-9]/g, "");

  sheet.clear();
  ensureSheetSize_(sheet, 320, 26);
  sheet.getRange("A1").setValue("척도 검사 결과 대시보드");
  sheet.getRange("A2").setValue("대상자명을 입력하면 날짜별 검사 결과와 척도별 점수 변화 그래프를 확인할 수 있습니다.");
  sheet.getRange("A3").setValue("대상자명");
  sheet.getRange("D3").setValue("생년월일");
  sheet.getRange("A5").setValue("검사 건수");
  sheet.getRange("D5").setValue("최근 검사일");
  sheet.getRange("A9:I9").setValues([["검사일", "척도", "점수값", "점수표시", "결과구간", "담당자", "비고", "경고여부", "기록고유값"]]);
  sheet.getRange("N8").setValue("점수 변화 그래프 데이터");
  sheet.getRange("T1").setValue("대상자 목록");
  sheet.getRange("B5").setFormula("=IF($B$3=\"\",\"\",COUNTA(A10:A))");
  sheet.getRange("E5").setFormula("=IF($B$3=\"\",\"\",IFERROR(MAX(A10:A),\"\"))");
  sheet.getRange("A10").setFormula(detailFormula);
  sheet.getRange(SCALE_SYNC_CONFIG.dashboard.trendStartCell).setFormula(trendFormula);
  sheet.getRange(helperCell).setFormula(namesFormula);

  const validation = SpreadsheetApp.newDataValidation()
    .requireValueInRange(sheet.getRange(helperColumnLetter + "2:" + helperColumnLetter), true)
    .setAllowInvalid(true)
    .build();
  sheet.getRange(SCALE_SYNC_CONFIG.dashboard.clientNameCell).setDataValidation(validation);
  sheet.getRange(SCALE_SYNC_CONFIG.dashboard.birthDateCell).setNumberFormat("yyyy-mm-dd");
  sheet.setFrozenRows(6);
  formatHeader_(sheet, 9);
  sheet.getRange("A1:F1").merge();
  sheet.getRange("A1:F1").setFontSize(16);
  sheet.getRange("N8:Z8").setBackground("#fff2cc").setFontWeight("bold");
  sheet.hideColumns(20, 7);

  const charts = sheet.getCharts();
  charts.forEach(function(chart) {
    sheet.removeChart(chart);
  });
  const chart = sheet.newChart()
    .asLineChart()
    .addRange(sheet.getRange("N9:Z200"))
    .setNumHeaders(1)
    .setOption("title", "척도별 점수 변화")
    .setOption("legend", { position: "right" })
    .setOption("hAxis", { title: "검사일" })
    .setOption("vAxis", { title: "점수" })
    .setPosition(1, 8, 0, 0)
    .build();
  sheet.insertChart(chart);
}

function upsertRecords_(payload) {
  const recordSheet = getOrCreateSheet_(SCALE_SYNC_CONFIG.sheetNames.records);
  const answerSheet = getOrCreateSheet_(SCALE_SYNC_CONFIG.sheetNames.answers);

  const recordRows = payload.records.map(function(record) {
    return buildRecordRow_(record, payload);
  });

  const answerRows = [];
  payload.records.forEach(function(record) {
    buildAnswerRows_(record, payload).forEach(function(row) {
      answerRows.push(row);
    });
  });

  const recordResult = upsertRowsByKey_(
    recordSheet,
    SCALE_SYNC_CONFIG.headers.records,
    recordRows,
    "record_id"
  );
  const answerResult = upsertRowsByKey_(
    answerSheet,
    SCALE_SYNC_CONFIG.headers.answers,
    answerRows,
    "detail_key"
  );

  return {
    recordsInserted: recordResult.inserted,
    recordsUpdated: recordResult.updated,
    answersInserted: answerResult.inserted,
    answersUpdated: answerResult.updated
  };
}

function repairExistingRecords_() {
  const recordSheet = getOrCreateSheet_(SCALE_SYNC_CONFIG.sheetNames.records);
  const answerSheet = getOrCreateSheet_(SCALE_SYNC_CONFIG.sheetNames.answers);
  const lastRow = recordSheet.getLastRow();

  if (lastRow <= 1) {
    clearDataRows_(answerSheet, SCALE_SYNC_CONFIG.headers.answers.length);
    return {
      recordsInserted: 0,
      recordsUpdated: 0,
      answersInserted: 0,
      answersUpdated: 0
    };
  }

  const rows = recordSheet.getRange(
    2,
    1,
    lastRow - 1,
    Math.max(recordSheet.getLastColumn(), SCALE_SYNC_CONFIG.headers.records.length)
  ).getDisplayValues();
  const recordRows = [];
  const answerRows = [];

  rows.forEach(function(row) {
    const parsed = parseLegacyRecordRow_(row);
    if (!parsed) {
      return;
    }

    const payload = {
      sentAt: normalizeText_(row[1]) || new Date().toISOString(),
      syncScope: normalizeText_(row[2]) || "repair_existing_records",
      source: normalizeText_(row[3]) || "scale-screening-web-app",
      appSettings: {
        organizationName: normalizeText_(row[4]),
        teamName: normalizeText_(row[5]),
        contactNote: normalizeText_(row[6])
      }
    };

    recordRows.push(buildRecordRow_(parsed, payload));
    buildAnswerRows_(parsed, payload).forEach(function(answerRow) {
      answerRows.push(answerRow);
    });
  });

  const recordResult = upsertRowsByKey_(
    recordSheet,
    SCALE_SYNC_CONFIG.headers.records,
    recordRows,
    "record_id"
  );
  const answerResult = upsertRowsByKey_(
    answerSheet,
    SCALE_SYNC_CONFIG.headers.answers,
    answerRows,
    "detail_key"
  );

  return {
    recordsInserted: recordResult.inserted,
    recordsUpdated: recordResult.updated,
    answersInserted: answerResult.inserted,
    answersUpdated: answerResult.updated
  };
}

function parseLegacyRecordRow_(row) {
  for (var index = row.length - 1; index >= 0; index -= 1) {
    const cell = normalizeText_(row[index]);
    if (!cell || cell.charAt(0) !== "{") {
      continue;
    }

    try {
      const parsed = JSON.parse(cell);
      if (parsed && parsed.questionnaireId && parsed.createdAt) {
        return parsed;
      }
    } catch (error) {
      // ignore non-record JSON cells
    }
  }

  return null;
}

function upsertQuestionnaires_(payload) {
  const questionnaireSheet = getOrCreateSheet_(SCALE_SYNC_CONFIG.sheetNames.questionnaires);
  const fieldSheet = getOrCreateSheet_(SCALE_SYNC_CONFIG.sheetNames.fields);
  const optionSheet = getOrCreateSheet_(SCALE_SYNC_CONFIG.sheetNames.options);

  clearDataRows_(questionnaireSheet, SCALE_SYNC_CONFIG.headers.questionnaires.length);
  clearDataRows_(fieldSheet, SCALE_SYNC_CONFIG.headers.fields.length);
  clearDataRows_(optionSheet, SCALE_SYNC_CONFIG.headers.options.length);

  const questionnaireRows = payload.questionnaires.map(function(questionnaire) {
    return buildQuestionnaireRow_(questionnaire);
  });

  const fieldRows = [];
  const optionRows = [];
  payload.questionnaires.forEach(function(questionnaire) {
    buildFieldRows_(questionnaire).forEach(function(row) {
      fieldRows.push(row);
    });
    buildOptionRows_(questionnaire).forEach(function(row) {
      optionRows.push(row);
    });
  });

  const questionnaireResult = upsertRowsByKey_(
    questionnaireSheet,
    SCALE_SYNC_CONFIG.headers.questionnaires,
    questionnaireRows,
    "questionnaire_id"
  );
  const fieldResult = upsertRowsByKey_(
    fieldSheet,
    SCALE_SYNC_CONFIG.headers.fields,
    fieldRows,
    "field_key"
  );
  const optionResult = upsertRowsByKey_(
    optionSheet,
    SCALE_SYNC_CONFIG.headers.options,
    optionRows,
    "option_key"
  );

  return {
    questionnairesInserted: questionnaireResult.inserted,
    questionnairesUpdated: questionnaireResult.updated,
    fieldsInserted: fieldResult.inserted,
    fieldsUpdated: fieldResult.updated,
    optionsInserted: optionResult.inserted,
    optionsUpdated: optionResult.updated
  };
}

function upsertRowsByKey_(sheet, headers, rowObjects, keyField) {
  const keyIndex = headers.indexOf(keyField);
  const rowMap = {};
  const insertedRows = [];
  const updateRows = {};
  let inserted = 0;
  let updated = 0;

  if (keyIndex === -1) {
    throw new Error("키 필드를 찾을 수 없습니다: " + keyField);
  }

  ensureSheetSize_(sheet, Math.max(sheet.getLastRow(), 2), headers.length);

  const lastRow = sheet.getLastRow();
  let existingData = [];

  if (lastRow >= 2) {
    existingData = sheet.getRange(2, 1, lastRow - 1, headers.length).getDisplayValues();
    existingData.forEach(function(row, index) {
      const key = normalizeText_(row[keyIndex]);
      if (key) {
        rowMap[key] = index;
      }
    });
  }

  rowObjects.forEach(function(rowObject) {
    const key = normalizeText_(rowObject[keyField]);
    if (!key) {
      return;
    }

    const rowValues = headers.map(function(header) {
      return toCellText_(rowObject[header]);
    });

    if (rowMap[key] !== undefined) {
      updateRows[rowMap[key]] = rowValues;
      updated += 1;
      return;
    }

    insertedRows.push(rowValues);
    inserted += 1;
  });

  if (insertedRows.length) {
    const startRow = sheet.getLastRow() + 1;
    sheet.getRange(startRow, 1, insertedRows.length, headers.length).setValues(insertedRows);
  }

  const updateIndexes = Object.keys(updateRows);
  if (updateIndexes.length && existingData.length) {
    updateIndexes.forEach(function(indexText) {
      const rowIndex = Number(indexText);
      existingData[rowIndex] = updateRows[indexText];
    });
    sheet.getRange(2, 1, existingData.length, headers.length).setValues(existingData);
  }

  formatHeader_(sheet, headers.length);

  return {
    inserted: inserted,
    updated: updated
  };
}

/**
 * @param {string[]} headers
 * @returns {Object<string, number>}
 */
function buildHeaderIndexMap_(headers) {
  return (headers || []).reduce(function(result, header, index) {
    result[header] = index;
    return result;
  }, {});
}

/**
 * @param {string[]} row
 * @param {Object<string, number>} headerIndex
 * @returns {Object|null}
 */
function buildRecentRecordSummaryFromRow_(row, headerIndex) {
  const recordId = normalizeText_(row[headerIndex.record_id]);
  const questionnaireId = normalizeText_(row[headerIndex.questionnaire_id]);
  if (!recordId || !questionnaireId) {
    return null;
  }

  return {
    id: recordId,
    questionnaireId: questionnaireId,
    questionnaireTitle: normalizeText_(row[headerIndex.questionnaire_title]),
    shortTitle: normalizeText_(row[headerIndex.questionnaire_short_title]),
    createdAt: normalizeText_(row[headerIndex.record_created_at]) || normalizeText_(row[headerIndex.exported_at]),
    meta: {
      sessionDate: normalizeText_(row[headerIndex.session_date]),
      workerName: normalizeText_(row[headerIndex.worker_name]),
      clientLabel: normalizeText_(row[headerIndex.client_label]),
      birthDate: normalizeText_(row[headerIndex.birth_date]),
      sessionNote: normalizeText_(row[headerIndex.session_note])
    },
    evaluation: {
      scoreText: normalizeText_(row[headerIndex.score_text]),
      bandText: normalizeText_(row[headerIndex.band_text]),
      highlights: normalizeText_(row[headerIndex.highlights])
        ? normalizeText_(row[headerIndex.highlights]).split(/\s*\|\s*/).filter(Boolean)
        : [],
      flags: normalizeText_(row[headerIndex.flags])
        ? normalizeText_(row[headerIndex.flags]).split(/\s*\|\s*/).filter(function(text) {
            return text;
          }).map(function(text) {
            return { text: text };
          })
        : []
    }
  };
}

function buildRecordRow_(record, payload) {
  const respondentDisplay = Array.isArray(record.respondentDisplay) ? record.respondentDisplay : [];
  const progress = record && record.progress ? record.progress : {};
  const flags = Array.isArray(record.evaluation && record.evaluation.flags)
    ? record.evaluation.flags.map(function(flag) {
      return flag && flag.text ? flag.text : "";
    }).filter(Boolean)
    : [];

  return {
    record_id: toCellText_(record.id),
    exported_at: toCellText_(payload.sentAt),
    sync_scope: toCellText_(payload.syncScope),
    source_app: toCellText_(payload.source),
    organization_name: toCellText_(payload.appSettings && payload.appSettings.organizationName),
    team_name: toCellText_(payload.appSettings && payload.appSettings.teamName),
    contact_note: toCellText_(payload.appSettings && payload.appSettings.contactNote),
    record_created_at: toCellText_(record.createdAt),
    session_date: toCellText_(record.meta && record.meta.sessionDate),
    questionnaire_id: toCellText_(record.questionnaireId),
    questionnaire_title: toCellText_(record.questionnaireTitle),
    questionnaire_short_title: toCellText_(record.shortTitle),
    score_text: toCellText_(record.evaluation && record.evaluation.scoreText),
    band_text: toCellText_(record.evaluation && record.evaluation.bandText),
    worker_name: toCellText_(record.meta && record.meta.workerName),
    client_label: toCellText_(record.meta && record.meta.clientLabel),
    birth_date: toCellText_(record.meta && record.meta.birthDate),
    gender: findDisplayValueByLabel_(respondentDisplay, "성별"),
    age_group: findDisplayValueByLabel_(respondentDisplay, "연령대"),
    progress_summary: buildProgressSummary_(progress),
    progress_percent: progress.percent === null || progress.percent === undefined ? "" : String(progress.percent),
    progress_answered: progress.answered === null || progress.answered === undefined ? "" : String(progress.answered),
    progress_total: progress.total === null || progress.total === undefined ? "" : String(progress.total),
    signature_present: record.meta && record.meta.signatureDataUrl ? "Y" : "N",
    session_note: toCellText_(record.meta && record.meta.sessionNote),
    highlights: joinTextList_(record.evaluation && record.evaluation.highlights),
    flags: joinTextList_(flags),
    respondent_summary: buildRespondentSummary_(respondentDisplay),
    breakdown_summary: buildBreakdownSummary_(record.breakdown),
    record_json: safeStringify_(record)
  };
}

function buildAnswerRows_(record, payload) {
  const rows = [];
  const breakdown = Array.isArray(record.breakdown) ? record.breakdown : [];

  breakdown.forEach(function(item) {
    const parentKey = toCellText_(record.id) + "::" + toCellText_(item.id || item.number);

    rows.push({
      detail_key: parentKey,
      record_id: toCellText_(record.id),
      exported_at: toCellText_(payload.sentAt),
      session_date: toCellText_(record.meta && record.meta.sessionDate),
      questionnaire_id: toCellText_(record.questionnaireId),
      questionnaire_title: toCellText_(record.questionnaireTitle),
      worker_name: toCellText_(record.meta && record.meta.workerName),
      client_label: toCellText_(record.meta && record.meta.clientLabel),
      birth_date: toCellText_(record.meta && record.meta.birthDate),
      is_subquestion: "N",
      parent_question_id: "",
      question_id: toCellText_(item.id || item.number),
      question_number: toCellText_(item.number),
      question_text: toCellText_(item.text),
      answer_label: toCellText_(item.answerLabel),
      score: item.score === null || item.score === undefined ? "" : String(item.score),
      raw_json: safeStringify_(item)
    });

    (Array.isArray(item.subAnswers) ? item.subAnswers : []).forEach(function(subItem, index) {
      rows.push({
        detail_key: parentKey + "::sub::" + String(index + 1),
        record_id: toCellText_(record.id),
        exported_at: toCellText_(payload.sentAt),
        session_date: toCellText_(record.meta && record.meta.sessionDate),
        questionnaire_id: toCellText_(record.questionnaireId),
        questionnaire_title: toCellText_(record.questionnaireTitle),
        worker_name: toCellText_(record.meta && record.meta.workerName),
        client_label: toCellText_(record.meta && record.meta.clientLabel),
        birth_date: toCellText_(record.meta && record.meta.birthDate),
        is_subquestion: "Y",
        parent_question_id: toCellText_(item.id || item.number),
        question_id: toCellText_(item.id || item.number) + "::sub::" + String(index + 1),
        question_number: toCellText_(subItem.number),
        question_text: toCellText_(subItem.text),
        answer_label: toCellText_(subItem.answerLabel),
        score: subItem.score === null || subItem.score === undefined ? "" : String(subItem.score),
        raw_json: safeStringify_(subItem)
      });
    });
  });

  return rows;
}

function buildQuestionnaireRow_(questionnaire) {
  return {
    questionnaire_id: toCellText_(questionnaire.id),
    self_seq: toCellText_(questionnaire.selfSeq),
    title: toCellText_(questionnaire.title),
    short_title: toCellText_(questionnaire.shortTitle),
    recommended_age: toCellText_(questionnaire.recommendedAge),
    question_count: String((questionnaire.questions || []).length),
    respondent_field_count: String((questionnaire.respondentFields || []).length),
    question_prompt: toCellText_(questionnaire.questionPrompt),
    intro_text: joinTextList_(questionnaire.intro),
    source_reference_page: toCellText_(questionnaire.source && questionnaire.source.referencePage),
    source_institution: toCellText_(questionnaire.source && questionnaire.source.institution),
    source_citation: toCellText_(questionnaire.source && questionnaire.source.citation),
    scoring_type: toCellText_(questionnaire.scoring && questionnaire.scoring.type),
    scoring_json: safeStringify_(questionnaire.scoring || {}),
    extraction_notes_json: safeStringify_(questionnaire.extractionNotes || []),
    questionnaire_json: safeStringify_(questionnaire)
  };
}

function buildFieldRows_(questionnaire) {
  const rows = [];

  (questionnaire.respondentFields || []).forEach(function(field) {
    rows.push(buildFieldRow_(questionnaire, "respondent", "", field));
  });

  (questionnaire.questions || []).forEach(function(question) {
    rows.push(buildFieldRow_(questionnaire, "question", "", question));
    (question.subQuestions || []).forEach(function(subQuestion) {
      rows.push(buildFieldRow_(questionnaire, "subquestion", question.id, subQuestion));
    });
  });

  return rows;
}

function buildFieldRow_(questionnaire, fieldScope, parentFieldId, field) {
  return {
    field_key: toCellText_(questionnaire.id) + "::" + fieldScope + "::" + toCellText_(field.id),
    questionnaire_id: toCellText_(questionnaire.id),
    field_scope: fieldScope,
    parent_field_id: toCellText_(parentFieldId),
    field_id: toCellText_(field.id),
    field_number: toCellText_(field.number),
    field_label: toCellText_(field.label),
    field_text: toCellText_(field.text),
    field_type: toCellText_(field.type || "single_choice"),
    is_required: field.required ? "Y" : "N",
    option_count: String((field.options || []).length),
    field_json: safeStringify_(field)
  };
}

function buildOptionRows_(questionnaire) {
  const rows = [];

  (questionnaire.respondentFields || []).forEach(function(field) {
    buildOptionRowsForField_(questionnaire, "respondent", "", field).forEach(function(row) {
      rows.push(row);
    });
  });

  (questionnaire.questions || []).forEach(function(question) {
    buildOptionRowsForField_(questionnaire, "question", "", question).forEach(function(row) {
      rows.push(row);
    });
    (question.subQuestions || []).forEach(function(subQuestion) {
      buildOptionRowsForField_(questionnaire, "subquestion", question.id, subQuestion).forEach(function(row) {
        rows.push(row);
      });
    });
  });

  return rows;
}

function buildOptionRowsForField_(questionnaire, fieldScope, parentFieldId, field) {
  return (field.options || []).map(function(option, index) {
    return {
      option_key: [
        toCellText_(questionnaire.id),
        fieldScope,
        toCellText_(field.id),
        String(index + 1)
      ].join("::"),
      questionnaire_id: toCellText_(questionnaire.id),
      field_scope: fieldScope,
      parent_field_id: toCellText_(parentFieldId),
      field_id: toCellText_(field.id),
      option_order: String(index + 1),
      option_value: toCellText_(option.value),
      option_label: toCellText_(option.label),
      option_score: option.score === null || option.score === undefined ? "" : String(option.score),
      option_json: safeStringify_(option)
    };
  });
}

function getTargetSpreadsheet_() {
  return SpreadsheetApp.openById(getTargetSpreadsheetId_());
}

function getScaleStatusSummary_() {
  const cacheKey = SCALE_AUTH_CACHE_CONFIG.keys.statusSummary;
  const cached = getScriptCacheJson_(cacheKey);
  if (cached && typeof cached === "object") {
    return cached;
  }

  const spreadsheet = getTargetSpreadsheet_();
  const summary = {
    targetSpreadsheetId: getTargetSpreadsheetId_(),
    targetSpreadsheetName: spreadsheet.getName(),
    tokenConfigured: Boolean(getSyncToken_()),
    sheets: getSheetStatsFromSpreadsheet_(spreadsheet)
  };

  putScriptCacheJson_(cacheKey, summary, SCALE_AUTH_CACHE_CONFIG.ttlSeconds.statusSummary);
  return summary;
}

function invalidateScaleStatusSummaryCache_() {
  const cache = getScriptCache_();
  if (!cache) {
    return;
  }

  try {
    cache.remove(SCALE_AUTH_CACHE_CONFIG.keys.statusSummary);
  } catch (error) {
    console.warn("status summary cache remove skipped", error && error.message);
  }
}

function getTargetSpreadsheetId_() {
  const storedId = normalizeText_(PropertiesService.getScriptProperties().getProperty(
    SCALE_SYNC_CONFIG.propertyKeys.spreadsheetId
  ));
  const defaultId = normalizeText_(SCALE_SYNC_CONFIG.defaults.spreadsheetId);

  if (!storedId) {
    if (defaultId) {
      return defaultId;
    }
    throw new Error(
      "대상 스프레드시트 ID가 설정되지 않았습니다. 현재 사용할 스프레드시트 ID를 다시 저장해주세요."
    );
  }

  if (SCALE_SYNC_CONFIG.legacySpreadsheetIds.indexOf(storedId) >= 0) {
    if (defaultId) {
      return defaultId;
    }
    throw new Error(
      "저장된 대상 스프레드시트 ID가 예전 기본값으로 남아 있습니다. 현재 사용할 스프레드시트 ID를 다시 저장해주세요."
    );
  }

  return storedId;
}

function getSyncToken_() {
  return normalizeText_(PropertiesService.getScriptProperties().getProperty(
    SCALE_SYNC_CONFIG.propertyKeys.token
  ));
}

function getOrCreateSheet_(sheetName) {
  const spreadsheet = getTargetSpreadsheet_();
  return spreadsheet.getSheetByName(sheetName) || spreadsheet.insertSheet(sheetName);
}

function getSheetStats_() {
  return getSheetStatsFromSpreadsheet_(getTargetSpreadsheet_());
}

function getSheetStatsFromSpreadsheet_(spreadsheet) {
  const names = SCALE_SYNC_CONFIG.sheetNames;
  const result = {};

  Object.keys(names).forEach(function(key) {
    const sheet = spreadsheet.getSheetByName(names[key]);
    result[key] = sheet ? Math.max(sheet.getLastRow() - 1, 0) : 0;
  });

  return result;
}

function formatHeader_(sheet, columnCount) {
  sheet.setFrozenRows(1);
  sheet.getRange(1, 1, 1, columnCount)
    .setFontWeight("bold")
    .setBackground("#d9ead3")
    .setHorizontalAlignment("center");
}

function ensureSheetSize_(sheet, minimumRows, minimumColumns) {
  if (sheet.getMaxRows() < minimumRows) {
    sheet.insertRowsAfter(sheet.getMaxRows(), minimumRows - sheet.getMaxRows());
  }
  if (sheet.getMaxColumns() < minimumColumns) {
    sheet.insertColumnsAfter(sheet.getMaxColumns(), minimumColumns - sheet.getMaxColumns());
  }
}

function clearDataRows_(sheet, minimumColumns) {
  ensureSheetSize_(sheet, 2, minimumColumns);

  if (sheet.getMaxRows() > 2) {
    sheet.deleteRows(3, sheet.getMaxRows() - 2);
  }

  sheet.getRange(2, 1, 1, Math.max(sheet.getLastColumn(), minimumColumns)).clearContent();
  ensureSheetSize_(sheet, 2, minimumColumns);
}

function createJsonOutput_(data) {
  return ContentService
    .createTextOutput(JSON.stringify(data, null, 2))
    .setMimeType(ContentService.MimeType.JSON);
}

function normalizeText_(value) {
  if (value === null || value === undefined) {
    return "";
  }
  return String(value).trim();
}

function toCellText_(value) {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  return safeStringify_(value);
}

function safeStringify_(value) {
  try {
    return JSON.stringify(value);
  } catch (error) {
    return "";
  }
}

function structuredCloneSafe_(value) {
  return JSON.parse(JSON.stringify(value));
}

function joinTextList_(items) {
  return (items || []).map(function(item) {
    return toCellText_(item);
  }).filter(Boolean).join(" | ");
}

function findDisplayValueByLabel_(items, label) {
  const matched = (items || []).filter(function(item) {
    return item && item.label === label;
  })[0];
  return matched ? toCellText_(matched.value) : "";
}

function buildRespondentSummary_(items) {
  return (items || []).map(function(item) {
    if (!item || !item.label || !item.value) {
      return "";
    }
    return item.label + ": " + item.value;
  }).filter(Boolean).join(" | ");
}

function buildProgressSummary_(progress) {
  if (!progress || progress.percent === null || progress.percent === undefined) {
    return "";
  }

  const answered = progress.answered === null || progress.answered === undefined ? "" : String(progress.answered);
  const total = progress.total === null || progress.total === undefined ? "" : String(progress.total);
  return String(progress.percent) + "% (" + answered + "/" + total + "항목)";
}

function buildBreakdownSummary_(items) {
  return (items || []).map(function(item) {
    if (!item) {
      return "";
    }

    const baseText = [
      toCellText_(item.number),
      toCellText_(item.text),
      "=>",
      toCellText_(item.answerLabel),
      item.score === null || item.score === undefined ? "" : "(" + item.score + "점)"
    ].filter(Boolean).join(" ");

    const subText = (item.subAnswers || []).map(function(subItem) {
      return [
        toCellText_(subItem.number),
        toCellText_(subItem.text),
        "=>",
        toCellText_(subItem.answerLabel),
        subItem.score === null || subItem.score === undefined ? "" : "(" + subItem.score + "점)"
      ].filter(Boolean).join(" ");
    }).filter(Boolean).join(" / ");

    return [baseText, subText].filter(Boolean).join(" || ");
  }).filter(Boolean).join(" ### ");
}

function mergeCounts_(target, partial) {
  Object.keys(partial).forEach(function(key) {
    if (typeof partial[key] === "number") {
      target[key] = (target[key] || 0) + partial[key];
      return;
    }
    if (partial[key]) {
      target[key] = partial[key];
    }
  });
}
