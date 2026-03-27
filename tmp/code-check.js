const SCALE_WEBAPP_CONFIG = {
  appName: "MindMap 다시서기",
  subtitle: "노숙인 정신건강 척도 관리 웹앱",
  sourceApp: "mindmap-dashiseogi-apps-script-webapp",
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
    spreadsheetId: "11y5p7Cp_yN2vggMOlCwn4pKNBEmio-CmkK25Nyd2nIk"
  },
  legacySpreadsheetIds: [
    "129w-AhjiLg2fxGqssFeC4vcQJv9hG89M4SAxXXEKrCU"
  ],
  sheetNames: {
    records: "척도검사기록",
    answers: "척도문항응답",
    questionnaires: "척도마스터",
    fields: "척도문항마스터",
    options: "척도선택지마스터"
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
  }
};

const SCALE_AUTH_CONFIG = {
  propertyKeys: {
    accounts: "mindmap_webapp_auth_accounts_v1",
    sessions: "mindmap_webapp_auth_sessions_v1"
  },
  sessionTtlHours: 24 * 7,
  defaultAccounts: [
    {
      username: "admin0109",
      password: "admin0109",
      displayName: "관리자",
      role: "admin"
    },
    {
      username: "ryusia7@homeless.go.kr",
      password: "admin0109",
      displayName: "ryusia7@homeless.go.kr",
      role: "admin"
    }
  ]
};

let questionnaireBundleCache_ = null;

function doGet(e) {
  if (e && e.parameter && e.parameter.ping === "1") {
    return ContentService.createTextOutput("ok");
  }

  if (e && e.parameter && e.parameter.format === "json") {
    return createJsonOutput_(getScaleWebappBootstrap(e.parameter.sessionToken || e.parameter.token || ""));
  }

  const template = HtmlService.createTemplateFromFile("Index");
  template.bootstrapJson = JSON.stringify(buildTemplateBootstrap_());

  return template.evaluate()
    .setTitle(SCALE_WEBAPP_CONFIG.appName)
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL)
    .addMetaTag("viewport", "width=device-width, initial-scale=1");
}

function doPost(e) {
  const lock = LockService.getScriptLock();
  let hasLock = false;

  try {
    lock.waitLock(30000);
    hasLock = true;

    const payload = parsePayload_(e);
    validateToken_(payload.token);
    const result = upsertPayload_(payload);

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

function loginScaleWebapp(username, password) {
  const account = authenticateScaleWebappUser_(username, password);
  const sessionContext = createScaleWebappSession_(account);
  return buildBootstrapPayload_(sessionContext);
}

function logoutScaleWebapp(sessionToken) {
  invalidateScaleWebappSession_(sessionToken);
  return buildPublicBootstrap_();
}

function getScaleWebappBootstrap(sessionToken) {
  const normalizedToken = normalizeText_(sessionToken);
  const sessionContext = getSessionContext_(normalizedToken);
  const bootstrap = buildBootstrapPayload_(sessionContext);

  if (normalizedToken && !sessionContext) {
    bootstrap.sessionExpired = true;
    bootstrap.loginHint = "세션이 만료되었습니다. 다시 로그인해주세요.";
  }

  return bootstrap;
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

function syncScaleQuestionnaireMaster(sessionToken) {
  const sessionContext = requireScaleWebappSession_(sessionToken);
  ensureManagedSheets_();
  return {
    ok: true,
    authenticated: true,
    masterSync: syncQuestionnaireMaster_(true),
    status: buildStatusPayload_(sessionContext)
  };
}

function setupScaleScreeningSheets(sessionToken) {
  requireScaleWebappSession_(sessionToken);
  ensureManagedSheets_();
  return {
    ok: true,
    authenticated: true,
    message: "척도검사 저장용 시트를 준비했습니다.",
    targetSpreadsheetId: getTargetSpreadsheetId_()
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

  return {
    ok: true,
    authenticated: true,
    targetSpreadsheetId: normalized
  };
}

function clearTargetSpreadsheetId(sessionToken) {
  requireScaleWebappSession_(sessionToken);
  PropertiesService.getScriptProperties().deleteProperty(SCALE_SYNC_CONFIG.propertyKeys.spreadsheetId);
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

  return {
    ok: true,
    authenticated: true,
    tokenConfigured: true
  };
}

function clearSyncToken(sessionToken) {
  requireScaleWebappSession_(sessionToken);
  PropertiesService.getScriptProperties().deleteProperty(SCALE_SYNC_CONFIG.propertyKeys.token);
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

function buildTemplateBootstrap_() {
  return buildPublicBootstrap_();
}

function buildPublicBootstrap_() {
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
    loginHint: "허용된 계정으로 로그인해주세요.",
    status: null
  };
}

function buildAuthenticatedBootstrap_(sessionContext) {
  return {
    ok: true,
    authenticated: true,
    sessionToken: sessionContext ? sessionContext.token : "",
    currentUser: getCurrentUserContext_(sessionContext),
    appName: SCALE_WEBAPP_CONFIG.appName,
    subtitle: SCALE_WEBAPP_CONFIG.subtitle,
    settings: structuredCloneSafe_(SCALE_WEBAPP_CONFIG.settings),
    loginHint: "로그인 상태가 유지됩니다.",
    status: buildStatusPayload_(sessionContext)
  };
}

function buildBootstrapPayload_(sessionContext) {
  return sessionContext ? buildAuthenticatedBootstrap_(sessionContext) : buildPublicBootstrap_();
}

function buildStatusPayload_(sessionContext) {
  return {
    ok: true,
    authenticated: true,
    currentUser: getCurrentUserContext_(sessionContext),
    targetSpreadsheetId: getTargetSpreadsheetId_(),
    targetSpreadsheetName: getTargetSpreadsheet_().getName(),
    tokenConfigured: Boolean(getSyncToken_()),
    sheets: getSheetStats_(),
    recentRecords: listRecentScaleRecordsInternal_(SCALE_WEBAPP_CONFIG.defaultHistoryLimit)
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

  const recordJsonIndex = SCALE_SYNC_CONFIG.headers.records.indexOf("record_json");
  const records = [];

  values.forEach(function(row) {
    const jsonCell = normalizeText_(row[recordJsonIndex]);
    if (!jsonCell) {
      return;
    }

    try {
      const parsed = JSON.parse(jsonCell);
      if (parsed && parsed.id && parsed.questionnaireId) {
        records.push(parsed);
      }
    } catch (error) {
      // skip invalid rows
    }
  });

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

  if (!account || account.active === false || normalizeText_(account.password) !== normalizedPassword) {
    throw new Error("로그인 정보를 확인해주세요.");
  }

  return normalizeScaleWebappAccount_(account, normalizedUsername);
}

function ensureScaleWebappAccounts_() {
  const properties = PropertiesService.getScriptProperties();
  const raw = normalizeText_(properties.getProperty(SCALE_AUTH_CONFIG.propertyKeys.accounts));
  let accounts = {};

  if (raw) {
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

  return accounts;
}

function normalizeScaleWebappAccount_(account, fallbackUsername) {
  const source = account && typeof account === "object" ? account : {};
  const username = normalizeText_(source.username || fallbackUsername);

  return {
    username: username,
    password: normalizeText_(source.password),
    displayName: normalizeText_(source.displayName) || username,
    role: normalizeText_(source.role) || "user",
    active: source.active !== false,
    createdAt: normalizeText_(source.createdAt) || new Date().toISOString(),
    updatedAt: normalizeText_(source.updatedAt) || new Date().toISOString()
  };
}

function getScaleWebappSessions_() {
  const properties = PropertiesService.getScriptProperties();
  const raw = normalizeText_(properties.getProperty(SCALE_AUTH_CONFIG.propertyKeys.sessions));

  if (!raw) {
    return {};
  }

  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : {};
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

  const lock = LockService.getScriptLock();
  lock.waitLock(30000);
  try {
    const sessions = getScaleWebappSessions_();
    sessions[token] = session;
    saveScaleWebappSessions_(sessions);
  } finally {
    lock.releaseLock();
  }

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

function parsePayload_(e) {
  if (!e || !e.postData || !e.postData.contents) {
    throw new Error("요청 본문이 비어 있습니다.");
  }

  let payload;
  try {
    payload = JSON.parse(e.postData.contents);
  } catch (error) {
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
  let inserted = 0;
  let updated = 0;

  if (keyIndex === -1) {
    throw new Error("키 필드를 찾을 수 없습니다: " + keyField);
  }

  ensureSheetSize_(sheet, Math.max(sheet.getLastRow(), 2), headers.length);

  if (sheet.getLastRow() >= 2) {
    const values = sheet.getRange(2, keyIndex + 1, sheet.getLastRow() - 1, 1).getDisplayValues();
    values.forEach(function(row, index) {
      const key = normalizeText_(row[0]);
      if (key) {
        rowMap[key] = index + 2;
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

    if (rowMap[key]) {
      sheet.getRange(rowMap[key], 1, 1, headers.length).setValues([rowValues]);
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

  formatHeader_(sheet, headers.length);

  return {
    inserted: inserted,
    updated: updated
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

function getTargetSpreadsheetId_() {
  const storedId = normalizeText_(PropertiesService.getScriptProperties().getProperty(
    SCALE_SYNC_CONFIG.propertyKeys.spreadsheetId
  ));

  if (!storedId) {
    return SCALE_SYNC_CONFIG.defaults.spreadsheetId;
  }

  if (SCALE_SYNC_CONFIG.legacySpreadsheetIds.indexOf(storedId) >= 0) {
    return SCALE_SYNC_CONFIG.defaults.spreadsheetId;
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
  const spreadsheet = getTargetSpreadsheet_();
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

