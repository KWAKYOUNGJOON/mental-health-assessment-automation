const crypto = require("crypto");

const GOOGLE_OAUTH_TOKEN_URL = "https://oauth2.googleapis.com/token";
const GOOGLE_SHEETS_API_BASE = "https://sheets.googleapis.com/v4/spreadsheets";
const GOOGLE_DRIVE_API_BASE = "https://www.googleapis.com/drive/v3/files";
const GOOGLE_SYNC_SCOPES = [
  "https://www.googleapis.com/auth/spreadsheets",
  "https://www.googleapis.com/auth/drive"
];
const DEFAULT_SPREADSHEET_TITLE = "MindMap 척도검사 DB";

const SCALE_SYNC_CONFIG = {
  defaults: {
    recordSheetName: "척도검사기록",
    answerSheetName: "척도문항응답",
    questionnaireSheetName: "척도마스터",
    fieldSheetName: "척도문항마스터",
    optionSheetName: "척도선택지마스터"
  },
  recordHeaders: [
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
  answerHeaders: [
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
  questionnaireHeaders: [
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
  fieldHeaders: [
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
  optionHeaders: [
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
  ],
  recordHeaderLabels: [
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
  answerHeaderLabels: [
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
  questionnaireHeaderLabels: [
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
  fieldHeaderLabels: [
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
  optionHeaderLabels: [
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
};

let accessTokenCache = null;

function getDirectGoogleSyncSettings({ env, config }) {
  const parsedJson = parseServiceAccountJson_(env.MH_GOOGLE_SERVICE_ACCOUNT_JSON || "");
  if (parsedJson.errorMessage) {
    return {
      configured: false,
      mode: "direct_api",
      errorMessage: parsedJson.errorMessage
    };
  }

  const clientEmail = normalizeText_(env.MH_GOOGLE_SERVICE_ACCOUNT_EMAIL || parsedJson.value?.client_email);
  const privateKey = normalizePrivateKey_(env.MH_GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY || parsedJson.value?.private_key);

  if (!clientEmail || !privateKey) {
    return {
      configured: false,
      mode: "direct_api"
    };
  }

  const spreadsheetId = normalizeText_(env.MH_GOOGLE_SYNC_SPREADSHEET_ID || config.googleSyncSpreadsheetId);
  const shareWithEmail = normalizeText_(env.MH_GOOGLE_SYNC_SHARE_WITH_EMAIL);
  if (!spreadsheetId && !shareWithEmail) {
    return {
      configured: false,
      mode: "direct_api",
      errorMessage: "Google Sheets API 직접 연동은 MH_GOOGLE_SYNC_SPREADSHEET_ID 또는 MH_GOOGLE_SYNC_SHARE_WITH_EMAIL 설정이 필요합니다."
    };
  }

  return {
    configured: true,
    mode: "direct_api",
    clientEmail,
    privateKey,
    spreadsheetId,
    spreadsheetTitle: normalizeText_(env.MH_GOOGLE_SYNC_SPREADSHEET_TITLE) || DEFAULT_SPREADSHEET_TITLE,
    shareWithEmail
  };
}

async function syncGoogleSheetsDirect(syncConfig, payload) {
  const accessToken = await getServiceAccountAccessToken_(syncConfig);
  let spreadsheetId = normalizeText_(syncConfig.spreadsheetId);
  let createdSpreadsheet = false;

  if (!spreadsheetId) {
    if (!syncConfig.shareWithEmail) {
      throw new Error(
        "직접 Google Sheets API 모드에서 새 시트를 자동 생성하려면 MH_GOOGLE_SYNC_SHARE_WITH_EMAIL 또는 기존 MH_GOOGLE_SYNC_SPREADSHEET_ID 설정이 필요합니다."
      );
    }

    const created = await createSpreadsheet_(accessToken, syncConfig.spreadsheetTitle);
    spreadsheetId = created.spreadsheetId;
    createdSpreadsheet = true;
    await shareSpreadsheet_(accessToken, spreadsheetId, syncConfig.shareWithEmail);
  }

  const metadata = await ensureSyncSheets_(accessToken, spreadsheetId);
  const result = {
    ok: true,
    mode: "direct_api",
    spreadsheetId,
    spreadsheetUrl: buildSpreadsheetUrl_(spreadsheetId),
    createdSpreadsheet,
    recordSheetName: SCALE_SYNC_CONFIG.defaults.recordSheetName,
    answerSheetName: SCALE_SYNC_CONFIG.defaults.answerSheetName,
    questionnaireSheetName: SCALE_SYNC_CONFIG.defaults.questionnaireSheetName,
    fieldSheetName: SCALE_SYNC_CONFIG.defaults.fieldSheetName,
    optionSheetName: SCALE_SYNC_CONFIG.defaults.optionSheetName,
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

  if (Array.isArray(payload.records) && payload.records.length) {
    mergeResult_(result, await upsertRecordPayload_(accessToken, spreadsheetId, metadata, payload));
  }

  if (Array.isArray(payload.questionnaires) && payload.questionnaires.length) {
    mergeResult_(result, await replaceQuestionnairePayload_(accessToken, spreadsheetId, metadata, payload));
  }

  return result;
}

async function getServiceAccountAccessToken_(syncConfig) {
  if (
    accessTokenCache &&
    accessTokenCache.accessToken &&
    accessTokenCache.clientEmail === syncConfig.clientEmail &&
    accessTokenCache.privateKey === syncConfig.privateKey &&
    accessTokenCache.expiresAt > (Date.now() + 60_000)
  ) {
    return accessTokenCache.accessToken;
  }

  const nowSeconds = Math.floor(Date.now() / 1000);
  const assertion = signJwt_({
    iss: syncConfig.clientEmail,
    scope: GOOGLE_SYNC_SCOPES.join(" "),
    aud: GOOGLE_OAUTH_TOKEN_URL,
    exp: nowSeconds + 3600,
    iat: nowSeconds
  }, syncConfig.privateKey);

  const response = await fetch(GOOGLE_OAUTH_TOKEN_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded"
    },
    body: new URLSearchParams({
      grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
      assertion
    })
  });

  const payload = await safeJsonResponse_(response);
  if (!response.ok || !payload?.access_token) {
    throw new Error(payload?.error_description || payload?.error || "Google OAuth 액세스 토큰을 발급받지 못했습니다.");
  }

  accessTokenCache = {
    accessToken: payload.access_token,
    expiresAt: Date.now() + (Number(payload.expires_in || 3600) * 1000),
    clientEmail: syncConfig.clientEmail,
    privateKey: syncConfig.privateKey
  };

  return payload.access_token;
}

function signJwt_(claims, privateKey) {
  const encodedHeader = base64UrlEncode_(JSON.stringify({ alg: "RS256", typ: "JWT" }));
  const encodedPayload = base64UrlEncode_(JSON.stringify(claims));
  const signingInput = `${encodedHeader}.${encodedPayload}`;
  const signer = crypto.createSign("RSA-SHA256");
  signer.update(signingInput);
  signer.end();
  const signature = signer.sign(privateKey);
  return `${signingInput}.${base64UrlEncode_(signature)}`;
}

function base64UrlEncode_(value) {
  const buffer = Buffer.isBuffer(value) ? value : Buffer.from(String(value), "utf8");
  return buffer
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

async function createSpreadsheet_(accessToken, title) {
  const response = await googleApiRequest_(accessToken, GOOGLE_SHEETS_API_BASE, {
    method: "POST",
    body: {
      properties: {
        title: normalizeText_(title) || DEFAULT_SPREADSHEET_TITLE
      },
      sheets: buildDefaultSheets_()
    }
  });

  return {
    spreadsheetId: response.spreadsheetId
  };
}

async function shareSpreadsheet_(accessToken, spreadsheetId, emailAddress) {
  const safeId = encodeURIComponent(spreadsheetId);
  const url = `${GOOGLE_DRIVE_API_BASE}/${safeId}/permissions?sendNotificationEmail=false&supportsAllDrives=true`;
  await googleApiRequest_(accessToken, url, {
    method: "POST",
    body: {
      role: "writer",
      type: "user",
      emailAddress
    }
  });
}

async function ensureSyncSheets_(accessToken, spreadsheetId) {
  let metadata = await getSpreadsheetMetadata_(accessToken, spreadsheetId);
  const existing = new Map((metadata.sheets || []).map((sheet) => [sheet.properties.title, sheet.properties]));
  const requests = [];

  Object.values(SCALE_SYNC_CONFIG.defaults).forEach((sheetName) => {
    if (!existing.has(sheetName)) {
      requests.push({
        addSheet: {
          properties: {
            title: sheetName,
            gridProperties: {
              rowCount: 1000,
              columnCount: 32,
              frozenRowCount: 1
            }
          }
        }
      });
    }
  });

  if (requests.length) {
    await batchUpdateSpreadsheet_(accessToken, spreadsheetId, requests);
    metadata = await getSpreadsheetMetadata_(accessToken, spreadsheetId);
  }

  return metadata;
}

function buildDefaultSheets_() {
  return Object.values(SCALE_SYNC_CONFIG.defaults).map((sheetName) => ({
    properties: {
      title: sheetName,
      gridProperties: {
        rowCount: 1000,
        columnCount: 32,
        frozenRowCount: 1
      }
    }
  }));
}

async function getSpreadsheetMetadata_(accessToken, spreadsheetId) {
  const safeId = encodeURIComponent(spreadsheetId);
  const url = `${GOOGLE_SHEETS_API_BASE}/${safeId}?fields=spreadsheetId,sheets(properties(sheetId,title,gridProperties(rowCount,columnCount,frozenRowCount)))`;
  return googleApiRequest_(accessToken, url, { method: "GET" });
}

async function batchUpdateSpreadsheet_(accessToken, spreadsheetId, requests) {
  const safeId = encodeURIComponent(spreadsheetId);
  const url = `${GOOGLE_SHEETS_API_BASE}/${safeId}:batchUpdate`;
  return googleApiRequest_(accessToken, url, {
    method: "POST",
    body: { requests }
  });
}

async function upsertRecordPayload_(accessToken, spreadsheetId, metadata, payload) {
  const recordRows = payload.records.map((record) => buildRecordRow_(record, payload));
  const answerRows = [];
  payload.records.forEach((record) => {
    buildAnswerRows_(record, payload).forEach((row) => answerRows.push(row));
  });

  const recordResult = await upsertSheetRows_(
    accessToken,
    spreadsheetId,
    metadata,
    SCALE_SYNC_CONFIG.defaults.recordSheetName,
    SCALE_SYNC_CONFIG.recordHeaders,
    SCALE_SYNC_CONFIG.recordHeaderLabels,
    "record_id",
    recordRows
  );
  const answerResult = await upsertSheetRows_(
    accessToken,
    spreadsheetId,
    metadata,
    SCALE_SYNC_CONFIG.defaults.answerSheetName,
    SCALE_SYNC_CONFIG.answerHeaders,
    SCALE_SYNC_CONFIG.answerHeaderLabels,
    "detail_key",
    answerRows
  );

  return {
    recordSheetName: SCALE_SYNC_CONFIG.defaults.recordSheetName,
    answerSheetName: SCALE_SYNC_CONFIG.defaults.answerSheetName,
    recordsInserted: recordResult.inserted,
    recordsUpdated: recordResult.updated,
    answersInserted: answerResult.inserted,
    answersUpdated: answerResult.updated
  };
}

async function replaceQuestionnairePayload_(accessToken, spreadsheetId, metadata, payload) {
  const questionnaireRows = payload.questionnaires.map((questionnaire) => buildQuestionnaireRow_(questionnaire));
  const fieldRows = [];
  const optionRows = [];

  payload.questionnaires.forEach((questionnaire) => {
    buildFieldRows_(questionnaire).forEach((row) => fieldRows.push(row));
    buildOptionRows_(questionnaire).forEach((row) => optionRows.push(row));
  });

  await replaceSheetRows_(
    accessToken,
    spreadsheetId,
    metadata,
    SCALE_SYNC_CONFIG.defaults.questionnaireSheetName,
    SCALE_SYNC_CONFIG.questionnaireHeaders,
    SCALE_SYNC_CONFIG.questionnaireHeaderLabels,
    questionnaireRows
  );
  await replaceSheetRows_(
    accessToken,
    spreadsheetId,
    metadata,
    SCALE_SYNC_CONFIG.defaults.fieldSheetName,
    SCALE_SYNC_CONFIG.fieldHeaders,
    SCALE_SYNC_CONFIG.fieldHeaderLabels,
    fieldRows
  );
  await replaceSheetRows_(
    accessToken,
    spreadsheetId,
    metadata,
    SCALE_SYNC_CONFIG.defaults.optionSheetName,
    SCALE_SYNC_CONFIG.optionHeaders,
    SCALE_SYNC_CONFIG.optionHeaderLabels,
    optionRows
  );

  return {
    questionnaireSheetName: SCALE_SYNC_CONFIG.defaults.questionnaireSheetName,
    fieldSheetName: SCALE_SYNC_CONFIG.defaults.fieldSheetName,
    optionSheetName: SCALE_SYNC_CONFIG.defaults.optionSheetName,
    questionnairesInserted: questionnaireRows.length,
    questionnairesUpdated: 0,
    fieldsInserted: fieldRows.length,
    fieldsUpdated: 0,
    optionsInserted: optionRows.length,
    optionsUpdated: 0
  };
}

async function upsertSheetRows_(accessToken, spreadsheetId, metadata, sheetName, headers, displayHeaders, keyField, rowObjects) {
  const existingValues = await readSheetValues_(accessToken, spreadsheetId, sheetName);
  const keyIndex = headers.indexOf(keyField);
  const rows = Array.isArray(existingValues) ? existingValues.slice(1).map((row) => normalizeRow_(row, headers.length)) : [];
  const rowIndexByKey = new Map();

  rows.forEach((row, index) => {
    const key = normalizeText_(row[keyIndex]);
    if (key) {
      rowIndexByKey.set(key, index);
    }
  });

  let inserted = 0;
  let updated = 0;
  rowObjects.forEach((rowObject) => {
    const key = normalizeText_(rowObject[keyField]);
    if (!key) {
      return;
    }

    const rowValues = headers.map((header) => toCellText_(rowObject[header]));
    if (rowIndexByKey.has(key)) {
      rows[rowIndexByKey.get(key)] = rowValues;
      updated += 1;
      return;
    }

    rowIndexByKey.set(key, rows.length);
    rows.push(rowValues);
    inserted += 1;
  });

  const nextValues = [displayHeaders, ...rows];
  await writeSheetValues_(accessToken, spreadsheetId, metadata, sheetName, nextValues, existingValues.length);

  return { inserted, updated };
}

async function replaceSheetRows_(accessToken, spreadsheetId, metadata, sheetName, headers, displayHeaders, rowObjects) {
  const existingValues = await readSheetValues_(accessToken, spreadsheetId, sheetName);
  const nextValues = [
    displayHeaders,
    ...rowObjects.map((rowObject) => headers.map((header) => toCellText_(rowObject[header])))
  ];
  await writeSheetValues_(accessToken, spreadsheetId, metadata, sheetName, nextValues, existingValues.length);
}

async function readSheetValues_(accessToken, spreadsheetId, sheetName) {
  const safeId = encodeURIComponent(spreadsheetId);
  const range = encodeURIComponent(`${quoteSheetName_(sheetName)}!A1:ZZ`);
  const url = `${GOOGLE_SHEETS_API_BASE}/${safeId}/values/${range}`;
  const response = await googleApiRequest_(accessToken, url, { method: "GET", allowNotFound: true });
  return Array.isArray(response.values) ? response.values : [];
}

async function writeSheetValues_(accessToken, spreadsheetId, metadata, sheetName, values, previousRowCount) {
  const safeId = encodeURIComponent(spreadsheetId);
  const totalRows = Math.max(values.length, 1);
  const totalColumns = Math.max(...values.map((row) => row.length), 1);
  await ensureSheetCapacity_(accessToken, spreadsheetId, metadata, sheetName, totalRows, totalColumns);

  const updateRangeText = `${quoteSheetName_(sheetName)}!A1:${columnToLetter_(totalColumns)}${totalRows}`;
  const updateRange = encodeURIComponent(updateRangeText);

  await googleApiRequest_(accessToken, `${GOOGLE_SHEETS_API_BASE}/${safeId}/values/${updateRange}?valueInputOption=RAW`, {
    method: "PUT",
    body: {
      range: updateRangeText,
      majorDimension: "ROWS",
      values
    }
  });

  if (previousRowCount > totalRows) {
    const clearRange = encodeURIComponent(
      `${quoteSheetName_(sheetName)}!A${totalRows + 1}:${columnToLetter_(totalColumns)}${previousRowCount}`
    );
    await googleApiRequest_(accessToken, `${GOOGLE_SHEETS_API_BASE}/${safeId}/values/${clearRange}:clear`, {
      method: "POST",
      body: {}
    });
  }
}

async function ensureSheetCapacity_(accessToken, spreadsheetId, metadata, sheetName, requiredRows, requiredColumns) {
  const sheetProperties = (metadata.sheets || [])
    .map((sheet) => sheet.properties || {})
    .find((properties) => properties.title === sheetName);

  if (!sheetProperties || !sheetProperties.sheetId) {
    return;
  }

  const currentRows = Number(sheetProperties.gridProperties?.rowCount || 0);
  const currentColumns = Number(sheetProperties.gridProperties?.columnCount || 0);
  const frozenRows = Number(sheetProperties.gridProperties?.frozenRowCount || 0);

  if (currentRows >= requiredRows && currentColumns >= requiredColumns && frozenRows >= 1) {
    return;
  }

  await batchUpdateSpreadsheet_(accessToken, spreadsheetId, [
    {
      updateSheetProperties: {
        properties: {
          sheetId: sheetProperties.sheetId,
          gridProperties: {
            rowCount: Math.max(currentRows, requiredRows),
            columnCount: Math.max(currentColumns, requiredColumns),
            frozenRowCount: 1
          }
        },
        fields: "gridProperties.rowCount,gridProperties.columnCount,gridProperties.frozenRowCount"
      }
    }
  ]);

  sheetProperties.gridProperties = {
    ...(sheetProperties.gridProperties || {}),
    rowCount: Math.max(currentRows, requiredRows),
    columnCount: Math.max(currentColumns, requiredColumns),
    frozenRowCount: 1
  };
}

async function googleApiRequest_(accessToken, url, options = {}) {
  const response = await fetch(url, {
    method: options.method || "GET",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/json; charset=utf-8"
    },
    body: options.body === undefined ? undefined : JSON.stringify(options.body)
  });

  if (options.allowNotFound && response.status === 404) {
    return {};
  }

  const payload = await safeJsonResponse_(response);
  if (!response.ok) {
    const message = payload?.error?.message || payload?.message || `Google API 호출 실패 (${response.status})`;
    throw new Error(message);
  }

  return payload || {};
}

async function safeJsonResponse_(response) {
  const text = await response.text();
  if (!text) {
    return {};
  }

  try {
    return JSON.parse(text);
  } catch (error) {
    throw new Error("Google API 응답을 JSON으로 해석하지 못했습니다.");
  }
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
    intro_text: joinScaleTextList_(questionnaire.intro),
    source_reference_page: toCellText_(questionnaire.source && questionnaire.source.referencePage),
    source_institution: toCellText_(questionnaire.source && questionnaire.source.institution),
    source_citation: toCellText_(questionnaire.source && questionnaire.source.citation),
    scoring_type: toCellText_(questionnaire.scoring && questionnaire.scoring.type),
    scoring_json: safeStringifyScaleValue_(questionnaire.scoring || {}),
    extraction_notes_json: safeStringifyScaleValue_(questionnaire.extractionNotes || []),
    questionnaire_json: safeStringifyScaleValue_(questionnaire)
  };
}

function buildFieldRows_(questionnaire) {
  const rows = [];

  (questionnaire.respondentFields || []).forEach((field) => {
    rows.push(buildFieldRow_(questionnaire, "respondent", "", field));
  });

  (questionnaire.questions || []).forEach((question) => {
    rows.push(buildFieldRow_(questionnaire, "question", "", question));
    (question.subQuestions || []).forEach((subQuestion) => {
      rows.push(buildFieldRow_(questionnaire, "subquestion", question.id, subQuestion));
    });
  });

  return rows;
}

function buildOptionRows_(questionnaire) {
  const rows = [];

  (questionnaire.respondentFields || []).forEach((field) => {
    buildOptionRowsForField_(questionnaire, "respondent", "", field).forEach((row) => rows.push(row));
  });

  (questionnaire.questions || []).forEach((question) => {
    buildOptionRowsForField_(questionnaire, "question", "", question).forEach((row) => rows.push(row));
    (question.subQuestions || []).forEach((subQuestion) => {
      buildOptionRowsForField_(questionnaire, "subquestion", question.id, subQuestion).forEach((row) => rows.push(row));
    });
  });

  return rows;
}

function buildFieldRow_(questionnaire, fieldScope, parentFieldId, field) {
  return {
    field_key: `${toCellText_(questionnaire.id)}::${fieldScope}::${toCellText_(field.id)}`,
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
    field_json: safeStringifyScaleValue_(field)
  };
}

function buildOptionRowsForField_(questionnaire, fieldScope, parentFieldId, field) {
  return (field.options || []).map((option, index) => ({
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
    option_json: safeStringifyScaleValue_(option)
  }));
}

function buildRecordRow_(record, payload) {
  const respondentDisplay = Array.isArray(record.respondentDisplay) ? record.respondentDisplay : [];
  const progress = record && record.progress ? record.progress : {};
  const flags = Array.isArray(record.evaluation && record.evaluation.flags)
    ? record.evaluation.flags
      .map((flag) => (flag && flag.text ? flag.text : ""))
      .filter(Boolean)
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
    highlights: joinScaleTextList_(record.evaluation && record.evaluation.highlights),
    flags: joinScaleTextList_(flags),
    respondent_summary: buildRespondentSummary_(respondentDisplay),
    breakdown_summary: buildBreakdownSummary_(record.breakdown),
    record_json: safeStringifyScaleValue_(record)
  };
}

function buildAnswerRows_(record, payload) {
  const rows = [];
  const breakdown = Array.isArray(record.breakdown) ? record.breakdown : [];

  breakdown.forEach((item) => {
    const parentKey = `${toCellText_(record.id)}::${toCellText_(item.id || item.number)}`;
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
      raw_json: safeStringifyScaleValue_(item)
    });

    (Array.isArray(item.subAnswers) ? item.subAnswers : []).forEach((subItem, index) => {
      rows.push({
        detail_key: `${parentKey}::sub::${String(index + 1)}`,
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
        question_id: `${toCellText_(item.id || item.number)}::sub::${String(index + 1)}`,
        question_number: toCellText_(subItem.number),
        question_text: toCellText_(subItem.text),
        answer_label: toCellText_(subItem.answerLabel),
        score: subItem.score === null || subItem.score === undefined ? "" : String(subItem.score),
        raw_json: safeStringifyScaleValue_(subItem)
      });
    });
  });

  return rows;
}

function mergeResult_(target, partial) {
  Object.keys(partial).forEach((key) => {
    if (typeof partial[key] === "number") {
      target[key] = (target[key] || 0) + partial[key];
      return;
    }
    if (partial[key]) {
      target[key] = partial[key];
    }
  });
}

function normalizeRow_(row, length) {
  const next = Array.isArray(row) ? row.slice(0, length) : [];
  while (next.length < length) {
    next.push("");
  }
  return next;
}

function columnToLetter_(index) {
  let column = "";
  let current = Number(index || 0);
  while (current > 0) {
    const remainder = (current - 1) % 26;
    column = String.fromCharCode(65 + remainder) + column;
    current = Math.floor((current - 1) / 26);
  }
  return column || "A";
}

function quoteSheetName_(sheetName) {
  return `'${String(sheetName).replace(/'/g, "''")}'`;
}

function buildSpreadsheetUrl_(spreadsheetId) {
  return `https://docs.google.com/spreadsheets/d/${encodeURIComponent(spreadsheetId)}/edit`;
}

function parseServiceAccountJson_(value) {
  const text = normalizeText_(value);
  if (!text) {
    return { value: null };
  }

  try {
    return { value: JSON.parse(text) };
  } catch (error) {
    return {
      value: null,
      errorMessage: "MH_GOOGLE_SERVICE_ACCOUNT_JSON 값을 JSON으로 해석하지 못했습니다."
    };
  }
}

function normalizePrivateKey_(value) {
  const text = normalizeText_(value);
  if (!text) {
    return "";
  }
  return text.replace(/\\n/g, "\n");
}

function findDisplayValueByLabel_(items, label) {
  const matched = (items || []).find((item) => item && item.label === label);
  return matched ? toCellText_(matched.value) : "";
}

function buildRespondentSummary_(items) {
  return (items || [])
    .map((item) => {
      if (!item || !item.label || !item.value) {
        return "";
      }
      return `${item.label}: ${item.value}`;
    })
    .filter(Boolean)
    .join(" | ");
}

function buildProgressSummary_(progress) {
  if (!progress || progress.percent === null || progress.percent === undefined) {
    return "";
  }
  const answered = progress.answered === null || progress.answered === undefined ? "" : String(progress.answered);
  const total = progress.total === null || progress.total === undefined ? "" : String(progress.total);
  return `${String(progress.percent)}% (${answered}/${total}항목)`;
}

function buildBreakdownSummary_(items) {
  return (items || [])
    .map((item) => {
      if (!item) {
        return "";
      }

      const baseText = [
        toCellText_(item.number),
        toCellText_(item.text),
        "=>",
        toCellText_(item.answerLabel),
        item.score === null || item.score === undefined ? "" : `(${item.score}점)`
      ].filter(Boolean).join(" ");

      const subText = (item.subAnswers || [])
        .map((subItem) => [
          toCellText_(subItem.number),
          toCellText_(subItem.text),
          "=>",
          toCellText_(subItem.answerLabel),
          subItem.score === null || subItem.score === undefined ? "" : `(${subItem.score}점)`
        ].filter(Boolean).join(" "))
        .filter(Boolean)
        .join(" / ");

      return [baseText, subText].filter(Boolean).join(" || ");
    })
    .filter(Boolean)
    .join(" ### ");
}

function joinScaleTextList_(items) {
  return (items || [])
    .map((item) => toCellText_(item))
    .filter(Boolean)
    .join(" | ");
}

function safeStringifyScaleValue_(value) {
  try {
    return JSON.stringify(value);
  } catch (error) {
    return "";
  }
}

function toCellText_(value) {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "string") {
    return value;
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  return safeStringifyScaleValue_(value);
}

function normalizeText_(value) {
  return value === null || value === undefined ? "" : String(value).trim();
}

module.exports = {
  getDirectGoogleSyncSettings,
  syncGoogleSheetsDirect
};
