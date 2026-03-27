const SCALE_SCREENING_SYNC_CONFIG = {
  propertyKeys: {
    token: "scale_screening_sync_token",
    spreadsheetId: "scale_screening_target_spreadsheet_id",
    recordSheetName: "scale_screening_record_sheet_name",
    answerSheetName: "scale_screening_answer_sheet_name",
    questionnaireSheetName: "scale_screening_questionnaire_sheet_name",
    fieldSheetName: "scale_screening_field_sheet_name",
    optionSheetName: "scale_screening_option_sheet_name"
  },
  defaults: {
    targetSpreadsheetId: "129w-AhjiLg2fxGqssFeC4vcQJv9hG89M4SAxXXEKrCU",
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

function setupScaleScreeningSyncSheets() {
  setScaleScreeningTargetToCurrentSpreadsheet_(false);

  const recordSheet = ensureScaleScreeningSyncSheet_(
    getScaleScreeningRecordSheetName_(),
    SCALE_SCREENING_SYNC_CONFIG.recordHeaders
  );
  const answerSheet = ensureScaleScreeningSyncSheet_(
    getScaleScreeningAnswerSheetName_(),
    SCALE_SCREENING_SYNC_CONFIG.answerHeaders
  );
  const questionnaireSheet = ensureScaleScreeningSyncSheet_(
    getScaleScreeningQuestionnaireSheetName_(),
    SCALE_SCREENING_SYNC_CONFIG.questionnaireHeaders
  );
  const fieldSheet = ensureScaleScreeningSyncSheet_(
    getScaleScreeningFieldSheetName_(),
    SCALE_SCREENING_SYNC_CONFIG.fieldHeaders
  );
  const optionSheet = ensureScaleScreeningSyncSheet_(
    getScaleScreeningOptionSheetName_(),
    SCALE_SCREENING_SYNC_CONFIG.optionHeaders
  );

  formatScaleScreeningSyncSheet_(recordSheet, SCALE_SCREENING_SYNC_CONFIG.recordHeaders.length);
  formatScaleScreeningSyncSheet_(answerSheet, SCALE_SCREENING_SYNC_CONFIG.answerHeaders.length);
  formatScaleScreeningSyncSheet_(questionnaireSheet, SCALE_SCREENING_SYNC_CONFIG.questionnaireHeaders.length);
  formatScaleScreeningSyncSheet_(fieldSheet, SCALE_SCREENING_SYNC_CONFIG.fieldHeaders.length);
  formatScaleScreeningSyncSheet_(optionSheet, SCALE_SCREENING_SYNC_CONFIG.optionHeaders.length);

  SpreadsheetApp.getUi().alert(
    "척도검사 시트 준비",
    [
      "척도검사 구글 시트 연동용 시트를 준비했습니다.",
      "기록 시트: " + recordSheet.getName(),
      "문항 시트: " + answerSheet.getName(),
      "척도 시트: " + questionnaireSheet.getName(),
      "문항마스터 시트: " + fieldSheet.getName(),
      "선택지마스터 시트: " + optionSheet.getName(),
      "다음 단계로 '척도검사 토큰 저장'을 실행한 뒤 웹앱에 같은 토큰을 입력하세요."
    ].join("\n"),
    SpreadsheetApp.getUi().ButtonSet.OK
  );
}

function setScaleScreeningTargetToCurrentSpreadsheet() {
  const spreadsheet = setScaleScreeningTargetToCurrentSpreadsheet_(true);

  SpreadsheetApp.getUi().alert(
    "척도검사 대상 시트 지정",
    [
      "현재 스프레드시트를 척도검사 DB 대상으로 저장했습니다.",
      "시트명: " + spreadsheet.getName(),
      "시트 ID: " + spreadsheet.getId()
    ].join("\n"),
    SpreadsheetApp.getUi().ButtonSet.OK
  );
}

function setScaleScreeningTargetToCurrentSpreadsheet_(showToast) {
  const spreadsheet = getSpreadsheet_();
  PropertiesService.getScriptProperties().setProperty(
    SCALE_SCREENING_SYNC_CONFIG.propertyKeys.spreadsheetId,
    spreadsheet.getId()
  );

  if (showToast) {
    showToast_("현재 스프레드시트를 척도검사 DB 대상으로 저장했습니다.");
  }

  return spreadsheet;
}

function promptScaleScreeningSyncToken() {
  const ui = SpreadsheetApp.getUi();
  const response = ui.prompt(
    "척도검사 토큰 저장",
    "웹앱과 동일하게 사용할 임의의 토큰을 입력하세요.",
    ui.ButtonSet.OK_CANCEL
  );

  if (response.getSelectedButton() !== ui.Button.OK) {
    return;
  }

  const token = normalizeText_(response.getResponseText());
  if (!token) {
    ui.alert("토큰 저장", "비어 있는 토큰은 저장할 수 없습니다.", ui.ButtonSet.OK);
    return;
  }

  PropertiesService.getScriptProperties().setProperty(SCALE_SCREENING_SYNC_CONFIG.propertyKeys.token, token);
  ui.alert("토큰 저장", "척도검사 동기화 토큰을 저장했습니다.", ui.ButtonSet.OK);
}

function showScaleScreeningSyncStatus() {
  const status = buildScaleScreeningSyncStatus_();

  SpreadsheetApp.getUi().alert(
    "척도검사 연동 상태",
    [
      "웹앱 배포 준비 상태를 확인했습니다.",
      "대상 스프레드시트: " + status.spreadsheetName,
      "대상 스프레드시트 ID: " + status.spreadsheetId,
      "토큰 저장 여부: " + (status.tokenConfigured ? "예" : "아니오"),
      "기록 시트: " + status.recordSheetName + " (" + status.recordRowCount + "행)",
      "문항 시트: " + status.answerSheetName + " (" + status.answerRowCount + "행)",
      "척도 시트: " + status.questionnaireSheetName + " (" + status.questionnaireRowCount + "행)",
      "문항마스터 시트: " + status.fieldSheetName + " (" + status.fieldRowCount + "행)",
      "선택지마스터 시트: " + status.optionSheetName + " (" + status.optionRowCount + "행)",
      "웹앱 URL은 '배포 > 새 배포 > 유형: 웹 앱'으로 발급하세요."
    ].join("\n"),
    SpreadsheetApp.getUi().ButtonSet.OK
  );
}

function doGet() {
  return createScaleScreeningJsonOutput_(buildScaleScreeningSyncStatus_());
}

function doPost(e) {
  const lock = LockService.getDocumentLock();
  let hasLock = false;

  try {
    lock.waitLock(30000);
    hasLock = true;

    const payload = parseScaleScreeningSyncPayload_(e);
    validateScaleScreeningSyncToken_(payload.token);

    const result = upsertScaleScreeningPayload_(payload);
    return createScaleScreeningJsonOutput_({
      ok: true,
      recordSheetName: result.recordSheetName,
      answerSheetName: result.answerSheetName,
      questionnaireSheetName: result.questionnaireSheetName,
      fieldSheetName: result.fieldSheetName,
      optionSheetName: result.optionSheetName,
      recordsInserted: result.recordsInserted,
      recordsUpdated: result.recordsUpdated,
      answersInserted: result.answersInserted,
      answersUpdated: result.answersUpdated,
      questionnairesInserted: result.questionnairesInserted,
      questionnairesUpdated: result.questionnairesUpdated,
      fieldsInserted: result.fieldsInserted,
      fieldsUpdated: result.fieldsUpdated,
      optionsInserted: result.optionsInserted,
      optionsUpdated: result.optionsUpdated
    });
  } catch (error) {
    console.error("척도검사 시트 저장 실패:", error);
    return createScaleScreeningJsonOutput_({
      ok: false,
      error: error.message
    });
  } finally {
    if (hasLock) {
      lock.releaseLock();
    }
  }
}

function buildScaleScreeningSyncStatus_() {
  const spreadsheet = getScaleScreeningTargetSpreadsheet_();
  const recordSheetName = getScaleScreeningRecordSheetName_();
  const answerSheetName = getScaleScreeningAnswerSheetName_();
  const questionnaireSheetName = getScaleScreeningQuestionnaireSheetName_();
  const fieldSheetName = getScaleScreeningFieldSheetName_();
  const optionSheetName = getScaleScreeningOptionSheetName_();
  const recordSheet = getScaleScreeningSheetIfExists_(recordSheetName);
  const answerSheet = getScaleScreeningSheetIfExists_(answerSheetName);
  const questionnaireSheet = getScaleScreeningSheetIfExists_(questionnaireSheetName);
  const fieldSheet = getScaleScreeningSheetIfExists_(fieldSheetName);
  const optionSheet = getScaleScreeningSheetIfExists_(optionSheetName);

  return {
    ok: true,
    tokenConfigured: Boolean(getScaleScreeningSyncToken_()),
    spreadsheetId: spreadsheet.getId(),
    spreadsheetName: spreadsheet.getName(),
    recordSheetName: recordSheetName,
    answerSheetName: answerSheetName,
    questionnaireSheetName: questionnaireSheetName,
    fieldSheetName: fieldSheetName,
    optionSheetName: optionSheetName,
    recordRowCount: Math.max((recordSheet && recordSheet.getLastRow()) || 0, 1) - 1,
    answerRowCount: Math.max((answerSheet && answerSheet.getLastRow()) || 0, 1) - 1,
    questionnaireRowCount: Math.max((questionnaireSheet && questionnaireSheet.getLastRow()) || 0, 1) - 1,
    fieldRowCount: Math.max((fieldSheet && fieldSheet.getLastRow()) || 0, 1) - 1,
    optionRowCount: Math.max((optionSheet && optionSheet.getLastRow()) || 0, 1) - 1
  };
}

function parseScaleScreeningSyncPayload_(e) {
  if (!e || !e.postData || !e.postData.contents) {
    throw new Error("요청 본문이 비어 있습니다.");
  }

  let payload;
  try {
    payload = JSON.parse(e.postData.contents);
  } catch (error) {
    throw new Error("JSON 본문을 해석할 수 없습니다.");
  }

  const hasRecords = payload && Array.isArray(payload.records) && payload.records.length;
  const hasQuestionnaires = payload && Array.isArray(payload.questionnaires) && payload.questionnaires.length;

  if (!hasRecords && !hasQuestionnaires) {
    throw new Error("전송할 검사 기록 또는 척도 마스터가 없습니다.");
  }

  return payload;
}

function validateScaleScreeningSyncToken_(token) {
  const configuredToken = getScaleScreeningSyncToken_();

  if (!configuredToken) {
    return;
  }

  if (normalizeText_(token) !== configuredToken) {
    throw new Error("동기화 토큰이 일치하지 않습니다.");
  }
}

function upsertScaleScreeningPayload_(payload) {
  const result = {
    recordSheetName: "",
    answerSheetName: "",
    questionnaireSheetName: "",
    fieldSheetName: "",
    optionSheetName: "",
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
    mergeScaleScreeningResult_(result, upsertScaleScreeningRecordPayload_(payload));
  }

  if (Array.isArray(payload.questionnaires) && payload.questionnaires.length) {
    mergeScaleScreeningResult_(result, upsertScaleScreeningQuestionnairePayload_(payload));
  }

  return result;
}

function mergeScaleScreeningResult_(target, partial) {
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

function upsertScaleScreeningRecordPayload_(payload) {
  const recordSheet = ensureScaleScreeningSyncSheet_(
    getScaleScreeningRecordSheetName_(),
    SCALE_SCREENING_SYNC_CONFIG.recordHeaders
  );
  const answerSheet = ensureScaleScreeningSyncSheet_(
    getScaleScreeningAnswerSheetName_(),
    SCALE_SCREENING_SYNC_CONFIG.answerHeaders
  );

  const recordRows = payload.records.map(function(record) {
    return buildScaleScreeningRecordRow_(record, payload);
  });

  const answerRows = [];
  payload.records.forEach(function(record) {
    buildScaleScreeningAnswerRows_(record, payload).forEach(function(row) {
      answerRows.push(row);
    });
  });

  const recordResult = upsertRowsByKey_(
    recordSheet,
    SCALE_SCREENING_SYNC_CONFIG.recordHeaders,
    recordRows,
    "record_id"
  );
  const answerResult = upsertRowsByKey_(
    answerSheet,
    SCALE_SCREENING_SYNC_CONFIG.answerHeaders,
    answerRows,
    "detail_key"
  );

  return {
    recordSheetName: recordSheet.getName(),
    answerSheetName: answerSheet.getName(),
    recordsInserted: recordResult.inserted,
    recordsUpdated: recordResult.updated,
    answersInserted: answerResult.inserted,
    answersUpdated: answerResult.updated
  };
}

function upsertScaleScreeningQuestionnairePayload_(payload) {
  const questionnaireSheet = ensureScaleScreeningSyncSheet_(
    getScaleScreeningQuestionnaireSheetName_(),
    SCALE_SCREENING_SYNC_CONFIG.questionnaireHeaders
  );
  const fieldSheet = ensureScaleScreeningSyncSheet_(
    getScaleScreeningFieldSheetName_(),
    SCALE_SCREENING_SYNC_CONFIG.fieldHeaders
  );
  const optionSheet = ensureScaleScreeningSyncSheet_(
    getScaleScreeningOptionSheetName_(),
    SCALE_SCREENING_SYNC_CONFIG.optionHeaders
  );

  clearScaleScreeningDataRows_(questionnaireSheet, SCALE_SCREENING_SYNC_CONFIG.questionnaireHeaders.length);
  clearScaleScreeningDataRows_(fieldSheet, SCALE_SCREENING_SYNC_CONFIG.fieldHeaders.length);
  clearScaleScreeningDataRows_(optionSheet, SCALE_SCREENING_SYNC_CONFIG.optionHeaders.length);

  const questionnaireRows = payload.questionnaires.map(function(questionnaire) {
    return buildScaleScreeningQuestionnaireRow_(questionnaire);
  });

  const fieldRows = [];
  const optionRows = [];
  payload.questionnaires.forEach(function(questionnaire) {
    buildScaleScreeningFieldRows_(questionnaire).forEach(function(row) {
      fieldRows.push(row);
    });
    buildScaleScreeningOptionRows_(questionnaire).forEach(function(row) {
      optionRows.push(row);
    });
  });

  const questionnaireResult = upsertRowsByKey_(
    questionnaireSheet,
    SCALE_SCREENING_SYNC_CONFIG.questionnaireHeaders,
    questionnaireRows,
    "questionnaire_id"
  );
  const fieldResult = upsertRowsByKey_(
    fieldSheet,
    SCALE_SCREENING_SYNC_CONFIG.fieldHeaders,
    fieldRows,
    "field_key"
  );
  const optionResult = upsertRowsByKey_(
    optionSheet,
    SCALE_SCREENING_SYNC_CONFIG.optionHeaders,
    optionRows,
    "option_key"
  );

  return {
    questionnaireSheetName: questionnaireSheet.getName(),
    fieldSheetName: fieldSheet.getName(),
    optionSheetName: optionSheet.getName(),
    questionnairesInserted: questionnaireResult.inserted,
    questionnairesUpdated: questionnaireResult.updated,
    fieldsInserted: fieldResult.inserted,
    fieldsUpdated: fieldResult.updated,
    optionsInserted: optionResult.inserted,
    optionsUpdated: optionResult.updated
  };
}

function buildScaleScreeningQuestionnaireRow_(questionnaire) {
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

function buildScaleScreeningFieldRows_(questionnaire) {
  const rows = [];

  (questionnaire.respondentFields || []).forEach(function(field) {
    rows.push(buildScaleFieldRow_(questionnaire, "respondent", "", field));
  });

  (questionnaire.questions || []).forEach(function(question) {
    rows.push(buildScaleFieldRow_(questionnaire, "question", "", question));
    (question.subQuestions || []).forEach(function(subQuestion) {
      rows.push(buildScaleFieldRow_(questionnaire, "subquestion", question.id, subQuestion));
    });
  });

  return rows;
}

function buildScaleScreeningOptionRows_(questionnaire) {
  const rows = [];

  (questionnaire.respondentFields || []).forEach(function(field) {
    buildScaleOptionRowsForField_(questionnaire, "respondent", "", field).forEach(function(row) {
      rows.push(row);
    });
  });

  (questionnaire.questions || []).forEach(function(question) {
    buildScaleOptionRowsForField_(questionnaire, "question", "", question).forEach(function(row) {
      rows.push(row);
    });
    (question.subQuestions || []).forEach(function(subQuestion) {
      buildScaleOptionRowsForField_(questionnaire, "subquestion", question.id, subQuestion).forEach(function(row) {
        rows.push(row);
      });
    });
  });

  return rows;
}

function buildScaleFieldRow_(questionnaire, fieldScope, parentFieldId, field) {
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
    field_json: safeStringifyScaleValue_(field)
  };
}

function buildScaleOptionRowsForField_(questionnaire, fieldScope, parentFieldId, field) {
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
      option_json: safeStringifyScaleValue_(option)
    };
  });
}

function buildScaleScreeningRecordRow_(record, payload) {
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
    progress_summary: buildScaleScreeningProgressSummary_(progress),
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

function buildScaleScreeningAnswerRows_(record, payload) {
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
      raw_json: safeStringifyScaleValue_(item)
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
        question_id: (toCellText_(item.id || item.number) + "::sub::" + String(index + 1)),
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

function upsertRowsByKey_(sheet, headers, rowObjects, keyField) {
  const existingRows = getExistingRowNumberMap_(sheet, headers, keyField);
  const appendRows = [];
  let inserted = 0;
  let updated = 0;

  rowObjects.forEach(function(rowObject) {
    const key = normalizeText_(rowObject[keyField]);
    if (!key) {
      return;
    }

    const rowValues = headers.map(function(header) {
      return toCellText_(rowObject[header]);
    });

    if (existingRows[key]) {
      sheet.getRange(existingRows[key], 1, 1, headers.length).setValues([rowValues]);
      updated += 1;
      return;
    }

    appendRows.push(rowValues);
    existingRows[key] = sheet.getLastRow() + appendRows.length;
    inserted += 1;
  });

  if (appendRows.length) {
    const startRow = sheet.getLastRow() + 1;
    ensureSheetSize_(sheet, startRow + appendRows.length, headers.length);
    sheet.getRange(startRow, 1, appendRows.length, headers.length).setValues(appendRows);
  }

  return {
    inserted: inserted,
    updated: updated
  };
}

function getExistingRowNumberMap_(sheet, headers, keyField) {
  const keyIndex = headers.indexOf(keyField);
  const result = {};

  if (keyIndex === -1 || sheet.getLastRow() < 2) {
    return result;
  }

  const values = sheet.getRange(2, keyIndex + 1, sheet.getLastRow() - 1, 1).getDisplayValues();
  values.forEach(function(row, index) {
    const key = normalizeText_(row[0]);
    if (key) {
      result[key] = index + 2;
    }
  });

  return result;
}

function ensureScaleScreeningSyncSheet_(sheetName, headers) {
  const sheet = getOrCreateScaleScreeningSheet_(sheetName);
  const normalizedHeaders = headers.map(function(header) {
    return normalizeText_(header);
  });
  const displayHeaders = getScaleScreeningDisplayHeaders_(headers).map(function(header) {
    return normalizeText_(header);
  });

  ensureSheetSize_(sheet, 2, normalizedHeaders.length);
  sheet.getRange(1, 1, 1, displayHeaders.length).setValues([displayHeaders]);

  formatScaleScreeningSyncSheet_(sheet, normalizedHeaders.length);
  return sheet;
}

function clearScaleScreeningDataRows_(sheet, minimumColumns) {
  ensureSheetSize_(sheet, 2, minimumColumns);

  if (sheet.getMaxRows() > 1) {
    sheet.deleteRows(2, sheet.getMaxRows() - 1);
  }

  ensureSheetSize_(sheet, 2, minimumColumns);
}

function getScaleScreeningDisplayHeaders_(headers) {
  if (headers === SCALE_SCREENING_SYNC_CONFIG.recordHeaders) {
    return SCALE_SCREENING_SYNC_CONFIG.recordHeaderLabels;
  }
  if (headers === SCALE_SCREENING_SYNC_CONFIG.answerHeaders) {
    return SCALE_SCREENING_SYNC_CONFIG.answerHeaderLabels;
  }
  if (headers === SCALE_SCREENING_SYNC_CONFIG.questionnaireHeaders) {
    return SCALE_SCREENING_SYNC_CONFIG.questionnaireHeaderLabels;
  }
  if (headers === SCALE_SCREENING_SYNC_CONFIG.fieldHeaders) {
    return SCALE_SCREENING_SYNC_CONFIG.fieldHeaderLabels;
  }
  if (headers === SCALE_SCREENING_SYNC_CONFIG.optionHeaders) {
    return SCALE_SCREENING_SYNC_CONFIG.optionHeaderLabels;
  }
  return headers;
}

function getScaleScreeningTargetSpreadsheet_() {
  const activeSpreadsheet = tryGetActiveSpreadsheet_();
  const configuredId = normalizeText_(PropertiesService.getScriptProperties().getProperty(
    SCALE_SCREENING_SYNC_CONFIG.propertyKeys.spreadsheetId
  ));
  const defaultId = SCALE_SCREENING_SYNC_CONFIG.defaults.targetSpreadsheetId;

  if (configuredId) {
    try {
      return SpreadsheetApp.openById(configuredId);
    } catch (error) {
      if (activeSpreadsheet && activeSpreadsheet.getId() === configuredId) {
        return activeSpreadsheet;
      }
      throw new Error(
        "저장된 대상 스프레드시트에 접근할 수 없습니다. '현재 시트를 척도검사 DB로 지정' 메뉴를 다시 실행하거나, 실행 계정에 시트 권한을 부여해주세요."
      );
    }
  }

  if (activeSpreadsheet) {
    return activeSpreadsheet;
  }

  if (defaultId) {
    try {
      return SpreadsheetApp.openById(defaultId);
    } catch (error) {
      throw new Error(
        "기본 대상 스프레드시트에 접근할 수 없습니다. 대상 시트에 바운드된 Apps Script에서 '현재 시트를 척도검사 DB로 지정'을 먼저 실행해주세요."
      );
    }
  }

  throw new Error("대상 스프레드시트를 확인할 수 없습니다.");
}

function getScaleScreeningSheetIfExists_(sheetName) {
  return getScaleScreeningTargetSpreadsheet_().getSheetByName(sheetName);
}

function getOrCreateScaleScreeningSheet_(sheetName) {
  const spreadsheet = getScaleScreeningTargetSpreadsheet_();
  return spreadsheet.getSheetByName(sheetName) || spreadsheet.insertSheet(sheetName);
}

function tryGetActiveSpreadsheet_() {
  try {
    return SpreadsheetApp.getActiveSpreadsheet();
  } catch (error) {
    return null;
  }
}

function isHeaderRowBlank_(sheet, headerLength) {
  if (!sheet || sheet.getLastRow() < 1) {
    return true;
  }
  const values = sheet.getRange(1, 1, 1, headerLength).getDisplayValues()[0];
  return values.every(function(value) {
    return !normalizeText_(value);
  });
}

function formatScaleScreeningSyncSheet_(sheet, columnCount) {
  sheet.setFrozenRows(1);
  styleHeaderRow_(sheet, 1, columnCount);
  sheet.autoResizeColumns(1, Math.min(columnCount, sheet.getMaxColumns()));
}

function getScaleScreeningSyncToken_() {
  return normalizeText_(PropertiesService.getScriptProperties().getProperty(
    SCALE_SCREENING_SYNC_CONFIG.propertyKeys.token
  ));
}

function getScaleScreeningRecordSheetName_() {
  return normalizeText_(PropertiesService.getScriptProperties().getProperty(
    SCALE_SCREENING_SYNC_CONFIG.propertyKeys.recordSheetName
  )) || SCALE_SCREENING_SYNC_CONFIG.defaults.recordSheetName;
}

function getScaleScreeningAnswerSheetName_() {
  return normalizeText_(PropertiesService.getScriptProperties().getProperty(
    SCALE_SCREENING_SYNC_CONFIG.propertyKeys.answerSheetName
  )) || SCALE_SCREENING_SYNC_CONFIG.defaults.answerSheetName;
}

function getScaleScreeningQuestionnaireSheetName_() {
  return normalizeText_(PropertiesService.getScriptProperties().getProperty(
    SCALE_SCREENING_SYNC_CONFIG.propertyKeys.questionnaireSheetName
  )) || SCALE_SCREENING_SYNC_CONFIG.defaults.questionnaireSheetName;
}

function getScaleScreeningFieldSheetName_() {
  return normalizeText_(PropertiesService.getScriptProperties().getProperty(
    SCALE_SCREENING_SYNC_CONFIG.propertyKeys.fieldSheetName
  )) || SCALE_SCREENING_SYNC_CONFIG.defaults.fieldSheetName;
}

function getScaleScreeningOptionSheetName_() {
  return normalizeText_(PropertiesService.getScriptProperties().getProperty(
    SCALE_SCREENING_SYNC_CONFIG.propertyKeys.optionSheetName
  )) || SCALE_SCREENING_SYNC_CONFIG.defaults.optionSheetName;
}

function createScaleScreeningJsonOutput_(data) {
  return ContentService
    .createTextOutput(JSON.stringify(data, null, 2))
    .setMimeType(ContentService.MimeType.JSON);
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

function buildScaleScreeningProgressSummary_(progress) {
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

function joinScaleTextList_(items) {
  return (items || []).map(function(item) {
    return toCellText_(item);
  }).filter(Boolean).join(" | ");
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
