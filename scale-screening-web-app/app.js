(() => {
  "use strict";

  const MANIFEST_URL = "./questionnaires/index.json";
  const HISTORY_KEY = "mh_scale_screening_history_v2";
  const WORKER_NAME_KEY = "mh_scale_worker_name_v1";
  const EMBEDDED_DATA = window.__SCALE_SCREENING_BUNDLE__ || null;
  const IS_ADMIN_PAGE = new URLSearchParams(window.location.search).get("screen") === "admin";
  const DEFAULT_LOCAL_PORT = 8134;
  const DEFAULT_LOCAL_APP_URL = `http://127.0.0.1:${DEFAULT_LOCAL_PORT}/`;
  let appConfig = {
    organizationName: "다시서기종합지원센터",
    teamName: "정신건강팀",
    contactNote: "",
    enabledScales: [],
    primaryColor: "",
    kioskPinSet: false,
    googleSyncConfigured: false
  };

  const KIOSK_MODE = new URLSearchParams(window.location.search).get("mode") === "kiosk";

  const state = {
    manifest: [],
    questionnaires: new Map(),
    currentId: null,
    currentQuestionnaire: null,
    lastResult: null,
    activeView: "screening",
    session: {
      user: null,
      bootstrapRequired: false,
      connectionErrorMessage: ""
    },
    signaturePad: {
      isDrawing: false,
      hasStroke: false
    },
    serverRecords: [],
    users: [],
    clients: [],
    riskFlags: [],
    riskUnacknowledgedCount: 0,
    kioskUnlocked: false,
    bundle: {
      active: false,
      id: null,
      scales: [],
      currentIndex: 0,
      results: [],
      sessionInfo: null
    }
  };

  const ui = {};

  document.addEventListener("DOMContentLoaded", init);

  async function init() {
    cacheUi();
    bindEvents();
    applyPageMode();
    updateGoogleSyncUi();
    await clearServerSessionSilently();
    await loadAppConfig();
    await loadSessionState();
    await loadManifest();
    renderHistory();
  }

  function isFileProtocol() {
    return window.location.protocol === "file:";
  }

  function isApiRequestUrl(url) {
    return typeof url === "string" && url.startsWith("/api/");
  }

  function getLocalAppUrl(screen) {
    const nextUrl = new URL(DEFAULT_LOCAL_APP_URL);
    if (screen) {
      nextUrl.searchParams.set("screen", screen);
    }
    return nextUrl.toString();
  }

  function buildApiConnectionMessage() {
    if (isFileProtocol()) {
      return `이 페이지를 파일로 직접 열면 로그인 기능이 동작하지 않습니다. .\\serve-local.ps1를 실행한 뒤 ${DEFAULT_LOCAL_APP_URL} 주소에서 다시 시도해주세요.`;
    }

    return `로컬 로그인 서버에 연결할 수 없습니다. .\\serve-local.ps1를 실행한 뒤 ${DEFAULT_LOCAL_APP_URL} 주소에서 다시 시도해주세요.`;
  }

  async function clearServerSessionSilently() {
    try {
      await fetch("/api/logout", {
        method: "POST",
        cache: "no-store"
      });
    } catch (error) {
      console.warn("initial logout skipped", error);
    }
  }

  function readAppSettings() {
    return { ...appConfig };
  }

  async function loadAppConfig() {
    try {
      const response = await fetch("/api/config", { cache: "no-store" });
      if (!response.ok) return;
      const data = await response.json();
      if (data && data.ok && data.config) {
        appConfig = { ...appConfig, ...data.config };
        applyConfigToUi();
      }
    } catch (error) {
      console.warn("앱 설정 로드 실패, 기본값 사용", error);
    }
  }

  function applyConfigToUi() {
    if (ui.configOrgName) ui.configOrgName.value = appConfig.organizationName || "";
    if (ui.configTeamName) ui.configTeamName.value = appConfig.teamName || "";
    if (ui.configContactNote) ui.configContactNote.value = appConfig.contactNote || "";
    if (ui.configKioskPin) ui.configKioskPin.value = "";
    if (ui.configKioskPinStatus) {
      ui.configKioskPinStatus.textContent = appConfig.kioskPinSet
        ? "PIN이 설정되어 있습니다. 새 PIN을 입력하면 변경됩니다."
        : "PIN이 설정되지 않았습니다. 키오스크 모드를 사용하려면 PIN을 설정해주세요.";
    }
    updateGoogleSyncUi();
    renderHistory();
  }

  function canUseGoogleSync() {
    return Boolean(appConfig.googleSyncConfigured && state.session.user);
  }

  function getGoogleSyncStatusMessage() {
    if (!appConfig.googleSyncConfigured) {
      return "서버에 구글 시트 연동이 설정되지 않았습니다. 관리자에게 서버 환경변수 설정을 요청해주세요.";
    }

    if (!state.session.user) {
      return "로그인하면 서버 프록시를 통해 구글 시트 전송을 사용할 수 있습니다.";
    }

    return "서버에 설정된 내부 연동으로 전송합니다. 브라우저에는 구글 연동 주소가 노출되지 않습니다.";
  }

  function updateGoogleSyncUi() {
    const canSync = canUseGoogleSync();
    [ui.syncQuestionnairesBtn, ui.syncCurrentBtn, ui.syncHistoryBtn].forEach((button) => {
      if (button) {
        button.disabled = !canSync;
      }
    });
    if (ui.googleSyncHelp) {
      ui.googleSyncHelp.textContent = getGoogleSyncStatusMessage();
    }
    if (ui.syncStatus && !ui.syncStatus.dataset.busy) {
      setSyncStatus(getGoogleSyncStatusMessage(), appConfig.googleSyncConfigured ? "" : "error");
    }
  }

  function cacheUi() {
    ui.questionnaireNav = document.getElementById("questionnaireNav");
    ui.questionnaireCount = document.getElementById("questionnaireCount");
    ui.authStatusText = document.getElementById("authStatusText");
    ui.authMessage = document.getElementById("authMessage");
    ui.bootstrapPanel = document.getElementById("bootstrapPanel");
    ui.bootstrapForm = document.getElementById("bootstrapForm");
    ui.bootstrapUsername = document.getElementById("bootstrapUsername");
    ui.bootstrapDisplayName = document.getElementById("bootstrapDisplayName");
    ui.bootstrapPassword = document.getElementById("bootstrapPassword");
    ui.bootstrapPasswordConfirm = document.getElementById("bootstrapPasswordConfirm");
    ui.loginPanel = document.getElementById("loginPanel");
    ui.loginForm = document.getElementById("loginForm");
    ui.loginUsername = document.getElementById("loginUsername");
    ui.loginPassword = document.getElementById("loginPassword");
    ui.showRegisterBtn = document.getElementById("showRegisterBtn");
    ui.adminAccessBtn = document.getElementById("adminAccessBtn");
    ui.registerPanel = document.getElementById("registerPanel");
    ui.registerForm = document.getElementById("registerForm");
    ui.regUsername = document.getElementById("regUsername");
    ui.regDisplayName = document.getElementById("regDisplayName");
    ui.regPassword = document.getElementById("regPassword");
    ui.regPasswordConfirm = document.getElementById("regPasswordConfirm");
    ui.showLoginBtn = document.getElementById("showLoginBtn");
    ui.sessionPanel = document.getElementById("sessionPanel");
    ui.sessionDisplayName = document.getElementById("sessionDisplayName");
    ui.sessionMeta = document.getElementById("sessionMeta");
    ui.refreshServerDataBtn = document.getElementById("refreshServerDataBtn");
    ui.logoutBtn = document.getElementById("logoutBtn");
    ui.googleSyncHelp = document.getElementById("googleSyncHelp");
    ui.syncQuestionnairesBtn = document.getElementById("syncQuestionnairesBtn");
    ui.syncCurrentBtn = document.getElementById("syncCurrentBtn");
    ui.syncHistoryBtn = document.getElementById("syncHistoryBtn");
    ui.syncStatus = document.getElementById("syncStatus");
    ui.scaleBadge = document.getElementById("scaleBadge");
    ui.scaleTitle = document.getElementById("scaleTitle");
    ui.scaleMeta = document.getElementById("scaleMeta");
    ui.scaleSource = document.getElementById("scaleSource");
    ui.questionPrompt = document.getElementById("questionPrompt");
    ui.screeningViewTab = document.getElementById("screeningViewTab");
    ui.resultViewTab = document.getElementById("resultViewTab");
    ui.localHistoryViewTab = document.getElementById("localHistoryViewTab");
    ui.managementViewTab = document.getElementById("managementViewTab");
    ui.adminWorkspace = document.getElementById("adminWorkspace");
    ui.adminAccessMessage = document.getElementById("adminAccessMessage");
    ui.screeningView = document.getElementById("screeningView");
    ui.resultView = document.getElementById("resultView");
    ui.localHistoryView = document.getElementById("localHistoryView");
    ui.managementView = document.getElementById("managementView");
    ui.respondentFields = document.getElementById("respondentFields");
    ui.questionsRoot = document.getElementById("questionsRoot");
    ui.screeningForm = document.getElementById("screeningForm");
    ui.sessionDate = document.getElementById("sessionDate");
    ui.workerName = document.getElementById("workerName");
    ui.clientLabel = document.getElementById("clientLabel");
    ui.birthDate = document.getElementById("birthDate");
    ui.sessionNote = document.getElementById("sessionNote");
    ui.progressFill = document.getElementById("progressFill");
    ui.progressLabel = document.getElementById("progressLabel");
    ui.resultEmpty = document.getElementById("resultEmpty");
    ui.resultPanel = document.getElementById("resultPanel");
    ui.printSheetHead = document.getElementById("printSheetHead");
    ui.printOrgLine = document.getElementById("printOrgLine");
    ui.printScaleLine = document.getElementById("printScaleLine");
    ui.printMetaLine = document.getElementById("printMetaLine");
    ui.resultTitle = document.getElementById("resultTitle");
    ui.resultScore = document.getElementById("resultScore");
    ui.resultBand = document.getElementById("resultBand");
    ui.resultHighlights = document.getElementById("resultHighlights");
    ui.resultFlags = document.getElementById("resultFlags");
    ui.resultMeta = document.getElementById("resultMeta");
    ui.resultBreakdown = document.getElementById("resultBreakdown");
    ui.resultNotes = document.getElementById("resultNotes");
    ui.signatureCanvas = document.getElementById("signatureCanvas");
    ui.clearSignatureBtn = document.getElementById("clearSignatureBtn");
    ui.saveResultBtn = document.getElementById("saveResultBtn");
    ui.exportHistoryCsvBtn = document.getElementById("exportHistoryCsvBtn");
    ui.exportHistoryJsonBtn = document.getElementById("exportHistoryJsonBtn");
    ui.clearHistoryBtn = document.getElementById("clearHistoryBtn");
    ui.historyTableBody = document.getElementById("historyTableBody");
    ui.historyTableEmpty = document.getElementById("historyTableEmpty");
    ui.serverRecordScope = document.getElementById("serverRecordScope");
    ui.serverRecordsMessage = document.getElementById("serverRecordsMessage");
    ui.serverRecordsList = document.getElementById("serverRecordsList");
    ui.refreshServerRecordsBtn = document.getElementById("refreshServerRecordsBtn");
    ui.adminPanel = document.getElementById("adminPanel");
    ui.createUserForm = document.getElementById("createUserForm");
    ui.newUserUsername = document.getElementById("newUserUsername");
    ui.newUserDisplayName = document.getElementById("newUserDisplayName");
    ui.newUserRole = document.getElementById("newUserRole");
    ui.newUserPassword = document.getElementById("newUserPassword");
    ui.adminMessage = document.getElementById("adminMessage");
    ui.refreshUsersBtn = document.getElementById("refreshUsersBtn");
    ui.usersList = document.getElementById("usersList");
    ui.radioFieldTemplate = document.getElementById("radioFieldTemplate");
    ui.questionTemplate = document.getElementById("questionTemplate");
    ui.clientIdSelect = document.getElementById("clientIdSelect");
    ui.registerClientBtn = document.getElementById("registerClientBtn");
    ui.clientRegisterPanel = document.getElementById("clientRegisterPanel");
    ui.newClientBirthDate = document.getElementById("newClientBirthDate");
    ui.newClientMemo = document.getElementById("newClientMemo");
    ui.confirmRegisterClientBtn = document.getElementById("confirmRegisterClientBtn");
    ui.cancelRegisterClientBtn = document.getElementById("cancelRegisterClientBtn");
    ui.clientHistoryPanel = document.getElementById("clientHistoryPanel");
    ui.clientHistoryTitle = document.getElementById("clientHistoryTitle");
    ui.clientHistoryList = document.getElementById("clientHistoryList");
    ui.clientHistoryChartArea = document.getElementById("clientHistoryChartArea");
    ui.closeClientHistoryBtn = document.getElementById("closeClientHistoryBtn");
    ui.riskViewTab = document.getElementById("riskViewTab");
    ui.riskView = document.getElementById("riskView");
    ui.riskBadge = document.getElementById("riskBadge");
    ui.riskFlagsList = document.getElementById("riskFlagsList");
    ui.riskFlagsEmpty = document.getElementById("riskFlagsEmpty");
    ui.refreshRiskBtn = document.getElementById("refreshRiskBtn");
    ui.reportFromDate = document.getElementById("reportFromDate");
    ui.reportToDate = document.getElementById("reportToDate");
    ui.generateReportBtn = document.getElementById("generateReportBtn");
    ui.reportOutput = document.getElementById("reportOutput");
    ui.copyReportBtn = document.getElementById("copyReportBtn");
    ui.downloadReportBtn = document.getElementById("downloadReportBtn");
    ui.configOrgName = document.getElementById("configOrgName");
    ui.configTeamName = document.getElementById("configTeamName");
    ui.configContactNote = document.getElementById("configContactNote");
    ui.configKioskPin = document.getElementById("configKioskPin");
    ui.configKioskPinStatus = document.getElementById("configKioskPinStatus");
    ui.saveConfigBtn = document.getElementById("saveConfigBtn");
    ui.configSaveStatus = document.getElementById("configSaveStatus");
    ui.kioskPinModal = document.getElementById("kioskPinModal");
    ui.kioskPinInput = document.getElementById("kioskPinInput");
    ui.kioskPinSubmitBtn = document.getElementById("kioskPinSubmitBtn");
    ui.kioskPinError = document.getElementById("kioskPinError");
    ui.bundleSetupPanel = document.getElementById("bundleSetupPanel");
    ui.bundleScaleCheckboxes = document.getElementById("bundleScaleCheckboxes");
    ui.startBundleBtn = document.getElementById("startBundleBtn");
    ui.bundleProgress = document.getElementById("bundleProgress");
    ui.bundleSummaryPanel = document.getElementById("bundleSummaryPanel");
    ui.bundleSummaryList = document.getElementById("bundleSummaryList");
    ui.bundleSaveAllBtn = document.getElementById("bundleSaveAllBtn");
    ui.bundleNewBtn = document.getElementById("bundleNewBtn");
  }

  function bindEvents() {
    ui.screeningForm.addEventListener("submit", onSubmit);
    ui.screeningForm.addEventListener("change", onFormChange);
    ui.bootstrapForm.addEventListener("submit", onBootstrapSubmit);
    ui.loginForm.addEventListener("submit", onLoginSubmit);
    ui.showRegisterBtn.addEventListener("click", () => showRegisterPanel(true));
    ui.adminAccessBtn.addEventListener("click", onAdminAccessClick);
    ui.showLoginBtn.addEventListener("click", () => showRegisterPanel(false));
    ui.registerForm.addEventListener("submit", onRegisterSubmit);
    ui.refreshServerDataBtn.addEventListener("click", onRefreshServerData);
    ui.logoutBtn.addEventListener("click", onLogoutClick);
    ui.workerName.addEventListener("input", onWorkerNameInput);
    ui.clearSignatureBtn.addEventListener("click", onClearSignatureClick);
    ui.signatureCanvas.addEventListener("pointerdown", onSignaturePointerDown);
    ui.signatureCanvas.addEventListener("pointermove", onSignaturePointerMove);
    ui.signatureCanvas.addEventListener("pointerup", onSignaturePointerUp);
    ui.signatureCanvas.addEventListener("pointerleave", onSignaturePointerUp);
    ui.signatureCanvas.addEventListener("pointercancel", onSignaturePointerUp);
    ui.saveResultBtn.addEventListener("click", onSaveResult);
    ui.syncQuestionnairesBtn.addEventListener("click", onSyncQuestionnaires);
    ui.syncCurrentBtn.addEventListener("click", onSyncCurrentResult);
    ui.syncHistoryBtn.addEventListener("click", onSyncHistory);
    ui.exportHistoryCsvBtn.addEventListener("click", onExportHistoryCsv);
    ui.exportHistoryJsonBtn.addEventListener("click", onExportHistoryJson);
    ui.clearHistoryBtn.addEventListener("click", onClearHistory);
    ui.historyTableBody.addEventListener("click", onHistoryClick);
    ui.serverRecordScope.addEventListener("change", onServerRecordScopeChange);
    ui.serverRecordsList.addEventListener("click", onServerRecordsClick);
    ui.refreshServerRecordsBtn.addEventListener("click", onRefreshServerRecords);
    ui.createUserForm.addEventListener("submit", onCreateUserSubmit);
    ui.refreshUsersBtn.addEventListener("click", onRefreshUsersClick);
    ui.usersList.addEventListener("click", onUsersListClick);
    ui.screeningViewTab.addEventListener("click", () => setActiveView("screening"));
    ui.resultViewTab.addEventListener("click", () => setActiveView("result"));
    ui.localHistoryViewTab.addEventListener("click", () => setActiveView("history"));
    ui.managementViewTab.addEventListener("click", () => setActiveView("management"));
    if (ui.registerClientBtn) ui.registerClientBtn.addEventListener("click", onRegisterClientClick);
    if (ui.confirmRegisterClientBtn) ui.confirmRegisterClientBtn.addEventListener("click", onConfirmRegisterClient);
    if (ui.cancelRegisterClientBtn) ui.cancelRegisterClientBtn.addEventListener("click", onCancelRegisterClient);
    if (ui.clientIdSelect) ui.clientIdSelect.addEventListener("change", onClientIdSelectChange);
    if (ui.closeClientHistoryBtn) ui.closeClientHistoryBtn.addEventListener("click", onCloseClientHistory);
    if (ui.riskViewTab) ui.riskViewTab.addEventListener("click", () => setActiveView("risk"));
    if (ui.refreshRiskBtn) ui.refreshRiskBtn.addEventListener("click", onRefreshRiskFlags);
    if (ui.riskFlagsList) ui.riskFlagsList.addEventListener("click", onRiskFlagsClick);
    if (ui.generateReportBtn) ui.generateReportBtn.addEventListener("click", onGenerateReport);
    if (ui.copyReportBtn) ui.copyReportBtn.addEventListener("click", onCopyReport);
    if (ui.downloadReportBtn) ui.downloadReportBtn.addEventListener("click", onDownloadReport);
    if (ui.saveConfigBtn) ui.saveConfigBtn.addEventListener("click", onSaveConfig);
    if (ui.kioskPinSubmitBtn) ui.kioskPinSubmitBtn.addEventListener("click", onKioskPinSubmit);
    if (ui.kioskPinInput) ui.kioskPinInput.addEventListener("keydown", (e) => { if (e.key === "Enter") onKioskPinSubmit(); });
    if (ui.startBundleBtn) ui.startBundleBtn.addEventListener("click", onStartBundle);
    if (ui.bundleSaveAllBtn) ui.bundleSaveAllBtn.addEventListener("click", onBundleSaveAll);
    if (ui.bundleNewBtn) ui.bundleNewBtn.addEventListener("click", onBundleNew);
  }

  function applyPageMode() {
    document.body.classList.toggle("admin-mode", IS_ADMIN_PAGE);
    document.body.classList.toggle("kiosk-mode", KIOSK_MODE);
    ui.adminWorkspace.classList.toggle("hidden", !IS_ADMIN_PAGE);
    if (IS_ADMIN_PAGE) {
      document.title = "관리자 기능 | 정신건강 척도검사 웹앱";
    }
    if (KIOSK_MODE) {
      document.title = "정신건강 척도검사";
    }
  }

  function getSignatureContext() {
    return ui.signatureCanvas.getContext("2d");
  }

  function resetSignatureCanvas() {
    const ctx = getSignatureContext();
    ctx.clearRect(0, 0, ui.signatureCanvas.width, ui.signatureCanvas.height);
    ctx.lineCap = "round";
    ctx.lineJoin = "round";
    ctx.lineWidth = 2.4;
    ctx.strokeStyle = "#143127";
    state.signaturePad.isDrawing = false;
    state.signaturePad.hasStroke = false;
  }

  function syncSignatureFromCanvas() {
    if (!state.lastResult) {
      return;
    }

    if (!state.lastResult.meta || typeof state.lastResult.meta !== "object") {
      state.lastResult.meta = {};
    }

    state.lastResult.meta.signatureDataUrl = state.signaturePad.hasStroke
      ? ui.signatureCanvas.toDataURL("image/png")
      : "";
  }

  function loadSignatureToCanvas(record) {
    resetSignatureCanvas();
    const dataUrl = record?.meta?.signatureDataUrl;
    if (!dataUrl) {
      return;
    }

    const image = new Image();
    image.onload = () => {
      const ctx = getSignatureContext();
      ctx.clearRect(0, 0, ui.signatureCanvas.width, ui.signatureCanvas.height);
      ctx.drawImage(image, 0, 0, ui.signatureCanvas.width, ui.signatureCanvas.height);
      state.signaturePad.hasStroke = true;
    };
    image.src = dataUrl;
  }

  function getSignaturePoint(event) {
    const rect = ui.signatureCanvas.getBoundingClientRect();
    return {
      x: (event.clientX - rect.left) * (ui.signatureCanvas.width / rect.width),
      y: (event.clientY - rect.top) * (ui.signatureCanvas.height / rect.height)
    };
  }

  function onSignaturePointerDown(event) {
    if (!state.lastResult) {
      return;
    }

    event.preventDefault();
    const point = getSignaturePoint(event);
    const ctx = getSignatureContext();
    state.signaturePad.isDrawing = true;
    state.signaturePad.hasStroke = true;
    ui.signatureCanvas.setPointerCapture?.(event.pointerId);
    ctx.beginPath();
    ctx.moveTo(point.x, point.y);
    ctx.lineTo(point.x + 0.01, point.y + 0.01);
    ctx.stroke();
  }

  function onSignaturePointerMove(event) {
    if (!state.signaturePad.isDrawing || !state.lastResult) {
      return;
    }

    event.preventDefault();
    const point = getSignaturePoint(event);
    const ctx = getSignatureContext();
    ctx.lineTo(point.x, point.y);
    ctx.stroke();
  }

  function onSignaturePointerUp(event) {
    if (!state.signaturePad.isDrawing) {
      return;
    }

    event.preventDefault();
    state.signaturePad.isDrawing = false;
    ui.signatureCanvas.releasePointerCapture?.(event.pointerId);
    syncSignatureFromCanvas();
  }

  function onClearSignatureClick() {
    resetSignatureCanvas();
    syncSignatureFromCanvas();
  }

  function setActiveView(view) {
    const canShowResult = Boolean(state.lastResult);
    const nextView = view === "result" ? (canShowResult ? "result" : "screening") : view;
    state.activeView = nextView;

    ui.screeningView.classList.toggle("hidden", nextView !== "screening");
    ui.resultView.classList.toggle("hidden", nextView !== "result");
    ui.localHistoryView.classList.toggle("hidden", nextView !== "history");
    ui.managementView.classList.toggle("hidden", nextView !== "management");
    if (ui.riskView) ui.riskView.classList.toggle("hidden", nextView !== "risk");

    ui.screeningViewTab.classList.toggle("is-active", nextView === "screening");
    ui.screeningViewTab.setAttribute("aria-selected", nextView === "screening" ? "true" : "false");

    ui.resultViewTab.disabled = !canShowResult;
    ui.resultViewTab.classList.toggle("is-active", nextView === "result");
    ui.resultViewTab.setAttribute("aria-selected", nextView === "result" ? "true" : "false");

    ui.localHistoryViewTab.classList.toggle("is-active", nextView === "history");
    ui.localHistoryViewTab.setAttribute("aria-selected", nextView === "history" ? "true" : "false");

    ui.managementViewTab.classList.toggle("is-active", nextView === "management");
    ui.managementViewTab.setAttribute("aria-selected", nextView === "management" ? "true" : "false");

    if (ui.riskViewTab) {
      ui.riskViewTab.classList.toggle("is-active", nextView === "risk");
      ui.riskViewTab.setAttribute("aria-selected", nextView === "risk" ? "true" : "false");
    }

    if (nextView === "risk") {
      void onRefreshRiskFlags();
    }
  }

  async function loadSessionState() {
    try {
      const response = await apiRequest("/api/session");
      state.session.user = response.user || null;
      state.session.bootstrapRequired = Boolean(response.bootstrapRequired);
      state.session.connectionErrorMessage = "";
      renderSessionState();

      if (state.session.user) {
        await Promise.all([loadServerRecords(true), loadUsers(true), loadClients(), loadRiskSummary()]);
      } else {
        state.serverRecords = [];
        state.users = [];
        state.clients = [];
        state.riskFlags = [];
        state.riskUnacknowledgedCount = 0;
        renderServerRecords();
        renderUsers();
        renderClientSelect();
        renderRiskBadge();
      }
    } catch (error) {
      console.error(error);
      state.session.user = null;
      state.session.bootstrapRequired = false;
      state.session.connectionErrorMessage = error.message;
      state.serverRecords = [];
      state.users = [];
      renderServerRecords();
      renderUsers();
      renderSessionState();
    }
  }

  function renderSessionState() {
    const user = state.session.user;
    const bootstrapRequired = state.session.bootstrapRequired;
    const authOnly = !user;
    const isAdminUser = Boolean(user && user.role === "admin");

    document.body.classList.toggle("auth-only", authOnly);

    ui.bootstrapPanel.classList.toggle("hidden", !bootstrapRequired);
    ui.loginPanel.classList.toggle("hidden", bootstrapRequired || Boolean(user));
    ui.registerPanel.classList.add("hidden");
    ui.sessionPanel.classList.toggle("hidden", !user);

    if (IS_ADMIN_PAGE) {
      ui.adminPanel.classList.toggle("hidden", !isAdminUser);
      ui.adminAccessMessage.classList.toggle("hidden", isAdminUser);
      if (!isAdminUser) {
        ui.adminAccessMessage.querySelector(".muted").textContent = user
          ? "관리자 권한이 있는 계정으로 로그인하면 계정 관리 화면이 열립니다."
          : "관리자 계정으로 로그인하면 계정 관리 화면이 열립니다.";
      }
    } else {
      ui.adminPanel.classList.add("hidden");
      ui.adminAccessMessage.classList.add("hidden");
    }

    if (bootstrapRequired) {
      ui.authStatusText.textContent = "초기 관리자 필요";
      ui.sessionDisplayName.textContent = "";
      ui.sessionMeta.textContent = "";
      setAuthMessage("초기 관리자 생성 필요", "");
    } else if (state.session.connectionErrorMessage) {
      ui.authStatusText.textContent = "서버 연결 실패";
      ui.sessionDisplayName.textContent = "";
      ui.sessionMeta.textContent = "";
      setAuthMessage(state.session.connectionErrorMessage, "error");
    } else if (user) {
      ui.authStatusText.textContent = "로그인됨";
      ui.sessionDisplayName.textContent = user.displayName;
      ui.sessionMeta.textContent = `권한 ${roleLabel(user.role)} · 상태 ${user.active ? "활성" : "비활성"}`;
      setAuthMessage(
        isAdminUser ? "관리자 사용 가능" : "저장 결과 사용 가능",
        "success"
      );
    } else {
      ui.authStatusText.textContent = "로그인 필요";
      ui.sessionDisplayName.textContent = "";
      ui.sessionMeta.textContent = "";
      setAuthMessage("로그인하면 저장 결과 사용 가능", "");
    }

    ui.logoutBtn.classList.toggle("hidden", !user);

    ui.serverRecordScope.disabled = !user || user.role !== "admin";
    if (!user || user.role !== "admin") {
      ui.serverRecordScope.value = "mine";
    }

    if (!user) {
      ui.serverRecordsMessage.textContent = bootstrapRequired
        ? "초기 관리자 계정을 만든 뒤 서버 저장 기능을 사용할 수 있습니다."
        : "로그인하면 서버 저장 결과를 확인할 수 있습니다.";
      if (!IS_ADMIN_PAGE) {
        setActiveView("management");
      }
    }

    if (ui.riskViewTab) {
      ui.riskViewTab.classList.toggle("hidden", !user);
    }
    renderRiskBadge();
    updateGoogleSyncUi();
    renderHistory();
  }

  function setAuthMessage(message, type) {
    ui.authMessage.textContent = message;
    ui.authMessage.classList.remove("success", "error");
    if (type) {
      ui.authMessage.classList.add(type);
    }
  }

  function roleLabel(role) {
    return role === "admin" ? "관리자" : "실무자";
  }

  async function onBootstrapSubmit(event) {
    event.preventDefault();

    const username = ui.bootstrapUsername.value.trim();
    const displayName = ui.bootstrapDisplayName.value.trim();
    const password = ui.bootstrapPassword.value;
    const confirmPassword = ui.bootstrapPasswordConfirm.value;

    if (!username || !displayName || !password) {
      alert("초기 관리자 계정 정보를 모두 입력해주세요.");
      return;
    }

    if (password !== confirmPassword) {
      alert("비밀번호와 비밀번호 확인이 일치하지 않습니다.");
      ui.bootstrapPasswordConfirm.focus();
      return;
    }

    try {
      await apiRequest("/api/bootstrap", {
        method: "POST",
        body: {
          username,
          displayName,
          password
        }
      });
      ui.bootstrapForm.reset();
      await loadSessionState();
      applySessionDefaults();
      setActiveView("screening");
      alert("초기 관리자 계정을 생성했습니다.");
    } catch (error) {
      console.error(error);
      setAuthMessage(error.message, "error");
      alert(error.message);
    }
  }

  async function onLoginSubmit(event) {
    event.preventDefault();

    const username = ui.loginUsername.value.trim();
    const password = ui.loginPassword.value;

    if (!username || !password) {
      alert("아이디와 비밀번호를 입력해주세요.");
      return;
    }

    try {
      await apiRequest("/api/login", {
        method: "POST",
        body: {
          username,
          password
        }
      });
      ui.loginForm.reset();
      await loadSessionState();
      applySessionDefaults();
      setActiveView("screening");
      alert("로그인되었습니다.");
    } catch (error) {
      console.error(error);
      state.session.connectionErrorMessage = error.message;
      setAuthMessage(error.message, "error");
      alert(error.message);
    }
  }

  function onAdminAccessClick() {
    const adminUrl = new URL(window.location.href);
    adminUrl.searchParams.set("screen", "admin");
    window.location.href = adminUrl.toString();
  }

  function showRegisterPanel(show) {
    ui.loginPanel.classList.toggle("hidden", show);
    ui.registerPanel.classList.toggle("hidden", !show);
    if (show) {
      ui.regUsername.focus();
    } else {
      ui.registerForm.reset();
      ui.loginUsername.focus();
    }
  }

  async function onRegisterSubmit(event) {
    event.preventDefault();

    const username = ui.regUsername.value.trim();
    const displayName = ui.regDisplayName.value.trim();
    const password = ui.regPassword.value;
    const passwordConfirm = ui.regPasswordConfirm.value;

    if (!username || !displayName || !password) {
      alert("모든 항목을 입력해주세요.");
      return;
    }

    if (password !== passwordConfirm) {
      alert("비밀번호가 일치하지 않습니다.");
      ui.regPasswordConfirm.focus();
      return;
    }

    try {
      await apiRequest("/api/register", {
        method: "POST",
        body: { username, displayName, password }
      });
      ui.registerForm.reset();
      showRegisterPanel(false);
      alert("가입 신청이 완료되었습니다.\n관리자 승인 후 로그인할 수 있습니다.");
    } catch (error) {
      console.error(error);
      alert(error.message);
    }
  }

  async function onLogoutClick() {
    try {
      await apiRequest("/api/logout", {
        method: "POST"
      });
      state.session.user = null;
      state.session.bootstrapRequired = false;
      state.serverRecords = [];
      state.users = [];
      renderSessionState();
      renderServerRecords();
      renderUsers();
      applySessionDefaults();
      setActiveView("management");
      alert("로그아웃되었습니다.");
    } catch (error) {
      console.error(error);
      alert(error.message);
    }
  }

  async function onRefreshServerData() {
    if (!state.session.user) {
      alert("먼저 로그인해주세요.");
      return;
    }

    try {
      await Promise.all([loadServerRecords(false), loadUsers(false)]);
      setAuthMessage("서버 데이터 새로고침을 마쳤습니다.", "success");
    } catch (error) {
      console.error(error);
      setAuthMessage(error.message, "error");
      alert(error.message);
    }
  }

  async function loadManifest() {
    if (EMBEDDED_DATA && Array.isArray(EMBEDDED_DATA.manifest) && EMBEDDED_DATA.manifest.length) {
      state.manifest = structuredCloneSafe(EMBEDDED_DATA.manifest);
    } else {
      const response = await fetch(MANIFEST_URL);
      if (!response.ok) {
        throw new Error("questionnaire manifest load failed");
      }
      state.manifest = await response.json();
    }

    ui.questionnaireCount.textContent = `총 ${state.manifest.length}개 척도`;
    renderNav();
    renderBundleSetup();

    if (state.manifest.length) {
      await loadQuestionnaire(state.manifest[0].id);
    }
  }

  function renderNav() {
    ui.questionnaireNav.innerHTML = "";
    state.manifest.forEach((item) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "nav-btn";
      button.dataset.id = item.id;
      button.innerHTML = `
        <span class="nav-title">${escapeHtml(item.title)}</span>
        <span class="nav-meta">${escapeHtml(item.recommendedAge || "")}</span>
      `;
      button.addEventListener("click", () => loadQuestionnaire(item.id));
      ui.questionnaireNav.appendChild(button);
    });
    syncNavSelection();
  }

  function syncNavSelection() {
    ui.questionnaireNav.querySelectorAll(".nav-btn").forEach((button) => {
      button.classList.toggle("active", button.dataset.id === state.currentId);
    });
  }

  async function loadQuestionnaire(id) {
    if (!state.questionnaires.has(id)) {
      const embeddedQuestionnaire = getEmbeddedQuestionnaire(id);
      if (embeddedQuestionnaire) {
        state.questionnaires.set(id, embeddedQuestionnaire);
      } else {
        const meta = state.manifest.find((item) => item.id === id);
        const response = await fetch(meta.path);
        if (!response.ok) {
          throw new Error(`questionnaire load failed: ${id}`);
        }
        state.questionnaires.set(id, await response.json());
      }
    }

    state.currentId = id;
    state.currentQuestionnaire = state.questionnaires.get(id);
    state.lastResult = null;

    syncNavSelection();
    renderQuestionnaire();
    clearResult();
    updateProgress();
    window.scrollTo({ top: 0, behavior: "smooth" });
  }

  function renderQuestionnaire() {
    const questionnaire = state.currentQuestionnaire;
    ui.scaleBadge.textContent = `${questionnaire.shortTitle || questionnaire.title} · 척도번호 ${questionnaire.selfSeq}`;
    ui.scaleTitle.textContent = questionnaire.title;
    ui.scaleMeta.textContent = [
      questionnaire.recommendedAge ? `권장연령 ${questionnaire.recommendedAge}` : "",
      questionnaire.questions ? `문항 ${questionnaire.questions.length}개` : ""
    ]
      .filter(Boolean)
      .join(" · ");

    const sourceParts = [
      questionnaire.source?.citation || "",
      questionnaire.source?.institution || ""
    ].filter(Boolean);
    ui.scaleSource.textContent = sourceParts.join(" / ") || "내부 운영용 척도 데이터";
    ui.questionPrompt.textContent = questionnaire.questionPrompt || "";

    renderRespondentFields(questionnaire.respondentFields || []);
    renderQuestions(questionnaire.questions || []);
    ui.screeningForm.reset();
    applySessionDefaults();
  }

  function renderRespondentFields(fields) {
    ui.respondentFields.innerHTML = "";
    fields.forEach((field) => {
      ui.respondentFields.appendChild(buildChoiceField(field, `respondent_${field.id}`));
    });
  }

  function renderQuestions(questions) {
    ui.questionsRoot.innerHTML = "";
    let previousSectionTitle = "";
    questions.forEach((question) => {
      if (question.sectionTitle && question.sectionTitle !== previousSectionTitle) {
        ui.questionsRoot.appendChild(buildQuestionSectionHeading(question.sectionTitle));
        previousSectionTitle = question.sectionTitle;
      }
      const section = ui.questionTemplate.content.firstElementChild.cloneNode(true);
      section.dataset.questionId = question.id;
      section.querySelector(".question-number").textContent = question.number;
      section.querySelector(".question-text").textContent = question.text;

      const choiceGrid = section.querySelector(".choice-grid");
      question.options.forEach((option, index) => {
        choiceGrid.appendChild(
          buildChoiceCard({
            inputName: question.id,
            inputId: `${question.id}_${index + 1}`,
            value: String(option.value),
            label: option.label,
            score: option.score
          })
        );
      });

      const subWrap = section.querySelector(".subquestions");
      (question.subQuestions || []).forEach((subQuestion) => {
        const subField = buildChoiceField(subQuestion, subQuestion.id, true);
        subField.classList.add("subquestion-card");
        subField.dataset.subQuestionId = subQuestion.id;
        subWrap.appendChild(subField);
      });

      if (!question.subQuestions || !question.subQuestions.length) {
        subWrap.remove();
      }

      ui.questionsRoot.appendChild(section);
    });
  }

  function buildQuestionSectionHeading(title) {
    const heading = document.createElement("div");
    heading.className = "question-section-heading";
    heading.textContent = title;
    return heading;
  }

  function buildChoiceField(field, inputName, isSubQuestion = false) {
    const node = ui.radioFieldTemplate.content.firstElementChild.cloneNode(true);
    node.dataset.fieldId = field.id;
    node.querySelector(".field-label").textContent = `${field.label}${field.required ? " *" : ""}`;

    if (isSubQuestion) {
      const number = document.createElement("p");
      number.className = "question-number";
      number.textContent = field.number || "Q.";
      node.prepend(number);
    }

    const choiceGrid = node.querySelector(".choice-grid");
    (field.options || []).forEach((option, index) => {
      choiceGrid.appendChild(
        buildChoiceCard({
          inputName,
          inputId: `${inputName}_${index + 1}`,
          value: String(option.value),
          label: option.label,
          score: option.score,
          showScore: !inputName.startsWith("respondent_")
        })
      );
    });
    return node;
  }

  function buildChoiceCard({ inputName, inputId, value, label, score, showScore = true }) {
    const hasScore =
      score !== null &&
      score !== undefined &&
      String(score).trim() !== "" &&
      !Number.isNaN(Number(score));
    const wrapper = document.createElement("label");
    wrapper.className = "choice-card";
    wrapper.setAttribute("for", inputId);
    wrapper.innerHTML = `
      <input type="radio" name="${escapeHtml(inputName)}" id="${escapeHtml(inputId)}" value="${escapeHtml(value)}" data-score="${escapeHtml(hasScore ? String(score) : "")}" />
      <span>
        <span class="choice-label-text">${escapeHtml(label)}</span>
        ${showScore && hasScore ? `<span class="choice-score">점수 ${escapeHtml(String(score))}</span>` : ""}
      </span>
    `;
    return wrapper;
  }

  function applySessionDefaults() {
    ui.sessionDate.value = todayForInput();
    ui.workerName.value = state.session.user?.displayName || localStorage.getItem(WORKER_NAME_KEY) || "";
    ui.clientLabel.value = "";
    ui.birthDate.value = "";
    ui.sessionNote.value = "";
  }

  function onWorkerNameInput() {
    const trimmed = ui.workerName.value.trim();
    if (trimmed) {
      localStorage.setItem(WORKER_NAME_KEY, trimmed);
    } else {
      localStorage.removeItem(WORKER_NAME_KEY);
    }
  }

  function onFormChange(event) {
    const target = event.target;
    if (!(target instanceof HTMLInputElement) || target.type !== "radio") {
      return;
    }

    syncSelectedState(target.name);
    syncSubQuestionsVisibility();
    updateProgress();
  }

  function syncSelectedState(groupName) {
    document.querySelectorAll(`input[name="${cssEscape(groupName)}"]`).forEach((input) => {
      input.closest(".choice-card")?.classList.remove("is-selected");
    });
    const checked = document.querySelector(`input[name="${cssEscape(groupName)}"]:checked`);
    checked?.closest(".choice-card")?.classList.add("is-selected");
  }

  function syncSubQuestionsVisibility() {
    const questionnaire = state.currentQuestionnaire;
    (questionnaire.questions || []).forEach((question) => {
      if (!question.subQuestions || !question.subQuestions.length) {
        return;
      }

      const showSubs = shouldShowSubQuestions(question, questionnaire);
      const block = ui.questionsRoot.querySelector(`[data-question-id="${cssEscape(question.id)}"]`);
      const subWrap = block?.querySelector(".subquestions");
      if (!subWrap) {
        return;
      }

      subWrap.classList.toggle("hidden", !showSubs);
      if (!showSubs) {
        subWrap.querySelectorAll('input[type="radio"]').forEach((input) => {
          input.checked = false;
          input.closest(".choice-card")?.classList.remove("is-selected");
        });
      }
    });
  }

  function shouldShowSubQuestions(question, questionnaire) {
    if (!question.subQuestions || !question.subQuestions.length) {
      return false;
    }

    if (questionnaire.scoring?.type === "k_mdq" && question.id === "q14") {
      return (questionnaire.scoring.symptomQuestionIds || []).some((id) => {
        const checked = document.querySelector(`input[name="${cssEscape(id)}"]:checked`);
        return Boolean(checked && Number(checked.dataset.score || 0) > 0);
      });
    }

    const checked = document.querySelector(`input[name="${cssEscape(question.id)}"]:checked`);
    return Boolean(checked && Number(checked.dataset.score || 0) > 0);
  }

  function shouldShowSubQuestionsFromResponses(question, questionnaire, responses) {
    if (!question.subQuestions || !question.subQuestions.length) {
      return false;
    }

    if (questionnaire.scoring?.type === "k_mdq" && question.id === "q14") {
      return (questionnaire.scoring.symptomQuestionIds || []).some((id) => {
        const answer = responses[id];
        return Boolean(answer && Number(answer.score || 0) > 0);
      });
    }

    const answer = responses[question.id];
    return Boolean(answer && Number(answer.score || 0) > 0);
  }

  function buildProgressSnapshot(questionnaire, payload) {
    const respondent = payload?.respondent || {};
    const responses = payload?.responses || payload?.answers || {};

    const respondentTotal = (questionnaire.respondentFields || []).length;
    const respondentAnswered = (questionnaire.respondentFields || []).filter((field) => {
      const value = respondent[field.id];
      return value !== null && value !== undefined && String(value).trim() !== "";
    }).length;

    let questionTotal = 0;
    let questionAnswered = 0;

    (questionnaire.questions || []).forEach((question) => {
      questionTotal += 1;
      if (responses[question.id]) {
        questionAnswered += 1;
      }

      if (question.subQuestions && question.subQuestions.length && shouldShowSubQuestionsFromResponses(question, questionnaire, responses)) {
        question.subQuestions.forEach((subQuestion) => {
          questionTotal += 1;
          if (responses[subQuestion.id]) {
            questionAnswered += 1;
          }
        });
      }
    });

    const total = respondentTotal + questionTotal;
    const answered = respondentAnswered + questionAnswered;
    const percent = total ? Math.round((answered / total) * 100) : 0;

    return {
      answered,
      total,
      percent,
      respondentAnswered,
      respondentTotal,
      questionAnswered,
      questionTotal
    };
  }

  function formatProgressSummary(progress) {
    if (!progress || !Number.isFinite(progress.percent)) {
      return "";
    }

    return `${progress.percent}% (${progress.answered}/${progress.total}항목)`;
  }

  function updateProgress() {
    const questionnaire = state.currentQuestionnaire;
    if (!questionnaire) {
      return;
    }

    const payload = collectAnswers(questionnaire);
    const progress = buildProgressSnapshot(questionnaire, payload);

    ui.progressFill.style.width = `${progress.percent}%`;
    ui.progressLabel.textContent = `${progress.percent}%`;
  }

  function onSubmit(event) {
    event.preventDefault();

    const questionnaire = state.currentQuestionnaire;
    const validation = validateResponses(questionnaire);
    if (!validation.ok) {
      alert(validation.message);
      validation.focusTarget?.focus();
      return;
    }

    const payload = collectAnswers(questionnaire);
    const evaluation = evaluateQuestionnaire(questionnaire, payload);
    const record = buildResultRecord(questionnaire, payload, evaluation);

    state.lastResult = record;
    renderResultRecord(record);

    if (KIOSK_MODE && !state.kioskUnlocked) {
      if (appConfig.kioskPinSet) {
        showKioskPinModal();
      }
    }
  }

  function validateResponses(questionnaire) {
    if (!ui.sessionDate.value) {
      return {
        ok: false,
        message: "검사일을 입력해주세요.",
        focusTarget: ui.sessionDate
      };
    }

    for (const field of questionnaire.respondentFields || []) {
      const checked = document.querySelector(`input[name="respondent_${cssEscape(field.id)}"]:checked`);
      if (!checked) {
        return {
          ok: false,
          message: `응답자 정보의 '${field.label}'을 선택해주세요.`,
          focusTarget: document.querySelector(`input[name="respondent_${cssEscape(field.id)}"]`)
        };
      }
    }

    for (const question of questionnaire.questions || []) {
      const checked = document.querySelector(`input[name="${cssEscape(question.id)}"]:checked`);
      if (!checked) {
        return {
          ok: false,
          message: `${question.number} 문항에 답변해주세요.`,
          focusTarget: document.querySelector(`input[name="${cssEscape(question.id)}"]`)
        };
      }

      if (question.subQuestions && question.subQuestions.length && shouldShowSubQuestions(question, questionnaire)) {
        for (const subQuestion of question.subQuestions) {
          const subChecked = document.querySelector(`input[name="${cssEscape(subQuestion.id)}"]:checked`);
          if (!subChecked) {
            return {
              ok: false,
              message: `${question.number} 문항의 추가 질문에 답변해주세요.`,
              focusTarget: document.querySelector(`input[name="${cssEscape(subQuestion.id)}"]`)
            };
          }
        }
      }
    }

    return { ok: true };
  }

  function collectAnswers(questionnaire) {
    const selectedClientId = ui.clientIdSelect ? ui.clientIdSelect.value : "";
    const meta = {
      sessionDate: ui.sessionDate.value,
      workerName: ui.workerName.value.trim(),
      clientLabel: ui.clientLabel.value.trim(),
      clientId: selectedClientId || null,
      birthDate: ui.birthDate.value,
      sessionNote: ui.sessionNote.value.trim()
    };

    const respondent = {};
    (questionnaire.respondentFields || []).forEach((field) => {
      const checked = document.querySelector(`input[name="respondent_${cssEscape(field.id)}"]:checked`);
      respondent[field.id] = checked ? checked.value : null;
    });

    const responses = {};
    (questionnaire.questions || []).forEach((question) => {
      const checked = document.querySelector(`input[name="${cssEscape(question.id)}"]:checked`);
      responses[question.id] = checked
        ? { value: checked.value, score: Number(checked.dataset.score || 0) }
        : null;

      (question.subQuestions || []).forEach((subQuestion) => {
        const subChecked = document.querySelector(`input[name="${cssEscape(subQuestion.id)}"]:checked`);
        responses[subQuestion.id] = subChecked
          ? { value: subChecked.value, score: Number(subChecked.dataset.score || 0) }
          : null;
      });
    });

    return { meta, respondent, responses };
  }

  function buildResultRecord(questionnaire, payload, evaluation) {
    return {
      id: createLocalId(),
      questionnaireId: questionnaire.id,
      questionnaireTitle: questionnaire.title,
      shortTitle: questionnaire.shortTitle || questionnaire.title,
      createdAt: new Date().toISOString(),
      meta: payload.meta,
      respondent: payload.respondent,
      respondentDisplay: buildRespondentDisplay(questionnaire, payload.respondent),
      answers: payload.responses,
      breakdown: buildAnswerBreakdown(questionnaire, payload.responses),
      progress: buildProgressSnapshot(questionnaire, payload),
      evaluation,
      bundleId: state.bundle.active ? state.bundle.id : null
    };
  }

  function buildRespondentDisplay(questionnaire, respondent) {
    return (questionnaire.respondentFields || []).map((field) => ({
      label: field.label,
      value: optionLabel(field.options || [], respondent[field.id]) || "미응답"
    }));
  }

  function buildAnswerBreakdown(questionnaire, responses) {
    return (questionnaire.questions || []).map((question) => {
      const answer = responses[question.id];
      return {
        id: question.id,
        number: question.number,
        text: question.text,
        answerLabel: optionLabel(question.options || [], answer?.value) || "미응답",
        score: answer?.score ?? null,
        subAnswers: (question.subQuestions || [])
          .map((subQuestion) => {
            const subAnswer = responses[subQuestion.id];
            if (!subAnswer) {
              return null;
            }
            return {
              number: subQuestion.number,
              text: subQuestion.label,
              answerLabel: optionLabel(subQuestion.options || [], subAnswer.value) || "미응답",
              score: subAnswer.score
            };
          })
          .filter(Boolean)
      };
    });
  }

  function optionLabel(options, value) {
    const found = (options || []).find((option) => String(option.value) === String(value));
    return found ? found.label : "";
  }

  function evaluateQuestionnaire(questionnaire, payload) {
    if (questionnaire.scoring?.type === "cri") {
      return evaluateCri(questionnaire, payload);
    }
    if (questionnaire.scoring?.type === "k_mdq") {
      return evaluateKmDQ(questionnaire, payload);
    }
    if (questionnaire.scoring?.type === "endorsement_with_distress") {
      return evaluateEndorsement(questionnaire, payload);
    }
    return evaluateSum(questionnaire, payload);
  }

  function evaluateSum(questionnaire, payload) {
    const total = (questionnaire.questions || []).reduce((sum, question) => {
      return sum + (payload.responses[question.id]?.score || 0);
    }, 0);

    const band = findBand(questionnaire.scoring?.bands || [], total);
    const flags = [];

    if (questionnaire.scoring?.suicideFlagQuestionId) {
      const suicideScore = payload.responses[questionnaire.scoring.suicideFlagQuestionId]?.score || 0;
      if (suicideScore > 0) {
        flags.push({
          level: "warn",
          text: "자해·자살사고 관련 문항에 응답이 있어 즉시 추가 확인이 필요합니다."
        });
      }
    }

    return {
      title: `${questionnaire.shortTitle || questionnaire.title} 결과`,
      scoreText: `${total}점`,
      bandText: band ? band.label : "참고 구간 없음",
      highlights: [
        `총점 ${total}점`,
        ...(band?.description ? [band.description] : []),
        ...(questionnaire.scoring?.highlights || [])
      ],
      flags,
      notes: questionnaire.scoring?.notes || []
    };
  }

  function evaluateCri(questionnaire, payload) {
    const responses = payload.responses || {};
    const scoring = questionnaire.scoring || {};
    const groups = scoring.groupQuestionIds || {};
    const groupLabels = scoring.groupLabels || {};
    const grades = scoring.grades || {};

    const groupScore = (ids) =>
      (ids || []).reduce((sum, id) => sum + Number(responses[id]?.score || 0), 0);

    const harmScore = groupScore(groups.harm);
    const mentalScore = groupScore(groups.mental);
    const functionScore = groupScore(groups.function);
    const supportScore = groupScore(groups.support);
    const totalScore = harmScore + mentalScore + functionScore + supportScore;
    const primaryRiskScore = Number(responses[scoring.primaryRiskQuestionId]?.score || 0);
    const psychosisRiskScore = Number(responses[scoring.psychosisRiskQuestionId]?.score || 0);
    const psychosisMentalSum = psychosisRiskScore + mentalScore;

    let gradeKey = "E";
    if (primaryRiskScore === 1) {
      if (harmScore >= 2) {
        gradeKey = psychosisMentalSum >= 1 ? "A" : "B";
      } else {
        gradeKey = psychosisMentalSum >= 1 ? "C" : "D";
      }
    } else if (harmScore >= 1) {
      gradeKey = psychosisMentalSum >= 1 ? "C" : "D";
    } else {
      gradeKey = psychosisMentalSum >= 1 ? "D" : "E";
    }

    const grade = grades[gradeKey] || null;
    const flags = [];
    if (grade?.flagLevel && grade?.flagText) {
      flags.push({
        level: grade.flagLevel,
        text: grade.flagText
      });
    }

    return {
      title: `${questionnaire.shortTitle || questionnaire.title} 결과`,
      scoreText: `${totalScore}점 / 23점`,
      bandText: grade ? grade.label : "참고 구간 없음",
      highlights: [
        `총점 ${totalScore}점 / 23점`,
        `${groupLabels.harm || "자타해 위험"} ${harmScore}점`,
        `${groupLabels.mental || "정신상태"} ${mentalScore}점`,
        `${groupLabels.function || "기능수준"} ${functionScore}점`,
        `${groupLabels.support || "지지체계"} ${supportScore}점`,
        ...(grade?.description ? [grade.description] : [])
      ],
      flags,
      notes: scoring.notes || []
    };
  }

  function evaluateEndorsement(questionnaire, payload) {
    const yesCount = (questionnaire.questions || []).reduce((count, question) => {
      return count + ((payload.responses[question.id]?.score || 0) > 0 ? 1 : 0);
    }, 0);

    const distressTotal = (questionnaire.questions || []).reduce((sum, question) => {
      return sum + (question.subQuestions || []).reduce((subSum, subQuestion) => {
        return subSum + (payload.responses[subQuestion.id]?.score || 0);
      }, 0);
    }, 0);

    return {
      title: `${questionnaire.shortTitle || questionnaire.title} 결과`,
      scoreText: `예 ${yesCount}개 / 힘듦 ${distressTotal}점`,
      bandText: questionnaire.scoring?.summaryLabel || "참고용 요약",
      highlights: [
        `예 응답 수 ${yesCount}개`,
        `힘듦 보조합계 ${distressTotal}점`,
        ...(questionnaire.scoring?.referenceCutoff ? [`참고 절단값 ${questionnaire.scoring.referenceCutoff}`] : [])
      ],
      flags: questionnaire.scoring?.referenceWarning
        ? [{ level: "info", text: questionnaire.scoring.referenceWarning }]
        : [],
      notes: questionnaire.scoring?.notes || []
    };
  }

  function evaluateKmDQ(questionnaire, payload) {
    const symptomYesCount = (questionnaire.scoring?.symptomQuestionIds || []).reduce((count, id) => {
      return count + ((payload.responses[id]?.score || 0) > 0 ? 1 : 0);
    }, 0);
    const samePeriodScore = payload.responses[questionnaire.scoring.samePeriodQuestionId]?.score || 0;
    const impairmentScore = payload.responses[questionnaire.scoring.impairmentQuestionId]?.score || 0;

    const positive =
      symptomYesCount >= (questionnaire.scoring.positiveRule?.symptomYesAtLeast || 7) &&
      samePeriodScore >= (questionnaire.scoring.positiveRule?.samePeriodAtLeast || 1) &&
      impairmentScore >= (questionnaire.scoring.positiveRule?.impairmentAtLeast || 1);

    return {
      title: `${questionnaire.shortTitle || questionnaire.title} 결과`,
      scoreText: `예 ${symptomYesCount}개`,
      bandText: positive ? "양성 참고 기준 충족" : "양성 참고 기준 미충족",
      highlights: [
        `증상 문항 예 응답 ${symptomYesCount}개`,
        `같은 시기 발생 여부 ${samePeriodScore > 0 ? "예" : "아니오"}`,
        `기능 문제 수준 ${impairmentLabel(impairmentScore)}`
      ],
      flags: positive
        ? [{ level: "warn", text: "K-MDQ 참고 양성 기준에 해당해 추가 평가를 권합니다." }]
        : [],
      notes: questionnaire.scoring?.notes || []
    };
  }

  function findBand(bands, score) {
    return bands.find((band) => score >= band.min && score <= band.max) || null;
  }

  function impairmentLabel(score) {
    if (score === 0) return "문제 없음";
    if (score === 1) return "경미";
    if (score === 2) return "중등도";
    if (score === 3) return "심각";
    return "미응답";
  }

  function renderResultRecord(record) {
    ui.resultEmpty.classList.add("hidden");
    ui.resultPanel.classList.remove("hidden");
    renderPrintHeader(record);
    ui.resultTitle.textContent = record.evaluation.title;
    ui.resultScore.textContent = record.evaluation.scoreText;
    ui.resultBand.textContent = record.evaluation.bandText;

    ui.resultHighlights.innerHTML = "";
    (record.evaluation.highlights || []).forEach((text) => {
      const li = document.createElement("li");
      li.textContent = text;
      ui.resultHighlights.appendChild(li);
    });

    ui.resultFlags.innerHTML = "";
    (record.evaluation.flags || []).forEach((flag) => {
      const div = document.createElement("div");
      div.className = `flag-item ${flag.level || "info"}`;
      div.textContent = flag.text;
      ui.resultFlags.appendChild(div);
    });

    renderResultMeta(record);
    renderResultBreakdown(record);

    ui.resultNotes.innerHTML = "";
    (record.evaluation.notes || []).forEach((text) => {
      const li = document.createElement("li");
      li.textContent = text;
      ui.resultNotes.appendChild(li);
    });

    loadSignatureToCanvas(record);

    setActiveView("result");
  }

  function renderResultMeta(record) {
    const progress = resolveRecordProgress(record);
    const items = [
      { label: "척도", value: record.questionnaireTitle },
      { label: "검사일", value: record.meta?.sessionDate || "" },
      { label: "응답 진행률", value: formatProgressSummary(progress) },
      { label: "저장시각", value: formatDateTime(record.createdAt) },
      { label: "서버 저장자", value: record.owner?.displayName || "" },
      { label: "담당자", value: record.meta?.workerName || "" },
      { label: "대상자", value: record.meta?.clientLabel || "" },
      { label: "생년월일", value: record.meta?.birthDate || "" },
      ...record.respondentDisplay,
      { label: "비고", value: record.meta?.sessionNote || "" }
    ].filter((item) => item.value);

    ui.resultMeta.innerHTML = "";
    items.forEach((item) => {
      const dt = document.createElement("dt");
      dt.textContent = item.label;
      const dd = document.createElement("dd");
      dd.textContent = item.value;
      ui.resultMeta.append(dt, dd);
    });
  }

  function resolveRecordProgress(record) {
    if (record.progress && Number.isFinite(record.progress.percent)) {
      return record.progress;
    }

    const questionnaire =
      state.questionnaires.get(record.questionnaireId) ||
      getEmbeddedQuestionnaire(record.questionnaireId);

    if (!questionnaire) {
      return null;
    }

    return buildProgressSnapshot(questionnaire, {
      respondent: record.respondent || {},
      answers: record.answers || {}
    });
  }

  function renderResultBreakdown(record) {
    ui.resultBreakdown.innerHTML = "";
    (record.breakdown || []).forEach((item) => {
      const article = document.createElement("article");
      article.className = "breakdown-item";
      article.innerHTML = `
        <div class="breakdown-head">
          <p class="breakdown-title">${escapeHtml(item.number)} ${escapeHtml(item.text)}</p>
          <p class="breakdown-score">${item.score === null ? "-" : `${item.score}점`}</p>
        </div>
        <p class="breakdown-answer">${escapeHtml(item.answerLabel)}</p>
      `;

      if (item.subAnswers && item.subAnswers.length) {
        const subWrap = document.createElement("div");
        subWrap.className = "breakdown-sublist";
        item.subAnswers.forEach((subItem) => {
          const row = document.createElement("div");
          row.className = "breakdown-subitem";
          row.innerHTML = `
            <p><strong>${escapeHtml(subItem.number)}</strong> ${escapeHtml(subItem.text)}</p>
            <p>${escapeHtml(subItem.answerLabel)} · ${escapeHtml(String(subItem.score))}점</p>
          `;
          subWrap.appendChild(row);
        });
        article.appendChild(subWrap);
      }

      ui.resultBreakdown.appendChild(article);
    });
  }

  function clearResult() {
    state.lastResult = null;
    ui.resultEmpty.classList.remove("hidden");
    ui.resultPanel.classList.add("hidden");
    ui.printOrgLine.textContent = "";
    ui.printScaleLine.textContent = "";
    ui.printMetaLine.textContent = "";
    ui.resultHighlights.innerHTML = "";
    ui.resultFlags.innerHTML = "";
    ui.resultMeta.innerHTML = "";
    ui.resultBreakdown.innerHTML = "";
    ui.resultNotes.innerHTML = "";
    resetSignatureCanvas();
    setActiveView("screening");
  }

  function renderPrintHeader(record) {
    const settings = readAppSettings();
    const orgLine = [settings.organizationName, settings.teamName].filter(Boolean).join(" · ");
    const metaLine = [
      record.meta?.sessionDate ? `검사일 ${record.meta.sessionDate}` : "",
      record.meta?.workerName ? `담당자 ${record.meta.workerName}` : "",
      record.meta?.clientLabel ? `대상자 ${record.meta.clientLabel}` : "",
      record.meta?.birthDate ? `생년월일 ${record.meta.birthDate}` : "",
      record.meta?.sessionNote ? `비고 ${record.meta.sessionNote}` : "",
      settings.contactNote || ""
    ]
      .filter(Boolean)
      .join(" / ");

    ui.printOrgLine.textContent = orgLine || "정신건강 척도검사 결과지";
    ui.printScaleLine.textContent = `${record.questionnaireTitle} 결과지`;
    ui.printMetaLine.textContent = metaLine;
  }

  function onSaveResult() {
    if (KIOSK_MODE && !state.kioskUnlocked) {
      if (!appConfig.kioskPinSet) {
        alert("키오스크 PIN이 설정되지 않아 저장할 수 없습니다. 관리자에게 문의해주세요.");
        return;
      }
      showKioskPinModal();
      return;
    }
    if (state.bundle.active) {
      onBundleScaleComplete(state.lastResult);
      return;
    }
    void onSaveAllResult();
  }

  function saveResultToLocal(record) {
    const history = loadHistory();
    const existingIndex = history.findIndex((entry) => entry.id === record.id);
    if (existingIndex >= 0) {
      history.splice(existingIndex, 1);
    }

    history.unshift(structuredCloneSafe(record));
    localStorage.setItem(HISTORY_KEY, JSON.stringify(history.slice(0, 50)));
    renderHistory();
  }

  async function saveResultToServer(record) {
    const response = await apiRequest("/api/records", {
      method: "POST",
      body: {
        record: structuredCloneSafe(record)
      }
    });

    if (response.record) {
      state.lastResult = response.record;
      renderResultRecord(state.lastResult);
    }

    await loadServerRecords(false);
    return response.record || record;
  }

  async function onSaveAllResult() {
    if (!state.lastResult) {
      alert("먼저 결과를 계산해주세요.");
      return;
    }

    let savedRecord = state.lastResult;
    const completedSteps = [];
    const failedSteps = [];

    try {
      saveResultToLocal(savedRecord);
      completedSteps.push("이 기기 저장");
    } catch (error) {
      console.error(error);
      failedSteps.push(`이 기기 저장 실패: ${error.message}`);
    }

    if (state.session.user) {
      try {
        savedRecord = await saveResultToServer(savedRecord);
        saveResultToLocal(savedRecord);
        completedSteps.push("서버 저장");
      } catch (error) {
        console.error(error);
        failedSteps.push(`서버 저장 실패: ${error.message}`);
      }
    }

    try {
      exportRecordAsJson(savedRecord);
      completedSteps.push("결과 파일 저장");
    } catch (error) {
      console.error(error);
      failedSteps.push(`결과 파일 저장 실패: ${error.message}`);
    }

    setActiveView("history");

    if (!state.session.user) {
      failedSteps.push("서버 저장은 로그인 후 사용할 수 있습니다.");
    }

    if (failedSteps.length) {
      const doneText = completedSteps.length ? `완료: ${completedSteps.join(", ")}.` : "";
      alert([doneText, ...failedSteps].filter(Boolean).join(" "));
      return;
    }

    alert(`${completedSteps.join(", ")}을 완료했습니다.`);
  }

  // ── FR-01: 이용자 클라이언트 관리 ────────────────────────────────

  async function loadClients() {
    try {
      const response = await apiRequest("/api/clients");
      state.clients = response.clients || [];
      renderClientSelect();
    } catch (error) {
      console.warn("클라이언트 로드 실패", error);
    }
  }

  function renderClientSelect() {
    if (!ui.clientIdSelect) return;
    const prev = ui.clientIdSelect.value;
    ui.clientIdSelect.innerHTML = '<option value="">-- 이용자 선택 또는 신규 등록 --</option>';
    state.clients.forEach((c) => {
      const opt = document.createElement("option");
      opt.value = c.id;
      const birth = c.birthDate ? ` (${c.birthDate})` : "";
      const memo = c.memo ? ` / ${c.memo}` : "";
      opt.textContent = `${c.id}${birth}${memo}`;
      ui.clientIdSelect.appendChild(opt);
    });
    if (prev && state.clients.some((c) => c.id === prev)) {
      ui.clientIdSelect.value = prev;
    }
  }

  function onClientIdSelectChange() {
    const clientId = ui.clientIdSelect ? ui.clientIdSelect.value : "";
    if (!clientId) {
      if (ui.clientHistoryPanel) ui.clientHistoryPanel.classList.add("hidden");
      return;
    }
    void loadClientHistory(clientId);
  }

  async function loadClientHistory(clientId) {
    if (!ui.clientHistoryPanel) return;
    try {
      const response = await apiRequest(`/api/clients/${encodeURIComponent(clientId)}/records`);
      const records = response.records || [];
      const client = state.clients.find((c) => c.id === clientId);
      renderClientHistory(clientId, client, records);
    } catch (error) {
      console.warn("이력 로드 실패", error);
    }
  }

  function renderClientHistory(clientId, client, records) {
    if (!ui.clientHistoryPanel || !ui.clientHistoryList) return;
    if (ui.clientHistoryTitle) {
      const birth = client && client.birthDate ? ` (${client.birthDate})` : "";
      ui.clientHistoryTitle.textContent = `${clientId}${birth} 검사 이력`;
    }
    ui.clientHistoryList.innerHTML = "";
    if (records.length === 0) {
      ui.clientHistoryList.innerHTML = '<p class="muted">검사 기록이 없습니다.</p>';
    } else {
      const byScale = {};
      records.forEach((r) => {
        const sid = r.questionnaireId;
        if (!byScale[sid]) byScale[sid] = { title: r.shortTitle || r.questionnaireTitle, records: [] };
        byScale[sid].records.push(r);
      });

      Object.entries(byScale).forEach(([sid, data]) => {
        const section = document.createElement("div");
        section.className = "client-history-scale";

        const heading = document.createElement("h4");
        heading.textContent = data.title;
        section.appendChild(heading);

        const sorted = data.records.slice().sort((a, b) =>
          String(a.meta?.sessionDate || a.createdAt || "").localeCompare(String(b.meta?.sessionDate || b.createdAt || ""))
        );

        sorted.forEach((r, idx) => {
          const row = document.createElement("div");
          row.className = "client-history-row";
          const date = r.meta?.sessionDate || r.createdAt?.slice(0, 10) || "";
          const score = r.evaluation?.scoreText || "";
          const band = r.evaluation?.band || "";
          let badge = "";
          if (idx > 0) {
            const prevScore = sorted[idx - 1].evaluation?.score;
            const curScore = r.evaluation?.score;
            if (typeof prevScore === "number" && typeof curScore === "number") {
              badge = curScore > prevScore ? " ▲" : curScore < prevScore ? " ▼" : " ─";
            }
          }
          row.innerHTML = `<span class="ch-date">${escapeHtml(date)}</span><span class="ch-score">${escapeHtml(score)} <em>${escapeHtml(band)}</em>${badge}</span>`;
          section.appendChild(row);
        });

        ui.clientHistoryList.appendChild(section);
      });
    }
    ui.clientHistoryPanel.classList.remove("hidden");
  }

  function onCloseClientHistory() {
    if (ui.clientHistoryPanel) ui.clientHistoryPanel.classList.add("hidden");
  }

  function onRegisterClientClick() {
    if (!ui.clientRegisterPanel) return;
    ui.clientRegisterPanel.classList.remove("hidden");
    if (ui.newClientBirthDate) ui.newClientBirthDate.value = "";
    if (ui.newClientMemo) ui.newClientMemo.value = "";
  }

  function onCancelRegisterClient() {
    if (ui.clientRegisterPanel) ui.clientRegisterPanel.classList.add("hidden");
  }

  async function onConfirmRegisterClient() {
    const birthDate = ui.newClientBirthDate ? ui.newClientBirthDate.value.trim() : "";
    const memo = ui.newClientMemo ? ui.newClientMemo.value.trim() : "";
    try {
      const response = await apiRequest("/api/clients", {
        method: "POST",
        body: { birthDate, memo }
      });
      if (response.ok && response.client) {
        state.clients.push(response.client);
        renderClientSelect();
        if (ui.clientIdSelect) ui.clientIdSelect.value = response.client.id;
        if (ui.clientRegisterPanel) ui.clientRegisterPanel.classList.add("hidden");
        alert(`이용자 등록 완료: ${response.client.id}`);
      }
    } catch (error) {
      alert(`등록 실패: ${error.message}`);
    }
  }

  // ── FR-02: 위험군 대시보드 ──────────────────────────────────────

  async function loadRiskSummary() {
    if (!state.session.user) return;
    try {
      const response = await apiRequest("/api/risk-flags");
      state.riskFlags = response.riskFlags || [];
      state.riskUnacknowledgedCount = response.unacknowledgedCount || 0;
      renderRiskBadge();
    } catch (error) {
      console.warn("위험 플래그 로드 실패", error);
    }
  }

  function renderRiskBadge() {
    if (!ui.riskBadge) return;
    const count = state.riskUnacknowledgedCount;
    ui.riskBadge.textContent = count > 0 ? String(count) : "";
    ui.riskBadge.classList.toggle("hidden", count === 0);
  }

  async function onRefreshRiskFlags() {
    await loadRiskSummary();
    renderRiskFlagsList();
  }

  function renderRiskFlagsList() {
    if (!ui.riskFlagsList) return;
    ui.riskFlagsList.innerHTML = "";
    const flags = state.riskFlags;
    if (ui.riskFlagsEmpty) {
      ui.riskFlagsEmpty.classList.toggle("hidden", flags.length > 0);
    }
    flags.forEach((flag) => {
      const card = document.createElement("div");
      card.className = `risk-flag-card${flag.acknowledged ? " acknowledged" : ""}`;
      card.dataset.recordId = flag.id;

      const flagLabels = (flag.flags || []).map((f) => `<span class="risk-tag">${escapeHtml(f)}</span>`).join(" ");
      const noteHtml = (flag.notes || [])
        .map((n) => `<div class="risk-note-item"><span class="risk-note-author">${escapeHtml(n.author)}</span> <span class="risk-note-text">${escapeHtml(n.text)}</span> <span class="risk-note-time muted">${n.createdAt ? n.createdAt.slice(0, 10) : ""}</span></div>`)
        .join("");

      card.innerHTML = `
        <div class="risk-card-head">
          <div>
            <strong>${escapeHtml(flag.questionnaireTitle)}</strong>
            <span class="muted"> · ${escapeHtml(flag.sessionDate)} · ${escapeHtml(flag.workerName)} · ${escapeHtml(flag.clientLabel || flag.clientId || "")}</span>
          </div>
          <div class="risk-card-score">${escapeHtml(flag.scoreText)}</div>
        </div>
        <div class="risk-tags">${flagLabels}</div>
        ${noteHtml ? `<div class="risk-notes">${noteHtml}</div>` : ""}
        <div class="risk-card-actions">
          <input type="text" class="risk-note-input" placeholder="확인 메모 입력..." data-record-id="${escapeHtml(flag.id)}" />
          <button class="btn btn-secondary risk-note-submit-btn" type="button" data-record-id="${escapeHtml(flag.id)}">메모 추가</button>
          <button class="btn ${flag.acknowledged ? "btn-secondary" : "btn-primary"} risk-ack-btn" type="button" data-record-id="${escapeHtml(flag.id)}" data-ack="${flag.acknowledged ? "true" : "false"}">
            ${flag.acknowledged ? "확인 완료 ✓" : "확인 처리"}
          </button>
        </div>
      `;
      ui.riskFlagsList.appendChild(card);
    });
  }

  async function onRiskFlagsClick(event) {
    const ackBtn = event.target.closest(".risk-ack-btn");
    if (ackBtn) {
      const recordId = ackBtn.dataset.recordId;
      const isAcked = ackBtn.dataset.ack === "true";
      try {
        await apiRequest(`/api/risk-flags/${encodeURIComponent(recordId)}/acknowledge`, {
          method: "POST",
          body: { acknowledged: !isAcked }
        });
        await loadRiskSummary();
        renderRiskFlagsList();
      } catch (error) {
        alert(`처리 실패: ${error.message}`);
      }
      return;
    }

    const noteBtn = event.target.closest(".risk-note-submit-btn");
    if (noteBtn) {
      const recordId = noteBtn.dataset.recordId;
      const input = ui.riskFlagsList.querySelector(`.risk-note-input[data-record-id="${CSS.escape(recordId)}"]`);
      const text = input ? input.value.trim() : "";
      if (!text) { alert("메모 내용을 입력해주세요."); return; }
      try {
        await apiRequest(`/api/risk-flags/${encodeURIComponent(recordId)}/notes`, {
          method: "POST",
          body: { text }
        });
        await loadRiskSummary();
        renderRiskFlagsList();
      } catch (error) {
        alert(`메모 추가 실패: ${error.message}`);
      }
    }
  }

  // ── FR-06: 실적 집계 보고서 ────────────────────────────────────

  async function onGenerateReport() {
    if (!ui.generateReportBtn) return;
    const from = ui.reportFromDate ? ui.reportFromDate.value : "";
    const to = ui.reportToDate ? ui.reportToDate.value : "";
    ui.generateReportBtn.disabled = true;
    try {
      const params = new URLSearchParams();
      if (from) params.set("from", from);
      if (to) params.set("to", to);
      const response = await apiRequest(`/api/reports/summary?${params.toString()}`);
      if (response.ok && response.summary) {
        const text = buildReportText(response.summary);
        if (ui.reportOutput) {
          ui.reportOutput.value = text;
          ui.reportOutput.classList.remove("hidden");
        }
        if (ui.copyReportBtn) ui.copyReportBtn.classList.remove("hidden");
        if (ui.downloadReportBtn) ui.downloadReportBtn.classList.remove("hidden");
      }
    } catch (error) {
      if (ui.reportOutput) ui.reportOutput.value = `보고서 생성 실패: ${error.message}`;
    } finally {
      ui.generateReportBtn.disabled = false;
    }
  }

  function buildReportText(summary) {
    const settings = readAppSettings();
    const periodStr = summary.period.from && summary.period.to
      ? `${summary.period.from} ~ ${summary.period.to}`
      : summary.period.from ? `${summary.period.from} 이후` : summary.period.to ? `${summary.period.to} 이전` : "전체 기간";
    const lines = [];
    lines.push(`[${settings.organizationName} ${settings.teamName}] 정신건강 스크리닝 실적 보고서`);
    lines.push(`기간: ${periodStr}`);
    lines.push(`작성일: ${new Date().toLocaleDateString("ko-KR")}`);
    lines.push("");
    lines.push("▣ 전체 시행 건수");
    lines.push(`  총 ${summary.totalCount}건`);
    lines.push(`  위험 플래그 발생: ${summary.riskCount}건`);
    lines.push("");
    lines.push("▣ 척도별 시행 현황");
    (summary.byScale || []).forEach((s) => {
      const avg = s.avgScore !== null ? ` (평균 ${s.avgScore}점)` : "";
      lines.push(`  · ${s.title}: ${s.count}건${avg}`);
    });
    lines.push("");
    lines.push("▣ 담당자별 시행 건수");
    Object.entries(summary.byWorker || {}).forEach(([worker, count]) => {
      lines.push(`  · ${worker}: ${count}건`);
    });
    lines.push("");
    lines.push("▣ 월별 시행 건수");
    Object.entries(summary.byMonth || {}).sort(([a], [b]) => a.localeCompare(b)).forEach(([month, count]) => {
      lines.push(`  · ${month}: ${count}건`);
    });
    lines.push("");
    lines.push("※ 본 보고서는 참고용이며, 척도 결과는 진단을 대신하지 않습니다.");
    return lines.join("\n");
  }

  function onCopyReport() {
    if (!ui.reportOutput) return;
    navigator.clipboard.writeText(ui.reportOutput.value).then(() => {
      alert("클립보드에 복사했습니다.");
    }).catch(() => {
      ui.reportOutput.select();
      document.execCommand("copy");
      alert("클립보드에 복사했습니다.");
    });
  }

  function onDownloadReport() {
    if (!ui.reportOutput) return;
    const blob = new Blob([ui.reportOutput.value], { type: "text/plain;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `정신건강_실적보고서_${new Date().toISOString().slice(0, 10)}.txt`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }

  async function onSaveConfig() {
    if (!ui.saveConfigBtn) return;
    ui.saveConfigBtn.disabled = true;
    if (ui.configSaveStatus) ui.configSaveStatus.textContent = "저장 중...";
    try {
      const body = {
        organizationName: ui.configOrgName ? ui.configOrgName.value.trim() : undefined,
        teamName: ui.configTeamName ? ui.configTeamName.value.trim() : undefined,
        contactNote: ui.configContactNote ? ui.configContactNote.value.trim() : undefined
      };
      const pinValue = ui.configKioskPin ? ui.configKioskPin.value.trim() : "";
      if (pinValue) body.kioskPin = pinValue;
      const response = await apiRequest("/api/config", { method: "PATCH", body });
      if (response.ok && response.config) {
        appConfig = { ...appConfig, ...response.config };
        if (ui.configKioskPin) ui.configKioskPin.value = "";
        applyConfigToUi();
        if (ui.configSaveStatus) ui.configSaveStatus.textContent = "저장했습니다.";
      }
    } catch (error) {
      if (ui.configSaveStatus) ui.configSaveStatus.textContent = `저장 실패: ${error.message}`;
    } finally {
      ui.saveConfigBtn.disabled = false;
    }
  }

  // ── 키오스크 모드 ──────────────────────────────────────────────

  function showKioskPinModal() {
    if (!ui.kioskPinModal) return;
    if (ui.kioskPinInput) ui.kioskPinInput.value = "";
    if (ui.kioskPinError) ui.kioskPinError.textContent = "";
    ui.kioskPinModal.classList.remove("hidden");
    if (ui.kioskPinInput) ui.kioskPinInput.focus();
  }

  function hideKioskPinModal() {
    if (ui.kioskPinModal) ui.kioskPinModal.classList.add("hidden");
  }

  async function onKioskPinSubmit() {
    if (!ui.kioskPinInput) return;
    const pin = ui.kioskPinInput.value.trim();
    if (!pin) {
      if (ui.kioskPinError) ui.kioskPinError.textContent = "PIN을 입력해주세요.";
      return;
    }
    if (ui.kioskPinSubmitBtn) ui.kioskPinSubmitBtn.disabled = true;
    try {
      const response = await fetch("/api/kiosk/verify-pin", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ pin }),
        cache: "no-store"
      });
      const data = await response.json();
      if (data.ok) {
        state.kioskUnlocked = true;
        hideKioskPinModal();
        document.body.classList.add("kiosk-unlocked");
        void onSaveAllResult();
      } else {
        if (ui.kioskPinError) ui.kioskPinError.textContent = data.error || "PIN이 올바르지 않습니다.";
        if (ui.kioskPinInput) ui.kioskPinInput.select();
      }
    } catch (error) {
      if (ui.kioskPinError) ui.kioskPinError.textContent = "서버 연결 오류가 발생했습니다.";
    } finally {
      if (ui.kioskPinSubmitBtn) ui.kioskPinSubmitBtn.disabled = false;
    }
  }

  // ── 세션 묶음(Bundle) 모드 ─────────────────────────────────────

  function renderBundleSetup() {
    if (!ui.bundleScaleCheckboxes) return;
    ui.bundleScaleCheckboxes.innerHTML = "";
    state.manifest.forEach((scale) => {
      const label = document.createElement("label");
      label.className = "bundle-scale-check";
      const checkbox = document.createElement("input");
      checkbox.type = "checkbox";
      checkbox.value = scale.id;
      checkbox.name = "bundleScale";
      label.appendChild(checkbox);
      label.appendChild(document.createTextNode(` ${scale.shortTitle || scale.title}`));
      ui.bundleScaleCheckboxes.appendChild(label);
    });
  }

  function onStartBundle() {
    if (!ui.bundleScaleCheckboxes) return;
    const checked = Array.from(ui.bundleScaleCheckboxes.querySelectorAll("input[name=bundleScale]:checked"));
    if (checked.length < 2) {
      alert("묶음 모드는 척도를 2개 이상 선택해야 합니다.");
      return;
    }
    const scaleIds = checked.map((cb) => cb.value);
    state.bundle = {
      active: true,
      id: `bnd_${Date.now().toString(36)}`,
      scales: scaleIds,
      currentIndex: 0,
      results: [],
      sessionInfo: null
    };
    if (ui.bundleSetupPanel) ui.bundleSetupPanel.classList.add("hidden");
    updateBundleProgress();
    void loadQuestionnaire(scaleIds[0]);
    setActiveView("screening");
  }

  function updateBundleProgress() {
    if (!state.bundle.active || !ui.bundleProgress) return;
    const total = state.bundle.scales.length;
    const current = state.bundle.currentIndex + 1;
    ui.bundleProgress.textContent = `묶음 진행: ${current} / ${total}`;
    ui.bundleProgress.classList.remove("hidden");
  }

  function onBundleScaleComplete(result) {
    if (!state.bundle.active) return;
    const resultWithBundle = { ...result, bundleId: state.bundle.id };
    state.bundle.results.push(resultWithBundle);
    state.bundle.currentIndex += 1;
    if (state.bundle.currentIndex < state.bundle.scales.length) {
      const nextScaleId = state.bundle.scales[state.bundle.currentIndex];
      updateBundleProgress();
      void loadQuestionnaire(nextScaleId);
    } else {
      showBundleSummary();
    }
  }

  function showBundleSummary() {
    if (!ui.bundleSummaryPanel || !ui.bundleSummaryList) return;
    ui.bundleProgress.classList.add("hidden");
    ui.bundleSummaryList.innerHTML = "";
    state.bundle.results.forEach((record) => {
      const li = document.createElement("div");
      li.className = "bundle-summary-item";
      const scoreText = record.evaluation ? record.evaluation.scoreText || "" : "";
      const bandText = record.evaluation ? record.evaluation.band || "" : "";
      li.textContent = `${record.questionnaireTitle}: ${scoreText} (${bandText})`;
      ui.bundleSummaryList.appendChild(li);
    });
    ui.bundleSummaryPanel.classList.remove("hidden");
    setActiveView("result");
  }

  async function onBundleSaveAll() {
    if (!state.bundle.active || state.bundle.results.length === 0) return;
    if (ui.bundleSaveAllBtn) ui.bundleSaveAllBtn.disabled = true;
    try {
      for (const record of state.bundle.results) {
        saveResultToLocal(record);
        if (state.session.user) {
          await saveResultToServer(record);
        }
      }
      alert(`${state.bundle.results.length}개 척도 결과를 저장했습니다.`);
      onBundleNew();
    } catch (error) {
      alert(`저장 중 오류가 발생했습니다: ${error.message}`);
    } finally {
      if (ui.bundleSaveAllBtn) ui.bundleSaveAllBtn.disabled = false;
    }
  }

  function onBundleNew() {
    state.bundle = { active: false, id: null, scales: [], currentIndex: 0, results: [], sessionInfo: null };
    if (ui.bundleSummaryPanel) ui.bundleSummaryPanel.classList.add("hidden");
    if (ui.bundleProgress) ui.bundleProgress.classList.add("hidden");
    if (ui.bundleSetupPanel) ui.bundleSetupPanel.classList.remove("hidden");
    renderBundleSetup();
    setActiveView("screening");
  }

  async function onSyncQuestionnaires() {
    const validation = validateGoogleSyncSettings();
    if (!validation.ok) {
      setSyncStatus(validation.message, "error");
      alert(validation.message);
      return;
    }

    setSyncBusy(true, "척도 마스터를 불러오는 중입니다...");

    try {
      await ensureAllQuestionnairesLoaded();
      const payload = buildQuestionnaireMasterPayload();
      await postGoogleSyncPayload(payload);
      setSyncStatus(
        `척도 마스터 ${payload.questionnaires.length}개 전송 요청을 서버 프록시로 보냈습니다. 구글 시트에서 반영 여부를 확인해주세요.`,
        "success"
      );
    } catch (error) {
      console.error(error);
      setSyncStatus(`척도 마스터 전송 실패: ${error.message}`, "error");
      alert(`척도 마스터 전송에 실패했습니다. ${error.message}`);
    } finally {
      setSyncBusy(false);
    }
  }

  async function onSyncCurrentResult() {
    if (!state.lastResult) {
      alert("먼저 결과를 계산해주세요.");
      return;
    }
    await sendRecordsToGoogleSheets([state.lastResult], "current_result");
  }

  async function onSyncHistory() {
    const history = loadHistory();
    if (!history.length) {
      alert("전송할 저장 기록이 없습니다.");
      return;
    }
    await sendRecordsToGoogleSheets(history, "history_all");
  }

  async function syncSingleRecordToGoogleSheets(record, syncScope, successMessage) {
    const ok = await sendRecordsToGoogleSheets([record], syncScope);
    if (ok && successMessage) {
      alert(successMessage);
    }
    return ok;
  }

  async function sendRecordsToGoogleSheets(records, syncScope) {
    const validation = validateGoogleSyncSettings();
    if (!validation.ok) {
      if (!IS_ADMIN_PAGE) {
        setActiveView("management");
      }
      setSyncStatus(validation.message, "error");
      alert(validation.message);
      return false;
    }

    const scopeLabel = getGoogleSyncScopeLabel(syncScope);
    setSyncBusy(true, `${scopeLabel} ${records.length}건 전송 요청을 준비하고 있습니다...`);

    try {
      await ensureAllQuestionnairesLoaded();
      const payload = buildGoogleSyncPayload(records, syncScope);
      await postGoogleSyncPayload(payload);

      setSyncStatus(
        `${scopeLabel} ${records.length}건 전송 요청을 서버 프록시로 보냈습니다. 구글 시트에서 반영 여부를 확인해주세요.`,
        "success"
      );
      return true;
    } catch (error) {
      console.error(error);
      setSyncStatus(`구글 시트 전송 실패: ${error.message}`, "error");
      alert(`구글 시트 전송에 실패했습니다. ${error.message}`);
      return false;
    } finally {
      setSyncBusy(false);
    }
  }

  function getGoogleSyncScopeLabel(syncScope) {
    switch (syncScope) {
      case "current_result":
        return "현재 결과";
      case "history_single":
        return "이 기기 저장 결과";
      case "server_single":
        return "계정 저장 결과";
      case "questionnaire_master":
        return "척도 마스터";
      default:
        return "저장 기록";
    }
  }

  function validateGoogleSyncSettings() {
    if (!appConfig.googleSyncConfigured) {
      return {
        ok: false,
        message: "서버에 구글 시트 연동이 설정되지 않았습니다. 관리자에게 문의해주세요."
      };
    }

    if (!state.session.user) {
      return {
        ok: false,
        message: "로그인한 뒤 구글 시트 전송을 사용할 수 있습니다."
      };
    }

    return { ok: true };
  }

  function buildGoogleSyncPayload(records, syncScope) {
    return {
      version: 1,
      source: "scale-screening-web-app",
      syncScope,
      sentAt: new Date().toISOString(),
      appSettings: readAppSettings(),
      records: records.map((record) => structuredCloneSafe(record)),
      questionnaires: state.manifest
        .map((item) => state.questionnaires.get(item.id))
        .filter(Boolean)
        .map((questionnaire) => structuredCloneSafe(questionnaire))
    };
  }

  function buildQuestionnaireMasterPayload() {
    return {
      version: 1,
      source: "scale-screening-web-app",
      syncScope: "questionnaire_master",
      sentAt: new Date().toISOString(),
      appSettings: readAppSettings(),
      questionnaires: state.manifest
        .map((item) => state.questionnaires.get(item.id))
        .filter(Boolean)
        .map((questionnaire) => structuredCloneSafe(questionnaire))
    };
  }

  async function ensureAllQuestionnairesLoaded() {
    for (const item of state.manifest) {
      if (state.questionnaires.has(item.id)) {
        continue;
      }

      const embeddedQuestionnaire = getEmbeddedQuestionnaire(item.id);
      if (embeddedQuestionnaire) {
        state.questionnaires.set(item.id, embeddedQuestionnaire);
        continue;
      }

      const response = await fetch(item.path);
      if (!response.ok) {
        throw new Error(`questionnaire load failed: ${item.id}`);
      }
      state.questionnaires.set(item.id, await response.json());
    }
  }

  function getEmbeddedQuestionnaire(id) {
    if (!EMBEDDED_DATA || !EMBEDDED_DATA.questionnaires || !EMBEDDED_DATA.questionnaires[id]) {
      return null;
    }
    return structuredCloneSafe(EMBEDDED_DATA.questionnaires[id]);
  }

  async function postGoogleSyncPayload(payload) {
    await apiRequest("/api/google-sync", {
      method: "POST",
      body: payload
    });
  }

  async function apiRequest(url, options = {}) {
    const requestOptions = {
      method: options.method || "GET",
      headers: {
        "Content-Type": "application/json"
      },
      cache: "no-store",
      ...options
    };

    if (options.body !== undefined) {
      requestOptions.body = JSON.stringify(options.body);
    }

    let response;

    try {
      response = await fetch(url, requestOptions);
    } catch (error) {
      if (isApiRequestUrl(url)) {
        throw new Error(buildApiConnectionMessage());
      }
      throw error;
    }

    const text = await response.text();
    let data = {};

    try {
      data = text ? JSON.parse(text) : {};
    } catch (error) {
      if (isApiRequestUrl(url)) {
        throw new Error(buildApiConnectionMessage());
      }
      throw new Error("서버 응답을 해석할 수 없습니다.");
    }

    if (!response.ok || data.ok === false) {
      if (isApiRequestUrl(url) && response.status === 404) {
        throw new Error(buildApiConnectionMessage());
      }
      throw new Error(data.error || "요청 처리에 실패했습니다.");
    }

    return data;
  }

  function setSyncBusy(isBusy, message) {
    const canSync = canUseGoogleSync();
    ui.syncQuestionnairesBtn.disabled = isBusy || !canSync;
    ui.syncCurrentBtn.disabled = isBusy || !canSync;
    ui.syncHistoryBtn.disabled = isBusy || !canSync;
    ui.syncStatus.dataset.busy = isBusy ? "true" : "";
    if (message) {
      setSyncStatus(message, isBusy ? "" : "success");
    }
  }

  function setSyncStatus(message, type) {
    ui.syncStatus.textContent = message;
    ui.syncStatus.classList.remove("success", "error");
    if (type) {
      ui.syncStatus.classList.add(type);
    }
  }

  function renderHistory() {
    const history = loadHistory();
    ui.historyTableBody.innerHTML = "";
    const syncDisabledAttr = canUseGoogleSync() ? "" : " disabled";

    if (!history.length) {
      ui.historyTableEmpty.classList.remove("hidden");
      return;
    }

    ui.historyTableEmpty.classList.add("hidden");

    history.forEach((entry) => {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${escapeHtml(formatDateTime(entry.createdAt))}</td>
        <td>${escapeHtml(entry.meta?.sessionDate || "")}</td>
        <td class="cell-strong">${escapeHtml(entry.questionnaireTitle || "")}</td>
        <td>${escapeHtml(entry.evaluation?.scoreText || "")}</td>
        <td>${escapeHtml(entry.evaluation?.bandText || "")}</td>
        <td>${escapeHtml(entry.meta?.workerName || "")}</td>
        <td>${escapeHtml(entry.meta?.clientLabel || "")}</td>
        <td>${escapeHtml(entry.meta?.birthDate || "")}</td>
        <td>${escapeHtml(displayValue(entry.respondentDisplay, "성별"))}</td>
        <td>${escapeHtml(displayValue(entry.respondentDisplay, "연령대"))}</td>
        <td>
          <div class="table-actions">
            <button class="history-btn" type="button" data-action="view" data-id="${escapeHtml(entry.id)}">보기</button>
            <button class="history-btn" type="button" data-action="export" data-id="${escapeHtml(entry.id)}">파일 저장</button>
            <button class="history-btn" type="button" data-action="sync" data-id="${escapeHtml(entry.id)}"${syncDisabledAttr}>구글 시트 전송</button>
            <button class="history-btn danger" type="button" data-action="delete" data-id="${escapeHtml(entry.id)}">삭제</button>
          </div>
        </td>
      `;
      ui.historyTableBody.appendChild(row);
    });
  }

  async function loadServerRecords(silent) {
    if (!state.session.user) {
      state.serverRecords = [];
      renderServerRecords();
      return;
    }

    const scope = state.session.user.role === "admin" ? ui.serverRecordScope.value : "mine";

    try {
      const response = await apiRequest(`/api/records?scope=${encodeURIComponent(scope)}`);
      state.serverRecords = Array.isArray(response.records) ? response.records : [];
      renderServerRecords();
      if (!silent) {
        ui.serverRecordsMessage.textContent = "서버 저장 결과를 불러왔습니다.";
      }
    } catch (error) {
      state.serverRecords = [];
      renderServerRecords();
      ui.serverRecordsMessage.textContent = error.message;
      if (!silent) {
        throw error;
      }
    }
  }

  function renderServerRecords() {
    ui.serverRecordsList.innerHTML = "";
    const syncDisabledAttr = canUseGoogleSync() ? "" : " disabled";

    if (!state.session.user) {
      ui.serverRecordsMessage.textContent = state.session.bootstrapRequired
        ? "초기 관리자 계정을 만든 뒤 서버 저장 결과를 사용할 수 있습니다."
        : "로그인하면 서버 저장 결과를 확인할 수 있습니다.";
      return;
    }

    if (!state.serverRecords.length) {
      ui.serverRecordsMessage.textContent = "서버에 저장된 결과가 없습니다.";
      return;
    }

    ui.serverRecordsMessage.textContent = `${state.serverRecords.length}건의 서버 저장 결과를 표시합니다.`;

    state.serverRecords.forEach((entry) => {
      const article = document.createElement("article");
      article.className = "history-item";
      article.innerHTML = `
        <p class="history-title">${escapeHtml(entry.questionnaireTitle || entry.questionnaireId || "척도 결과")}</p>
        <p class="history-meta">${escapeHtml(formatDateTime(entry.updatedAt || entry.createdAt || ""))}</p>
        <p class="history-score">${escapeHtml(entry.scoreText || "")} · ${escapeHtml(entry.bandText || "")}</p>
        <p class="history-owner">${escapeHtml(entry.ownerDisplayName || "")}${entry.ownerUsername ? ` · ${escapeHtml(entry.ownerUsername)}` : ""}</p>
        <div class="history-actions">
          <button class="history-btn" type="button" data-source="server" data-action="view" data-id="${escapeHtml(entry.id)}">보기</button>
          <button class="history-btn" type="button" data-source="server" data-action="export" data-id="${escapeHtml(entry.id)}">파일 저장</button>
          <button class="history-btn" type="button" data-source="server" data-action="sync" data-id="${escapeHtml(entry.id)}"${syncDisabledAttr}>구글 시트 전송</button>
          <button class="history-btn danger" type="button" data-source="server" data-action="delete" data-id="${escapeHtml(entry.id)}">삭제</button>
        </div>
      `;
      ui.serverRecordsList.appendChild(article);
    });
  }

  async function onRefreshServerRecords() {
    if (!state.session.user) {
      alert("먼저 로그인해주세요.");
      return;
    }

    try {
      await loadServerRecords(false);
    } catch (error) {
      console.error(error);
      alert(error.message);
    }
  }

  async function onServerRecordScopeChange() {
    if (!state.session.user) {
      return;
    }

    try {
      await loadServerRecords(false);
    } catch (error) {
      console.error(error);
      alert(error.message);
    }
  }

  async function onServerRecordsClick(event) {
    const button = event.target.closest("[data-source='server'][data-action]");
    if (!button) {
      return;
    }

    if (!state.session.user) {
      alert("먼저 로그인해주세요.");
      return;
    }

    const recordId = button.dataset.id;

    if (button.dataset.action === "delete") {
      if (!confirm("이 서버 저장 결과를 삭제할까요?")) {
        return;
      }

      try {
        await apiRequest(`/api/records/${encodeURIComponent(recordId)}`, {
          method: "DELETE"
        });
        await loadServerRecords(false);
      } catch (error) {
        console.error(error);
        alert(error.message);
      }
      return;
    }

    if (button.dataset.action === "export") {
      try {
        const response = await apiRequest(`/api/records/${encodeURIComponent(recordId)}`);
        exportRecordAsJson(response.record);
      } catch (error) {
        console.error(error);
        alert(error.message);
      }
      return;
    }

    if (button.dataset.action === "sync") {
      try {
        const response = await apiRequest(`/api/records/${encodeURIComponent(recordId)}`);
        await syncSingleRecordToGoogleSheets(
          response.record,
          "server_single",
          "선택한 계정 저장 결과의 구글 시트 전송 요청을 보냈습니다."
        );
      } catch (error) {
        console.error(error);
        alert(error.message);
      }
      return;
    }

    if (button.dataset.action === "view") {
      try {
        const response = await apiRequest(`/api/records/${encodeURIComponent(recordId)}`);
        const entry = response.record;
        await loadQuestionnaire(entry.questionnaireId);
        populateFormFromRecord(entry);
        state.lastResult = entry;
        renderResultRecord(entry);
      } catch (error) {
        console.error(error);
        alert(error.message);
      }
    }
  }

  async function loadUsers(silent) {
    if (!state.session.user || state.session.user.role !== "admin") {
      state.users = [];
      renderUsers();
      return;
    }

    try {
      const response = await apiRequest("/api/users");
      state.users = Array.isArray(response.users) ? response.users : [];
      renderUsers();
      if (!silent) {
        ui.adminMessage.textContent = "계정 목록을 불러왔습니다.";
      }
    } catch (error) {
      state.users = [];
      renderUsers();
      ui.adminMessage.textContent = error.message;
      if (!silent) {
        throw error;
      }
    }
  }

  function renderUsers() {
    ui.usersList.innerHTML = "";

    if (!state.session.user || state.session.user.role !== "admin") {
      ui.adminMessage.textContent = "관리자로 로그인하면 계정을 관리할 수 있습니다.";
      return;
    }

    if (!state.users.length) {
      ui.adminMessage.textContent = "등록된 계정이 없습니다.";
      return;
    }

    ui.adminMessage.textContent = `${state.users.length}개의 계정을 표시합니다.`;

    state.users.forEach((user) => {
      const article = document.createElement("article");
      article.className = "admin-user-item";
      article.innerHTML = `
        <div class="admin-user-header">
          <div>
            <p class="admin-user-name">${escapeHtml(user.displayName)}</p>
            <p class="admin-user-meta">아이디 ${escapeHtml(user.username)} · 생성일 ${escapeHtml(formatDateTime(user.createdAt))}</p>
          </div>
          <div class="history-actions">
            <span class="role-badge ${escapeHtml(user.role)}">${escapeHtml(roleLabel(user.role))}</span>
            <span class="state-badge ${user.active ? "active" : "inactive"}">${user.active ? "활성" : (user.lastLoginAt ? "비활성" : "승인 대기")}</span>
          </div>
        </div>
        <div class="history-actions">
          <button class="history-btn" type="button" data-source="user" data-action="toggle-role" data-id="${escapeHtml(user.id)}">${user.role === "admin" ? "실무자로 변경" : "관리자로 변경"}</button>
          <button class="history-btn" type="button" data-source="user" data-action="toggle-active" data-id="${escapeHtml(user.id)}">${user.active ? "사용 중지" : (user.lastLoginAt ? "재활성화" : "가입 승인")}</button>
          <button class="history-btn" type="button" data-source="user" data-action="reset-password" data-id="${escapeHtml(user.id)}">비밀번호 재설정</button>
          <button class="history-btn danger-btn" type="button" data-source="user" data-action="delete-user" data-id="${escapeHtml(user.id)}">계정 삭제</button>
        </div>
      `;
      ui.usersList.appendChild(article);
    });
  }

  async function onCreateUserSubmit(event) {
    event.preventDefault();

    if (!state.session.user || state.session.user.role !== "admin") {
      alert("관리자 권한이 필요합니다.");
      return;
    }

    const username = ui.newUserUsername.value.trim();
    const displayName = ui.newUserDisplayName.value.trim();
    const role = ui.newUserRole.value;
    const password = ui.newUserPassword.value;

    if (!username || !displayName || !password) {
      alert("새 계정 정보를 모두 입력해주세요.");
      return;
    }

    try {
      await apiRequest("/api/users", {
        method: "POST",
        body: {
          username,
          displayName,
          role,
          password
        }
      });
      ui.createUserForm.reset();
      await loadUsers(false);
      alert("계정을 생성했습니다.");
    } catch (error) {
      console.error(error);
      alert(error.message);
    }
  }

  async function onRefreshUsersClick() {
    if (!state.session.user || state.session.user.role !== "admin") {
      alert("관리자 권한이 필요합니다.");
      return;
    }

    try {
      await loadUsers(false);
    } catch (error) {
      console.error(error);
      alert(error.message);
    }
  }

  async function onUsersListClick(event) {
    const button = event.target.closest("[data-source='user'][data-action]");
    if (!button) {
      return;
    }

    const targetUser = state.users.find((item) => item.id === button.dataset.id);
    if (!targetUser) {
      return;
    }

    try {
      if (button.dataset.action === "toggle-role") {
        const nextRole = targetUser.role === "admin" ? "worker" : "admin";
        if (!confirm(`${targetUser.displayName} 계정을 ${roleLabel(nextRole)}로 변경할까요?`)) {
          return;
        }
        await apiRequest(`/api/users/${encodeURIComponent(targetUser.id)}`, {
          method: "PATCH",
          body: {
            role: nextRole
          }
        });
      } else if (button.dataset.action === "toggle-active") {
        if (!confirm(`${targetUser.displayName} 계정의 사용 상태를 변경할까요?`)) {
          return;
        }
        await apiRequest(`/api/users/${encodeURIComponent(targetUser.id)}`, {
          method: "PATCH",
          body: {
            active: !targetUser.active
          }
        });
      } else if (button.dataset.action === "reset-password") {
        const password = prompt(`${targetUser.displayName} 계정의 새 비밀번호를 입력하세요.`, "");
        if (!password) {
          return;
        }
        await apiRequest(`/api/users/${encodeURIComponent(targetUser.id)}/password`, {
          method: "POST",
          body: {
            password
          }
        });
        alert("비밀번호를 재설정했습니다.");
      } else if (button.dataset.action === "delete-user") {
        if (!confirm(`'${targetUser.displayName}' 계정을 삭제할까요?\n이 작업은 되돌릴 수 없습니다.`)) {
          return;
        }
        await apiRequest(`/api/users/${encodeURIComponent(targetUser.id)}`, {
          method: "DELETE"
        });
        alert("계정을 삭제했습니다.");
      }

      await loadUsers(false);
    } catch (error) {
      console.error(error);
      alert(error.message);
    }
  }

  async function onHistoryClick(event) {
    const button = event.target.closest("[data-action]");
    if (!button) {
      return;
    }

    const history = loadHistory();
    const entry = history.find((item) => item.id === button.dataset.id);
    if (!entry) {
      return;
    }

    if (button.dataset.action === "view") {
      await loadQuestionnaire(entry.questionnaireId);
      populateFormFromRecord(entry);
      state.lastResult = entry;
      renderResultRecord(entry);
      return;
    }

    if (button.dataset.action === "export") {
      exportRecordAsJson(entry);
      return;
    }

    if (button.dataset.action === "sync") {
      await syncSingleRecordToGoogleSheets(
        entry,
        "history_single",
        "선택한 이 기기 저장 결과의 구글 시트 전송 요청을 보냈습니다."
      );
      return;
    }

    if (button.dataset.action === "delete") {
      if (!confirm("이 검사 기록을 삭제할까요?")) {
        return;
      }
      const nextHistory = history.filter((item) => item.id !== button.dataset.id);
      localStorage.setItem(HISTORY_KEY, JSON.stringify(nextHistory));
      renderHistory();
    }
  }

  function populateFormFromRecord(record) {
    resetCurrentForm();

    ui.sessionDate.value = record.meta?.sessionDate || todayForInput();
    ui.workerName.value = record.meta?.workerName || "";
    ui.clientLabel.value = record.meta?.clientLabel || "";
    ui.birthDate.value = record.meta?.birthDate || "";
    ui.sessionNote.value = record.meta?.sessionNote || "";

    Object.entries(record.respondent || {}).forEach(([fieldId, value]) => {
      setRadioValue(`respondent_${fieldId}`, value);
    });

    (state.currentQuestionnaire.questions || []).forEach((question) => {
      const answer = record.answers?.[question.id];
      if (answer) {
        setRadioValue(question.id, answer.value);
      }
    });

    syncSubQuestionsVisibility();

    (state.currentQuestionnaire.questions || []).forEach((question) => {
      (question.subQuestions || []).forEach((subQuestion) => {
        const answer = record.answers?.[subQuestion.id];
        if (answer) {
          setRadioValue(subQuestion.id, answer.value);
        }
      });
    });

    ui.screeningForm.querySelectorAll('input[type="radio"]').forEach((input) => {
      syncSelectedState(input.name);
    });
    updateProgress();
  }

  function setRadioValue(name, value) {
    const input = document.querySelector(`input[name="${cssEscape(name)}"][value="${cssEscape(String(value))}"]`);
    if (input) {
      input.checked = true;
    }
  }

  function onClearHistory() {
    if (!confirm("최근 저장 결과를 모두 삭제할까요?")) {
      return;
    }
    localStorage.removeItem(HISTORY_KEY);
    renderHistory();
  }

  function onExportHistoryCsv() {
    const history = loadHistory();
    if (!history.length) {
      alert("내보낼 저장 기록이 없습니다.");
      return;
    }
    const rows = history.map((entry) => flattenRecordForCsv(entry));
    downloadText(`저장결과_${todayForInput().replace(/-/g, "")}.csv`, toCsv(rows), "text/csv;charset=utf-8");
  }

  function onExportHistoryJson() {
    const history = loadHistory();
    if (!history.length) {
      alert("내보낼 저장 기록이 없습니다.");
      return;
    }
    downloadText(
      `저장결과_${todayForInput().replace(/-/g, "")}.json`,
      JSON.stringify(history, null, 2),
      "application/json"
    );
  }

  function resetCurrentForm() {
    if (!state.currentQuestionnaire) {
      return;
    }
    ui.screeningForm.reset();
    applySessionDefaults();
    ui.screeningForm.querySelectorAll(".choice-card").forEach((card) => {
      card.classList.remove("is-selected");
    });
    syncSubQuestionsVisibility();
    clearResult();
    updateProgress();
  }

  function loadHistory() {
    try {
      return JSON.parse(localStorage.getItem(HISTORY_KEY) || "[]");
    } catch (error) {
      console.warn(error);
      return [];
    }
  }

  function exportRecordAsJson(record) {
    const datePart = (record.meta?.sessionDate || todayForInput()).replace(/-/g, "");
    const scaleName = sanitizeFileNamePart(record.questionnaireTitle || record.questionnaireId || "검사결과");
    downloadText(`${scaleName}_${datePart}.json`, JSON.stringify(record, null, 2), "application/json");
  }

  function flattenRecordForCsv(record) {
    return {
      저장시각: record.createdAt || "",
      검사일: record.meta?.sessionDate || "",
      척도ID: record.questionnaireId || "",
      척도명: record.questionnaireTitle || "",
      점수: record.evaluation?.scoreText || "",
      결과구간: record.evaluation?.bandText || "",
      응답진행률: formatProgressSummary(resolveRecordProgress(record)),
      담당자: record.meta?.workerName || "",
      대상자: record.meta?.clientLabel || "",
      생년월일: record.meta?.birthDate || "",
      성별: displayValue(record.respondentDisplay, "성별"),
      연령대: displayValue(record.respondentDisplay, "연령대"),
      비고: record.meta?.sessionNote || "",
      핵심요약: (record.evaluation?.highlights || []).join(" | ")
    };
  }

  function displayValue(items, label) {
    const found = (items || []).find((item) => item.label === label);
    return found ? found.value : "";
  }

  function toCsv(rows) {
    if (!rows.length) {
      return "";
    }
    const headers = Object.keys(rows[0]);
    const lines = [headers.join(",")];
    rows.forEach((row) => {
      const line = headers
        .map((header) => {
          const raw = row[header] === null || row[header] === undefined ? "" : String(row[header]);
          return `"${raw.replace(/"/g, '""')}"`;
        })
        .join(",");
      lines.push(line);
    });
    return "\ufeff" + lines.join("\n");
  }

  function downloadText(fileName, text, mimeType) {
    const blob = new Blob([text], { type: mimeType });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = fileName;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    URL.revokeObjectURL(url);
  }

  function sanitizeFileNamePart(value) {
    return String(value || "")
      .trim()
      .replace(/[\\/:*?"<>|]/g, "_")
      .replace(/\s+/g, "_")
      .slice(0, 60) || "검사결과";
  }

  function todayForInput() {
    const date = new Date();
    const yyyy = date.getFullYear();
    const mm = String(date.getMonth() + 1).padStart(2, "0");
    const dd = String(date.getDate()).padStart(2, "0");
    return `${yyyy}-${mm}-${dd}`;
  }

  function formatDateTime(iso) {
    const date = new Date(iso);
    if (Number.isNaN(date.getTime())) {
      return iso;
    }
    const yyyy = date.getFullYear();
    const mm = String(date.getMonth() + 1).padStart(2, "0");
    const dd = String(date.getDate()).padStart(2, "0");
    const hh = String(date.getHours()).padStart(2, "0");
    const min = String(date.getMinutes()).padStart(2, "0");
    return `${yyyy}-${mm}-${dd} ${hh}:${min}`;
  }

  function structuredCloneSafe(value) {
    return JSON.parse(JSON.stringify(value));
  }

  function createLocalId() {
    if (window.crypto && typeof window.crypto.randomUUID === "function") {
      return window.crypto.randomUUID();
    }
    return `id_${Date.now()}_${Math.random().toString(16).slice(2)}`;
  }

  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function cssEscape(value) {
    return String(value).replace(/(["\\.#:[\]])/g, "\\$1");
  }
})();
