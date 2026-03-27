const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const PORT = Number(process.argv[2] || process.env.PORT || 8134);
const HOST = process.env.HOST || "0.0.0.0";
const ROOT_DIR = __dirname;
const DATA_DIR = resolveDataDir(process.env.MH_SCALE_DATA_DIR || "runtime-data");
const USERS_FILE = path.join(DATA_DIR, "users.json");
const RECORDS_FILE = path.join(DATA_DIR, "records.json");
const CONFIG_FILE = path.join(DATA_DIR, "config.json");
const CLIENTS_FILE = path.join(DATA_DIR, "clients.json");
const RISK_NOTES_FILE = path.join(DATA_DIR, "risk-notes.json");
const SESSION_COOKIE = "mh_scale_session";
const SESSION_TTL_MS = 1000 * 60 * 60 * 12;
const SESSION_COOKIE_SECURE = process.env.SESSION_COOKIE_SECURE === "1" || process.env.NODE_ENV === "production";
const PASSWORD_MIN_LENGTH = 8;
const DEFAULT_ADMIN_USERNAME = "admin0109";
const DEFAULT_ADMIN_PASSWORD = "admin0109";
const DEFAULT_ADMIN_DISPLAY_NAME = "관리자";
const MIME_TYPES = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".ico": "image/x-icon",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".md": "text/markdown; charset=utf-8",
  ".png": "image/png",
  ".svg": "image/svg+xml",
  ".txt": "text/plain; charset=utf-8"
};

const sessions = new Map();

ensureStorage();

const server = http.createServer(async (req, res) => {
  try {
    const requestUrl = new URL(req.url, `http://${req.headers.host || `127.0.0.1:${PORT}`}`);

    if (requestUrl.pathname.startsWith("/api/")) {
      await handleApiRequest(req, res, requestUrl);
      return;
    }

    serveStaticFile(res, requestUrl.pathname);
  } catch (error) {
    const statusCode = error && error.statusCode ? error.statusCode : 500;
    if (statusCode >= 500) {
      console.error("[scale-webapp] server error", error);
    }
    sendJson(res, statusCode, {
      ok: false,
      error: statusCode >= 500 ? "서버 내부 오류가 발생했습니다." : error.message
    });
  }
});

server.listen(PORT, HOST, () => {
  console.log(`[scale-webapp] http://${HOST}:${PORT}/`);
  console.log(`[scale-webapp] data store: ${DATA_DIR}`);
});

function ensureStorage() {
  fs.mkdirSync(DATA_DIR, { recursive: true });

  if (!fs.existsSync(USERS_FILE)) {
    writeJsonFile(USERS_FILE, []);
  }

  if (!fs.existsSync(RECORDS_FILE)) {
    writeJsonFile(RECORDS_FILE, []);
  }

  ensureDefaultAdminAccount();
}

function resolveDataDir(input) {
  if (!input) {
    return path.join(ROOT_DIR, "runtime-data");
  }

  if (path.isAbsolute(input)) {
    return input;
  }

  return path.resolve(ROOT_DIR, input);
}

async function handleApiRequest(req, res, requestUrl) {
  cleanupExpiredSessions();

  if (req.method === "GET" && requestUrl.pathname === "/api/health") {
    sendJson(res, 200, {
      ok: true,
      service: "scale-screening-local-server"
    });
    return;
  }

  if (req.method === "GET" && requestUrl.pathname === "/api/config") {
    const config = readConfig();
    const { kioskPin: _pin, ...publicConfig } = config;
    publicConfig.kioskPinSet = Boolean(config.kioskPin && config.kioskPin.length >= 4);
    sendJson(res, 200, { ok: true, config: publicConfig });
    return;
  }

  if (req.method === "POST" && requestUrl.pathname === "/api/kiosk/verify-pin") {
    const body = await readRequestBody(req);
    const config = readConfig();
    const submitted = normalizeText(body.pin);
    if (!config.kioskPin || config.kioskPin.length < 4) {
      sendJson(res, 403, { ok: false, error: "키오스크 PIN이 설정되지 않았습니다." });
      return;
    }
    if (submitted !== config.kioskPin) {
      sendJson(res, 401, { ok: false, error: "PIN이 올바르지 않습니다." });
      return;
    }
    sendJson(res, 200, { ok: true });
    return;
  }

  if (req.method === "GET" && requestUrl.pathname === "/api/session") {
    const users = readUsers();
    const session = getSessionUser(req, users);
    sendJson(res, 200, {
      ok: true,
      bootstrapRequired: users.length === 0,
      user: session ? sanitizeUser(session.user) : null
    });
    return;
  }

  if (req.method === "POST" && requestUrl.pathname === "/api/bootstrap") {
    const users = readUsers();
    if (users.length > 0) {
      sendJson(res, 409, {
        ok: false,
        error: "이미 초기 관리자 계정이 존재합니다."
      });
      return;
    }

    const body = await readRequestBody(req);
    const username = normalizeUsername(body.username);
    const displayName = normalizeText(body.displayName);
    const password = normalizeText(body.password);

    validateUserCreateInput(username, displayName, password);

    const user = buildUser({
      username,
      displayName,
      password,
      role: "admin"
    });

    writeUsers([user]);
    createSession(res, user.id);

    sendJson(res, 201, {
      ok: true,
      user: sanitizeUser(user)
    });
    return;
  }

  if (req.method === "POST" && requestUrl.pathname === "/api/login") {
    const users = readUsers();
    const body = await readRequestBody(req);
    const username = normalizeUsername(body.username);
    const password = normalizeText(body.password);
    const user = users.find((item) => item.username === username);

    if (!user || !verifyPassword(password, user.passwordSalt, user.passwordHash)) {
      sendJson(res, 401, {
        ok: false,
        error: "아이디 또는 비밀번호가 올바르지 않습니다."
      });
      return;
    }

    if (!user.active) {
      sendJson(res, 403, {
        ok: false,
        error: "비활성화된 계정입니다. 관리자에게 문의해주세요."
      });
      return;
    }

    user.lastLoginAt = new Date().toISOString();
    user.updatedAt = user.lastLoginAt;
    writeUsers(users);
    createSession(res, user.id);

    sendJson(res, 200, {
      ok: true,
      user: sanitizeUser(user)
    });
    return;
  }

  if (req.method === "POST" && requestUrl.pathname === "/api/logout") {
    clearSession(res, readCookies(req)[SESSION_COOKIE]);
    sendJson(res, 200, { ok: true });
    return;
  }

  if (req.method === "POST" && requestUrl.pathname === "/api/register") {
    const users = readUsers();
    const body = await readRequestBody(req);
    const username = normalizeUsername(body.username);
    const displayName = normalizeText(body.displayName);
    const password = normalizeText(body.password);

    validateUserCreateInput(username, displayName, password);

    if (users.some((item) => item.username === username)) {
      sendJson(res, 409, { ok: false, error: "이미 사용 중인 아이디입니다." });
      return;
    }

    const user = buildUser({ username, displayName, password, role: "worker" });
    user.active = false;
    users.push(user);
    writeUsers(users);

    sendJson(res, 201, { ok: true });
    return;
  }

  const users = readUsers();
  const session = getSessionUser(req, users);

  if (req.method === "GET" && requestUrl.pathname === "/api/users") {
    requireAdmin(session);
    sendJson(res, 200, {
      ok: true,
      users: users.map(sanitizeUser)
    });
    return;
  }

  if (req.method === "POST" && requestUrl.pathname === "/api/users") {
    requireAdmin(session);

    const body = await readRequestBody(req);
    const username = normalizeUsername(body.username);
    const displayName = normalizeText(body.displayName);
    const password = normalizeText(body.password);
    const role = normalizeRole(body.role);

    validateUserCreateInput(username, displayName, password);

    if (users.some((item) => item.username === username)) {
      sendJson(res, 409, {
        ok: false,
        error: "이미 존재하는 아이디입니다."
      });
      return;
    }

    const user = buildUser({
      username,
      displayName,
      password,
      role
    });

    users.push(user);
    writeUsers(users);

    sendJson(res, 201, {
      ok: true,
      user: sanitizeUser(user)
    });
    return;
  }

  const userMatch = requestUrl.pathname.match(/^\/api\/users\/([^/]+)$/);
  if (req.method === "PATCH" && userMatch) {
    requireAdmin(session);

    const userId = decodeURIComponent(userMatch[1]);
    const target = users.find((item) => item.id === userId);
    if (!target) {
      sendJson(res, 404, {
        ok: false,
        error: "대상 계정을 찾을 수 없습니다."
      });
      return;
    }

    if (target.id === session.user.id) {
      sendJson(res, 400, {
        ok: false,
        error: "현재 로그인한 본인 계정의 권한/상태는 여기서 바꿀 수 없습니다."
      });
      return;
    }

    const body = await readRequestBody(req);
    const nextRole = body.role === undefined ? target.role : normalizeRole(body.role);
    const nextActive = body.active === undefined ? target.active : Boolean(body.active);
    const nextDisplayName = body.displayName === undefined ? target.displayName : normalizeText(body.displayName);

    if (!nextDisplayName) {
      sendJson(res, 400, {
        ok: false,
        error: "표시 이름은 비워둘 수 없습니다."
      });
      return;
    }

    if (target.role === "admin" && (!nextActive || nextRole !== "admin")) {
      const activeAdminCount = users.filter((item) => item.active && item.role === "admin").length;
      if (activeAdminCount <= 1) {
        sendJson(res, 400, {
          ok: false,
          error: "최소 1명의 활성 관리자 계정은 유지되어야 합니다."
        });
        return;
      }
    }

    target.displayName = nextDisplayName;
    target.role = nextRole;
    target.active = nextActive;
    target.updatedAt = new Date().toISOString();
    writeUsers(users);

    sendJson(res, 200, {
      ok: true,
      user: sanitizeUser(target)
    });
    return;
  }

  const userPasswordMatch = requestUrl.pathname.match(/^\/api\/users\/([^/]+)\/password$/);
  if (req.method === "POST" && userPasswordMatch) {
    requireAdmin(session);

    const userId = decodeURIComponent(userPasswordMatch[1]);
    const target = users.find((item) => item.id === userId);
    if (!target) {
      sendJson(res, 404, {
        ok: false,
        error: "대상 계정을 찾을 수 없습니다."
      });
      return;
    }

    const body = await readRequestBody(req);
    const password = normalizeText(body.password);
    validatePassword(password);

    const passwordPack = hashPassword(password);
    target.passwordSalt = passwordPack.salt;
    target.passwordHash = passwordPack.hash;
    target.updatedAt = new Date().toISOString();
    writeUsers(users);

    sendJson(res, 200, {
      ok: true,
      user: sanitizeUser(target)
    });
    return;
  }

  const userDeleteMatch = requestUrl.pathname.match(/^\/api\/users\/([^/]+)$/);
  if (req.method === "DELETE" && userDeleteMatch) {
    requireAdmin(session);

    const userId = decodeURIComponent(userDeleteMatch[1]);

    if (userId === session.user.id) {
      sendJson(res, 400, { ok: false, error: "본인 계정은 삭제할 수 없습니다." });
      return;
    }

    const target = users.find((item) => item.id === userId);
    if (!target) {
      sendJson(res, 404, { ok: false, error: "대상 계정을 찾을 수 없습니다." });
      return;
    }

    if (target.role === "admin") {
      const activeAdminCount = users.filter((item) => item.active && item.role === "admin").length;
      if (activeAdminCount <= 1) {
        sendJson(res, 400, { ok: false, error: "최소 1명의 활성 관리자 계정은 유지되어야 합니다." });
        return;
      }
    }

    const next = users.filter((item) => item.id !== userId);
    writeUsers(next);
    sendJson(res, 200, { ok: true });
    return;
  }

  if (req.method === "PATCH" && requestUrl.pathname === "/api/config") {
    requireAdmin(session);
    const body = await readRequestBody(req);
    const current = readConfig();
    const updated = { ...current };
    if (body.organizationName !== undefined) updated.organizationName = normalizeText(body.organizationName);
    if (body.teamName !== undefined) updated.teamName = normalizeText(body.teamName);
    if (body.contactNote !== undefined) updated.contactNote = normalizeText(body.contactNote);
    if (body.primaryColor !== undefined) updated.primaryColor = normalizeText(body.primaryColor);
    if (body.kioskPin !== undefined) {
      const pin = normalizeText(body.kioskPin);
      if (pin && !/^\d{4,}$/.test(pin)) {
        sendJson(res, 400, { ok: false, error: "키오스크 PIN은 숫자 4자리 이상이어야 합니다." });
        return;
      }
      updated.kioskPin = pin;
    }
    if (Array.isArray(body.enabledScales)) {
      updated.enabledScales = body.enabledScales.filter((s) => typeof s === "string");
    }
    writeConfig(updated);
    const { kioskPin: _pin, ...publicConfig } = updated;
    publicConfig.kioskPinSet = Boolean(updated.kioskPin && updated.kioskPin.length >= 4);
    sendJson(res, 200, { ok: true, config: publicConfig });
    return;
  }

  if (req.method === "GET" && requestUrl.pathname === "/api/records") {
    requireAuth(session);

    const allRecords = readRecords();
    const requestedScope = requestUrl.searchParams.get("scope") === "all" ? "all" : "mine";
    const effectiveScope = session.user.role === "admin" ? requestedScope : "mine";
    const filtered = effectiveScope === "all"
      ? allRecords
      : allRecords.filter((item) => item.owner && item.owner.userId === session.user.id);

    sendJson(res, 200, {
      ok: true,
      records: filtered
        .slice()
        .sort(sortRecordsDescending)
        .map(toRecordSummary)
    });
    return;
  }

  const recordMatch = requestUrl.pathname.match(/^\/api\/records\/([^/]+)$/);
  if (req.method === "GET" && recordMatch) {
    requireAuth(session);

    const recordId = decodeURIComponent(recordMatch[1]);
    const record = readRecords().find((item) => item.id === recordId);
    if (!record) {
      sendJson(res, 404, {
        ok: false,
        error: "저장 결과를 찾을 수 없습니다."
      });
      return;
    }

    ensureRecordAccess(record, session.user);
    sendJson(res, 200, {
      ok: true,
      record
    });
    return;
  }

  if (req.method === "POST" && requestUrl.pathname === "/api/records") {
    requireAuth(session);

    const body = await readRequestBody(req);
    const input = sanitizeRecordInput(body.record || body);
    const records = readRecords();
    const now = new Date().toISOString();
    const existingIndex = records.findIndex((item) => item.id === input.id);
    let savedRecord;

    if (existingIndex >= 0) {
      const existing = records[existingIndex];
      ensureRecordAccess(existing, session.user);
      savedRecord = {
        ...existing,
        ...input,
        owner: existing.owner,
        createdAt: existing.createdAt || input.createdAt || now,
        updatedAt: now,
        lastSavedBy: buildOwnerSummary(session.user)
      };
      records[existingIndex] = savedRecord;
    } else {
      savedRecord = {
        ...input,
        createdAt: input.createdAt || now,
        updatedAt: now,
        owner: buildOwnerSummary(session.user),
        lastSavedBy: buildOwnerSummary(session.user)
      };
      records.push(savedRecord);
    }

    writeRecords(records);

    sendJson(res, 200, {
      ok: true,
      record: savedRecord,
      summary: toRecordSummary(savedRecord)
    });
    return;
  }

  if (req.method === "DELETE" && recordMatch) {
    requireAuth(session);

    const recordId = decodeURIComponent(recordMatch[1]);
    const records = readRecords();
    const target = records.find((item) => item.id === recordId);
    if (!target) {
      sendJson(res, 404, {
        ok: false,
        error: "저장 결과를 찾을 수 없습니다."
      });
      return;
    }

    ensureRecordAccess(target, session.user);
    writeRecords(records.filter((item) => item.id !== recordId));

    sendJson(res, 200, {
      ok: true
    });
    return;
  }

  // ── FR-01: 이용자 클라이언트 관리 ────────────────────────────────

  if (req.method === "GET" && requestUrl.pathname === "/api/clients") {
    requireAuth(session);
    const clients = readClients();
    sendJson(res, 200, { ok: true, clients });
    return;
  }

  if (req.method === "POST" && requestUrl.pathname === "/api/clients") {
    requireAuth(session);
    const body = await readRequestBody(req);
    const clients = readClients();
    const now = new Date().toISOString();
    const seq = clients.length + 1;
    const client = {
      id: `client_${String(seq).padStart(3, "0")}`,
      birthDate: normalizeText(body.birthDate),
      memo: normalizeText(body.memo),
      assignedWorker: normalizeText(body.assignedWorker) || (session.user ? session.user.displayName : ""),
      registeredAt: now,
      updatedAt: now
    };
    clients.push(client);
    writeClients(clients);
    sendJson(res, 201, { ok: true, client });
    return;
  }

  const clientMatch = requestUrl.pathname.match(/^\/api\/clients\/([^/]+)$/);
  if (req.method === "PATCH" && clientMatch) {
    requireAuth(session);
    const clientId = decodeURIComponent(clientMatch[1]);
    const clients = readClients();
    const target = clients.find((c) => c.id === clientId);
    if (!target) {
      sendJson(res, 404, { ok: false, error: "이용자를 찾을 수 없습니다." });
      return;
    }
    const body = await readRequestBody(req);
    if (body.memo !== undefined) target.memo = normalizeText(body.memo);
    if (body.assignedWorker !== undefined) target.assignedWorker = normalizeText(body.assignedWorker);
    if (body.birthDate !== undefined) target.birthDate = normalizeText(body.birthDate);
    target.updatedAt = new Date().toISOString();
    writeClients(clients);
    sendJson(res, 200, { ok: true, client: target });
    return;
  }

  const clientRecordsMatch = requestUrl.pathname.match(/^\/api\/clients\/([^/]+)\/records$/);
  if (req.method === "GET" && clientRecordsMatch) {
    requireAuth(session);
    const clientId = decodeURIComponent(clientRecordsMatch[1]);
    const allRecords = readRecords();
    const clientRecords = allRecords
      .filter((r) => r.meta && r.meta.clientId === clientId)
      .sort(sortRecordsDescending);
    sendJson(res, 200, { ok: true, records: clientRecords });
    return;
  }

  // ── FR-02: 위험군 관리 ────────────────────────────────────────

  if (req.method === "GET" && requestUrl.pathname === "/api/risk-flags") {
    requireAuth(session);
    const allRecords = readRecords();
    const riskNotes = readRiskNotes();
    const flagged = allRecords
      .filter((r) => r.evaluation && Array.isArray(r.evaluation.flags) && r.evaluation.flags.length > 0)
      .sort(sortRecordsDescending)
      .map((r) => {
        const notes = riskNotes.filter((n) => n.recordId === r.id);
        const acknowledged = notes.some((n) => n.acknowledged);
        return {
          id: r.id,
          questionnaireTitle: r.questionnaireTitle || "",
          shortTitle: r.shortTitle || "",
          sessionDate: r.meta && r.meta.sessionDate ? r.meta.sessionDate : "",
          workerName: r.meta && r.meta.workerName ? r.meta.workerName : "",
          clientLabel: r.meta && r.meta.clientLabel ? r.meta.clientLabel : "",
          clientId: r.meta && r.meta.clientId ? r.meta.clientId : null,
          scoreText: r.evaluation && r.evaluation.scoreText ? r.evaluation.scoreText : "",
          flags: r.evaluation.flags,
          acknowledged,
          notes: notes.map((n) => ({
            id: n.id,
            text: n.text,
            author: n.author,
            createdAt: n.createdAt,
            acknowledged: n.acknowledged
          }))
        };
      });

    const unacknowledgedCount = flagged.filter((f) => !f.acknowledged).length;
    sendJson(res, 200, { ok: true, riskFlags: flagged, unacknowledgedCount });
    return;
  }

  const riskFlagNoteMatch = requestUrl.pathname.match(/^\/api\/risk-flags\/([^/]+)\/notes$/);
  if (req.method === "POST" && riskFlagNoteMatch) {
    requireAuth(session);
    const recordId = decodeURIComponent(riskFlagNoteMatch[1]);
    const body = await readRequestBody(req);
    const text = normalizeText(body.text);
    if (!text) {
      sendJson(res, 400, { ok: false, error: "메모 내용을 입력해주세요." });
      return;
    }
    const notes = readRiskNotes();
    const note = {
      id: createId("note"),
      recordId,
      text,
      author: session.user ? session.user.displayName : "",
      authorUsername: session.user ? session.user.username : "",
      createdAt: new Date().toISOString(),
      acknowledged: false
    };
    notes.push(note);
    writeRiskNotes(notes);
    sendJson(res, 201, { ok: true, note });
    return;
  }

  const riskFlagAckMatch = requestUrl.pathname.match(/^\/api\/risk-flags\/([^/]+)\/acknowledge$/);
  if (req.method === "POST" && riskFlagAckMatch) {
    requireAuth(session);
    const recordId = decodeURIComponent(riskFlagAckMatch[1]);
    const body = await readRequestBody(req);
    const notes = readRiskNotes();
    const acknowledged = Boolean(body.acknowledged !== false);
    let found = false;
    notes.forEach((n) => {
      if (n.recordId === recordId) {
        n.acknowledged = acknowledged;
        found = true;
      }
    });
    if (!found) {
      const note = {
        id: createId("note"),
        recordId,
        text: acknowledged ? "(확인 완료)" : "(확인 취소)",
        author: session.user ? session.user.displayName : "",
        authorUsername: session.user ? session.user.username : "",
        createdAt: new Date().toISOString(),
        acknowledged
      };
      notes.push(note);
    }
    writeRiskNotes(notes);
    sendJson(res, 200, { ok: true, acknowledged });
    return;
  }

  // ── FR-06: 실적 집계 보고서 ───────────────────────────────────

  if (req.method === "GET" && requestUrl.pathname === "/api/reports/summary") {
    requireAuth(session);
    const from = requestUrl.searchParams.get("from") || "";
    const to = requestUrl.searchParams.get("to") || "";
    const allRecords = readRecords();
    const filtered = allRecords.filter((r) => {
      const d = r.meta && r.meta.sessionDate ? r.meta.sessionDate : (r.createdAt ? r.createdAt.slice(0, 10) : "");
      if (from && d < from) return false;
      if (to && d > to) return false;
      return true;
    });

    const totalCount = filtered.length;
    const byScale = {};
    const byWorker = {};
    const byMonth = {};
    let riskCount = 0;

    filtered.forEach((r) => {
      const scaleId = r.questionnaireId || "unknown";
      const title = r.shortTitle || r.questionnaireTitle || scaleId;
      if (!byScale[scaleId]) byScale[scaleId] = { title, count: 0, scoreSum: 0, scoreCount: 0 };
      byScale[scaleId].count += 1;
      if (r.evaluation && typeof r.evaluation.score === "number") {
        byScale[scaleId].scoreSum += r.evaluation.score;
        byScale[scaleId].scoreCount += 1;
      }

      const worker = r.meta && r.meta.workerName ? r.meta.workerName : "미지정";
      byWorker[worker] = (byWorker[worker] || 0) + 1;

      const month = r.meta && r.meta.sessionDate
        ? r.meta.sessionDate.slice(0, 7)
        : (r.createdAt ? r.createdAt.slice(0, 7) : "unknown");
      byMonth[month] = (byMonth[month] || 0) + 1;

      if (r.evaluation && Array.isArray(r.evaluation.flags) && r.evaluation.flags.length > 0) {
        riskCount += 1;
      }
    });

    const scaleSummary = Object.values(byScale).map((s) => ({
      title: s.title,
      count: s.count,
      avgScore: s.scoreCount > 0 ? Math.round((s.scoreSum / s.scoreCount) * 10) / 10 : null
    }));

    sendJson(res, 200, {
      ok: true,
      summary: {
        period: { from, to },
        totalCount,
        riskCount,
        byScale: scaleSummary,
        byWorker,
        byMonth
      }
    });
    return;
  }

  sendJson(res, 404, {
    ok: false,
    error: "지원하지 않는 API 경로입니다."
  });
}

function serveStaticFile(res, pathname) {
  const decodedPath = decodeURIComponent(pathname);
  const safePath = decodedPath === "/" ? "/index.html" : decodedPath;
  const relativePath = safePath.replace(/^\/+/, "");
  const absolutePath = path.resolve(ROOT_DIR, relativePath);

  if (!absolutePath.startsWith(ROOT_DIR) || absolutePath.startsWith(DATA_DIR)) {
    sendJson(res, 403, {
      ok: false,
      error: "접근할 수 없는 경로입니다."
    });
    return;
  }

  let filePath = absolutePath;
  if (fs.existsSync(filePath) && fs.statSync(filePath).isDirectory()) {
    filePath = path.join(filePath, "index.html");
  }

  if (!fs.existsSync(filePath) || !fs.statSync(filePath).isFile()) {
    sendJson(res, 404, {
      ok: false,
      error: "파일을 찾을 수 없습니다."
    });
    return;
  }

  const ext = path.extname(filePath).toLowerCase();
  const mimeType = MIME_TYPES[ext] || "application/octet-stream";
  res.writeHead(200, {
    "Content-Type": mimeType,
    "Cache-Control": "no-store"
  });
  fs.createReadStream(filePath).pipe(res);
}

async function readRequestBody(req) {
  const chunks = [];
  for await (const chunk of req) {
    chunks.push(chunk);
  }

  const raw = Buffer.concat(chunks).toString("utf8").trim();
  if (!raw) {
    return {};
  }

  try {
    return JSON.parse(raw);
  } catch (error) {
    throw createHttpError(400, "JSON 요청 본문을 해석할 수 없습니다.");
  }
}

function sendJson(res, statusCode, data) {
  const body = JSON.stringify(data, null, 2);
  res.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store"
  });
  res.end(body);
}

function createHttpError(statusCode, message) {
  const error = new Error(message);
  error.statusCode = statusCode;
  return error;
}

function readUsers() {
  return readJsonFile(USERS_FILE, []);
}

function writeUsers(users) {
  writeJsonFile(USERS_FILE, users);
}

function readRecords() {
  return readJsonFile(RECORDS_FILE, []);
}

function writeRecords(records) {
  writeJsonFile(RECORDS_FILE, records);
}

function defaultConfig() {
  return {
    organizationName: "다시서기종합지원센터",
    teamName: "정신건강팀",
    contactNote: "",
    enabledScales: [],
    primaryColor: "",
    kioskPin: ""
  };
}

function readConfig() {
  return { ...defaultConfig(), ...readJsonFile(CONFIG_FILE, {}) };
}

function writeConfig(config) {
  writeJsonFile(CONFIG_FILE, config);
}

function readClients() {
  return readJsonFile(CLIENTS_FILE, []);
}

function writeClients(clients) {
  writeJsonFile(CLIENTS_FILE, clients);
}

function readRiskNotes() {
  return readJsonFile(RISK_NOTES_FILE, []);
}

function writeRiskNotes(notes) {
  writeJsonFile(RISK_NOTES_FILE, notes);
}

function readJsonFile(filePath, fallback) {
  try {
    const raw = fs.readFileSync(filePath, "utf8");
    return JSON.parse(raw);
  } catch (error) {
    return fallback;
  }
}

function writeJsonFile(filePath, value) {
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2), "utf8");
}

function ensureDefaultAdminAccount() {
  const users = readJsonFile(USERS_FILE, []);
  const now = new Date().toISOString();
  const existing = users.find((item) => item.username === DEFAULT_ADMIN_USERNAME);
  const passwordPack = hashPassword(DEFAULT_ADMIN_PASSWORD);

  if (existing) {
    existing.displayName = existing.displayName || DEFAULT_ADMIN_DISPLAY_NAME;
    existing.role = "admin";
    existing.active = true;
    existing.passwordSalt = passwordPack.salt;
    existing.passwordHash = passwordPack.hash;
    existing.updatedAt = now;
    writeUsers(users);
    return;
  }

  const user = buildUser({
    username: DEFAULT_ADMIN_USERNAME,
    displayName: DEFAULT_ADMIN_DISPLAY_NAME,
    password: DEFAULT_ADMIN_PASSWORD,
    role: "admin"
  });

  users.unshift(user);
  writeUsers(users);
}

function buildUser({ username, displayName, password, role }) {
  const now = new Date().toISOString();
  const passwordPack = hashPassword(password);

  return {
    id: createId("usr"),
    username,
    displayName,
    role,
    active: true,
    createdAt: now,
    updatedAt: now,
    lastLoginAt: "",
    passwordSalt: passwordPack.salt,
    passwordHash: passwordPack.hash
  };
}

function sanitizeUser(user) {
  return {
    id: user.id,
    username: user.username,
    displayName: user.displayName,
    role: user.role,
    active: Boolean(user.active),
    createdAt: user.createdAt,
    updatedAt: user.updatedAt,
    lastLoginAt: user.lastLoginAt || ""
  };
}

function hashPassword(password) {
  const salt = crypto.randomBytes(16).toString("hex");
  const hash = crypto.pbkdf2Sync(password, salt, 120000, 32, "sha256").toString("hex");
  return { salt, hash };
}

function verifyPassword(password, salt, expectedHash) {
  if (!password || !salt || !expectedHash) {
    return false;
  }

  const actualHash = crypto.pbkdf2Sync(password, salt, 120000, 32, "sha256").toString("hex");
  return crypto.timingSafeEqual(Buffer.from(actualHash, "hex"), Buffer.from(expectedHash, "hex"));
}

function createSession(res, userId) {
  const token = crypto.randomBytes(24).toString("hex");
  const expiresAt = Date.now() + SESSION_TTL_MS;
  sessions.set(token, { userId, expiresAt });
  res.setHeader("Set-Cookie", buildSessionCookie(token, SESSION_TTL_MS));
}

function clearSession(res, token) {
  if (token) {
    sessions.delete(token);
  }

  res.setHeader(
    "Set-Cookie",
    `${SESSION_COOKIE}=; HttpOnly; Path=/; SameSite=Lax${SESSION_COOKIE_SECURE ? "; Secure" : ""}; Max-Age=0`
  );
}

function buildSessionCookie(token, maxAgeMs) {
  return `${SESSION_COOKIE}=${token}; HttpOnly; Path=/; SameSite=Lax${SESSION_COOKIE_SECURE ? "; Secure" : ""}; Max-Age=${Math.floor(maxAgeMs / 1000)}`;
}

function cleanupExpiredSessions() {
  const now = Date.now();
  Array.from(sessions.entries()).forEach(([token, session]) => {
    if (!session || session.expiresAt <= now) {
      sessions.delete(token);
    }
  });
}

function readCookies(req) {
  const raw = req.headers.cookie || "";
  return raw.split(";").reduce((accumulator, part) => {
    const [key, ...rest] = part.split("=");
    const name = normalizeText(key);
    if (!name) {
      return accumulator;
    }
    accumulator[name] = decodeURIComponent(rest.join("="));
    return accumulator;
  }, {});
}

function getSessionUser(req, users) {
  const token = readCookies(req)[SESSION_COOKIE];
  if (!token || !sessions.has(token)) {
    return null;
  }

  const session = sessions.get(token);
  if (!session || session.expiresAt <= Date.now()) {
    sessions.delete(token);
    return null;
  }

  const user = users.find((item) => item.id === session.userId);
  if (!user || !user.active) {
    sessions.delete(token);
    return null;
  }

  return {
    token,
    user
  };
}

function requireAuth(session) {
  if (!session || !session.user) {
    throw createHttpError(401, "로그인이 필요합니다.");
  }
}

function requireAdmin(session) {
  requireAuth(session);
  if (session.user.role !== "admin") {
    throw createHttpError(403, "관리자 권한이 필요합니다.");
  }
}

function validateUserCreateInput(username, displayName, password) {
  if (!username) {
    throw createHttpError(400, "아이디를 입력해주세요.");
  }
  if (!/^[a-zA-Z0-9._-]{3,30}$/.test(username)) {
    throw createHttpError(400, "아이디는 영문/숫자/._- 조합 3~30자로 입력해주세요.");
  }
  if (!displayName) {
    throw createHttpError(400, "표시 이름을 입력해주세요.");
  }
  validatePassword(password);
}

function validatePassword(password) {
  if (!password || password.length < PASSWORD_MIN_LENGTH) {
    throw createHttpError(400, `비밀번호는 ${PASSWORD_MIN_LENGTH}자 이상이어야 합니다.`);
  }
}

function normalizeUsername(value) {
  return normalizeText(value).toLowerCase();
}

function normalizeRole(value) {
  return value === "admin" ? "admin" : "worker";
}

function sanitizeRecordInput(record) {
  if (!record || typeof record !== "object") {
    throw createHttpError(400, "저장할 검사 결과가 없습니다.");
  }

  const normalized = JSON.parse(JSON.stringify(record));
  normalized.id = normalizeText(normalized.id) || createId("rec");
  normalized.questionnaireId = normalizeText(normalized.questionnaireId);
  normalized.questionnaireTitle = normalizeText(normalized.questionnaireTitle);
  normalized.shortTitle = normalizeText(normalized.shortTitle) || normalized.questionnaireTitle;
  normalized.createdAt = normalizeText(normalized.createdAt);
  normalized.meta = normalized.meta && typeof normalized.meta === "object" ? normalized.meta : {};
  normalized.respondent = normalized.respondent && typeof normalized.respondent === "object" ? normalized.respondent : {};
  normalized.respondentDisplay = Array.isArray(normalized.respondentDisplay) ? normalized.respondentDisplay : [];
  normalized.answers = normalized.answers && typeof normalized.answers === "object" ? normalized.answers : {};
  normalized.breakdown = Array.isArray(normalized.breakdown) ? normalized.breakdown : [];
  normalized.evaluation = normalized.evaluation && typeof normalized.evaluation === "object" ? normalized.evaluation : {};

  if (!normalized.questionnaireId || !normalized.questionnaireTitle) {
    throw createHttpError(400, "척도 정보가 누락되어 있습니다.");
  }

  return normalized;
}

function buildOwnerSummary(user) {
  return {
    userId: user.id,
    username: user.username,
    displayName: user.displayName,
    role: user.role
  };
}

function ensureRecordAccess(record, user) {
  if (user.role === "admin") {
    return;
  }

  if (!record.owner || record.owner.userId !== user.id) {
    throw createHttpError(403, "이 저장 결과에 접근할 수 없습니다.");
  }
}

function toRecordSummary(record) {
  return {
    id: record.id,
    questionnaireId: record.questionnaireId || "",
    questionnaireTitle: record.questionnaireTitle || "",
    shortTitle: record.shortTitle || "",
    createdAt: record.createdAt || "",
    updatedAt: record.updatedAt || "",
    sessionDate: record.meta && record.meta.sessionDate ? record.meta.sessionDate : "",
    workerName: record.meta && record.meta.workerName ? record.meta.workerName : "",
    clientLabel: record.meta && record.meta.clientLabel ? record.meta.clientLabel : "",
    sessionNote: record.meta && record.meta.sessionNote ? record.meta.sessionNote : "",
    scoreText: record.evaluation && record.evaluation.scoreText ? record.evaluation.scoreText : "",
    bandText: record.evaluation && record.evaluation.bandText ? record.evaluation.bandText : "",
    ownerDisplayName: record.owner && record.owner.displayName ? record.owner.displayName : "",
    ownerUsername: record.owner && record.owner.username ? record.owner.username : "",
    ownerRole: record.owner && record.owner.role ? record.owner.role : ""
  };
}

function sortRecordsDescending(a, b) {
  return String(b.updatedAt || b.createdAt || "").localeCompare(String(a.updatedAt || a.createdAt || ""));
}

function normalizeText(value) {
  return value === null || value === undefined ? "" : String(value).trim();
}

function createId(prefix) {
  return `${prefix}_${Date.now().toString(36)}_${crypto.randomBytes(4).toString("hex")}`;
}
