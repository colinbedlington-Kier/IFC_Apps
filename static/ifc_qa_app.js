const qaState = {
  sessionId: "",
  canonicalSessionId: "",
  sessionReady: false,
  sessionStateText: "Session establishing...",
  activeJobId: "",
  uploadState: "idle",
  runStartState: "idle",
  uploadPercent: 0,
  uploadBytesLoaded: 0,
  uploadBytesTotal: 0,
  runError: null,
  isStartingRun: false,
  _loggedPollStart: false,
  _loggedFirstStatus: false,
  qaConfig: {
    shortCodes: {},
    layers: {},
    entityTypes: {},
    systemCategory: {},
    psetTemplate: {},
  },
  warning: "",
  configLoaded: false,
  fileQueue: [],
  selectedFiles: [],
  uploadedFiles: [],
  selectedTotalBytes: 0,
  canRunQa: false,
  sessionFiles: [],
  uploadWarning: "",
  sessionWarning: "",
  sessionReadyCount: 0,
  lastUpdated: "",
  modelCount: 0,
  hasZip: false,
  sessionSourceFiles: [],
  maxUploadBytes: 1_200_000_000,
  maxUploadDisplay: "1.2 GB",
  statusBanner: "No files selected",
  overallProgress: 0,
  buildInfo: null,
  selectionErrors: [],
  lastUploadResult: "none",
  selectedSessionFiles: [],
  sessionIfcFiles: [],
  rawSessionFiles: [],
  rawSessionFileNames: [],
  rawSessionFilesCount: 0,
  filteredIfcFilesCount: 0,
  filteredOutFilesCount: 0,
  filterSkipReasons: [],
  filteredIfcFileNames: [],
  rawResponseShape: "",
  extractionResults: [],
  extractionSummary: null,
  fetchUrl: "",
  fetchStatus: "",
  lastFetchError: "",
  isFetchingSessionFiles: false,
};

function isDebugPanelEnabled() {
  return !!qs("#qaDebugState");
}

function debugLog(...args) {
  if (!isDebugPanelEnabled()) return;
  console.info(...args);
}

function debugWarn(...args) {
  if (!isDebugPanelEnabled()) return;
  console.warn(...args);
}

const QA_UPLOAD_MAX_HINT_TEXT = "Maximum file size: 1.2 GB";

const DEFAULT_SHEETS = [
  ["model", "Model Data Table"],
  ["project", "Project Data Table"],
  ["object", "Object Data Table"],
  ["properties", "Property Data Table"],
  ["classification", "Classification Data Table"],
  ["spatial", "Spatial Structure Data Table"],
  ["system", "System Data Table"],
  ["pset_template", "Pset Template Data Table"],
];

const qs = (s) => document.querySelector(s);

function showErrorToast(message) {
  let toast = qs("#qaErrorToast");
  if (!toast) {
    toast = document.createElement("div");
    toast.id = "qaErrorToast";
    toast.style.position = "fixed";
    toast.style.right = "16px";
    toast.style.bottom = "16px";
    toast.style.zIndex = "9999";
    toast.style.background = "#b91c1c";
    toast.style.color = "#fff";
    toast.style.padding = "10px 12px";
    toast.style.borderRadius = "8px";
    toast.style.boxShadow = "0 8px 20px rgba(0,0,0,.25)";
    toast.style.maxWidth = "420px";
    toast.style.fontSize = "14px";
    document.body.appendChild(toast);
  }
  toast.textContent = message || "IFC QA run failed.";
  toast.hidden = false;
  window.setTimeout(() => {
    if (toast) toast.hidden = true;
  }, 5000);
}

function normalizeConfig(raw) {
  if (!raw || typeof raw !== "object") return qaState.qaConfig;
  const config = raw.config && typeof raw.config === "object" ? raw.config : raw;
  return {
    shortCodes: config.short_codes || config.shortCodes || {},
    layers: config.layers || {},
    entityTypes: config.entity_types || config.entityTypes || {},
    systemCategory: config.uniclass_system_category || config.systemCategory || {},
    psetTemplate: config.pset_template || config.psetTemplate || {},
  };
}

function collectLegacySessionStorageState() {
  const canonicalKey = window.IFCSession?.storageKey || "ifcToolkitSessionId";
  const sharedLegacyKeys = Array.isArray(window.IFCSession?.legacyStorageKeys) ? window.IFCSession.legacyStorageKeys : [];
  const fallbackLegacyKeys = ["sessionId", "ifcSessionId", "qaSessionId", "uploadSessionId", "ifc_session_id"];
  const keys = Array.from(new Set([...sharedLegacyKeys, ...fallbackLegacyKeys])).filter((key) => key && key !== canonicalKey);
  const values = {};
  keys.forEach((key) => {
    try {
      const localValue = localStorage.getItem(key);
      const sessionValue = sessionStorage.getItem(key);
      if (localValue || sessionValue) {
        values[key] = {
          localStorage: localValue || "",
          sessionStorage: sessionValue || "",
        };
      }
    } catch (_) {
      // no-op when storage is unavailable
    }
  });
  return values;
}

async function ensureSession() {
  const shared = window.IFCSession;
  const canonicalExisting = shared?.getCurrentSessionId?.() || "";
  let resolved = canonicalExisting;
  if (!resolved && shared?.ensureSession) {
    resolved = await shared.ensureSession({ createIfMissing: false });
  }
  if (!resolved && shared?.ensureSession) {
    resolved = await shared.ensureSession({ createIfMissing: true });
  }
  const normalized = String(resolved || "").trim();
  qaState.sessionId = normalized;
  qaState.canonicalSessionId = normalized;
  qaState.sessionReady = !!normalized;
  qaState.sessionStateText = qaState.sessionReady
    ? `Session ready · ${shared?.shortSessionId ? shared.shortSessionId(normalized) : normalized.slice(0, 8)}`
    : "Session establishing...";
  debugLog("IFC QA session id availability", { canonicalSessionId: qaState.canonicalSessionId, sessionReady: qaState.sessionReady });
  if (qaState.sessionId) {
    if (shared?.setCurrentSessionId) shared.setCurrentSessionId(qaState.sessionId);
  }
  updateGlobalSessionBadge();
  renderSessionState();
  renderActionButtons();
  renderDebugState();
  return qaState.sessionId;
}

async function refreshSessionSummary() {
  if (!qaState.sessionId) return;
  try {
    const resp = await fetch(`/api/ifc-qa/session/${qaState.sessionId}/summary`);
    if (!resp.ok) return;
    const data = await resp.json();
    qaState.modelCount = data.model_count || 0;
    qaState.lastUpdated = data.updated_at || "";
    qaState.hasZip = !!data.has_zip;
    qaState.sessionSourceFiles = Array.isArray(data.source_files) ? data.source_files : [];
    if (!qaState.canRunQa && qaState.sessionSourceFiles.length > 0) {
      qaState.canRunQa = true;
    }
  } catch (_) {}
  renderSessionSummary();
  renderActionButtons();
  renderDebugState();
}

async function loadQaConfig() {
  try {
    const resp = await fetch("/api/ifc-qa/config");
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    qaState.qaConfig = normalizeConfig(data);
    qaState.warning = "";
    qaState.configLoaded = true;
    return;
  } catch (err) {
    console.warn("Failed to load IFC QA config", err);
  }
  qaState.warning = "Failed to load IFC QA config. Extraction is disabled until config loads.";
  qaState.configLoaded = false;
}

async function loadUploadLimits() {
  try {
    const resp = await fetch("/api/upload/limits");
    if (resp.ok) {
      const data = await resp.json();
      if (Number.isFinite(Number(data.max_upload_bytes))) qaState.maxUploadBytes = Number(data.max_upload_bytes);
      if (data.max_upload_display) qaState.maxUploadDisplay = String(data.max_upload_display);
    }
  } catch (_) {}
  const hint = qs("#qaMaxUploadHint");
  if (hint) hint.textContent = `Maximum file size: ${qaState.maxUploadDisplay}`;
}

function warningBanner() {
  if (!qaState.warning) return "";
  return `<div class="card" style="border-left:4px solid #f59e0b;background:#fff8e6">${qaState.warning}</div>`;
}

function extractorTemplate() {
  const disabledAttr = qaState.configLoaded ? "" : "disabled";
  return `
  ${warningBanner()}
  <div class="stack">
    <div id="qaSessionState" class="muted">${qaState.sessionStateText}</div>
    <div class="section-title"><h3>Session IFC files</h3></div>
    <div id="qaSessionFilesSummary" class="muted">Loading session files…</div>
    <ul id="qaSessionFileList" class="qa-file-list muted"></ul>
    <div class="inline">
      <button class="btn secondary" id="qaSelectAllBtn" type="button">Select All</button>
      <button class="btn secondary" id="qaClearSelectionBtn" type="button">Clear Selection</button>
      <button class="btn secondary" id="qaRefreshSessionFilesBtn" type="button">Refresh Session Files</button>
      <button class="btn" id="qaStartBtn" type="button" ${disabledAttr}>Start QA Extraction</button>
    </div>
    <div id="qaSelectionSummary" class="muted">No files selected.</div>
    <div id="qaRunError" class="qa-error-banner" hidden></div>
    <div id="qaSessionWarning" class="qa-warning-banner" hidden></div>

    <label>Outputs</label>
    <div id="qaSheetChecks" class="qa-grid"></div>
    <div class="inline"><button class="btn secondary" id="qaConfigureBtn" type="button" ${disabledAttr}>Configure</button><button class="btn secondary" id="qaDownloadBtn" type="button" disabled>Download ZIP</button></div>
    <div id="qaResultsSummary" class="muted"></div>
    <ul id="qaResultsList" class="qa-file-list muted"></ul>

    <div id="qaSessionSummary" class="muted"></div>
    <div id="qaBuildInfo" class="muted">Build: loading…</div>
    <div id="qaDebugState" class="muted" style="font-family:monospace;border:1px dashed #cbd5e1;padding:8px;border-radius:6px"></div>

    <div class="progress-track"><div id="qaProgressFill" class="progress-fill" style="width:0%"></div></div>
    <div id="qaProgressLabel" class="muted"></div>
    <textarea id="qaLog" class="log-box" rows="10" readonly></textarea>
  </div>

  <div class="modal" id="qaConfigModal" hidden>
    <div class="modal-content" style="width:min(900px,95vw)">
      <div class="section-title"><h3>IFC QA JSON Configuration (session only)</h3></div>
      <textarea id="qaConfigText" rows="18" style="width:100%;font-family:monospace"></textarea>
      <div class="inline" style="margin-top:12px">
        <button class="btn" id="qaConfigApply" type="button">Apply</button>
        <button class="btn secondary" id="qaConfigExport" type="button">Export JSON</button>
        <label class="btn secondary" for="qaConfigImport" style="cursor:pointer">Import JSON</label>
        <input id="qaConfigImport" type="file" accept="application/json" hidden />
        <button class="btn ghost" id="qaConfigClose" type="button">Close</button>
      </div>
    </div>
  </div>`;
}

function formatBytes(bytes) {
  const value = Number(bytes) || 0;
  if (value <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let idx = 0;
  let size = value;
  while (size >= 1024 && idx < units.length - 1) {
    size /= 1024;
    idx += 1;
  }
  const precision = size >= 100 || idx === 0 ? 0 : 1;
  return `${size.toFixed(precision)} ${units[idx]}`;
}

function makeFileId(file, idx) {
  return `${file.name}::${file.size}::${file.lastModified || 0}::${idx}`;
}

function validateSelectedFiles(files) {
  const acceptedFiles = [];
  const rejectedFiles = [];
  const seenKeys = new Set();
  files.forEach((file) => {
    const key = `${file.name}::${file.size}::${file.lastModified || 0}`;
    const lowerName = String(file.name || "").toLowerCase();
    if (!lowerName.endsWith(".ifc")) {
      rejectedFiles.push({ name: file.name, reason: "Only .ifc files are allowed." });
      return;
    }
    if (Number(file.size) > qaState.maxUploadBytes) {
      rejectedFiles.push({ name: file.name, reason: `Exceeds maximum upload size of ${qaState.maxUploadDisplay}.` });
      return;
    }
    if (seenKeys.has(key)) {
      rejectedFiles.push({ name: file.name, reason: "Duplicate file in selection." });
      return;
    }
    seenKeys.add(key);
    acceptedFiles.push(file);
  });
  return { acceptedFiles, rejectedFiles };
}

function createUploadModel(files) {
  return files.map((file, idx) => ({
    id: makeFileId(file, idx),
    name: file.name,
    size: Number(file.size) || 0,
    uploadedBytes: 0,
    progressPct: 0,
    status: "queued",
    stageText: "Queued",
  }));
}

function isValidIfcFile(file) {
  const name = String(file?.name || "").toLowerCase();
  const mime = String(file?.type || "").toLowerCase();
  return name.endsWith(".ifc") || mime === "application/x-step" || mime === "application/octet-stream";
}

function syncQueueFromSelectedFiles() {
  qaState.selectedTotalBytes = (qaState.selectedFiles || []).reduce((sum, file) => sum + (Number(file.size) || 0), 0);
  qaState.fileQueue = createUploadModel(qaState.selectedFiles || []);
  qaState.statusBanner = qaState.selectedFiles.length
    ? `${qaState.selectedFiles.length} file${qaState.selectedFiles.length === 1 ? "" : "s"} queued for upload`
    : "No files selected";
}

function setUploadWarning(message) {
  qaState.uploadWarning = message || "";
  const el = qs("#qaUploadWarning");
  if (!el) return;
  el.hidden = !qaState.uploadWarning;
  el.textContent = qaState.uploadWarning;
}

function setSessionWarning(message) {
  qaState.sessionWarning = message || "";
  const el = qs("#qaSessionWarning");
  if (!el) return;
  el.hidden = !qaState.sessionWarning;
  el.textContent = qaState.sessionWarning;
}

function renderSessionState() {
  const el = qs("#qaSessionState");
  if (!el) return;
  if (qaState.sessionReady && qaState.sessionId) {
    const shortId = window.IFCSession?.shortSessionId
      ? window.IFCSession.shortSessionId(qaState.sessionId)
      : qaState.sessionId.slice(0, 8);
    el.textContent = `Session ready · ${shortId}`;
    return;
  }
  el.textContent = "Session establishing...";
}

function updateGlobalSessionBadge() {
  const badge = document.querySelector("[data-session-badge]");
  if (!badge) return;
  if (qaState.sessionReady && qaState.sessionId) {
    const shortId = window.IFCSession?.shortSessionId
      ? window.IFCSession.shortSessionId(qaState.sessionId)
      : qaState.sessionId.slice(0, 8);
    badge.textContent = `Session ready · ${shortId}`;
    badge.classList.add("success-text");
    badge.classList.remove("danger-text");
  } else {
    badge.textContent = "Session establishing...";
    badge.classList.remove("success-text");
  }
}

function statusLabel(status) {
  return ({ queued: "Queued", uploading: "Uploading", uploaded: "Uploaded", processing: "Processing", complete: "Complete", failed: "Failed" }[status] || "Queued");
}

function renderFileQueue() {
  const wrap = qs("#qaFileQueue");
  if (!wrap) return;
  if (!qaState.fileQueue.length) {
    wrap.innerHTML = "<div class='muted'>No local upload queue items.</div>";
    return;
  }
  wrap.innerHTML = qaState.fileQueue.map((row) => {
    const pct = Math.max(0, Math.min(100, row.progressPct || 0));
    const err = row.error ? `<div class='muted' style='color:#b91c1c'>${row.error}</div>` : "";
    return `
      <div class="qa-file-row">
        <div class="qa-file-row-head">
          <strong>${row.name}</strong>
          <span class="muted">${formatBytes(row.size)}</span>
        </div>
        <div class="muted">${statusLabel(row.status)} • ${Math.round(pct)}% • ${formatBytes(row.uploadedBytes || 0)} / ${formatBytes(row.size)}</div>
        <div class="qa-upload-progress-track"><div class="qa-upload-progress-bar" style="width:${pct}%"></div></div>
        ${err}
      </div>`;
  }).join("");
}

function renderSessionSummary() {
  const el = qs("#qaSessionSummary");
  if (!el) return;
  el.textContent = `Session ZIP contains: ${qaState.modelCount} models | Uploaded IFC files in session: ${qaState.sessionReadyCount} | Last updated: ${qaState.lastUpdated || "-"}`;
}

function renderActionButtons() {
  const startBtn = qs("#qaStartBtn");
  const dlBtn = qs("#qaDownloadBtn");
  if (startBtn) startBtn.disabled = qaState.isRunning || qaState.isStartingRun || !qaState.configLoaded || qaState.selectedSessionFiles.length <= 0;
  if (dlBtn) dlBtn.disabled = qaState.isRunning || !qaState.hasZip;
}

function selectedSheets() {
  const out = {};
  DEFAULT_SHEETS.forEach(([k]) => { out[k] = !!qs(`[data-sheet="${k}"]`)?.checked; });
  return out;
}

function bindFilesList() {
  const input = qs("#qaIfcFiles");
  if (!input) return;
  input.addEventListener("change", (event) => {
    const rawFiles = event?.target?.files || input.files || [];
    const files = Array.from(rawFiles);
    console.info(`IFC QA file input onChange fired, files=${files.length}`);
    console.info("IFC QA file input change payload", { names: files.map((file) => file.name) });
    const rejected = [];
    const acceptedFiles = [];
    files.forEach((file) => {
      if (!isValidIfcFile(file)) {
        rejected.push(`${file.name} is not an IFC file`);
        return;
      }
      if (Number(file.size) > qaState.maxUploadBytes) {
        rejected.push(`${file.name} exceeds ${qaState.maxUploadDisplay}`);
        return;
      }
      acceptedFiles.push(file);
    });
    console.info(`IFC QA acceptedFiles=${acceptedFiles.length}`);
    if (!acceptedFiles.length) {
      qaState.selectedFiles = [];
      syncQueueFromSelectedFiles();
      qaState.overallProgress = 0;
      setUploadWarning(rejected.length ? `Upload rejected: ${rejected.join("; ")}` : "No valid IFC files selected.");
      setUploadState("failed", { uploadBytesLoaded: 0, uploadBytesTotal: 0, uploadPercent: 0 });
      console.warn("IFC QA selection rejected", { rejected });
      renderSelectedFilesState();
      renderFileQueue();
      renderActionButtons();
      return;
    }
    qaState.selectedFiles = acceptedFiles;
    syncQueueFromSelectedFiles();
    console.info(`IFC QA selectedFiles state=${qaState.selectedFiles.length}`);
    qaState.overallProgress = 0;
    setUploadWarning(rejected.length ? `Some files were rejected: ${rejected.join("; ")}` : "");
    setSessionWarning("");
    console.info("IFC QA upload queue creation", { count: qaState.fileQueue.length, totalBytes: qaState.selectedTotalBytes, rejectedCount: rejected.length });
    renderSelectedFilesState();
    clearRunError();
    setUploadState("queued", { uploadBytesLoaded: 0, uploadBytesTotal: qaState.selectedTotalBytes, uploadPercent: 0 });
    renderFileQueue();
    renderActionButtons();
  });
}

function setUploadControlsDisabled(disabled) {
  const fileInput = qs("#qaIfcFiles");
  if (fileInput) fileInput.disabled = disabled;
}

function renderSelectedFilesState() {
  const summary = qs("#qaSelectionSummary");
  if (summary) {
    const count = qaState.selectedSessionFiles.length;
    if (count > 0) {
      summary.textContent = `${count} file${count === 1 ? "" : "s"} selected from this session`;
    } else if ((qaState.sessionIfcFiles || []).length > 0) {
      summary.textContent = "Select one or more IFC session files to run extraction.";
    } else {
      summary.textContent = "No IFC files selected.";
    }
  }
}

function renderSessionFiles() {
  const files = Array.isArray(qaState.sessionIfcFiles) ? qaState.sessionIfcFiles : [];
  const rawFiles = Array.isArray(qaState.rawSessionFiles) ? qaState.rawSessionFiles : [];
  const summary = qs("#qaSessionFilesSummary");
  const list = qs("#qaSessionFileList");
  if (summary) {
    if (qaState.isFetchingSessionFiles) {
      summary.textContent = "Loading session files...";
    } else if (String(qaState.fetchStatus || "").startsWith("error")) {
      summary.textContent = "Failed to fetch session files.";
    } else if (files.length) {
      summary.textContent = `Files found: ${files.length} IFC file${files.length === 1 ? "" : "s"} ready.`;
    } else if (rawFiles.length) {
      summary.textContent = "Session files found but none are IFC files.";
    } else {
      summary.textContent = "No session files found.";
    }
  }
  if (!list) return;
  if (!files.length) {
    if (qaState.isFetchingSessionFiles) {
      list.innerHTML = "<li>Loading session files...</li>";
      return;
    }
    if (String(qaState.fetchStatus || "").startsWith("error")) {
      list.innerHTML = `<li>Failed to fetch session files. ${qaState.lastFetchError || "Unknown error."}</li>`;
      return;
    }
    if (!rawFiles.length) {
      list.innerHTML = "<li>No session files found.</li>";
      return;
    }
    const skipRows = (qaState.filterSkipReasons || [])
      .map((item) => `<li><span>${item.name || "(unknown)"}</span><span class="muted">${item.reason}</span></li>`)
      .join("");
    list.innerHTML = `
      <li>Session files found but none are IFC files.</li>
      <li class="muted">Raw filenames: ${(qaState.rawSessionFileNames || []).join(", ") || "-"}</li>
      ${skipRows}
    `;
    return;
  }
  list.innerHTML = files.map((f) => {
    const name = getSessionFileName(f) || "unknown";
    const size = Number(f.size || f.bytes || 0);
    const modified = f.modified || f.uploaded || f.uploaded_at || "-";
    const checked = qaState.selectedSessionFiles.includes(name) ? "checked" : "";
    return `<li><label style="display:flex;justify-content:space-between;gap:12px;width:100%"><span><input type="checkbox" class="qa-session-file-checkbox" data-name="${name}" ${checked}/> ${name}</span><span>${formatBytes(size)} • ${modified}</span></label></li>`;
  }).join("");
  list.querySelectorAll(".qa-session-file-checkbox").forEach((box) => {
    box.addEventListener("change", (event) => {
      const name = event.target?.dataset?.name;
      if (!name) return;
      const set = new Set(qaState.selectedSessionFiles);
      if (event.target.checked) set.add(name);
      else set.delete(name);
      qaState.selectedSessionFiles = Array.from(set);
      console.info("IFC QA selected session files", { sessionId: qaState.sessionId, selectedCount: qaState.selectedSessionFiles.length });
      renderSelectedFilesState();
      renderActionButtons();
      renderDebugState();
    });
  });
}

function renderDebugState() {
  const el = qs("#qaDebugState");
  if (!el) return;
  const previewNames = (qaState.rawSessionFileNames || []).slice(0, 5).join(", ") || "-";
  const filteredNames = (qaState.filteredIfcFileNames || []).slice(0, 5).join(", ") || "-";
  const skipPreview = (qaState.filterSkipReasons || []).slice(0, 5).map((row) => `${row.name}: ${row.reason}`).join(" | ") || "-";
  const legacySessionKeys = collectLegacySessionStorageState();
  el.innerHTML = [
    `<strong>Debug state</strong>`,
    `canonicalSessionId: ${qaState.canonicalSessionId || "-"}`,
    `localStateSessionId: ${qaState.sessionId || "-"}`,
    `sessionReady: ${qaState.sessionReady}`,
    `legacySessionKeys: ${JSON.stringify(legacySessionKeys)}`,
    `fetchUrl: ${qaState.fetchUrl || "-"}`,
    `fetchStatus: ${qaState.fetchStatus || "-"}`,
    `rawResponseShape: ${qaState.rawResponseShape || "-"}`,
    `rawSessionFilesCount: ${qaState.rawSessionFilesCount || 0}`,
    `rawSessionFileNames: ${previewNames}`,
    `filteredIfcFilesCount: ${qaState.filteredIfcFilesCount || 0}`,
    `filteredIfcFileNames: ${filteredNames}`,
    `filterSkipReasons: ${skipPreview}`,
    `selectedSessionFiles: ${JSON.stringify(qaState.selectedSessionFiles || [])}`,
    `lastFetchError: ${qaState.lastFetchError || "-"}`,
    `uploadQueue: ${(qaState.fileQueue || []).length}`,
    `sessionFiles: ${(qaState.sessionIfcFiles || []).length}`,
    `lastUploadResult: ${qaState.lastUploadResult || "none"}`,
  ].join("<br>");
}

function renderBuildInfo() {
  const el = qs("#qaBuildInfo");
  if (!el) return;
  const info = qaState.buildInfo;
  if (!info) {
    const fallback = window.__IFC_QA_BUILD_ID__ || "unknown";
    el.textContent = `Build: ${fallback}`;
    return;
  }
  const shortSha = (info.git_sha || "unknown").slice(0, 8);
  el.textContent = `Build: ${info.frontend_build_id || "unknown"} (sha ${shortSha})`;
  renderDebugState();
}

async function loadBuildInfo() {
  try {
    const resp = await fetch("/api/ifc-qa/build-info");
    if (!resp.ok) return;
    const data = await resp.json();
    qaState.buildInfo = data;
    console.info("IFC QA frontend build info", data);
    renderBuildInfo();
  } catch (_) {
    renderBuildInfo();
  }
}

function clearRunError() {
  qaState.runError = null;
  renderRunError();
}

function setRunError(stage, message, detail = "") {
  qaState.runError = { stage, message, detail };
  renderRunError();
}

function renderRunError() {
  const error = qs("#qaRunError");
  if (!error) return;
  if (!qaState.runError) {
    error.hidden = true;
    error.innerHTML = "";
    return;
  }
  const stage = qaState.runError.stage || "unknown";
  const message = qaState.runError.message || "Unknown error";
  const detail = qaState.runError.detail ? `<details><summary>Details</summary><pre>${String(qaState.runError.detail)}</pre></details>` : "";
  error.hidden = false;
  error.innerHTML = `<strong>${stage.toUpperCase()}:</strong> ${message}${detail}`;
}

function setUploadState(nextState, patch = {}) {
  qaState.uploadState = nextState;
  Object.assign(qaState, patch);
  qaState.overallProgress = Number(qaState.uploadPercent) || 0;
  renderUploadProgress();
}

function setRunStartState(nextState) {
  qaState.runStartState = nextState;
  const status = qs("#qaUploadStatusText");
  if (!status) return;
  if (nextState === "starting") {
    qaState.uploadState = "running";
    status.textContent = "Running QA";
  }
  if (nextState === "running") {
    qaState.uploadState = "running";
    status.textContent = "Running QA";
  }
}

function renderUploadProgress() {
  const status = qs("#qaUploadStatusText");
  const pct = qs("#qaUploadPercentText");
  const track = qs("#qaUploadProgressTrack");
  const bar = qs("#qaUploadProgressBar");
  const bytes = qs("#qaUploadBytesText");

  const loaded = Number(qaState.uploadBytesLoaded) || 0;
  const total = Number(qaState.uploadBytesTotal) || 0;
  const percent = Math.max(0, Math.min(100, Number(qaState.uploadPercent) || 0));
  const isUploading = qaState.uploadState === "uploading";
  const totalQueued = qaState.selectedFiles.length;
  const uploadedCount = qaState.fileQueue.filter((row) => row.status === "uploaded" || row.status === "complete").length;
  const uploadingIndex = qaState.fileQueue.findIndex((row) => row.status === "uploading");
  const sessionCount = (qaState.sessionFiles || []).length;
  const statusMap = {
    idle: sessionCount > 0 ? `${sessionCount} file${sessionCount === 1 ? "" : "s"} uploaded to session` : "No files selected",
    queued: `${totalQueued} file${totalQueued === 1 ? "" : "s"} queued for upload`,
    uploading: uploadingIndex >= 0 ? `Uploading ${uploadingIndex + 1} of ${totalQueued} files...` : "Uploading files...",
    uploaded: `${uploadedCount} file${uploadedCount === 1 ? "" : "s"} uploaded successfully`,
    running: "Running QA",
    complete: "Complete",
    failed: "Upload failed",
  };

  qaState.statusBanner = statusMap[qaState.uploadState] || (sessionCount > 0 ? `${sessionCount} file${sessionCount === 1 ? "" : "s"} uploaded to session` : "No files selected");
  if (status) status.textContent = qaState.statusBanner;
  if (pct) pct.textContent = `${percent}%`;
  if (bar) bar.style.width = `${percent}%`;
  if (bytes) bytes.textContent = total > 0 ? `${formatBytes(loaded)} / ${formatBytes(total)}` : `${formatBytes(loaded)} uploaded`;
  if (track) track.classList.toggle("indeterminate", isUploading && total <= 0);
  setUploadControlsDisabled(isUploading || qaState.isStartingRun);
  renderActionButtons();
  renderDebugState();
}

function parseXhrJson(xhr) {
  if (xhr.response && typeof xhr.response === "object") return xhr.response;
  const text = xhr.responseText || "";
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch {
    throw new Error("Invalid start-job response");
  }
}

function getSessionFileName(file) {
  const raw = file && typeof file === "object" ? file : {};
  const candidates = [
    raw.name,
    raw.filename,
    raw.file_name,
    raw.original_name,
    raw.path,
    raw.relative_path,
    raw.display_name,
  ];
  for (const value of candidates) {
    const normalized = String(value || "").trim();
    if (normalized) return normalized.split(/[\\/]/).pop() || normalized;
  }
  return "";
}

function reconcileSessionFiles(files) {
  const rawFiles = Array.isArray(files) ? files : [];
  const skipReasons = [];
  const uploadedIfc = rawFiles.filter((file) => {
    const fileName = getSessionFileName(file);
    if (!fileName) {
      skipReasons.push({ name: "(missing name)", reason: "Missing filename fields (name/filename/file_name/original_name/path/relative_path)." });
      return false;
    }
    const normalized = fileName.toLowerCase();
    const accepted = normalized.endsWith(".ifc") || normalized.endsWith(".ifczip") || normalized.endsWith(".ifcxml");
    if (!accepted) {
      skipReasons.push({ name: fileName, reason: "Extension is not .ifc, .ifczip, or .ifcxml." });
      return false;
    }
    return true;
  });
  qaState.rawSessionFiles = rawFiles;
  qaState.sessionFiles = rawFiles;
  qaState.rawSessionFilesCount = rawFiles.length;
  qaState.rawSessionFileNames = rawFiles.map((f) => getSessionFileName(f) || "unknown");
  qaState.filteredIfcFilesCount = uploadedIfc.length;
  qaState.filteredOutFilesCount = Math.max(0, rawFiles.length - uploadedIfc.length);
  qaState.filteredIfcFileNames = uploadedIfc.map((file) => getSessionFileName(file)).filter(Boolean);
  qaState.filterSkipReasons = skipReasons;
  qaState.sessionIfcFiles = uploadedIfc;
  qaState.sessionReadyCount = uploadedIfc.length;
  qaState.uploadedFiles = uploadedIfc;
  qaState.canRunQa = uploadedIfc.length > 0;
  const validNames = new Set(uploadedIfc.map((f) => getSessionFileName(f)).filter(Boolean));
  qaState.selectedSessionFiles = qaState.selectedSessionFiles.filter((name) => validNames.has(name));
  debugLog("IFC QA session list reconciled", {
    canonicalSessionId: qaState.canonicalSessionId,
    returnedRaw: rawFiles.length,
    returnedIfc: uploadedIfc.length,
    rawFilenames: qaState.rawSessionFileNames,
    filteredFilenames: qaState.filteredIfcFileNames,
    filterSkipReasons: qaState.filterSkipReasons,
    selected: qaState.selectedSessionFiles.length,
  });
  renderSessionFiles();
  renderSelectedFilesState();
  renderSessionSummary();
  renderActionButtons();
  renderDebugState();
}

async function loadSessionFiles() {
  const canonicalSessionId = String(qaState.canonicalSessionId || qaState.sessionId || "").trim();
  if (!qaState.sessionReady || !canonicalSessionId) return [];
  qaState.fetchUrl = `/api/session/${canonicalSessionId}/files`;
  qaState.fetchStatus = "pending";
  qaState.lastFetchError = "";
  qaState.isFetchingSessionFiles = true;
  renderSessionFiles();
  renderDebugState();
  try {
    const onResponse = (meta) => {
      qaState.fetchStatus = String(meta?.status || "");
      qaState.rawResponseShape = String(meta?.shape || "");
      debugLog("IFC QA session file fetch response", {
        canonicalSessionId,
        fetchUrl: qaState.fetchUrl,
        httpStatus: meta?.status,
        rawResponseShape: meta?.shape,
      });
    };
    let allFiles = [];
    if (window.IFCSession?.getSessionFiles) {
      allFiles = await window.IFCSession.getSessionFiles(canonicalSessionId, { onResponse });
    } else {
      const resp = await fetch(qaState.fetchUrl);
      const payload = await resp.json();
      qaState.fetchStatus = String(resp.status);
      qaState.rawResponseShape = Array.isArray(payload) ? "array" : (payload && Array.isArray(payload.files) ? "object.files" : (payload && Array.isArray(payload.items) ? "object.items" : typeof payload));
      if (!resp.ok) {
        throw Object.assign(new Error(`Failed to refresh session files (HTTP ${resp.status})`), { status: resp.status, body: payload });
      }
      const rawRecords = Array.isArray(payload)
        ? payload
        : Array.isArray(payload?.files)
          ? payload.files
          : Array.isArray(payload?.items)
            ? payload.items
            : [];
      allFiles = rawRecords.map((record) => (window.IFCSession?.normalizeSessionFile ? window.IFCSession.normalizeSessionFile(record) : record));
    }
    qaState.fetchStatus = "ok";
    reconcileSessionFiles(allFiles);
    return allFiles;
  } catch (err) {
    const status = err?.status || "error";
    const detail = typeof err?.body === "string" ? err.body : err?.body ? JSON.stringify(err.body) : "";
    qaState.fetchStatus = `error:${status}`;
    qaState.lastFetchError = `HTTP ${status}${detail ? ` ${detail}` : ""}`;
    debugWarn("IFC QA session file fetch failed", {
      canonicalSessionId,
      fetchUrl: qaState.fetchUrl,
      fetchStatus: qaState.fetchStatus,
      error: qaState.lastFetchError,
    });
    setSessionWarning(`Failed to load session files. ${qaState.lastFetchError}`);
    reconcileSessionFiles([]);
    return [];
  } finally {
    qaState.isFetchingSessionFiles = false;
    renderSessionFiles();
    renderDebugState();
  }
}

async function uploadSelectedFiles(targetStatuses = ["queued"]) {
  const queuedRows = qaState.fileQueue.filter((row) => targetStatuses.includes(row.status));
  const queuedIds = new Set(queuedRows.map((row) => row.id));
  const files = (qaState.selectedFiles || []).filter((file, idx) => queuedIds.has(makeFileId(file, idx)));
  if (!files.length || !qaState.sessionId) return;
  const baselineSessionId = qaState.sessionId;
  const form = new FormData();
  files.forEach((file) => form.append("files", file, file.name));
  const totalBytes = files.reduce((sum, file) => sum + (Number(file.size) || 0), 0);
  setUploadState("uploading", { uploadBytesTotal: totalBytes, uploadBytesLoaded: 0, uploadPercent: 0 });
  queuedRows.forEach((row) => { row.status = "uploading"; row.stageText = "Uploading"; row.error = ""; row.uploadedBytes = 0; row.progressPct = 0; });
  renderFileQueue();
  console.info("IFC QA upload started", { sessionId: qaState.sessionId, fileCount: files.length, totalBytes });
  await new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", `/api/session/${qaState.sessionId}/upload`);
    xhr.upload.onprogress = (evt) => {
      const loaded = evt.loaded || 0;
      const total = evt.total || totalBytes || 0;
      const percent = total > 0 ? Math.round((loaded / total) * 100) : 0;
      let running = 0;
      queuedRows.forEach((row) => {
        const remaining = Math.max(0, loaded - running);
        const uploaded = Math.min(row.size, remaining);
        row.uploadedBytes = uploaded;
        row.progressPct = row.size > 0 ? Math.round((uploaded / row.size) * 100) : 100;
        row.status = row.progressPct >= 100 ? "uploaded" : "uploading";
        running += row.size;
      });
      setUploadState("uploading", { uploadBytesLoaded: loaded, uploadBytesTotal: total || totalBytes, uploadPercent: percent });
      console.info("IFC QA upload progress", { loaded, total, percent });
      renderFileQueue();
    };
    xhr.onerror = () => reject(new Error("Network error during upload"));
    xhr.onload = () => {
      if (xhr.status < 200 || xhr.status >= 300) {
        let payload = {};
        try {
          payload = parseXhrJson(xhr);
        } catch (_) {}
        reject(new Error(
          payload?.message
          || payload?.detail?.message
          || payload?.detail
          || `Upload failed with status ${xhr.status}`
        ));
        return;
      }
      console.info("IFC QA upload success", { sessionId: qaState.sessionId, fileCount: files.length });
      resolve();
    };
    xhr.send(form);
  });
  qaState.lastUploadResult = `success @ ${new Date().toISOString()}`;
  if (qaState.sessionId !== baselineSessionId) {
    setSessionWarning("Session changed during upload. Please reselect files before running QA.");
    return;
  }
  console.info("IFC QA upload complete", { sessionId: qaState.sessionId });
  const filesFromSession = await loadSessionFiles();
  setUploadWarning(filesFromSession.length ? "" : "Upload completed but the session file list did not refresh.");
  const progressUnavailable = (Number(qaState.uploadBytesLoaded) || 0) <= 0;
  const loadedBytes = progressUnavailable ? totalBytes : Math.max(qaState.uploadBytesLoaded, totalBytes);
  setUploadState("uploaded", {
    uploadPercent: totalBytes > 0 ? 100 : qaState.uploadPercent,
    uploadBytesLoaded: loadedBytes,
    uploadBytesTotal: totalBytes,
  });
}

async function triggerUpload(targetStatuses = ["queued"]) {
  try {
    await uploadSelectedFiles(targetStatuses);
    setUploadWarning("");
  } catch (err) {
    const message = err instanceof Error ? err.message : "Upload failed";
    console.error("IFC QA upload failure", { message, err });
    setUploadWarning(message);
    qaState.lastUploadResult = `failed: ${message}`;
    setUploadState("failed", { uploadPercent: 0, uploadBytesLoaded: 0, uploadBytesTotal: qaState.selectedTotalBytes });
    qaState.fileQueue
      .filter((row) => targetStatuses.includes(row.status) || row.status === "uploading")
      .forEach((row) => { row.status = "failed"; row.stageText = "Failed"; row.error = message; });
    renderFileQueue();
    renderActionButtons();
  }
}

function startQaUpload(form, fileCount) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", "/api/ifc-qa/run");
    xhr.upload.onprogress = (evt) => {
      const loaded = evt.loaded || 0;
      const total = evt.total || 0;
      const percent = total > 0 ? Math.round((loaded / total) * 100) : qaState.uploadPercent;
      setUploadState("uploading", {
        uploadBytesLoaded: loaded,
        uploadBytesTotal: total,
        uploadPercent: percent,
      });
      const status = qs("#qaUploadStatusText");
      if (status) status.textContent = `Uploading ${fileCount} file${fileCount === 1 ? "" : "s"}…`;
    };
    xhr.onerror = () => reject({ stage: "upload", message: "Network error during upload", detail: "" });
    xhr.onload = () => {
      let payload = {};
      try {
        payload = parseXhrJson(xhr);
      } catch (error) {
        reject({ stage: "start", message: "Invalid start-job response", detail: error instanceof Error ? error.message : "" });
        return;
      }
      console.info("IFC QA /run response", { status: xhr.status, payload });
      if (xhr.status < 200 || xhr.status >= 300) {
        if (xhr.status >= 500) {
          const hint = payload?.hint ? ` (${payload.hint})` : "";
          showErrorToast(`IFC QA start failed: ${payload?.error || "server error"}${hint}`);
        }
        reject({
          stage: "start",
          message: payload?.message || payload?.error || payload?.detail || `Request failed with status ${xhr.status}`,
          detail: payload?.detail || "",
        });
        return;
      }
      resolve(xhr.response || {});
    };
    xhr.send(form);
  });
}

function updateRowFromStatus(row, statusFile) {
  row.status = statusFile.status || row.status;
  row.uploadPercent = statusFile.upload_percent ?? row.uploadPercent;
  row.processPercent = statusFile.process_percent ?? row.processPercent;
  row.overallPercent = statusFile.overall_percent ?? row.overallPercent;
  row.stageText = statusFile.message || statusFile.stage || row.stageText;
}

async function pollJob(jobId, row) {
  while (true) {
    const resp = await fetch(`/api/ifc-qa/status/${jobId}`);
    if (!resp.ok) throw new Error("status failed");
    const data = await resp.json();
    const statusFile = (data.files || []).find((f) => f.source_file === row.name || f.name === row.name);
    if (statusFile) updateRowFromStatus(row, statusFile);
    const fill = qs("#qaProgressFill");
    const label = qs("#qaProgressLabel");
    const log = qs("#qaLog");
    if (fill) fill.style.width = `${data.overall_percent || 0}%`;
    if (label) label.textContent = `${data.currentStep || ""} ${data.currentFile ? `(${data.currentFile})` : ""}`;
    if (log) log.value = (data.logs || []).join("\n");
    renderFileQueue();
    if (data.status === "complete") return;
    if (data.status === "failed") throw new Error("processing failed");
    await new Promise((r) => setTimeout(r, 1000));
  }
}

async function runQueue(mode) {
  if (qaState.isRunning) return;
  const queued = qaState.fileQueue.filter((row) => row.status === "queued" || row.status === "failed");
  if (!queued.length) return;
  qaState.isRunning = true;
  renderActionButtons();
  for (const row of queued) {
    row.error = "";
    try {
      const payload = await uploadSingleFile(row, mode);
      qaState.activeJobId = payload.job_id || "";
      qaState.sessionId = payload.session_id || qaState.sessionId;
      row.status = "uploaded";
      row.stageText = "Uploaded, waiting to process…";
      row.overallPercent = Math.max(row.overallPercent, 50);
      renderFileQueue();
      await pollJob(qaState.activeJobId, row);
      row.status = "complete";
      row.stageText = "Complete";
      row.overallPercent = 100;
    } catch (err) {
      row.status = "failed";
      row.stageText = "Failed";
      row.error = err instanceof Error ? err.message : "Failed";
    }
  }
  qaState.isRunning = false;
  renderActionButtons();
}

async function startRun() {
  clearRunError();
  if (!qaState.configLoaded) {
    setRunError("start", "Configuration is not loaded yet.");
    return;
  }
  if (!qaState.canonicalSessionId && !qaState.sessionId) {
    setRunError("start", "Session is not ready.");
    return;
  }
  if (!qaState.selectedSessionFiles.length) {
    setRunError("start", "Select at least one IFC session file before starting extraction.");
    return;
  }
  const selected = selectedSheets();
  if (!Object.values(selected).some(Boolean)) {
    setRunError("start", "Select at least one output table before starting.");
    return;
  }
  if (qaState.uploadState === "uploading" || qaState.isStartingRun) return;

  const downloadBtn = qs("#qaDownloadBtn");
  if (downloadBtn) downloadBtn.disabled = true;
  const canonicalSessionId = String(qaState.canonicalSessionId || qaState.sessionId || "").trim();
  qaState.isStartingRun = true;
  qaState.activeJobId = "";
  qaState.runStartState = "idle";
  qaState._loggedPollStart = false;
  qaState._loggedFirstStatus = false;

  setUploadState("running", { uploadPercent: 0, uploadBytesLoaded: 0, uploadBytesTotal: 0 });
  const payload = {
    file_ids: Array.from(qaState.selectedSessionFiles),
    file_names: Array.from(qaState.selectedSessionFiles),
    options: { selected_sheets: selected },
    config_override: qaState.qaConfig || {},
  };
  debugLog("IFC QA run request", {
    canonicalSessionId,
    selectedCount: qaState.selectedSessionFiles.length,
    payload,
  });

  try {
    const resp = await fetch(`/api/session/${canonicalSessionId}/ifc-qa/extract`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await resp.json();
    if (!resp.ok) throw { stage: "start", message: data?.detail || data?.error || `HTTP ${resp.status}`, detail: JSON.stringify(data) };
    setRunStartState("starting");
    if (data.success !== true) {
      throw { stage: "start", message: data.error || "Failed to start job", detail: data.detail || "" };
    }
    const jobId = typeof data.job_id === "string" ? data.job_id.trim() : "";
    if (!jobId) {
      throw { stage: "start", message: "Job started but no job_id was returned", detail: JSON.stringify(data) };
    }

    qaState.activeJobId = jobId;
    console.info("IFC QA session extraction started", { sessionId: qaState.sessionId, jobId });
    setRunStartState("running");
    setTimeout(() => pollStatus(jobId), 400);
  } catch (err) {
    const fallbackMessage = err?.stage === "upload" ? "Upload failed" : "Failed to start job";
    setUploadState("failed");
    setRunError(err?.stage || "start", err?.message || fallbackMessage, err?.detail || "");
  } finally {
    qaState.isStartingRun = false;
    renderUploadProgress();
  }
  qaState.isRunning = false;
  renderActionButtons();
}

async function pollStatus(jobId) {
  const normalizedJobId = typeof jobId === "string" ? jobId.trim() : "";
  if (!normalizedJobId) {
    setRunError("poll", "Polling skipped: invalid job_id.");
    return;
  }
  if (normalizedJobId !== qaState.activeJobId) return;

  if (!qaState._loggedPollStart) {
    console.info("IFC QA polling start", { jobId: normalizedJobId });
    qaState._loggedPollStart = true;
  }

  const resp = await fetch(`/api/ifc-qa/status/${normalizedJobId}`);
  if (!resp.ok) {
    let detail = "";
    try {
      const payload = await resp.json();
      detail = payload?.detail || payload?.error || "";
    } catch {
      detail = "";
    }
    setRunError("poll", `Status endpoint returned HTTP ${resp.status}`, detail);
    return;
  }

  const data = await resp.json();
  if (!qaState._loggedFirstStatus) {
    console.info("IFC QA first status response", {
      jobId: normalizedJobId,
      status: data?.status,
      overallPercent: data?.overall_percent ?? data?.percent ?? 0,
    });
    qaState._loggedFirstStatus = true;
  }

  const fill = qs("#qaProgressFill");
  const label = qs("#qaProgressLabel");
  const log = qs("#qaLog");
  const perFile = qs("#qaPerFile");
  const dashboard = qs("#qaDashboardStatus");

  const overallPercent = data.overall_percent ?? data.percent ?? 0;
  if (fill) fill.style.width = `${overallPercent}%`;
  if (label) label.textContent = `${data.currentStep || ""} ${data.currentFile ? `(${data.currentFile})` : ""}`;
  if (log) log.value = (data.logs || []).join("\n");
  if (perFile) {
    perFile.textContent = (data.files || [])
      .map((f) => `${f.name}: ${f.percent || 0}% ${f.stage ? `(${f.stage})` : ""}`)
      .join(" | ");
  }
  if (dashboard) dashboard.textContent = `Status: ${data.status || "unknown"} (${overallPercent}%)`;

  if (data.status === "complete" || data.status === "complete_with_errors") {
    const downloadBtn = qs("#qaDownloadBtn");
    if (downloadBtn) downloadBtn.disabled = false;
    setUploadState("complete", {
      uploadPercent: 100,
      uploadBytesLoaded: qaState.selectedTotalBytes,
      uploadBytesTotal: qaState.selectedTotalBytes,
    });
    qaState.extractionResults = Array.isArray(data.files) ? data.files : [];
    console.info("IFC QA per-file extraction outcomes", qaState.extractionResults);
    renderResultsPanel();
    await loadSessionFiles();
    await refreshSessionSummary();
    return;
  }
  if (data.status === "failed") {
    qaState.extractionResults = Array.isArray(data.files) ? data.files : [];
    renderResultsPanel();
    setRunError("result", "Extraction job failed.", (data.logs || []).slice(-3).join("\n"));
    return;
  }
  qaState.extractionResults = Array.isArray(data.files) ? data.files : qaState.extractionResults;
  renderResultsPanel();
  setTimeout(() => pollStatus(normalizedJobId), 1200);
}

function downloadZip() {
  if (!qaState.activeJobId) return;
  const a = document.createElement("a");
  a.href = `/api/ifc-qa/result/${qaState.activeJobId}`;
  a.click();
}

function renderResultsPanel() {
  const summaryEl = qs("#qaResultsSummary");
  const listEl = qs("#qaResultsList");
  const results = Array.isArray(qaState.extractionResults) ? qaState.extractionResults : [];
  const total = results.length;
  const succeeded = results.filter((row) => row.status === "complete").length;
  const failed = results.filter((row) => row.status === "failed").length;
  if (summaryEl) summaryEl.textContent = total ? `Total selected: ${total} • Succeeded: ${succeeded} • Failed: ${failed}` : "";
  if (!listEl) return;
  if (!total) {
    listEl.innerHTML = "";
    return;
  }
  listEl.innerHTML = results.map((row) => {
    const name = row.source_file || row.name || "-";
    const status = row.status || "unknown";
    const outputs = Object.values(row.outputs || {}).map((value) => String(value).split("/").pop()).filter(Boolean);
    const outputText = outputs.length ? outputs.join(", ") : "-";
    const errorText = row.error ? ` • ${row.error}` : "";
    return `<li><span>${name}</span><span>${status} • ${outputText}${errorText}</span></li>`;
  }).join("");
}

function bindExtractor() {
  renderSheetChecks();
  setUploadState("idle");
  clearRunError();
  renderSessionState();
  renderSelectedFilesState();
  renderSessionFiles();
  renderResultsPanel();
  renderBuildInfo();
  renderDebugState();
  qs("#qaConfigureBtn")?.addEventListener("click", openConfig);
  qs("#qaConfigClose")?.addEventListener("click", closeConfig);
  qs("#qaConfigApply")?.addEventListener("click", applyConfig);
  qs("#qaSelectAllBtn")?.addEventListener("click", () => {
    qaState.selectedSessionFiles = qaState.sessionIfcFiles.map((file) => getSessionFileName(file)).filter(Boolean);
    renderSessionFiles();
    renderSelectedFilesState();
    renderActionButtons();
  });
  qs("#qaClearSelectionBtn")?.addEventListener("click", () => {
    qaState.selectedSessionFiles = [];
    renderSessionFiles();
    renderSelectedFilesState();
    renderActionButtons();
  });
  qs("#qaRefreshSessionFilesBtn")?.addEventListener("click", async () => {
    await loadSessionFiles();
  });
  qs("#qaStartBtn")?.addEventListener("click", startRun);
  qs("#qaDownloadBtn")?.addEventListener("click", () => {
    if (!qaState.sessionId) return;
    const a = document.createElement("a");
    a.href = `/api/ifc-qa/result/${qaState.sessionId}`;
    a.click();
  });
  renderActionButtons();
}

function configTemplate() {
  return `<div class="stack"><div class="section-title"><h3>Current IFC QA Config</h3></div>
  <textarea id="qaConfigStandalone" rows="22" style="width:100%;font-family:monospace">${JSON.stringify(qaState.qaConfig, null, 2)}</textarea></div>`;
}
function dashboardTemplate() {
  return `<div class="stack"><div class="section-title"><h3>IFC QA Dashboard</h3></div><div id="qaDashboardStatus" class="card">No active job.</div></div>`;
}

async function init() {
  console.info("IFC QA bundle loaded", { buildId: window.__IFC_QA_BUILD_ID__ || "unknown" });
  if (!window.location.pathname.startsWith("/ifc-qa/")) return;

  const root = qs("#ifc-qa-root");
  if (!root) {
    console.error("IFC QA mount failed: #ifc-qa-root not found");
    const errorBox = document.createElement("div");
    errorBox.style.margin = "12px";
    errorBox.style.padding = "10px 12px";
    errorBox.style.background = "#fee2e2";
    errorBox.style.border = "1px solid #ef4444";
    errorBox.style.borderRadius = "8px";
    errorBox.style.color = "#991b1b";
    errorBox.textContent = "IFC QA mount failed: #ifc-qa-root not found";
    document.body.appendChild(errorBox);
    return;
  }
  console.info("IFC QA mount target found");
  const page = root.dataset.qaPage || "extractor";

  loadQaConfig();

  if (page === "extractor") {
    root.innerHTML = extractorTemplate();
    await loadBuildInfo();
    await loadUploadLimits();
    root.insertAdjacentHTML("afterbegin", `<div class="muted" style="margin-bottom:8px">IFC QA UI mounted</div>`);
    bindExtractor();
    const sharedUnsub = window.IFCSession?.subscribe?.((sessionId) => {
      const normalized = String(sessionId || "").trim();
      qaState.sessionId = normalized;
      qaState.canonicalSessionId = normalized;
      qaState.sessionReady = !!normalized;
      qaState.sessionStateText = qaState.sessionReady
        ? `Session ready · ${window.IFCSession?.shortSessionId ? window.IFCSession.shortSessionId(normalized) : normalized.slice(0, 8)}`
        : "Session establishing...";
      renderSessionState();
      updateGlobalSessionBadge();
      if (qaState.sessionReady) {
        loadSessionFiles();
      }
      renderDebugState();
    });
    const eventName = window.IFCSession?.sessionChangeEvent || "ifc-toolkit-session-changed";
    const onToolkitSessionChanged = (event) => {
      const normalized = String(event?.detail?.sessionId || "").trim();
      qaState.sessionId = normalized;
      qaState.canonicalSessionId = normalized;
      qaState.sessionReady = !!normalized;
      qaState.sessionStateText = qaState.sessionReady
        ? `Session ready · ${window.IFCSession?.shortSessionId ? window.IFCSession.shortSessionId(normalized) : normalized.slice(0, 8)}`
        : "Session establishing...";
      renderSessionState();
      updateGlobalSessionBadge();
      if (qaState.sessionReady) loadSessionFiles();
      renderDebugState();
      console.info("IFC QA session changed event handled", { sessionId: normalized, eventName });
    };
    window.addEventListener(eventName, onToolkitSessionChanged);
    if (typeof sharedUnsub === "function") {
      window.addEventListener("beforeunload", () => {
        sharedUnsub();
        window.removeEventListener(eventName, onToolkitSessionChanged);
      }, { once: true });
    }
    ensureSession()
      .then(async () => {
        await refreshSessionSummary();
        await loadSessionFiles();
      })
      .catch((err) => {
        console.error("IFC QA session bootstrap failed", err);
        qaState.sessionReady = false;
        qaState.sessionStateText = "Session establishing...";
        setSessionWarning("Failed to establish session. Retry shortly.");
        renderSessionState();
        updateGlobalSessionBadge();
        renderActionButtons();
        renderDebugState();
      });
    await loadQaConfig();
    renderActionButtons();
  } else if (page === "config") {
    await loadQaConfig();
    root.innerHTML = configTemplate();
  } else {
    await loadQaConfig();
    root.innerHTML = dashboardTemplate();
    if (qaState.activeJobId) pollStatus(qaState.activeJobId);
  }
  console.info("IFC QA app mounted");
}

document.addEventListener("DOMContentLoaded", init);
