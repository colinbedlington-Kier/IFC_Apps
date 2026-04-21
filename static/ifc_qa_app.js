const qaState = {
  sessionId: "",
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
};

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

async function ensureSession() {
  const existing = localStorage.getItem("ifc_session_id") || "";
  try {
    const resp = await fetch("/api/session", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ session_id: existing }),
    });
    if (!resp.ok) throw new Error("session error");
    const data = await resp.json();
    qaState.sessionId = data.session_id || existing;
    if (qaState.sessionId) localStorage.setItem("ifc_session_id", qaState.sessionId);
  } catch (e) {
    console.warn("IFC QA session unavailable", e);
  }
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

function warningBanner() {
  if (!qaState.warning) return "";
  return `<div class="card" style="border-left:4px solid #f59e0b;background:#fff8e6">${qaState.warning}</div>`;
}

function extractorTemplate() {
  const disabledAttr = qaState.configLoaded ? "" : "disabled";
  return `
  ${warningBanner()}
  <div class="stack">
    <label>IFC files</label>
    <input id="qaIfcFiles" type="file" multiple accept=".ifc" />
    <ul id="qaFileList" class="qa-file-list muted"></ul>
    <div id="qaRunError" class="qa-error-banner" hidden></div>
    <div class="qa-upload-progress">
      <div class="qa-upload-progress-top">
        <strong id="qaUploadStatusText">Waiting for files…</strong>
        <span id="qaUploadPercentText">0%</span>
      </div>
      <div id="qaUploadProgressTrack" class="qa-upload-progress-track indeterminate">
        <div id="qaUploadProgressBar" class="qa-upload-progress-bar" style="width:0%"></div>
      </div>
      <div id="qaUploadBytesText" class="muted">0 B / 0 B</div>
    </div>

    <label>Outputs</label>
    <div id="qaSheetChecks" class="qa-grid"></div>

    <div class="inline">
      <button class="btn secondary" id="qaConfigureBtn" type="button" ${disabledAttr}>Configure</button>
      <button class="btn" id="qaStartBtn" type="button" ${disabledAttr}>Start QA Extraction</button>
      <button class="btn secondary" id="qaDownloadBtn" type="button" disabled>Download ZIP</button>
    </div>

    <div class="progress-track"><div id="qaProgressFill" class="progress-fill" style="width:0%"></div></div>
    <div id="qaProgressLabel" class="muted"></div>
    <div id="qaPerFile" class="muted"></div>
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

function configTemplate() {
  return `
  ${warningBanner()}
  <div class="stack">
    <div class="section-title"><h3>Current IFC QA Config</h3></div>
    <textarea id="qaConfigStandalone" rows="22" style="width:100%;font-family:monospace">${JSON.stringify(qaState.qaConfig, null, 2)}</textarea>
    <div class="inline">
      <button class="btn secondary" id="qaConfigExportStandalone" type="button">Export JSON</button>
    </div>
  </div>`;
}

function dashboardTemplate() {
  return `
  ${warningBanner()}
  <div class="stack">
    <div class="section-title"><h3>IFC QA Dashboard</h3></div>
    <p class="muted">Run an extraction from the extractor page to populate progress and downloadable results.</p>
    <div id="qaDashboardStatus" class="card">No active job.</div>
  </div>`;
}

function renderSheetChecks() {
  const wrap = qs("#qaSheetChecks");
  if (!wrap) return;
  wrap.innerHTML = DEFAULT_SHEETS.map(([k, label]) => `<label><input type="checkbox" data-sheet="${k}" checked /> ${label}</label>`).join("");
}

function selectedSheets() {
  const out = {};
  DEFAULT_SHEETS.forEach(([k]) => {
    out[k] = !!qs(`[data-sheet="${k}"]`)?.checked;
  });
  return out;
}

function bindFilesList() {
  const input = qs("#qaIfcFiles");
  if (!input) return;
  input.addEventListener("change", () => {
    const files = Array.from(input.files || []);
    const names = files.map((f) => `<li><span>${f.name}</span><span>${formatBytes(f.size)}</span></li>`).join("");
    const list = qs("#qaFileList");
    if (list) list.innerHTML = names || "<li>No files selected.</li>";
    clearRunError();
    if (!files.length) {
      setUploadState("idle");
      return;
    }
    setUploadState("ready");
  });
}

function setUploadControlsDisabled(disabled) {
  const startBtn = qs("#qaStartBtn");
  const fileInput = qs("#qaIfcFiles");
  if (startBtn) startBtn.disabled = disabled;
  if (fileInput) fileInput.disabled = disabled;
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
  renderUploadProgress();
}

function setRunStartState(nextState) {
  qaState.runStartState = nextState;
  const status = qs("#qaUploadStatusText");
  if (!status) return;
  if (nextState === "starting") status.textContent = "Starting job…";
  if (nextState === "running") status.textContent = "Processing…";
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
  const statusMap = {
    idle: "Waiting for files…",
    ready: "Ready to upload",
    uploading: "Uploading...",
    uploaded: "Upload complete",
    failed: "Upload failed",
  };

  if (status) status.textContent = statusMap[qaState.uploadState] || "Waiting for files…";
  if (pct) pct.textContent = `${percent}%`;
  if (bar) bar.style.width = `${percent}%`;
  if (bytes) bytes.textContent = total > 0 ? `${formatBytes(loaded)} / ${formatBytes(total)}` : `${formatBytes(loaded)} uploaded`;
  if (track) track.classList.toggle("indeterminate", isUploading && total <= 0);
  setUploadControlsDisabled(isUploading || qaState.isStartingRun);
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
        reject({
          stage: "start",
          message: payload?.error || payload?.detail || `Request failed with status ${xhr.status}`,
          detail: payload?.detail || "",
        });
        return;
      }
      resolve(payload);
    };
    xhr.send(form);
  });
}

function openConfig() {
  const txt = qs("#qaConfigText");
  const modal = qs("#qaConfigModal");
  if (!txt || !modal) return;
  txt.value = JSON.stringify(qaState.qaConfig, null, 2);
  modal.hidden = false;
}
function closeConfig() { const modal = qs("#qaConfigModal"); if (modal) modal.hidden = true; }
function applyConfig() {
  try {
    qaState.qaConfig = JSON.parse(qs("#qaConfigText")?.value || "{}");
    closeConfig();
  } catch {
    alert("Invalid JSON");
  }
}
function exportConfig(config = qaState.qaConfig) {
  const blob = new Blob([JSON.stringify(config, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "ifc_qa_config.json";
  a.click();
  URL.revokeObjectURL(url);
}
function importConfig(ev) {
  const file = ev.target.files?.[0];
  if (!file) return;
  file.text().then((txt) => {
    try {
      qaState.qaConfig = JSON.parse(txt);
      const editor = qs("#qaConfigText");
      if (editor) editor.value = JSON.stringify(qaState.qaConfig, null, 2);
    } catch {
      alert("Invalid JSON");
    }
  });
}

async function startRun() {
  clearRunError();
  if (!qaState.configLoaded) {
    setRunError("start", "Configuration is not loaded yet.");
    return;
  }
  const input = qs("#qaIfcFiles");
  const files = Array.from(input?.files || []);
  if (!files.length) {
    setRunError("upload", "Please select at least one IFC file.");
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
  qaState.isStartingRun = true;
  qaState.activeJobId = "";
  qaState.runStartState = "idle";
  qaState._loggedPollStart = false;
  qaState._loggedFirstStatus = false;

  setUploadState("uploading", {
    uploadPercent: 0,
    uploadBytesLoaded: 0,
    uploadBytesTotal: 0,
  });

  const form = new FormData();
  console.info("IFC QA run request", {
    fileCount: files.length,
    fileNames: files.map((f) => f.name),
    selectedOutputs: Object.entries(selected).filter(([, enabled]) => enabled).map(([key]) => key),
    configLoaded: qaState.configLoaded,
  });
  files.forEach((f) => form.append("files", f, f.name));
  form.append("options_json", JSON.stringify({ selected_sheets: selected, session_id: qaState.sessionId || "" }));
  form.append("config_override_json", JSON.stringify(qaState.qaConfig || {}));

  try {
    const data = await startQaUpload(form, files.length);
    setUploadState("uploaded", {
      uploadPercent: 100,
      uploadBytesLoaded: qaState.uploadBytesTotal || qaState.uploadBytesLoaded,
      uploadBytesTotal: qaState.uploadBytesTotal || qaState.uploadBytesLoaded,
    });
    setRunStartState("starting");

    if (!data || typeof data !== "object") {
      throw { stage: "start", message: "Invalid start-job response", detail: "Expected JSON object payload." };
    }
    if (data.success !== true) {
      throw { stage: "start", message: data.error || "Failed to start job", detail: data.detail || "" };
    }
    const jobId = typeof data.job_id === "string" ? data.job_id.trim() : "";
    if (!jobId) {
      throw { stage: "start", message: "Job started but no job_id was returned", detail: JSON.stringify(data) };
    }

    qaState.activeJobId = jobId;
    console.info("IFC QA job started", { jobId });
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

  if (data.status === "complete") {
    const downloadBtn = qs("#qaDownloadBtn");
    if (downloadBtn) downloadBtn.disabled = false;
    return;
  }
  if (data.status === "failed") {
    setRunError("result", "Extraction job failed.", (data.logs || []).slice(-3).join("\n"));
    return;
  }
  setTimeout(() => pollStatus(normalizedJobId), 1200);
}

function downloadZip() {
  if (!qaState.activeJobId) return;
  const a = document.createElement("a");
  a.href = `/api/ifc-qa/result/${qaState.activeJobId}`;
  a.click();
}

function bindExtractor() {
  renderSheetChecks();
  bindFilesList();
  setUploadState("idle");
  clearRunError();
  qs("#qaConfigureBtn")?.addEventListener("click", openConfig);
  qs("#qaConfigClose")?.addEventListener("click", closeConfig);
  qs("#qaConfigApply")?.addEventListener("click", applyConfig);
  qs("#qaConfigExport")?.addEventListener("click", () => exportConfig());
  qs("#qaConfigImport")?.addEventListener("change", importConfig);
  qs("#qaStartBtn")?.addEventListener("click", startRun);
  qs("#qaDownloadBtn")?.addEventListener("click", downloadZip);
}

function bindConfigPage() {
  qs("#qaConfigExportStandalone")?.addEventListener("click", () => {
    const text = qs("#qaConfigStandalone")?.value || "{}";
    try {
      exportConfig(JSON.parse(text));
    } catch {
      alert("Invalid JSON in viewer");
    }
  });
}

async function init() {
  const root = qs("#ifc-qa-root");
  if (!root) return;
  const page = root.dataset.qaPage || "extractor";

  if (page === "extractor") root.innerHTML = extractorTemplate();
  if (page === "config") root.innerHTML = configTemplate();
  if (page === "dashboard") root.innerHTML = dashboardTemplate();

  await ensureSession();
  await loadQaConfig();

  if (page === "extractor") {
    root.innerHTML = extractorTemplate();
    bindExtractor();
  } else if (page === "config") {
    root.innerHTML = configTemplate();
    bindConfigPage();
  } else if (page === "dashboard") {
    root.innerHTML = dashboardTemplate();
    if (qaState.activeJobId) pollStatus(qaState.activeJobId);
  }
}

document.addEventListener("DOMContentLoaded", init);
