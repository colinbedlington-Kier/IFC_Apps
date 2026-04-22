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
  fileQueue: [],
  lastUpdated: "",
  modelCount: 0,
  hasZip: false,
  sessionSourceFiles: [],
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

async function ensureSession() {
  const existing = localStorage.getItem("ifc_session_id") || "";
  const resp = await fetch("/api/session", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ session_id: existing }),
  });
  if (!resp.ok) throw new Error("session error");
  const data = await resp.json();
  qaState.sessionId = data.session_id || existing;
  if (qaState.sessionId) localStorage.setItem("ifc_session_id", qaState.sessionId);
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
  } catch (_) {}
  renderSessionSummary();
  renderActionButtons();
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
      <button class="btn secondary" id="qaAddBtn" type="button" disabled>Add to ZIP</button>
      <button class="btn secondary" id="qaDownloadBtn" type="button" disabled>Download ZIP</button>
    </div>

    <div id="qaSessionSummary" class="muted"></div>

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

function statusLabel(status) {
  return ({ queued: "Queued", uploading: "Uploading", uploaded: "Uploaded", processing: "Processing", complete: "Complete", failed: "Failed" }[status] || "Queued");
}

function renderFileQueue() {
  const wrap = qs("#qaFileQueue");
  if (!wrap) return;
  if (!qaState.fileQueue.length) {
    wrap.innerHTML = "<div class='muted'>No files selected.</div>";
    return;
  }
  wrap.innerHTML = qaState.fileQueue.map((row) => {
    const pct = Math.max(0, Math.min(100, row.overallPercent || 0));
    const dup = row.duplicate ? `<div class='muted' style='color:#b45309'>Duplicate filename detected. Existing outputs will be replaced.</div>` : "";
    const err = row.error ? `<div class='muted' style='color:#b91c1c'>${row.error}</div>` : "";
    return `
      <div class="card" style="padding:10px;margin-bottom:8px">
        <div style="display:flex;justify-content:space-between;gap:10px"><strong>${row.name}</strong><span>${formatBytes(row.size)}</span></div>
        <div class="muted" style="margin:4px 0">${statusLabel(row.status)} — ${row.stageText || "Queued"}</div>
        <div class="progress-track" style="height:8px"><div class="progress-fill" style="width:${pct}%"></div></div>
        ${dup}
        ${err}
      </div>`;
  }).join("");
}

function renderSessionSummary() {
  const el = qs("#qaSessionSummary");
  if (!el) return;
  const pending = qaState.fileQueue.filter((row) => ["queued", "uploading", "uploaded", "processing", "failed"].includes(row.status)).length;
  el.textContent = `Session ZIP contains: ${qaState.modelCount} models | Last updated: ${qaState.lastUpdated || "-"} | Pending selected files: ${pending}`;
}

function renderActionButtons() {
  const startBtn = qs("#qaStartBtn");
  const addBtn = qs("#qaAddBtn");
  const dlBtn = qs("#qaDownloadBtn");
  const hasQueued = qaState.fileQueue.some((row) => row.status === "queued" || row.status === "failed");
  if (startBtn) startBtn.disabled = qaState.isRunning || !hasQueued || qaState.modelCount > 0;
  if (addBtn) addBtn.disabled = qaState.isRunning || !hasQueued || qaState.modelCount === 0;
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
        if (xhr.status >= 500) {
          const hint = payload?.hint ? ` (${payload.hint})` : "";
          showErrorToast(`IFC QA start failed: ${payload?.error || "server error"}${hint}`);
        }
        reject({
          stage: "start",
          message: payload?.error || payload?.detail || `Request failed with status ${xhr.status}`,
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
  qs("#qaStartBtn")?.addEventListener("click", () => runQueue("run"));
  qs("#qaAddBtn")?.addEventListener("click", () => runQueue("add"));
  qs("#qaDownloadBtn")?.addEventListener("click", () => {
    if (!qaState.sessionId) return;
    const a = document.createElement("a");
    a.href = `/api/ifc-qa/result/${qaState.sessionId}`;
    a.click();
  });
}

function configTemplate() {
  return `<div class="stack"><div class="section-title"><h3>Current IFC QA Config</h3></div>
  <textarea id="qaConfigStandalone" rows="22" style="width:100%;font-family:monospace">${JSON.stringify(qaState.qaConfig, null, 2)}</textarea></div>`;
}
function dashboardTemplate() {
  return `<div class="stack"><div class="section-title"><h3>IFC QA Dashboard</h3></div><div id="qaDashboardStatus" class="card">No active job.</div></div>`;
}

async function init() {
  console.info("IFC QA bundle loaded");
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

  try {
    await ensureSession();
  } catch (err) {
    console.error("IFC QA session bootstrap failed", err);
    qaState.warning = "Failed to bootstrap IFC QA session. Reload and try again.";
  }
  await loadQaConfig();

  if (page === "extractor") {
    root.innerHTML = extractorTemplate();
    root.insertAdjacentHTML("afterbegin", `<div class="muted" style="margin-bottom:8px">IFC QA UI mounted</div>`);
    bindExtractor();
    await refreshSessionSummary();
  } else if (page === "config") {
    root.innerHTML = configTemplate();
  } else {
    root.innerHTML = dashboardTemplate();
    if (qaState.activeJobId) pollStatus(qaState.activeJobId);
  }
  console.info("IFC QA app mounted");
}

document.addEventListener("DOMContentLoaded", init);
