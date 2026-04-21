const qaState = {
  sessionId: "",
  activeJobId: "",
  isRunning: false,
  uploadState: "idle",
  uploadPercent: 0,
  uploadBytesLoaded: 0,
  uploadBytesTotal: 0,
  uploadError: "",
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
    <input id="qaIfcFiles" type="file" multiple accept=".ifc" ${disabledAttr}/>
    <div id="qaFileQueue" class="qa-file-queue muted"></div>

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
    const existing = new Set(qaState.fileQueue.map((f) => f.name));
    files.forEach((f) => {
      if (existing.has(f.name)) return;
      qaState.fileQueue.push({
        id: `${Date.now()}_${f.name}`,
        file: f,
        name: f.name,
        size: f.size,
        uploadPercent: 0,
        processPercent: 0,
        overallPercent: 0,
        status: "queued",
        stageText: "Queued",
        error: "",
        duplicate: qaState.sessionSourceFiles.includes(f.name),
      });
    });
    renderFileQueue();
    renderSessionSummary();
    renderActionButtons();
  });
}

function renderSheetChecks() {
  const wrap = qs("#qaSheetChecks");
  if (!wrap) return;
  wrap.innerHTML = DEFAULT_SHEETS.map(([k, label]) => `<label><input type="checkbox" data-sheet="${k}" checked /> ${label}</label>`).join("");
}

async function uploadSingleFile(row, mode) {
  const form = new FormData();
  form.append("files", row.file, row.name);
  form.append("session_id", qaState.sessionId);
  form.append("options_json", JSON.stringify({ selected_sheets: selectedSheets() }));
  form.append("config_override_json", JSON.stringify(qaState.qaConfig || {}));
  const endpoint = mode === "add" ? "/api/ifc-qa/add-to-zip" : "/api/ifc-qa/run";

  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", endpoint);
    xhr.responseType = "json";
    xhr.upload.onprogress = (evt) => {
      const total = evt.total || row.size || 1;
      const loaded = evt.loaded || 0;
      const pct = Math.round((loaded / total) * 100);
      row.status = "uploading";
      row.uploadPercent = pct;
      row.overallPercent = Math.round(pct * 0.5);
      row.stageText = `Uploading… ${pct}%`;
      renderFileQueue();
      renderSessionSummary();
    };
    xhr.onerror = () => reject(new Error("Network error"));
    xhr.onload = () => {
      if (xhr.status < 200 || xhr.status >= 300) {
        reject(new Error(xhr.response?.detail || `Upload failed (${xhr.status})`));
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
    renderFileQueue();
    await refreshSessionSummary();
  }
  qaState.isRunning = false;
  renderActionButtons();
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
  try { qaState.qaConfig = JSON.parse(qs("#qaConfigText")?.value || "{}"); closeConfig(); }
  catch { alert("Invalid JSON"); }
}

function bindExtractor() {
  renderSheetChecks();
  bindFilesList();
  renderFileQueue();
  renderSessionSummary();
  renderActionButtons();
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
  const root = qs("#ifc-qa-root");
  if (!root) return;
  const page = root.dataset.qaPage || "extractor";

  await ensureSession();
  await loadQaConfig();

  if (page === "extractor") {
    root.innerHTML = extractorTemplate();
    bindExtractor();
    await refreshSessionSummary();
  } else if (page === "config") {
    root.innerHTML = configTemplate();
  } else {
    root.innerHTML = dashboardTemplate();
  }
}

document.addEventListener("DOMContentLoaded", init);
