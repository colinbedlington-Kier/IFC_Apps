const qaState = {
  sessionId: "",
  jobId: "",
  qaConfig: {
    shortCodes: {},
    layers: {},
    entityTypes: {},
    systemCategory: {},
    psetTemplate: {},
  },
  warning: "",
  configReady: false,
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
    qaState.configReady = true;
  } catch (err) {
    console.warn("Failed to load IFC QA default config", err);
    qaState.warning = "IFC QA config failed to load. Extraction is disabled until config is available.";
    qaState.configReady = false;
  }
}

function warningBanner() {
  if (!qaState.warning) return "";
  return `<div class="card" style="border-left:4px solid #dc2626;background:#fef2f2">${qaState.warning}</div>`;
}

function extractorTemplate() {
  return `
  ${warningBanner()}
  <div class="stack">
    <label>IFC files</label>
    <input id="qaIfcFiles" type="file" multiple accept=".ifc" />
    <ul id="qaFileList" class="muted"></ul>

    <label>Outputs</label>
    <div id="qaSheetChecks" class="qa-grid"></div>

    <div class="inline">
      <button class="btn secondary" id="qaConfigureBtn" type="button">Configure</button>
      <button class="btn" id="qaStartBtn" type="button" ${qaState.configReady ? "" : "disabled"}>Start QA Extraction</button>
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
    const names = Array.from(input.files || []).map((f) => `<li>${f.name}</li>`).join("");
    const list = qs("#qaFileList");
    if (list) list.innerHTML = names;
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
async function applyConfig() {
  try {
    const edited = JSON.parse(qs("#qaConfigText")?.value || "{}");
    const validation = await fetch("/api/ifc-qa/config/validate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(edited),
    });
    const result = await validation.json();
    if (!result.valid) {
      alert(`Invalid config: ${(result.errors || []).join("; ")}`);
      return;
    }
    const mergedResp = await fetch("/api/ifc-qa/config/merge", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(edited),
    });
    const mergedData = await mergedResp.json();
    if (!mergedData.valid) {
      alert(`Merge failed: ${(mergedData.errors || []).join("; ")}`);
      return;
    }
    qaState.qaConfig = normalizeConfig(mergedData.config || edited);
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
async function importConfig(ev) {
  const file = ev.target.files?.[0];
  if (!file) return;
  file.text().then(async (txt) => {
    try {
      const parsed = JSON.parse(txt);
      const mergedResp = await fetch("/api/ifc-qa/config/merge", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(parsed),
      });
      const mergedData = await mergedResp.json();
      if (!mergedData.valid) {
        alert(`Invalid override: ${(mergedData.errors || []).join("; ")}`);
        return;
      }
      qaState.qaConfig = normalizeConfig(mergedData.config || parsed);
      const editor = qs("#qaConfigText");
      if (editor) editor.value = JSON.stringify(qaState.qaConfig, null, 2);
    } catch {
      alert("Invalid JSON");
    }
  });
}

async function startRun() {
  const input = qs("#qaIfcFiles");
  const files = Array.from(input?.files || []);
  if (!qaState.configReady) return alert("IFC QA config unavailable. Cannot start extraction.");
  if (!files.length) return alert("Please select IFC files");

  const downloadBtn = qs("#qaDownloadBtn");
  if (downloadBtn) downloadBtn.disabled = true;

  const form = new FormData();
  files.forEach((f) => form.append("files", f, f.name));
  form.append("options_json", JSON.stringify({ selected_sheets: selectedSheets() }));
  form.append("config_override_json", JSON.stringify(qaState.qaConfig || {}));

  const resp = await fetch("/api/ifc-qa/run", { method: "POST", body: form });
  if (!resp.ok) return alert("Failed to start job");
  const data = await resp.json();
  qaState.jobId = data.job_id;
  pollStatus();
}

async function pollStatus() {
  if (!qaState.jobId) return;
  const resp = await fetch(`/api/ifc-qa/status/${qaState.jobId}`);
  if (!resp.ok) return;
  const data = await resp.json();

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
  if (data.status !== "failed") setTimeout(pollStatus, 1200);
}

function downloadZip() {
  if (!qaState.jobId) return;
  const a = document.createElement("a");
  a.href = `/api/ifc-qa/result/${qaState.jobId}`;
  a.click();
}

function bindExtractor() {
  renderSheetChecks();
  bindFilesList();
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

  // Re-render with possible warning/config updates.
  if (page === "extractor") {
    root.innerHTML = extractorTemplate();
    bindExtractor();
  } else if (page === "config") {
    root.innerHTML = configTemplate();
    bindConfigPage();
  } else if (page === "dashboard") {
    root.innerHTML = dashboardTemplate();
    if (qaState.jobId) pollStatus();
  }
}

document.addEventListener("DOMContentLoaded", init);
