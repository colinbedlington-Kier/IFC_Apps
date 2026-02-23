const qaState = {
  jobId: "",
  config: {},
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

function pageTemplate() {
  return `
  <div class="stack">
    <label>IFC files</label>
    <input id="qaIfcFiles" type="file" multiple accept=".ifc" />
    <ul id="qaFileList" class="muted"></ul>

    <label>Outputs</label>
    <div id="qaSheetChecks" class="qa-grid"></div>

    <div class="inline">
      <button class="btn secondary" id="qaConfigureBtn" type="button">Configure</button>
      <button class="btn" id="qaStartBtn" type="button">Start QA Extraction</button>
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

function renderSheetChecks() {
  const wrap = qs("#qaSheetChecks");
  wrap.innerHTML = DEFAULT_SHEETS.map(([k, label]) => `<label><input type="checkbox" data-sheet="${k}" checked /> ${label}</label>`).join("");
}

function selectedSheets() {
  const out = {};
  DEFAULT_SHEETS.forEach(([k]) => {
    out[k] = !!qs(`[data-sheet="${k}"]`)?.checked;
  });
  return out;
}

async function loadDefaultConfig() {
  const resp = await fetch('/api/ifc-qa/config/default');
  if (!resp.ok) return;
  qaState.config = await resp.json();
}

function bindFilesList() {
  qs('#qaIfcFiles').addEventListener('change', () => {
    const names = Array.from(qs('#qaIfcFiles').files || []).map((f) => `<li>${f.name}</li>`).join('');
    qs('#qaFileList').innerHTML = names;
  });
}

function openConfig() {
  qs('#qaConfigText').value = JSON.stringify(qaState.config, null, 2);
  qs('#qaConfigModal').hidden = false;
}

function closeConfig() { qs('#qaConfigModal').hidden = true; }

function applyConfig() {
  try { qaState.config = JSON.parse(qs('#qaConfigText').value || '{}'); closeConfig(); }
  catch { alert('Invalid JSON'); }
}

function exportConfig() {
  const blob = new Blob([JSON.stringify(qaState.config, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'ifc_qa_config.json';
  a.click();
  URL.revokeObjectURL(url);
}

function importConfig(ev) {
  const file = ev.target.files?.[0];
  if (!file) return;
  file.text().then((txt) => {
    try {
      qaState.config = JSON.parse(txt);
      qs('#qaConfigText').value = JSON.stringify(qaState.config, null, 2);
    } catch { alert('Invalid JSON'); }
  });
}

async function startRun() {
  const files = Array.from(qs('#qaIfcFiles').files || []);
  if (!files.length) return alert('Please select IFC files');
  qs('#qaDownloadBtn').disabled = true;
  const form = new FormData();
  files.forEach((f) => form.append('files', f, f.name));
  form.append('options_json', JSON.stringify({ selected_sheets: selectedSheets() }));
  form.append('config_override_json', JSON.stringify(qaState.config || {}));
  const resp = await fetch('/api/ifc-qa/run', { method: 'POST', body: form });
  if (!resp.ok) return alert('Failed to start job');
  const data = await resp.json();
  qaState.jobId = data.job_id;
  pollStatus();
}

async function pollStatus() {
  if (!qaState.jobId) return;
  const resp = await fetch(`/api/ifc-qa/status/${qaState.jobId}`);
  if (!resp.ok) return;
  const data = await resp.json();
  qs('#qaProgressFill').style.width = `${data.percent || 0}%`;
  qs('#qaProgressLabel').textContent = `${data.currentStep || ''} ${data.currentFile ? `(${data.currentFile})` : ''}`;
  qs('#qaLog').value = (data.logs || []).join('\n');
  const pf = (data.files || []).map((f) => `${f.name}: ${f.percent || 0}%`).join(' | ');
  qs('#qaPerFile').textContent = pf;
  if (data.status === 'complete') {
    qs('#qaDownloadBtn').disabled = false;
    return;
  }
  if (data.status !== 'failed') setTimeout(pollStatus, 1200);
}

function downloadZip() {
  if (!qaState.jobId) return;
  const a = document.createElement('a');
  a.href = `/api/ifc-qa/result/${qaState.jobId}`;
  a.click();
}

async function init() {
  const root = qs('#ifc-qa-root');
  if (!root) return;
  root.innerHTML = pageTemplate();
  renderSheetChecks();
  await loadDefaultConfig();
  bindFilesList();
  qs('#qaConfigureBtn').addEventListener('click', openConfig);
  qs('#qaConfigClose').addEventListener('click', closeConfig);
  qs('#qaConfigApply').addEventListener('click', applyConfig);
  qs('#qaConfigExport').addEventListener('click', exportConfig);
  qs('#qaConfigImport').addEventListener('change', importConfig);
  qs('#qaStartBtn').addEventListener('click', startRun);
  qs('#qaDownloadBtn').addEventListener('click', downloadZip);
}

document.addEventListener('DOMContentLoaded', init);
