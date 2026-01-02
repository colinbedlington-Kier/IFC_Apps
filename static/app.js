const state = {
  files: [],
  sessionId: null,
};

const endpoints = {
  session: '/api/session',
  upload: '/api/upload',
  files: '/api/files',
  clean: '/api/clean',
  extract: '/api/extract',
  update: '/api/update',
  storeys: '/api/storeys',
  global: '/api/global-z',
  proxy: '/api/proxy-map',
  levelsList: '/api/levels/list',
  levelsAdd: '/api/levels/add',
  levelsMove: '/api/levels/move',
  levelsDelete: '/api/levels/delete',
  close: '/api/session/close',
};

function humanSize(bytes) {
  if (!bytes && bytes !== 0) return '';
  const units = ['B', 'KB', 'MB', 'GB'];
  let size = bytes;
  let i = 0;
  while (size >= 1024 && i < units.length - 1) {
    size /= 1024;
    i += 1;
  }
  return `${size.toFixed(size >= 10 ? 0 : 1)} ${units[i]}`;
}

function option(label, value) {
  const opt = document.createElement('option');
  opt.value = value;
  opt.textContent = label;
  return opt;
}

async function fetchJSON(url, options = {}) {
  const resp = await fetch(url, {
    headers: { 'Content-Type': 'application/json' },
    credentials: 'include',
    ...options,
  });
  if (!resp.ok) {
    const detail = await resp.json().catch(() => ({}));
    throw new Error(detail.detail || `Request failed: ${resp.status}`);
  }
  return resp.json();
}

function setActiveNav() {
  const page = document.body.dataset.page;
  document.querySelectorAll('.nav-link').forEach((link) => {
    const active = link.dataset.page === page;
    link.classList.toggle('active', active);
  });
}

async function initSession() {
  const data = await fetchJSON(endpoints.session);
  state.sessionId = data.session_id;
  state.files = data.files || [];
  const pill = document.getElementById('session-pill');
  if (pill) pill.textContent = `Session ${state.sessionId.slice(0, 6)}…`;
  renderFiles();
  populateSelects();
}

async function refreshFiles() {
  const data = await fetchJSON(endpoints.files);
  state.files = data.files || [];
  renderFiles();
  populateSelects();
}

function renderFiles() {
  const list = document.getElementById('file-list');
  if (!list) return;
  list.innerHTML = '';
  if (!state.files.length) {
    list.innerHTML = '<p class="hint">No files uploaded yet.</p>';
    return;
  }
  state.files.forEach((file) => {
    const pill = document.createElement('div');
    pill.className = 'file-pill';
    const left = document.createElement('div');
    left.innerHTML = `<strong>${file.name}</strong><br><span class="hint">${humanSize(file.size)}</span>`;
    const link = document.createElement('a');
    link.href = `/api/files/${file.id}`;
    link.textContent = 'Download';
    link.className = 'ghost';
    pill.append(left, link);
    list.appendChild(pill);
  });
}

function populateSelects() {
  const ifcSelects = ['clean-files', 'extract-ifc', 'update-ifc', 'global-ifc', 'proxy-ifc', 'levels-ifc'];
  const excelSelects = ['update-excel'];
  ifcSelects.forEach((id) => {
    const sel = document.getElementById(id);
    if (!sel) return;
    const wasValue = sel.value;
    sel.innerHTML = '';
    state.files
      .filter((f) => f.name.toLowerCase().endsWith('.ifc') || f.name.toLowerCase().endsWith('.ifczip'))
      .forEach((f) => {
        sel.appendChild(option(f.name, f.id));
      });
    if (wasValue) sel.value = wasValue;
  });
  excelSelects.forEach((id) => {
    const sel = document.getElementById(id);
    if (!sel) return;
    const wasValue = sel.value;
    sel.innerHTML = '';
    state.files.filter((f) => f.name.toLowerCase().endsWith('.xlsx')).forEach((f) => sel.appendChild(option(f.name, f.id)));
    if (wasValue) sel.value = wasValue;
  });
  populateCleanerMulti();
}

function populateCleanerMulti() {
  const select = document.getElementById('clean-files');
  if (!select) return;
  select.innerHTML = '';
  state.files
    .filter((f) => f.name.toLowerCase().endsWith('.ifc') || f.name.toLowerCase().endsWith('.ifczip'))
    .forEach((f) => {
      select.appendChild(option(f.name, f.id));
    });
}

async function uploadFiles(event) {
  event?.preventDefault();
  const input = document.getElementById('file-input');
  if (!input || !input.files.length) return;
  const formData = new FormData();
  Array.from(input.files).forEach((file) => formData.append('files', file));
  const resp = await fetch(endpoints.upload, { method: 'POST', body: formData, credentials: 'include' });
  if (!resp.ok) {
    alert('Upload failed');
    return;
  }
  input.value = '';
  await refreshFiles();
}

function renderDownloads(containerId, files) {
  const container = document.getElementById(containerId);
  if (!container) return;
  container.innerHTML = '';
  if (!files || !files.length) return;
  files.forEach((f) => {
    const link = document.createElement('a');
    link.href = `/api/files/${f.id}`;
    link.textContent = `Download ${f.name}`;
    container.appendChild(link);
  });
}

function appendText(targetId, text) {
  const el = document.getElementById(targetId);
  if (el) el.textContent = text || '';
}

async function runCleaner(event) {
  event.preventDefault();
  const select = document.getElementById('clean-files');
  if (!select) return;
  const files = Array.from(select.selectedOptions).map((o) => o.value);
  if (!files.length) {
    alert('Select at least one IFC file');
    return;
  }
  const payload = {
    file_ids: files,
    prefix: document.getElementById('prefix').value || 'InfoDrainage',
    case_insensitive: document.getElementById('case-insensitive').checked,
    delete_psets_with_prefix: document.getElementById('pset-delete').checked,
    delete_properties_in_other_psets: document.getElementById('prop-delete').checked,
    drop_empty_psets: document.getElementById('drop-empty').checked,
    also_remove_loose_props: document.getElementById('loose').checked,
  };
  const data = await fetchJSON(endpoints.clean, { method: 'POST', body: JSON.stringify(payload) });
  const lines = [];
  data.reports.forEach((r) => {
    lines.push(`=== ${r.input} -> ${r.output} ===`);
    lines.push(`Status: ${r.status}`);
    lines.push(`Prefix: ${r.prefix}`);
    lines.push(`Case-insensitive: ${r.case_insensitive}`);
    lines.push('Removed:');
    Object.entries(r.removed || {}).forEach(([k, v]) => lines.push(`  - ${k}: ${v}`));
    if (r.notes && r.notes.length) {
      lines.push('Notes:');
      r.notes.forEach((n) => lines.push(`  * ${n}`));
    }
    lines.push('');
  });
  appendText('clean-log', lines.join('\n'));
  renderDownloads('clean-downloads', data.outputs);
  await refreshFiles();
}

async function runExtract(event) {
  event.preventDefault();
  const fileId = document.getElementById('extract-ifc').value;
  if (!fileId) return alert('Choose an IFC file');
  const data = await fetchJSON(endpoints.extract, { method: 'POST', body: JSON.stringify({ file_id: fileId }) });
  renderDownloads('extract-output', [data.file]);
  await refreshFiles();
}

async function runUpdate(event) {
  event.preventDefault();
  const ifc = document.getElementById('update-ifc').value;
  const excel = document.getElementById('update-excel').value;
  if (!ifc || !excel) return alert('Select both IFC and Excel files');
  const payload = {
    ifc_file_id: ifc,
    excel_file_id: excel,
    update_mode: document.getElementById('update-mode').value,
    add_new: document.getElementById('add-new').value,
  };
  const data = await fetchJSON(endpoints.update, { method: 'POST', body: JSON.stringify(payload) });
  renderDownloads('update-output', [data.file]);
  await refreshFiles();
}

async function loadStoreys(ifcId, selectId = 'storey') {
  const sel = document.getElementById(selectId);
  if (!ifcId || !sel) return;
  const data = await fetchJSON(endpoints.storeys, { method: 'POST', body: JSON.stringify({ file_id: ifcId }) });
  sel.innerHTML = '';
  (data.storeys || []).forEach((s) => sel.appendChild(option(s.label, s.id)));
}

async function runGlobal(event) {
  event.preventDefault();
  const ifcSel = document.getElementById('global-ifc');
  const storeySel = document.getElementById('storey');
  if (!ifcSel || !storeySel) return;
  const ifc = ifcSel.value;
  const storey = storeySel.value;
  if (!ifc || !storey) return alert('Select an IFC and storey');
  const payload = {
    ifc_file_id: ifc,
    storey_id: Number(storey),
    units_code: document.getElementById('units').value,
    gross: document.getElementById('gross').value || null,
    net: document.getElementById('net').value || null,
    mom: document.getElementById('mom').value || null,
    mirror: document.getElementById('mirror').checked,
    target_z: document.getElementById('target-z').value || null,
    countershift: document.getElementById('countershift').checked,
    crs_mode: document.getElementById('crs-mode').checked,
    update_all_mcs: document.getElementById('all-mcs').checked,
    show_diag: document.getElementById('diag').checked,
    crs_set_storey_elev: document.getElementById('storey-elev').checked,
  };
  const data = await fetchJSON(endpoints.global, { method: 'POST', body: JSON.stringify(payload) });
  appendText('global-log', data.summary);
  renderDownloads('global-download', [data.file]);
  await refreshFiles();
}

async function runProxy(event) {
  event.preventDefault();
  const ifcSel = document.getElementById('proxy-ifc');
  if (!ifcSel) return;
  const ifc = ifcSel.value;
  if (!ifc) return alert('Select an IFC file');
  const data = await fetchJSON(endpoints.proxy, { method: 'POST', body: JSON.stringify({ file_id: ifc }) });
  appendText('proxy-log', data.summary);
  renderDownloads('proxy-download', [data.file]);
  await refreshFiles();
}

function renderLevels(levels) {
  const container = document.getElementById('levels-table');
  const selects = ['move-source', 'move-target', 'delete-source', 'delete-target'];
  if (container) {
    if (!levels || !levels.length) {
      container.textContent = 'No levels found.';
    } else {
      const lines = levels.map((l) => `${l.label} • GUID=${l.guid} • elements=${l.element_count}`);
      container.textContent = lines.join('\n');
    }
  }
  selects.forEach((id) => {
    const sel = document.getElementById(id);
    if (!sel) return;
    const previous = sel.value;
    sel.innerHTML = '';
    (levels || []).forEach((l) => sel.appendChild(option(l.label, l.id)));
    if (previous) sel.value = previous;
  });
}

async function refreshLevelsList() {
  const ifcSel = document.getElementById('levels-ifc');
  if (!ifcSel || !ifcSel.value) return;
  const data = await fetchJSON(endpoints.levelsList, { method: 'POST', body: JSON.stringify({ file_id: ifcSel.value }) });
  renderLevels(data.levels);
}

async function addLevel(event) {
  event.preventDefault();
  const ifcSel = document.getElementById('levels-ifc');
  if (!ifcSel || !ifcSel.value) return alert('Select an IFC file');
  const payload = {
    ifc_file_id: ifcSel.value,
    name: document.getElementById('level-name').value,
    elevation: document.getElementById('level-elev').value || null,
    units_code: document.getElementById('level-units').value,
  };
  const data = await fetchJSON(endpoints.levelsAdd, { method: 'POST', body: JSON.stringify(payload) });
  renderDownloads('level-add-download', [data.file]);
  renderLevels(data.levels);
  await refreshFiles();
}

async function moveLevelElements(event) {
  event.preventDefault();
  const ifcSel = document.getElementById('levels-ifc');
  if (!ifcSel || !ifcSel.value) return alert('Select an IFC file');
  const payload = {
    ifc_file_id: ifcSel.value,
    source_storey_id: Number(document.getElementById('move-source').value),
    target_storey_id: Number(document.getElementById('move-target').value),
  };
  const data = await fetchJSON(endpoints.levelsMove, { method: 'POST', body: JSON.stringify(payload) });
  renderDownloads('level-move-download', [data.file]);
  appendText('level-move-log', `Moved ${data.moved_count} elements.`);
  renderLevels(data.levels);
  await refreshFiles();
}

async function deleteLevel(event) {
  event.preventDefault();
  const ifcSel = document.getElementById('levels-ifc');
  if (!ifcSel || !ifcSel.value) return alert('Select an IFC file');
  const payload = {
    ifc_file_id: ifcSel.value,
    delete_storey_id: Number(document.getElementById('delete-source').value),
    target_storey_id: Number(document.getElementById('delete-target').value),
  };
  const data = await fetchJSON(endpoints.levelsDelete, { method: 'POST', body: JSON.stringify(payload) });
  renderDownloads('level-delete-download', [data.file]);
  renderLevels(data.levels);
  await refreshFiles();
}

function setupForms() {
  const uploadForm = document.getElementById('upload-form');
  if (uploadForm) uploadForm.addEventListener('submit', uploadFiles);
  const refreshBtn = document.getElementById('refresh-files');
  if (refreshBtn) refreshBtn.addEventListener('click', refreshFiles);

  const cleanForm = document.getElementById('clean-form');
  if (cleanForm) cleanForm.addEventListener('submit', runCleaner);

  const extractForm = document.getElementById('extract-form');
  if (extractForm) extractForm.addEventListener('submit', runExtract);
  const updateForm = document.getElementById('update-form');
  if (updateForm) updateForm.addEventListener('submit', runUpdate);

  const globalForm = document.getElementById('global-form');
  if (globalForm) globalForm.addEventListener('submit', runGlobal);
  const globalIfc = document.getElementById('global-ifc');
  if (globalIfc) globalIfc.addEventListener('change', (e) => loadStoreys(e.target.value));

  const proxyForm = document.getElementById('proxy-form');
  if (proxyForm) proxyForm.addEventListener('submit', runProxy);

  const levelsRefresh = document.getElementById('levels-refresh');
  if (levelsRefresh) levelsRefresh.addEventListener('click', refreshLevelsList);
  const levelsIfc = document.getElementById('levels-ifc');
  if (levelsIfc) levelsIfc.addEventListener('change', refreshLevelsList);
  const levelAddForm = document.getElementById('level-add-form');
  if (levelAddForm) levelAddForm.addEventListener('submit', addLevel);
  const levelMoveForm = document.getElementById('level-move-form');
  if (levelMoveForm) levelMoveForm.addEventListener('submit', moveLevelElements);
  const levelDeleteForm = document.getElementById('level-delete-form');
  if (levelDeleteForm) levelDeleteForm.addEventListener('submit', deleteLevel);
}

function setupSessionCleanup() {
  window.addEventListener('beforeunload', () => {
    const blob = new Blob([], { type: 'application/json' });
    navigator.sendBeacon(endpoints.close, blob);
  });
}

async function boot() {
  setActiveNav();
  setupForms();
  setupSessionCleanup();
  await initSession();
  const levelsPage = document.body.dataset.page === 'levels';
  if (levelsPage) await refreshLevelsList();
}

document.addEventListener('DOMContentLoaded', boot);
