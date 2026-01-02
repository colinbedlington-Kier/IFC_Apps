const state = {
  sessionId: null,
  files: [],
  levels: [],
  selectedLevelId: null,
  selectedFiles: new Set(),
};

const el = (id) => document.getElementById(id);

function setSessionBadge(text, ok = true) {
  const badge = document.querySelector("[data-session-badge]");
  if (!badge) return;
  badge.textContent = text;
  badge.classList.toggle("danger-text", !ok);
  badge.classList.toggle("success-text", ok);
}

async function ensureSession() {
  try {
    const existing = localStorage.getItem("ifc_session_id");
    const resp = await fetch("/api/session", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ session_id: existing }),
    });
    const data = await resp.json();
    state.sessionId = data.session_id;
    localStorage.setItem("ifc_session_id", state.sessionId);
    setSessionBadge(`Session ${state.sessionId.slice(0, 8)}…`, true);
    await refreshFiles();
  } catch (err) {
    setSessionBadge("Session error", false);
    console.error(err);
  }
}

async function refreshFiles() {
  if (!state.sessionId) return;
  try {
    const resp = await fetch(`/api/session/${state.sessionId}/files`);
    if (!resp.ok) throw new Error("Could not list files");
    const data = await resp.json();
    state.files = data.files || [];
    renderFilesLists();
    populateFileSelects();
  } catch (err) {
    console.error(err);
  }
}

function renderFilesLists() {
  document.querySelectorAll("[data-files-list]").forEach((ul) => {
    ul.innerHTML = "";
    state.files.forEach((f) => {
      const li = document.createElement("li");
      const checked = state.selectedFiles.has(f.name) ? "checked" : "";
      li.innerHTML = `
        <label class="file-row">
          <input type="checkbox" class="file-checkbox" value="${f.name}" ${checked}>
          <span class="file-name">${f.name}</span>
          <span class="muted">${(f.size / 1024).toFixed(1)} KB</span>
        </label>
        <button class="btn secondary sm download-file" data-file="${f.name}">Download</button>
      `;
      ul.appendChild(li);
    });
  });
  document.querySelectorAll(".file-checkbox").forEach((cb) => {
    cb.addEventListener("change", (e) => {
      const name = e.target.value;
      if (e.target.checked) {
        state.selectedFiles.add(name);
      } else {
        state.selectedFiles.delete(name);
      }
    });
  });
  document.querySelectorAll(".download-file").forEach((btn) => {
    btn.addEventListener("click", () => downloadFile(btn.dataset.file));
  });
}

function populateFileSelects() {
  document.querySelectorAll("[data-files-select]").forEach((sel) => {
    sel.innerHTML = "";
    state.files.forEach((f) => {
      const opt = document.createElement("option");
      opt.value = f.name;
      opt.textContent = f.name;
      sel.appendChild(opt);
    });
  });
}

async function uploadFiles() {
  const input = el("fileInput");
  if (!input || !input.files.length) {
    alert("Choose file(s) to upload.");
    return;
  }
  const form = new FormData();
  for (const f of input.files) {
    form.append("files", f);
  }
  const resp = await fetch(`/api/session/${state.sessionId}/upload`, { method: "POST", body: form });
  if (!resp.ok) {
    alert("Upload failed");
    return;
  }
  input.value = "";
  await refreshFiles();
}

async function endSession() {
  if (!state.sessionId) return;
  await fetch(`/api/session/${state.sessionId}`, { method: "DELETE" });
  localStorage.removeItem("ifc_session_id");
  state.sessionId = null;
  state.files = [];
  state.selectedFiles = new Set();
  renderFilesLists();
  populateFileSelects();
  setSessionBadge("Session ended. Reload to start a new one.", false);
}

function getSelectedMultiple(selectEl) {
  return Array.from(selectEl.selectedOptions || []).map((o) => o.value);
}

async function runCleaner() {
  const select = el("cleanerFiles");
  if (!select) return;
  const files = getSelectedMultiple(select);
  if (!files.length) return alert("Select file(s) to clean.");
  const payload = {
    files,
    prefix: (el("prefix")?.value || "InfoDrainage").trim(),
    case_insensitive: el("caseInsensitive")?.checked ?? true,
    delete_psets_with_prefix: el("deletePsets")?.checked ?? true,
    delete_properties_in_other_psets: el("deleteProps")?.checked ?? true,
    drop_empty_psets: el("dropEmpty")?.checked ?? true,
    also_remove_loose_props: el("looseProps")?.checked ?? true,
  };
  const resp = await fetch(`/api/session/${state.sessionId}/clean`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await resp.json();
  if (data.reports) {
    const text = data.reports
      .map((r) => `=== ${r.input} → ${r.output}\nstatus: ${r.status}\nremoved: ${JSON.stringify(r.removed, null, 2)}`)
      .join("\n\n");
    el("cleanerStatus").textContent = text;
    await refreshFiles();
  } else {
    el("cleanerStatus").textContent = JSON.stringify(data);
  }
}

async function extractExcel() {
  const file = el("excelIfc")?.value;
  if (!file) return alert("Select an IFC file.");
  const payload = { ifc_file: file };
  const resp = await fetch(`/api/session/${state.sessionId}/excel/extract`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await resp.json();
  if (data.excel) {
    el("excelStatus").textContent = `Excel ready: ${data.excel.name}`;
    await refreshFiles();
  } else {
    el("excelStatus").textContent = JSON.stringify(data);
  }
}

async function applyExcel() {
  const ifcFile = el("excelIfcUpdate")?.value;
  const xlsFile = el("excelFileUpdate")?.value;
  if (!ifcFile || !xlsFile) return alert("Select IFC and Excel files.");
  const payload = {
    ifc_file: ifcFile,
    excel_file: xlsFile,
    update_mode: document.querySelector('input[name="updateMode"]:checked')?.value || "update",
    add_new: document.querySelector('input[name="addNew"]:checked')?.value || "no",
  };
  const resp = await fetch(`/api/session/${state.sessionId}/excel/update`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await resp.json();
  if (data.ifc) {
    el("excelStatus").textContent = `Updated IFC: ${data.ifc.name}`;
    await refreshFiles();
  } else {
    el("excelStatus").textContent = JSON.stringify(data);
  }
}

async function parseStoreyInfo() {
  const file = el("storeyIfc")?.value;
  if (!file) return alert("Select an IFC file.");
  const payload = { ifc_file: file };
  const resp = await fetch(`/api/session/${state.sessionId}/storeys/parse`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await resp.json();
  if (el("storeyMeta")) {
    el("storeyMeta").textContent = `${data.summary || ""} (MapConversions: ${data.map_conversions || 0})`;
  }
  const select = el("storeySelect");
  if (select) {
    select.innerHTML = "";
    (data.storeys || []).forEach((s) => {
      const opt = document.createElement("option");
      opt.value = s.id;
      opt.textContent = s.label;
      select.appendChild(opt);
    });
  }
}

async function applyStoreyChanges() {
  const file = el("storeyIfc")?.value;
  if (!file) return alert("Select IFC file.");
  const storeyId = el("storeySelect")?.value;
  if (!storeyId) return alert("Choose a storey.");
  const payload = {
    ifc_file: file,
    storey_id: Number(storeyId),
    units: el("unitsSelect")?.value || "m",
    gross: el("grossHeight")?.value ? Number(el("grossHeight").value) : null,
    net: el("netHeight")?.value ? Number(el("netHeight").value) : null,
    mom: el("mom")?.value || null,
    mirror: el("mirrorQto")?.checked ?? false,
    target_z: el("targetZ")?.value ? Number(el("targetZ").value) : null,
    countershift_geometry: el("countershift")?.checked ?? true,
    use_crs_mode: el("useCRS")?.checked ?? true,
    update_all_mcs: el("allMC")?.checked ?? true,
    show_diag: el("diag")?.checked ?? true,
    crs_set_storey_elev: el("crsElev")?.checked ?? true,
  };
  const resp = await fetch(`/api/session/${state.sessionId}/storeys/apply`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await resp.json();
  if (data.summary && el("storeyStatus")) {
    el("storeyStatus").textContent = data.summary;
    await refreshFiles();
  } else if (el("storeyStatus")) {
    el("storeyStatus").textContent = JSON.stringify(data);
  }
}

async function runProxyMapper() {
  const file = el("proxyIfc")?.value;
  if (!file) return alert("Select IFC file.");
  const payload = { ifc_file: file };
  const resp = await fetch(`/api/session/${state.sessionId}/proxy`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await resp.json();
  if (data.summary && el("proxyStatus")) {
    el("proxyStatus").textContent = data.summary;
    await refreshFiles();
  } else if (el("proxyStatus")) {
    el("proxyStatus").textContent = JSON.stringify(data);
  }
}

async function downloadFile(name) {
  if (!name) return;
  const url = `/api/session/${state.sessionId}/download?name=${encodeURIComponent(name)}`;
  try {
    const resp = await fetch(url);
    if (!resp.ok) throw new Error("Download failed");
    const blob = await resp.blob();
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = name;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(link.href);
  } catch (e) {
    alert(`Could not download ${name}`);
  }
}

async function downloadSelected() {
  if (!state.selectedFiles.size) return alert("Select at least one file to download.");
  for (const name of state.selectedFiles) {
    // eslint-disable-next-line no-await-in-loop
    await downloadFile(name);
  }
}

// ------------------------------
// Level Manager
// ------------------------------

function renderLevels() {
  const container = el("levelsTable");
  if (!container) return;
  if (!state.levels.length) {
    container.innerHTML = "<div class=\"muted\">No levels loaded.</div>";
    return;
  }
  const rows = state.levels
    .map(
      (lvl) => `
      <tr>
        <td><input type="radio" name="levelSelect" value="${lvl.id}" ${state.selectedLevelId === lvl.id ? "checked" : ""}></td>
        <td>${lvl.name || "(unnamed)"}</td>
        <td>${lvl.description || ""}</td>
        <td>${lvl.elevation ?? ""}</td>
        <td>${lvl.comp_height ?? ""}</td>
        <td>${lvl.object_count}</td>
      </tr>`
    )
    .join("");
  container.innerHTML = `
    <table>
      <thead>
        <tr><th></th><th>Name</th><th>Description</th><th>Elevation</th><th>Comp height</th><th>Objects</th></tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>`;
  container.querySelectorAll('input[name="levelSelect"]').forEach((radio) => {
    radio.addEventListener("change", () => {
      state.selectedLevelId = Number(radio.value);
      fillLevelForms();
    });
  });
}

function fillLevelForms() {
  const lvl = state.levels.find((l) => l.id === state.selectedLevelId);
  if (!lvl) return;
  if (el("levelName")) el("levelName").value = lvl.name || "";
  if (el("levelDescription")) el("levelDescription").value = lvl.description || "";
  if (el("levelElevation")) el("levelElevation").value = lvl.elevation ?? "";
  if (el("levelCompHeight")) el("levelCompHeight").value = lvl.comp_height ?? "";
  renderDeleteObjects(lvl);
  renderDeleteTargets();
}

function renderDeleteTargets() {
  const sel = el("deleteTarget");
  if (!sel) return;
  sel.innerHTML = "";
  state.levels
    .filter((l) => l.id !== state.selectedLevelId)
    .forEach((l) => {
      const opt = document.createElement("option");
      opt.value = l.id;
      opt.textContent = l.name || `(ID ${l.id})`;
      sel.appendChild(opt);
    });
}

function renderDeleteObjects(level) {
  const wrap = el("deleteObjects");
  if (!wrap) return;
  wrap.innerHTML = "";
  (level.objects || []).forEach((o) => {
    const id = `delobj-${o.id}`;
    const label = document.createElement("label");
    label.innerHTML = `<input type="checkbox" id="${id}" value="${o.id}" checked> ${o.name || o.type || o.id} (${o.type})`;
    wrap.appendChild(label);
  });
}

function renderAddObjects() {
  const wrap = el("addLevelObjects");
  if (!wrap) return;
  wrap.innerHTML = "";
  const allObjs = state.levels.flatMap((l) => l.objects || []);
  allObjs.forEach((o) => {
    const id = `addobj-${o.id}`;
    const label = document.createElement("label");
    label.innerHTML = `<input type="checkbox" id="${id}" value="${o.id}"> ${o.name || o.type || o.id} (${o.type})`;
    wrap.appendChild(label);
  });
}

async function loadLevels() {
  const file = el("levelsIfc")?.value;
  if (!file) return alert("Select an IFC file.");
  const resp = await fetch(`/api/session/${state.sessionId}/levels/list`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ifc_file: file }),
  });
  const data = await resp.json();
  state.levels = data.levels || [];
  state.selectedLevelId = state.levels[0]?.id ?? null;
  if (el("levelsMeta")) el("levelsMeta").textContent = `${state.levels.length} level(s) loaded`;
  renderLevels();
  renderAddObjects();
  fillLevelForms();
}

function collectChecked(containerId) {
  const wrap = el(containerId);
  if (!wrap) return [];
  return Array.from(wrap.querySelectorAll("input[type='checkbox']:checked")).map((c) => Number(c.value));
}

async function updateLevelRequest() {
  const file = el("levelsIfc")?.value;
  if (!file || !state.selectedLevelId) return alert("Choose an IFC file and level.");
  const payload = {
    ifc_file: file,
    storey_id: state.selectedLevelId,
    name: el("levelName")?.value,
    description: el("levelDescription")?.value,
    elevation: el("levelElevation")?.value ? Number(el("levelElevation").value) : null,
    comp_height: el("levelCompHeight")?.value ? Number(el("levelCompHeight").value) : null,
  };
  const resp = await fetch(`/api/session/${state.sessionId}/levels/update`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await resp.json();
  if (el("levelUpdateStatus")) el("levelUpdateStatus").textContent = data.ifc ? `Updated: ${data.ifc.name}` : JSON.stringify(data);
  await refreshFiles();
}

async function deleteLevelRequest() {
  const file = el("levelsIfc")?.value;
  if (!file || !state.selectedLevelId) return alert("Choose an IFC file and level.");
  const target = el("deleteTarget")?.value;
  if (!target) return alert("Choose a target level.");
  const object_ids = collectChecked("deleteObjects");
  const payload = {
    ifc_file: file,
    storey_id: state.selectedLevelId,
    target_storey_id: Number(target),
    object_ids,
  };
  const resp = await fetch(`/api/session/${state.sessionId}/levels/delete`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await resp.json();
  if (el("levelDeleteStatus")) el("levelDeleteStatus").textContent = data.ifc ? `Deleted & saved: ${data.ifc.name}` : JSON.stringify(data);
  await refreshFiles();
}

async function addLevelRequest() {
  const file = el("levelsIfc")?.value;
  if (!file) return alert("Choose an IFC file.");
  const payload = {
    ifc_file: file,
    name: el("newLevelName")?.value,
    description: el("newLevelDescription")?.value,
    elevation: el("newLevelElevation")?.value ? Number(el("newLevelElevation").value) : null,
    comp_height: el("newLevelCompHeight")?.value ? Number(el("newLevelCompHeight").value) : null,
    object_ids: collectChecked("addLevelObjects"),
  };
  if (!payload.name) return alert("Provide a name for the new level.");
  const resp = await fetch(`/api/session/${state.sessionId}/levels/add`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await resp.json();
  if (el("levelAddStatus")) el("levelAddStatus").textContent = data.ifc ? `Added: ${data.ifc.name}` : JSON.stringify(data);
  await refreshFiles();
}

function wireEvents() {
  const uploadBtn = el("uploadBtn");
  if (uploadBtn) uploadBtn.addEventListener("click", uploadFiles);

  const refreshBtn = el("refreshFiles");
  if (refreshBtn) refreshBtn.addEventListener("click", refreshFiles);

  const resetBtn = el("resetSession");
  if (resetBtn) resetBtn.addEventListener("click", endSession);

  const cleanerBtn = el("runCleaner");
  if (cleanerBtn) cleanerBtn.addEventListener("click", runCleaner);

  const extractBtn = el("extractExcel");
  if (extractBtn) extractBtn.addEventListener("click", extractExcel);

  const applyExcelBtn = el("applyExcel");
  if (applyExcelBtn) applyExcelBtn.addEventListener("click", applyExcel);

  const parseStoreysBtn = el("parseStoreys");
  if (parseStoreysBtn) parseStoreysBtn.addEventListener("click", parseStoreyInfo);

  const applyStoreysBtn = el("applyStoreys");
  if (applyStoreysBtn) applyStoreysBtn.addEventListener("click", applyStoreyChanges);

  const proxyBtn = el("runProxy");
  if (proxyBtn) proxyBtn.addEventListener("click", runProxyMapper);

  const loadLevelsBtn = el("loadLevels");
  if (loadLevelsBtn) loadLevelsBtn.addEventListener("click", loadLevels);

  const updateLevelBtn = el("updateLevel");
  if (updateLevelBtn) updateLevelBtn.addEventListener("click", updateLevelRequest);

  const deleteLevelBtn = el("deleteLevel");
  if (deleteLevelBtn) deleteLevelBtn.addEventListener("click", deleteLevelRequest);

  const addLevelBtn = el("addLevel");
  if (addLevelBtn) addLevelBtn.addEventListener("click", addLevelRequest);

  document.querySelectorAll("[data-download-selected]").forEach((btn) => {
    btn.addEventListener("click", downloadSelected);
  });
}

document.addEventListener("DOMContentLoaded", async () => {
  await ensureSession();
  wireEvents();
});
