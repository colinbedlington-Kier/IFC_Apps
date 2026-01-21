const state = {
  sessionId: null,
  files: [],
  levels: [],
  selectedLevelId: null,
  selectedFiles: new Set(),
  uploadStatusEl: null,
  uploadProgressEl: null,
  step2ifcJobId: null,
  pendingActions: [],
};

const el = (id) => document.getElementById(id);

function setSessionBadge(text, ok = true) {
  const badges = [
    document.querySelector("[data-session-badge]"),
    el("sessionStatus"),
    el("session-pill"),
  ].filter(Boolean);
  badges.forEach((badge) => {
    badge.textContent = text;
    badge.classList.toggle("danger-text", !ok);
    badge.classList.toggle("success-text", ok);
  });
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
    if (state.uploadStatusEl) state.uploadStatusEl.textContent = "Session ready.";
  } catch (err) {
    setSessionBadge("Session error", false);
    if (state.uploadStatusEl) state.uploadStatusEl.textContent = "Session error. Reload to retry.";
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
  const renderTargets = [
    ...document.querySelectorAll("[data-files-list]"),
    el("filesList"),
  ].filter(Boolean);

  renderTargets.forEach((ul) => {
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
  const simpleList = el("file-list");
  if (simpleList) {
    simpleList.innerHTML = "";
    if (!state.files.length) {
      simpleList.innerHTML = '<div class="muted">No files uploaded yet.</div>';
    } else {
      state.files.forEach((f) => {
        const item = document.createElement("div");
        item.className = "file-pill";
        item.innerHTML = `<div><div class="file-name">${f.name}</div><div class="muted">${(f.size / 1024).toFixed(1)} KB</div></div>`;
        const btn = document.createElement("button");
        btn.className = "ghost";
        btn.textContent = "Download";
        btn.addEventListener("click", () => downloadFile(f.name));
        item.appendChild(btn);
        simpleList.appendChild(item);
      });
    }
  }

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

function resetUploadProgress() {
  const wrap = document.querySelector("[data-upload-progress-wrap]");
  const bar = document.querySelector("[data-upload-progress]");
  const pct = document.querySelector("[data-upload-percent]");
  const status = document.querySelector("[data-upload-status]");
  if (wrap) wrap.classList.remove("visible", "done", "error");
  if (bar) bar.style.width = "0%";
  if (pct) pct.textContent = "";
  if (status) status.textContent = "Waiting to start…";
}

function updateUploadProgress({ percent, message, done = false, error = false }) {
  const wrap = document.querySelector("[data-upload-progress-wrap]");
  const bar = document.querySelector("[data-upload-progress]");
  const pct = document.querySelector("[data-upload-percent]");
  const status = document.querySelector("[data-upload-status]");
  if (!wrap || !bar || !status || !pct) return;
  wrap.classList.add("visible");
  wrap.classList.toggle("done", done);
  wrap.classList.toggle("error", error);
  if (typeof percent === "number" && Number.isFinite(percent)) {
    const clamped = Math.max(0, Math.min(100, Math.round(percent)));
    bar.style.width = `${clamped}%`;
    pct.textContent = `${clamped}%`;
    bar.setAttribute("aria-valuenow", String(clamped));
  } else if (!percent) {
    pct.textContent = "";
  }
  status.textContent = message || "";
}

function updateStep2ifcProgress({ percent, message, done = false, error = false }) {
  const wrap = document.querySelector("[data-step2ifc-progress-wrap]");
  const bar = document.querySelector("[data-step2ifc-progress]");
  const pct = document.querySelector("[data-step2ifc-progress-percent]");
  const status = document.querySelector("[data-step2ifc-progress-status]");
  if (!wrap || !bar || !status || !pct) return;
  wrap.classList.add("visible");
  wrap.classList.toggle("done", done);
  wrap.classList.toggle("error", error);
  if (typeof percent === "number" && Number.isFinite(percent)) {
    const clamped = Math.max(0, Math.min(100, Math.round(percent)));
    bar.style.width = `${clamped}%`;
    pct.textContent = `${clamped}%`;
    bar.setAttribute("aria-valuenow", String(clamped));
  } else if (!percent) {
    pct.textContent = "";
  }
  status.textContent = message || "";
}

function renderStep2ifcOutputs(outputs) {
  const container = el("step2ifcOutputs");
  if (!container) return;
  if (!outputs || !outputs.length) {
    container.textContent = "No outputs yet.";
    return;
  }
  container.innerHTML = "";
  outputs.forEach((output) => {
    const row = document.createElement("div");
    row.className = "file-row";
    row.innerHTML = `<span class="file-name">${output.name}</span>`;
    const btn = document.createElement("button");
    btn.className = "btn secondary sm";
    btn.textContent = "Download";
    btn.addEventListener("click", () => downloadFile(output.name));
    row.appendChild(btn);
    container.appendChild(row);
  });
}

async function pollStep2ifc(jobId) {
  if (!state.sessionId || !jobId) return;
  try {
    const resp = await fetch(`/api/session/${state.sessionId}/step2ifc/auto/${jobId}`);
    if (!resp.ok) throw new Error("Failed to read conversion status");
    const data = await resp.json();
    updateStep2ifcProgress({
      percent: data.progress,
      message: data.message,
      done: data.done,
      error: data.error,
    });
    if (data.outputs) {
      renderStep2ifcOutputs(data.outputs);
    }
    if (data.done) {
      await refreshFiles();
      const runBtn = el("step2ifcRun");
      if (runBtn) runBtn.disabled = false;
      return;
    }
  } catch (err) {
    updateStep2ifcProgress({ message: "Failed to fetch status", error: true });
    console.error(err);
    const runBtn = el("step2ifcRun");
    if (runBtn) runBtn.disabled = false;
    return;
  }
  setTimeout(() => pollStep2ifc(jobId), 1200);
}

async function runStep2ifcAuto() {
  const fileSelect = el("step2ifcFiles");
  if (!fileSelect || !fileSelect.value) {
    alert("Choose a STEP file to convert.");
    return;
  }
  if (!state.sessionId) {
    alert("Session not ready yet. Please wait and retry.");
    return;
  }
  const runBtn = el("step2ifcRun");
  if (runBtn) runBtn.disabled = true;
  renderStep2ifcOutputs([]);
  updateStep2ifcProgress({ percent: 0, message: "Submitting conversion request…" });
  const outputName = el("step2ifcOutputName")?.value?.trim();
  try {
    const resp = await fetch(`/api/session/${state.sessionId}/step2ifc/auto`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        input_file: fileSelect.value,
        output_name: outputName || null,
      }),
    });
    if (!resp.ok) {
      let detail = "Unable to start auto conversion";
      try {
        const errorData = await resp.json();
        if (errorData?.detail) detail = errorData.detail;
      } catch (err) {
        // ignore parsing errors
      }
      throw new Error(detail);
    }
    const data = await resp.json();
    state.step2ifcJobId = data.job_id;
    updateStep2ifcProgress({ percent: 5, message: "Auto conversion started…" });
    pollStep2ifc(state.step2ifcJobId);
  } catch (err) {
    console.error(err);
    updateStep2ifcProgress({ message: err.message || "Failed to start conversion", error: true });
    if (runBtn) runBtn.disabled = false;
  }
}

function uploadWithProgress(url, form) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", url);
    xhr.upload.onprogress = (evt) => {
      if (evt.lengthComputable) {
        const pct = (evt.loaded / evt.total) * 100;
        updateUploadProgress({ percent: pct, message: "Uploading files…" });
      } else {
        updateUploadProgress({ message: "Uploading files…" });
      }
    };
    xhr.onerror = () => reject(new Error("Network error during upload"));
    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        try {
          const data = xhr.responseText ? JSON.parse(xhr.responseText) : null;
          resolve(data);
        } catch (err) {
          resolve(null);
        }
      } else {
        reject(new Error(`Upload failed (${xhr.status})`));
      }
    };
    xhr.send(form);
  });
}

async function uploadFiles() {
  const input = el("fileInput") || el("file-input");
  if (!input || !input.files.length) {
    alert("Choose file(s) to upload.");
    return;
  }
  if (!state.sessionId) {
    alert("Session not ready yet. Please wait a moment and retry.");
    return;
  }
  if (state.uploadStatusEl) state.uploadStatusEl.textContent = "Uploading…";
  if (state.uploadProgressEl) state.uploadProgressEl.classList.remove("hidden");
  const form = new FormData();
  for (const f of input.files) {
    form.append("files", f);
  }
  const resp = await fetch(`/api/session/${state.sessionId}/upload`, { method: "POST", body: form });
  if (!resp.ok) {
    if (state.uploadStatusEl) state.uploadStatusEl.textContent = "Upload failed. Try again.";
    alert("Upload failed");
    if (state.uploadProgressEl) state.uploadProgressEl.classList.add("hidden");
    return;
  }
  input.value = "";
  await refreshFiles();
  if (state.uploadStatusEl) state.uploadStatusEl.textContent = "Upload complete.";
  if (state.uploadProgressEl) state.uploadProgressEl.classList.add("hidden");
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
  state.pendingActions = [];
  renderPendingChanges();
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
function renderPendingChanges() {
  const wrap = el("pendingList");
  const status = el("pendingStatus");
  if (status) status.textContent = "";
  if (!wrap) return;
  wrap.innerHTML = "";
  if (!state.pendingActions.length) {
    wrap.innerHTML = '<div class="muted">No pending changes queued.</div>';
    return;
  }
  state.pendingActions.forEach((act, idx) => {
    const div = document.createElement("div");
    div.className = "pending-item";
    const meta = document.createElement("div");
    meta.innerHTML = `<div><strong>${act.type}</strong> — ${act.label || ""}</div><div class="meta">${act.summary || ""}</div>`;
    const remove = document.createElement("button");
    remove.className = "ghost sm";
    remove.textContent = "Remove";
    remove.addEventListener("click", () => {
      state.pendingActions.splice(idx, 1);
      renderPendingChanges();
    });
    div.appendChild(meta);
    div.appendChild(remove);
    wrap.appendChild(div);
  });
}

function queueAction(action) {
  state.pendingActions.push(action);
  renderPendingChanges();
}

async function applyPendingChanges() {
  if (!state.pendingActions.length) {
    alert("No pending changes to apply.");
    return;
  }
  const file = el("levelsIfc")?.value;
  if (!file) return alert("Select an IFC file.");
  const status = el("pendingStatus");
  if (status) status.textContent = "Writing IFC with queued changes…";
  const payload = {
    ifc_file: file,
    actions: state.pendingActions.map((a) => {
      const { label, summary, ...rest } = a;
      return rest;
    }),
  };
  const resp = await fetch(`/api/session/${state.sessionId}/levels/batch`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await resp.json();
  if (resp.ok) {
    if (status) status.textContent = data.ifc ? `Wrote: ${data.ifc.name}` : "Saved.";
    state.pendingActions = [];
    renderPendingChanges();
    await refreshFiles();
    if (data.ifc?.name && el("levelsIfc")) {
      el("levelsIfc").value = data.ifc.name;
    }
    await loadLevels(true);
  } else {
    if (status) status.textContent = data.detail || "Failed to write IFC.";
  }
}

function groupObjectsByType(objects) {
  const byType = new Map();
  (objects || []).forEach((o) => {
    const key = o.type || "Other";
    if (!byType.has(key)) byType.set(key, []);
    byType.get(key).push(o);
  });
  return byType;
}

function renderGroupedObjects(containerId, objects, { checked = false, prefix = "obj" } = {}) {
  const wrap = el(containerId);
  if (!wrap) return;
  wrap.innerHTML = "";
  if (!objects || !objects.length) {
    wrap.innerHTML = '<div class="muted">No objects found for this level.</div>';
    return;
  }

  const byType = groupObjectsByType(objects);
  Array.from(byType.keys())
    .sort()
    .forEach((type) => {
      const items = byType.get(type).slice().sort((a, b) => (a.name || "").localeCompare(b.name || ""));
      const details = document.createElement("details");
      details.open = true;
      const summary = document.createElement("summary");
      summary.textContent = `${type} (${items.length})`;
      details.appendChild(summary);

      const inner = document.createElement("div");
      inner.className = "checkbox-grid";

      items.forEach((o) => {
        const id = `${prefix}-${containerId}-${o.id}`;
        const label = document.createElement("label");
        label.className = "checkbox";
        label.innerHTML = `<input type="checkbox" id="${id}" value="${o.id}" ${checked ? "checked" : ""}> ${o.name || o.id} (${o.type})`;
        inner.appendChild(label);
      });

      details.appendChild(inner);
      wrap.appendChild(details);
    });
}

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
        <td>${lvl.global_id || ""}</td>
        <td>${lvl.cobie_floor || ""}</td>
      </tr>`
    )
    .join("");
  container.innerHTML = `
    <table>
      <thead>
        <tr><th></th><th>Name</th><th>Description</th><th>Elevation</th><th>Comp height</th><th>Objects</th><th>GlobalId</th><th>COBie Floors</th></tr>
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
  if (el("levelGlobalId")) el("levelGlobalId").value = lvl.global_id || "";
  if (el("levelCobie")) el("levelCobie").value = lvl.cobie_floor || "";
  renderDeleteObjects(lvl);
  renderDeleteTargets();
  renderReassignControls();
  renderPendingChanges();
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
  renderGroupedObjects("deleteObjects", level.objects, { checked: true, prefix: "delete" });
}

function renderAddObjects() {
  const wrap = el("addLevelObjects");
  if (!wrap) return;
  wrap.innerHTML = "";
  const allObjs = state.levels.flatMap((l) => l.objects || []);
  renderGroupedObjects("addLevelObjects", allObjs, { checked: false, prefix: "add" });
}

function renderReassignControls() {
  const sourceSel = el("reassignSource");
  const targetSel = el("reassignTarget");
  if (!sourceSel || !targetSel) return;

  const currentSource = Number(sourceSel.value) || state.selectedLevelId || state.levels[0]?.id;
  sourceSel.innerHTML = "";
  state.levels.forEach((l) => {
    const opt = document.createElement("option");
    opt.value = l.id;
    opt.textContent = l.name || `(ID ${l.id})`;
    opt.selected = l.id === currentSource;
    sourceSel.appendChild(opt);
  });

  const chosenSource = Number(sourceSel.value);
  targetSel.innerHTML = "";
  state.levels
    .filter((l) => l.id !== chosenSource)
    .forEach((l) => {
      const opt = document.createElement("option");
      opt.value = l.id;
      opt.textContent = l.name || `(ID ${l.id})`;
      targetSel.appendChild(opt);
    });

  renderReassignObjects();
}

function renderReassignObjects() {
  const sourceSel = el("reassignSource");
  const sourceId = Number(sourceSel?.value);
  const level = state.levels.find((l) => l.id === sourceId);
  renderGroupedObjects("reassignObjects", level?.objects || [], { checked: false, prefix: "reassign" });
}

async function loadLevels(silent = false) {
  const file = el("levelsIfc")?.value;
  if (!file) {
    if (!silent) alert("Select an IFC file.");
    return;
  }
  const prevSelected = state.selectedLevelId;
  const resp = await fetch(`/api/session/${state.sessionId}/levels/list`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ifc_file: file }),
  });
  const data = await resp.json();
  state.levels = data.levels || [];
  const stillExists = state.levels.some((l) => l.id === prevSelected);
  state.selectedLevelId = stillExists ? prevSelected : state.levels[0]?.id ?? null;
  if (el("levelsMeta")) el("levelsMeta").textContent = `${state.levels.length} level(s) loaded`;
  renderLevels();
  renderAddObjects();
  fillLevelForms();
  renderReassignControls();
}

function collectChecked(containerId) {
  const wrap = el(containerId);
  if (!wrap) return [];
  return Array.from(wrap.querySelectorAll("input[type='checkbox']:checked")).map((c) => Number(c.value));
}

async function updateLevelRequest() {
  const file = el("levelsIfc")?.value;
  if (!file || !state.selectedLevelId) return alert("Choose an IFC file and level.");
  const action = {
    type: "update",
    storey_id: state.selectedLevelId,
    payload: {
      name: el("levelName")?.value,
      description: el("levelDescription")?.value,
      elevation: el("levelElevation")?.value ? Number(el("levelElevation").value) : null,
      comp_height: el("levelCompHeight")?.value ? Number(el("levelCompHeight").value) : null,
      global_id: el("levelGlobalId")?.value || null,
      cobie_floor: el("levelCobie")?.value || null,
    },
    label: `Update ${el("levelName")?.value || state.selectedLevelId}`,
    summary: `GlobalId: ${el("levelGlobalId")?.value || "unchanged"}, COBie: ${el("levelCobie")?.value || "unchanged"}`,
  };
  queueAction(action);
  if (el("levelUpdateStatus")) el("levelUpdateStatus").textContent = "Queued update.";
}

async function deleteLevelRequest() {
  const file = el("levelsIfc")?.value;
  if (!file || !state.selectedLevelId) return alert("Choose an IFC file and level.");
  const target = el("deleteTarget")?.value;
  if (!target) return alert("Choose a target level.");
  const object_ids = collectChecked("deleteObjects");
  const action = {
    type: "delete",
    storey_id: state.selectedLevelId,
    target_storey_id: Number(target),
    object_ids,
    label: `Delete level ${state.selectedLevelId}`,
    summary: `Move ${object_ids.length} object(s) to ${target}`,
  };
  queueAction(action);
  if (el("levelDeleteStatus")) el("levelDeleteStatus").textContent = "Queued delete & reassign.";
}

async function addLevelRequest() {
  const file = el("levelsIfc")?.value;
  if (!file) return alert("Choose an IFC file.");
  const action = {
    type: "add",
    name: el("newLevelName")?.value,
    description: el("newLevelDescription")?.value,
    elevation: el("newLevelElevation")?.value ? Number(el("newLevelElevation").value) : null,
    comp_height: el("newLevelCompHeight")?.value ? Number(el("newLevelCompHeight").value) : null,
    object_ids: collectChecked("addLevelObjects"),
    label: `Add level ${el("newLevelName")?.value || ""}`,
    summary: `Move ${collectChecked("addLevelObjects").length} object(s)`,
  };
  if (!action.name) return alert("Provide a name for the new level.");
  queueAction(action);
  if (el("levelAddStatus")) el("levelAddStatus").textContent = "Queued new level.";
}

async function reassignLevelRequest() {
  const file = el("levelsIfc")?.value;
  const source = el("reassignSource")?.value;
  const target = el("reassignTarget")?.value;
  if (!file || !source || !target) return alert("Choose IFC file, source level, and target level.");
  const object_ids = collectChecked("reassignObjects");
  if (!object_ids.length) return alert("Select at least one object to reassign.");
  const action = {
    type: "reassign",
    source_storey_id: Number(source),
    target_storey_id: Number(target),
    object_ids,
    label: `Reassign ${object_ids.length} object(s)`,
    summary: `${source} → ${target}`,
  };
  queueAction(action);
  if (el("reassignStatus")) el("reassignStatus").textContent = "Queued reassignment.";
}

function wireEvents() {
  const uploadBtn = el("uploadBtn");
  if (uploadBtn) uploadBtn.addEventListener("click", uploadFiles);
  const uploadForm = el("upload-form");
  if (uploadForm) {
    state.uploadStatusEl = el("upload-status");
    state.uploadProgressEl = el("upload-progress");
    uploadForm.addEventListener("submit", (e) => {
      e.preventDefault();
      uploadFiles();
    });
  }

  const refreshBtn = el("refreshFiles");
  if (refreshBtn) refreshBtn.addEventListener("click", refreshFiles);
  const refreshBtnStatic = el("refresh-files");
  if (refreshBtnStatic) refreshBtnStatic.addEventListener("click", refreshFiles);

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

  const reassignLevelBtn = el("reassignLevel");
  if (reassignLevelBtn) reassignLevelBtn.addEventListener("click", reassignLevelRequest);

  const step2ifcFiles = el("step2ifcFiles");
  if (step2ifcFiles) {
    step2ifcFiles.addEventListener("change", (e) => {
      const outputInput = el("step2ifcOutputName");
      if (!outputInput || outputInput.value) return;
      const name = e.target.value || "";
      if (!name) return;
      const base = name.replace(/\.[^/.]+$/, "");
      outputInput.value = `${base}.ifc`;
    });
  }

  const step2ifcForm = el("step2ifcForm");
  if (step2ifcForm) {
    step2ifcForm.addEventListener("submit", (e) => {
      e.preventDefault();
      runStep2ifcAuto();
    });
  }

  el("reassignSource")?.addEventListener("change", () => {
    renderReassignControls();
  });

  const applyPendingBtn = el("applyPending");
  if (applyPendingBtn) applyPendingBtn.addEventListener("click", applyPendingChanges);

  document.querySelectorAll("[data-download-selected]").forEach((btn) => {
    btn.addEventListener("click", downloadSelected);
  });
}

document.addEventListener("DOMContentLoaded", async () => {
  state.uploadStatusEl = el("upload-status") || state.uploadStatusEl;
  state.uploadProgressEl = el("upload-progress") || state.uploadProgressEl;
  await ensureSession();
  wireEvents();
  renderPendingChanges();
});
