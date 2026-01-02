const state = {
  sessionId: null,
  files: [],
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
      li.innerHTML = `<span>${f.name}</span><span class="muted">${(f.size / 1024).toFixed(1)} KB</span>`;
      ul.appendChild(li);
    });
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
}

document.addEventListener("DOMContentLoaded", async () => {
  await ensureSession();
  wireEvents();
});
