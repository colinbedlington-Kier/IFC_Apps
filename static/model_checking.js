const mcState = {
  definitions: [],
  sections: [],
  tables: {},
  pending: [],
  audit: [],
  lastFile: null,
  downloadName: null,
  entityScopes: [],
};

const withProcessing = window.withProcessing || (async (_message, fn) => fn());

function mcEsc(val) {
  if (val === null || val === undefined) return "";
  return String(val).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function mcRowTemplate(columns) {
  return ["220px", ...columns.map(() => "minmax(160px, 1fr)")].join(" ");
}

function mcStageOptions(defs) {
  const stages = new Set();
  defs.forEach((d) => (d.milestones || []).forEach((m) => stages.add(m)));
  return Array.from(stages).sort();
}

async function loadDefinitions() {
  const resp = await fetch("/api/checks/definitions");
  const data = await resp.json();
  mcState.definitions = data.definitions || [];
  mcState.sections = Object.keys(data.sections || {});
  mcState.entityScopes = Array.from(
    new Set(
      (mcState.definitions || []).flatMap((d) => d.entity_scope || [])
    )
  ).sort();
  const stageSel = document.getElementById("mcStage");
  if (stageSel) {
    stageSel.innerHTML = '<option value="">All</option>';
    mcStageOptions(mcState.definitions).forEach((s) => {
      const opt = document.createElement("option");
      opt.value = s;
      opt.textContent = s;
      stageSel.appendChild(opt);
    });
  }
  renderEntityGroups();
  renderSectionPlaceholders();
}

function renderSectionPlaceholders() {
  const container = document.getElementById("mcSections");
  if (!container) return;
  container.innerHTML = "";
  mcState.sections.forEach((section) => {
    const wrap = document.createElement("div");
    wrap.className = "mc-section";
    wrap.dataset.section = section;
    wrap.innerHTML = `
      <div class="mc-section-header">
        <div>
          <h3>${section}</h3>
          <div class="muted">Checks: ${mcState.definitions.filter((d) => d.section === section).length}</div>
        </div>
        <div class="chips">
          <span class="chip">Issues: <span data-issues="0">0</span></span>
          <span class="chip">Rows: <span data-rows="0">0</span></span>
        </div>
      </div>
      <div class="mc-table-wrap" data-table="${section}">
        <div class="muted">Load to view data…</div>
      </div>
    `;
    container.appendChild(wrap);
  });
}

function renderEntityGroups() {
  const wrap = document.getElementById("mcEntityGroups");
  if (!wrap) return;
  wrap.innerHTML = "";
  mcState.entityScopes.forEach((ent) => {
    const id = `mc-ent-${ent}`;
    const label = document.createElement("label");
    label.className = "chip chip-toggle";
    label.innerHTML = `<input type="checkbox" id="${id}" value="${ent}" checked> ${ent}`;
    wrap.appendChild(label);
  });
  const selectAll = document.getElementById("mcEntitySelectAll");
  if (selectAll) {
    selectAll.addEventListener("click", () => {
      wrap.querySelectorAll('input[type="checkbox"]').forEach((cb) => {
        cb.checked = true;
      });
    });
  }
}

function selectedEntities() {
  const wrap = document.getElementById("mcEntityGroups");
  if (!wrap) return [];
  const checked = Array.from(wrap.querySelectorAll('input[type="checkbox"]:checked')).map((cb) => cb.value);
  return checked.length ? checked : mcState.entityScopes;
}

function renderChangeLog() {
  const target = document.getElementById("mcChanges");
  if (!target) return;
  if (!mcState.pending.length) {
    target.innerHTML = '<div class="muted">No pending edits</div>';
    return;
  }
  target.innerHTML = mcState.pending
    .map(
      (p, idx) => `
      <div class="change-row">
        <div><strong>${mcEsc(p.label)}</strong><br/><span class="muted">${mcEsc(p.check_id)} · ${mcEsc(p.object_label)}</span></div>
        <div class="muted">${mcEsc(p.old ?? "")} → ${mcEsc(p.value ?? "")}</div>
        <button class="btn secondary sm" data-undo="${idx}">Undo</button>
      </div>`
    )
    .join("");
  target.querySelectorAll("[data-undo]").forEach((btn) =>
    btn.addEventListener("click", () => {
      const idx = Number(btn.dataset.undo);
      mcState.pending.splice(idx, 1);
      renderChangeLog();
    })
  );
}

function renderAuditLog() {
  const target = document.getElementById("mcAudit");
  if (!target) return;
  if (!mcState.audit.length) {
    target.innerHTML = '<div class="muted">No applied edits yet</div>';
    return;
  }
  target.innerHTML = mcState.audit
    .slice()
    .reverse()
    .map(
      (entry) => `
      <div class="change-row">
        <div><strong>${mcEsc(entry.check_id)}</strong> (${mcEsc(entry.global_id)})</div>
        <div class="muted">${mcEsc(entry.old)} → ${mcEsc(entry.new)}</div>
        <div class="muted">${mcEsc(entry.timestamp)}</div>
      </div>`
    )
    .join("");
}

function recordEdit(section, row, col, value, mode = "manual") {
  const key = `${row.id}-${col.check_id}`;
  const existingIdx = mcState.pending.findIndex((p) => p.key === key);
  const payload = {
    key,
    section,
    object_id: row.id,
    check_id: col.check_id,
    value,
    mode,
    old: row.values[col.check_id]?.value ?? "",
    label: col.description || col.info || col.check_id,
    object_label: row.name || row.global_id || row.id,
  };
  if (existingIdx >= 0) {
    mcState.pending[existingIdx] = payload;
  } else {
    mcState.pending.push(payload);
  }
  renderChangeLog();
}

function wireCellEvents(container, columns, rows, section) {
  container.querySelectorAll("[data-cell-input]").forEach((input) => {
    input.addEventListener("change", (e) => {
      const rowId = Number(e.target.dataset.row);
      const checkId = e.target.dataset.check;
      const row = rows.find((r) => r.id === rowId);
      const col = columns.find((c) => c.check_id === checkId);
      if (!row || !col) return;
      recordEdit(section, row, col, e.target.value, "manual");
    });
  });
  container.querySelectorAll("[data-generate]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const rowId = Number(btn.dataset.row);
      const checkId = btn.dataset.check;
      const row = rows.find((r) => r.id === rowId);
      const col = columns.find((c) => c.check_id === checkId);
      if (!row || !col) return;
      const generated = row.values[checkId]?.generated;
      recordEdit(section, row, col, generated, "generated");
      const input = container.querySelector(`input[data-row="${rowId}"][data-check="${checkId}"]`);
      if (input) input.value = generated ?? "";
    });
  });
}

function renderVirtualTable(wrapper, section, columns, rows) {
  const rowHeight = 46;
  const header = document.createElement("div");
  header.className = "mc-row mc-header";
  header.style.gridTemplateColumns = mcRowTemplate(columns);
  header.innerHTML = `
    <div class="mc-cell"><strong>Object</strong></div>
    ${columns.map((c) => `<div class="mc-cell"><strong>${mcEsc(c.info || c.description || c.check_id)}</strong><div class="muted">${mcEsc(c.entity_scope.join(", "))}</div></div>`).join("")}
  `;
  wrapper.innerHTML = "";
  wrapper.appendChild(header);

  const viewport = document.createElement("div");
  viewport.className = "virtual-viewport";
  const spacerTop = document.createElement("div");
  const spacerBottom = document.createElement("div");
  spacerTop.style.height = "0px";
  spacerBottom.style.height = "0px";
  const inner = document.createElement("div");
  viewport.appendChild(spacerTop);
  viewport.appendChild(inner);
  viewport.appendChild(spacerBottom);
  wrapper.appendChild(viewport);

  const renderSlice = () => {
    const start = Math.max(0, Math.floor(viewport.scrollTop / rowHeight) - 5);
    const end = Math.min(rows.length, start + Math.ceil(viewport.clientHeight / rowHeight) + 10);
    spacerTop.style.height = `${start * rowHeight}px`;
    spacerBottom.style.height = `${(rows.length - end) * rowHeight}px`;
    const slice = rows.slice(start, end);
    inner.innerHTML = slice
      .map((row) => {
        const values = columns
          .map((c) => {
            const cell = row.values[c.check_id] || {};
            const issues = cell.issues || [];
            const issueBadge = issues.length ? `<span class="issue-pill">${issues.length}</span>` : "";
            const generated = cell.generated ? `<div class="generated">Gen: ${mcEsc(cell.generated)}</div>` : "";
            return `
              <div class="mc-cell">
                <input data-cell-input data-row="${row.id}" data-check="${c.check_id}" value="${mcEsc(cell.value ?? "")}" />
                ${issueBadge}
                ${generated ? `<button class="btn secondary sm" data-generate data-row="${row.id}" data-check="${c.check_id}">Use generated</button>` : ""}
              </div>`;
          })
          .join("");
        return `
          <div class="mc-row" style="grid-template-columns:${mcRowTemplate(columns)}">
            <div class="mc-cell">
              <div><strong>${mcEsc(row.name || row.global_id || row.id)}</strong></div>
              <div class="muted">${mcEsc(row.type)} · Issues: ${row.issues}</div>
            </div>
            ${values}
          </div>`;
      })
      .join("");
    wireCellEvents(inner, columns, rows, section);
  };
  viewport.addEventListener("scroll", renderSlice);
  renderSlice();
}

function renderSection(section, data) {
  const wrap = document.querySelector(`[data-table="${section}"]`);
  if (!wrap) return;
  const parent = wrap.closest(".mc-section");
  if (parent) {
    const issuesEl = parent.querySelector("[data-issues]");
    const rowsEl = parent.querySelector("[data-rows]");
    if (issuesEl) issuesEl.textContent = data.summary?.issues ?? 0;
    if (rowsEl) rowsEl.textContent = data.summary?.rows ?? 0;
  }
  renderVirtualTable(wrap, section, data.columns || [], data.rows || []);
}

async function loadSectionData(section) {
  const file = document.getElementById("mcIfc")?.value;
  if (!file) return alert("Select an IFC file.");
  mcState.lastFile = file;
  const payload = {
    ifc_file: file,
    section,
    riba_stage: document.getElementById("mcStage")?.value || null,
    entity_filters: selectedEntities(),
  };
  const resp = await fetch(`/api/session/${state.sessionId}/checks/data`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await resp.json();
  mcState.tables[section] = data;
  if (Array.isArray(data.change_log)) {
    mcState.audit = data.change_log;
    renderAuditLog();
  }
  renderSection(section, data);
  const summary = document.getElementById("mcSummary");
  if (summary) summary.textContent = `Loaded ${section}: ${data.summary?.rows ?? 0} rows, ${data.summary?.issues ?? 0} issues`;
}

async function loadAllSections() {
  mcState.pending = [];
  renderChangeLog();
  return withProcessing("Loading model checking data…", async () => {
    for (const sec of mcState.sections) {
      // eslint-disable-next-line no-await-in-loop
      await loadSectionData(sec);
    }
  });
}

async function applyModelChanges() {
  if (!mcState.pending.length) {
    alert("No pending edits to apply.");
    return;
  }
  const file = document.getElementById("mcIfc")?.value;
  const payload = {
    ifc_file: file,
    edits: mcState.pending.map((p) => ({
      object_id: p.object_id,
      check_id: p.check_id,
      value: p.value,
      mode: p.mode,
    })),
  };
  return withProcessing("Applying model checking edits…", async () => {
    const resp = await fetch(`/api/session/${state.sessionId}/checks/apply`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await resp.json();
    mcState.pending = [];
    renderChangeLog();
    mcState.audit = data.audit || [];
    renderAuditLog();
    const dlBtn = document.getElementById("mcDownload");
    if (dlBtn && data.ifc?.name) {
      dlBtn.disabled = false;
      dlBtn.dataset.file = data.ifc.name;
    }
    const summary = document.getElementById("mcSummary");
    if (summary) summary.textContent = `Applied edits and saved ${data.ifc?.name || ""}`;
    await refreshFiles();
  });
}

async function downloadLatestChecked() {
  const btn = document.getElementById("mcDownload");
  const file = btn?.dataset.file;
  if (!file) return;
  await downloadFile(file);
}

function wireModelCheckingEvents() {
  const loadBtn = document.getElementById("mcLoad");
  if (loadBtn) loadBtn.addEventListener("click", loadAllSections);
  const applyBtn = document.getElementById("mcApply");
  if (applyBtn) applyBtn.addEventListener("click", applyModelChanges);
  const dlBtn = document.getElementById("mcDownload");
  if (dlBtn) dlBtn.addEventListener("click", downloadLatestChecked);
}

document.addEventListener("DOMContentLoaded", async () => {
  if (!document.getElementById("mcSections")) return;
  await ensureSession();
  await loadDefinitions();
  renderChangeLog();
  renderAuditLog();
  wireModelCheckingEvents();
});
