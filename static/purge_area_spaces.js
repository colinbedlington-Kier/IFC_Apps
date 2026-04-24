(() => {
  const byId = (id) => document.getElementById(id);
  const filesSelect = byId("pasFiles");
  if (!filesSelect) return;

  const state = {
    scanResults: [],
    selectedKeys: new Set(),
  };

  const status = byId("pasSessionStatus");
  const summaryCards = byId("pasSummaryCards");
  const resultsBody = byId("pasResultsBody");
  const outputs = byId("pasOutputs");
  const log = byId("pasLog");
  const purgeBtn = byId("pasPurgeBtn");

  const getSessionId = () => localStorage.getItem("ifc_session_id");

  function renderSummary() {
    const filesScanned = state.scanResults.length;
    const totalSpaces = state.scanResults.reduce((acc, row) => acc + Number(row.total_spaces || 0), 0);
    const candidates = state.scanResults.reduce((acc, row) => acc + (row.candidates || []).length, 0);
    const selected = state.selectedKeys.size;
    const cards = [
      ["IFC files scanned", String(filesScanned)],
      ["Total IfcSpace count", String(totalSpaces)],
      ["Area-space candidates", String(candidates)],
      ["Candidates selected for purge", String(selected)],
    ];
    summaryCards.innerHTML = cards.map(([k, v]) => `<div class="stat-card"><div class="stat-label">${k}</div><div class="stat-value">${v}</div></div>`).join("");
    purgeBtn.disabled = selected === 0 || candidates === 0;
  }

  function rowKey(file, candidate) {
    return `${file}::${candidate.global_id || candidate.step_id}`;
  }

  function renderResults() {
    const rows = [];
    for (const scan of state.scanResults) {
      for (const candidate of scan.candidates || []) {
        const key = rowKey(scan.source_file, candidate);
        const checked = state.selectedKeys.has(key) ? "checked" : "";
        rows.push(`<tr>
          <td><input type="checkbox" data-key="${key}" ${checked}></td>
          <td>${scan.source_file}</td>
          <td>${candidate.step_id || ""}</td>
          <td>${candidate.global_id || ""}</td>
          <td>${candidate.name || ""}</td>
          <td>${candidate.long_name || ""}</td>
          <td>${candidate.object_type || ""}</td>
          <td>${candidate.matched_source || ""} / ${candidate.matched_name || ""}</td>
          <td>${candidate.matched_value || ""}</td>
          <td>${candidate.reason || ""}</td>
        </tr>`);
      }
    }
    resultsBody.innerHTML = rows.join("") || '<tr><td colspan="10" class="muted">No candidates.</td></tr>';
    resultsBody.querySelectorAll("input[type=checkbox][data-key]").forEach((node) => {
      node.addEventListener("change", () => {
        const key = node.getAttribute("data-key");
        if (!key) return;
        if (node.checked) state.selectedKeys.add(key);
        else state.selectedKeys.delete(key);
        renderSummary();
      });
    });
  }

  async function loadSessionFiles() {
    const sessionId = getSessionId();
    if (!sessionId) {
      status.textContent = "No active session. Create/upload in Upload & Session first.";
      return;
    }
    status.textContent = "resolving session files";
    const resp = await fetch(`/api/ifc/area-spaces/session-files?session_id=${encodeURIComponent(sessionId)}`);
    const data = await resp.json();
    if (!resp.ok) {
      status.textContent = `Failed to load session files: ${data.detail || "Unknown error"}`;
      return;
    }
    filesSelect.innerHTML = (data.files || []).map((item) => `<option value="${item.name}">${item.name}</option>`).join("");
    status.textContent = `Session ${sessionId} • ${data.count} IFC files found`;
  }

  function getSelectedFiles() {
    return Array.from(filesSelect.selectedOptions || []).map((option) => option.value).filter(Boolean);
  }

  async function runScan() {
    const sessionId = getSessionId();
    const fileNames = getSelectedFiles();
    if (!sessionId || fileNames.length === 0) {
      status.textContent = "Select at least one session IFC file before scanning.";
      return;
    }
    status.textContent = "scanning IFC";
    const resp = await window.withProcessing("Scanning IFC for area spaces…", async () => fetch("/api/ifc/area-spaces/scan", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ session_id: sessionId, file_names: fileNames }),
    }));
    const data = await resp.json();
    if (!resp.ok) {
      status.textContent = `Scan failed: ${data.detail || "Unknown error"}`;
      return;
    }
    state.scanResults = data.results || [];
    state.selectedKeys = new Set();
    for (const scan of state.scanResults) {
      for (const candidate of scan.candidates || []) {
        state.selectedKeys.add(rowKey(scan.source_file, candidate));
      }
    }
    renderResults();
    renderSummary();
    status.textContent = "candidates found";
    log.textContent = JSON.stringify(data, null, 2);
  }

  async function runPurge() {
    const sessionId = getSessionId();
    if (!sessionId || state.selectedKeys.size === 0) {
      status.textContent = "Select at least one candidate before purge.";
      return;
    }
    const selected = [];
    for (const scan of state.scanResults) {
      for (const candidate of scan.candidates || []) {
        const key = rowKey(scan.source_file, candidate);
        if (!state.selectedKeys.has(key)) continue;
        selected.push({ source_file: scan.source_file, global_id: candidate.global_id, step_id: candidate.step_id });
      }
    }
    if (selected.length === 0) {
      status.textContent = "No selected candidates remain for purge.";
      return;
    }

    status.textContent = "purging";
    const resp = await window.withProcessing("Purging area-derived IfcSpace entities…", async () => fetch("/api/ifc/area-spaces/purge", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ session_id: sessionId, selected_candidates: selected, file_names: getSelectedFiles() }),
    }));
    const data = await resp.json();
    if (!resp.ok) {
      status.textContent = `Purge failed: ${data.detail || "Unknown error"}`;
      return;
    }
    status.textContent = "writing cleaned IFC → complete";
    outputs.innerHTML = (data.output_files || []).map((item) => `<div class="file-pill"><div class="file-name">${item.name}</div><a class="ghost" href="${item.download_url}">Download</a></div>`).join("");
    log.textContent = JSON.stringify(data, null, 2);
    if (typeof refreshFiles === "function") refreshFiles();
  }

  byId("pasRefreshFiles").addEventListener("click", loadSessionFiles);
  byId("pasScanBtn").addEventListener("click", runScan);
  byId("pasPurgeBtn").addEventListener("click", runPurge);

  loadSessionFiles();
  renderSummary();
})();
