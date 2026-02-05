const QA_PAGES = [
  { key: "summary", label: "QA Summary" },
  { key: "project_naming", label: "Project Naming" },
  { key: "occurrence_naming", label: "Occurrence Naming" },
  { key: "type_naming", label: "Type Naming" },
  { key: "classification_template", label: "Classification Template" },
  { key: "classification_values", label: "Classification Values" },
  { key: "pset_template", label: "PSet Template" },
  { key: "property_values", label: "Property Values" },
  { key: "system", label: "System" },
  { key: "zone", label: "Zone" },
];

const qaState = {
  sessionId: null,
  regexPatterns: [],
  currentJobId: localStorage.getItem("ifc_qa_job_id") || "",
};

const qs = (sel) => document.querySelector(sel);

async function ensureQaSession() {
  const existing = localStorage.getItem("ifc_session_id");
  const resp = await fetch("/api/session", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ session_id: existing }),
  });
  if (!resp.ok) throw new Error("Session request failed");
  const data = await resp.json();
  qaState.sessionId = data.session_id;
  localStorage.setItem("ifc_session_id", data.session_id);
  return data.session_id;
}

async function loadRegexPatterns() {
  if (!qaState.sessionId) return;
  const resp = await fetch(`/api/ifc-qa/config/${qaState.sessionId}/regex`);
  if (!resp.ok) return;
  const data = await resp.json();
  qaState.regexPatterns = data.patterns || [];
}

function renderRegexModal() {
  const body = qs("#regexModalBody");
  if (!body) return;
  body.innerHTML = qaState.regexPatterns
    .map(
      (p, idx) => `
      <div class="qa-grid" style="margin-bottom: 8px">
        <label>Key
          <input type="text" data-regex-key="${idx}" value="${p.key || ""}" />
        </label>
        <label>Pattern
          <input type="text" data-regex-pattern="${idx}" value="${p.pattern || ""}" />
        </label>
        <label>Enabled
          <select data-regex-enabled="${idx}">
            <option value="true" ${p.enabled === "true" ? "selected" : ""}>true</option>
            <option value="false" ${p.enabled === "false" ? "selected" : ""}>false</option>
          </select>
        </label>
      </div>`
    )
    .join("");
}

function openRegexModal() {
  const modal = qs("#regexModal");
  if (!modal) return;
  renderRegexModal();
  modal.hidden = false;
}

function closeRegexModal() {
  const modal = qs("#regexModal");
  if (modal) modal.hidden = true;
}

function saveRegexModal() {
  qaState.regexPatterns = qaState.regexPatterns.map((p, idx) => {
    const key = qs(`[data-regex-key="${idx}"]`)?.value || p.key;
    const pattern = qs(`[data-regex-pattern="${idx}"]`)?.value || p.pattern;
    const enabled = qs(`[data-regex-enabled="${idx}"]`)?.value || p.enabled;
    return { key, pattern, enabled };
  });
  closeRegexModal();
}

function exportRegexCsv() {
  const header = "key,pattern,enabled\n";
  const rows = qaState.regexPatterns
    .map((p) => `${p.key || ""},${(p.pattern || "").replace(/"/g, '""')},${p.enabled || "true"}`)
    .join("\n");
  const blob = new Blob([header + rows], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = "regex_patterns.csv";
  link.click();
  URL.revokeObjectURL(url);
}

function regexCsvBlob() {
  if (!qaState.regexPatterns.length) return null;
  const rows = qaState.regexPatterns
    .map((p) => `${p.key || ""},${(p.pattern || "").replace(/"/g, '""')},${p.enabled || "true"}`)
    .join("\n");
  const csv = `key,pattern,enabled\n${rows}`;
  return new Blob([csv], { type: "text/csv" });
}

function extractorTemplate() {
  return `
  <div class="stack">
    <div class="two-col">
      <div>
        <label for="qaIfcFiles">IFC files</label>
        <input id="qaIfcFiles" type="file" multiple accept=".ifc" />
        <div class="inline" style="margin-top: 8px">
          <label><input type="checkbox" id="qaUseDefaults" checked /> Use default configs</label>
          <label><input type="checkbox" id="qaShowOverrides" disabled /> Override configs</label>
        </div>
        <div id="qaOverrideSection" class="qa-grid" style="margin-top: 12px; display:none;">
          <label>QA Rules CSV<input id="qaRulesCsv" type="file" accept=".csv" /></label>
          <label>QA Property Requirements CSV<input id="qaPropertyCsv" type="file" accept=".csv" /></label>
          <label>QA Unacceptable Values CSV<input id="qaUnacceptableCsv" type="file" accept=".csv" /></label>
          <label>Regex Patterns CSV<input id="qaRegexCsv" type="file" accept=".csv" /></label>
          <label>Exclude Filter CSV<input id="qaExcludeCsv" type="file" accept=".csv" /></label>
          <label>Pset Template CSV<input id="qaPsetCsv" type="file" accept=".csv" /></label>
        </div>
      </div>
      <div>
        <label>Regex</label>
        <button class="btn secondary" id="qaRegexBtn" type="button">Configure Regex</button>
        <p class="muted">Regex overrides are applied per session and can be exported as CSV.</p>
      </div>
    </div>
    <button class="btn" id="qaStartBtn" style="margin-top: 12px">Start QA Extraction</button>
    <div class="progress-track" style="margin-top: 16px">
      <div class="progress-fill" id="qaProgressFill" style="width:0%"></div>
    </div>
    <div class="muted" id="qaProgressLabel"></div>
    <textarea class="log-box" rows="8" readonly id="qaProgressLog"></textarea>
    <div id="qaDownloadWrap"></div>
    <div id="qaPreviewWrap"></div>
  </div>
  `;
}

async function startExtraction() {
  const ifcInput = qs("#qaIfcFiles");
  const ifcFiles = Array.from(ifcInput?.files || []);
  if (!ifcFiles.length) return alert("Upload IFC file(s).");

  const form = new FormData();
  ifcFiles.forEach((file) => form.append("files", file, file.name));
  form.append("session_id", qaState.sessionId || "");

  const useDefaults = qs("#qaUseDefaults")?.checked ?? true;
  const showOverrides = qs("#qaShowOverrides")?.checked ?? false;
  if (!useDefaults && showOverrides) {
    const mapping = [
      ["qaRulesCsv", "qa_rules_csv"],
      ["qaPropertyCsv", "qa_property_requirements_csv"],
      ["qaUnacceptableCsv", "qa_unacceptable_values_csv"],
      ["qaRegexCsv", "regex_patterns_csv"],
      ["qaExcludeCsv", "exclude_filter_csv"],
      ["qaPsetCsv", "pset_template_csv"],
    ];
    mapping.forEach(([id, field]) => {
      const file = qs(`#${id}`)?.files?.[0];
      if (file) form.append(field, file, file.name);
    });
  }

  const regexBlob = regexCsvBlob();
  if (regexBlob) form.append("regex_patterns_csv", regexBlob, "regex_patterns.csv");

  qs("#qaProgressLabel").textContent = "Starting...";
  qs("#qaProgressLog").value = "";
  qs("#qaDownloadWrap").innerHTML = "";
  qs("#qaPreviewWrap").innerHTML = "";

  const resp = await fetch("/api/ifc-qa/extract", { method: "POST", body: form });
  if (!resp.ok) return alert("Failed to start extraction.");
  const data = await resp.json();
  qaState.currentJobId = data.jobId;
  localStorage.setItem("ifc_qa_job_id", data.jobId);
  pollProgress(data.jobId);
}

async function pollProgress(jobId) {
  const resp = await fetch(`/api/ifc-qa/progress/${jobId}`);
  if (!resp.ok) return;
  const data = await resp.json();
  qs("#qaProgressFill").style.width = `${data.percent || 0}%`;
  qs("#qaProgressLabel").textContent = `${data.currentStep || ""} ${data.currentFile ? `(${data.currentFile})` : ""}`;
  qs("#qaProgressLog").value = (data.logs || []).join("\n");
  if (data.status !== "complete" && data.status !== "failed") {
    setTimeout(() => pollProgress(jobId), 1200);
    return;
  }
  if (data.status === "complete") {
    qs("#qaDownloadWrap").innerHTML = `<a class="btn secondary" href="/api/ifc-qa/result/${jobId}">Download ZIP</a>`;
    const summaryResp = await fetch(`/api/ifc-qa/summary/${jobId}`);
    if (summaryResp.ok) {
      const summary = await summaryResp.json();
      renderPreview(summary.failures || []);
    }
  }
}

function renderPreview(failures) {
  const preview = failures.slice(0, 10);
  if (!preview.length) return;
  qs("#qaPreviewWrap").innerHTML = `
    <div class="card" style="margin-top: 12px">
      <h4>Recent failures</h4>
      <table class="data-table">
        <thead><tr><th>Page</th><th>Rule</th><th>Severity</th><th>Message</th></tr></thead>
        <tbody>
          ${preview
            .map(
              (row) => `
            <tr>
              <td>${row.page || ""}</td>
              <td>${row.rule_id || ""}</td>
              <td>${row.severity || ""}</td>
              <td>${row.message || ""}</td>
            </tr>`
            )
            .join("")}
        </tbody>
      </table>
    </div>
  `;
}

function renderBarChart(perPage) {
  const rows = Object.entries(perPage).map(([key, stats]) => {
    const fail = stats.fails || 0;
    return `<div class="qa-bar-row"><span>${key}</span><div class="qa-bar" style="width:${Math.min(fail * 10, 100)}%"></div><span>${fail}</span></div>`;
  });
  return `<div class="qa-bar-chart">${rows.join("")}</div>`;
}

function dashboardTemplate() {
  return `
    <div class="stack">
      <div class="inline">
        <input type="text" id="qaJobIdInput" placeholder="Job ID" />
        <button class="btn" id="qaLoadSummary">Load</button>
      </div>
      <div id="qaDashboardContent"></div>
    </div>
  `;
}

function renderDashboard(summary) {
  const failures = summary.failures || [];
  const perPage = summary.per_page || {};
  const filteredFailures = failures.filter((row) => {
    const page = qs("#qaPageFilter")?.value || "";
    const severity = qs("#qaSeverityFilter")?.value || "";
    if (page && row.page !== page) return false;
    if (severity && row.severity !== severity) return false;
    return true;
  });

  qs("#qaDashboardContent").innerHTML = `
    <div class="qa-kpis">
      <div class="qa-kpi"><div class="label">Files checked</div><div class="value">${summary.overall?.files_checked ?? 0}</div></div>
      <div class="qa-kpi"><div class="label">Objects checked</div><div class="value">${summary.overall?.objects_checked ?? 0}</div></div>
      <div class="qa-kpi"><div class="label">Pass %</div><div class="value">${summary.overall?.pass_percent ?? 0}%</div></div>
      <div class="qa-kpi"><div class="label">Failures</div><div class="value">${summary.overall?.total_failures ?? 0}</div></div>
    </div>
    <div class="qa-charts">
      <div>
        <h4>Failures by page</h4>
        ${renderBarChart(perPage)}
      </div>
    </div>
    <div class="inline" style="margin-top: 12px">
      <select id="qaPageFilter">
        <option value="">All pages</option>
        ${QA_PAGES.slice(1)
          .map((p) => `<option value="${p.key}">${p.label}</option>`)
          .join("")}
      </select>
      <select id="qaSeverityFilter">
        <option value="">All severity</option>
        <option value="low">low</option>
        <option value="medium">medium</option>
        <option value="high">high</option>
      </select>
    </div>
    <table class="data-table" style="margin-top: 12px">
      <thead>
        <tr><th>Page</th><th>Rule</th><th>Severity</th><th>Source file</th><th>IFC GlobalId</th><th>Message</th></tr>
      </thead>
      <tbody>
        ${filteredFailures
          .map(
            (row) => `
          <tr>
            <td>${row.page || ""}</td>
            <td>${row.rule_id || ""}</td>
            <td>${row.severity || ""}</td>
            <td>${row.source_file || ""}</td>
            <td>${row.ifc_globalid || ""}</td>
            <td>${row.message || ""}</td>
          </tr>`
          )
          .join("")}
      </tbody>
    </table>
  `;

  qs("#qaPageFilter").addEventListener("change", () => renderDashboard(summary));
  qs("#qaSeverityFilter").addEventListener("change", () => renderDashboard(summary));
}

function configTemplate() {
  return `
    <div class="stack">
      <p class="muted">Overrides apply only to the active session.</p>
      <div class="inline">
        <a class="btn secondary" id="qaDownloadConfigs">Download configs</a>
        <button class="btn ghost" id="qaResetConfigs">Reset to defaults</button>
      </div>
      <div>
        <label>Upload config ZIP<input id="qaConfigZip" type="file" accept=".zip" /></label>
        <button class="btn" id="qaUploadConfigs">Load ZIP</button>
      </div>
      <div id="qaConfigStatus"></div>
    </div>
  `;
}

async function loadConfigStatus() {
  const resp = await fetch(`/api/ifc-qa/config/${qaState.sessionId}`);
  if (!resp.ok) return;
  const data = await resp.json();
  qs("#qaConfigStatus").innerHTML = `
    <div class="card" style="margin-top: 12px">
      <h4>Active overrides</h4>
      ${data.overrides?.length ? `<ul>${data.overrides.map((o) => `<li>${o}</li>`).join("")}</ul>` : '<div class="muted">No overrides loaded.</div>'}
    </div>
  `;
  qs("#qaDownloadConfigs").setAttribute("href", `/api/ifc-qa/config/${qaState.sessionId}/download`);
}

async function uploadConfigZip() {
  const file = qs("#qaConfigZip")?.files?.[0];
  if (!file) return;
  const form = new FormData();
  form.append("config_zip", file, file.name);
  const resp = await fetch(`/api/ifc-qa/config/${qaState.sessionId}/upload`, { method: "POST", body: form });
  if (resp.ok) loadConfigStatus();
}

async function resetConfigs() {
  const resp = await fetch(`/api/ifc-qa/config/${qaState.sessionId}/reset`, { method: "POST" });
  if (resp.ok) loadConfigStatus();
}

async function initExtractor(root) {
  root.innerHTML = extractorTemplate();
  qs("#qaRegexBtn").addEventListener("click", openRegexModal);
  qs("#qaStartBtn").addEventListener("click", startExtraction);
  qs("#qaUseDefaults").addEventListener("change", (e) => {
    qs("#qaShowOverrides").disabled = e.target.checked;
    if (e.target.checked) {
      qs("#qaShowOverrides").checked = false;
      qs("#qaOverrideSection").style.display = "none";
    }
  });
  qs("#qaShowOverrides").addEventListener("change", (e) => {
    qs("#qaOverrideSection").style.display = e.target.checked ? "grid" : "none";
  });
}

async function initDashboard(root) {
  root.innerHTML = dashboardTemplate();
  qs("#qaJobIdInput").value = qaState.currentJobId || "";
  qs("#qaLoadSummary").addEventListener("click", async () => {
    const jobId = qs("#qaJobIdInput").value.trim();
    if (!jobId) return;
    const resp = await fetch(`/api/ifc-qa/summary/${jobId}`);
    if (!resp.ok) return alert("Summary not ready.");
    const summary = await resp.json();
    renderDashboard(summary);
  });
  if (qaState.currentJobId) {
    const resp = await fetch(`/api/ifc-qa/summary/${qaState.currentJobId}`);
    if (resp.ok) {
      const summary = await resp.json();
      renderDashboard(summary);
    }
  }
}

async function initConfig(root) {
  root.innerHTML = configTemplate();
  qs("#qaUploadConfigs").addEventListener("click", uploadConfigZip);
  qs("#qaResetConfigs").addEventListener("click", resetConfigs);
  await loadConfigStatus();
}

async function initIfcQaApp() {
  const root = qs("#ifc-qa-root");
  if (!root) return;
  await ensureQaSession();
  await loadRegexPatterns();

  qs("#regexSave").addEventListener("click", saveRegexModal);
  qs("#regexClose").addEventListener("click", closeRegexModal);
  qs("#regexExport").addEventListener("click", exportRegexCsv);

  const page = root.dataset.qaPage || "extractor";
  if (page === "dashboard") return initDashboard(root);
  if (page === "config") return initConfig(root);
  return initExtractor(root);
}

document.addEventListener("DOMContentLoaded", initIfcQaApp);
