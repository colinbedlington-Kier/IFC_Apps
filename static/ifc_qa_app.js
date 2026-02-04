const { useEffect, useMemo, useState } = React;
const h = React.createElement;

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

const state = { sessionId: null };

async function ensureQaSession() {
  const existing = localStorage.getItem("ifc_session_id");
  const resp = await fetch("/api/session", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ session_id: existing }),
  });
  if (!resp.ok) throw new Error("Session request failed");
  const data = await resp.json();
  state.sessionId = data.session_id;
  localStorage.setItem("ifc_session_id", data.session_id);
  return data.session_id;
}

function useSession() {
  const [sessionId, setSessionId] = useState(state.sessionId);
  useEffect(() => {
    ensureQaSession().then(setSessionId).catch(console.error);
  }, []);
  return sessionId;
}

function useRegexModal(onApply) {
  const [patterns, setPatterns] = useState([]);

  async function loadPatterns(sessionId) {
    if (!sessionId) return;
    const resp = await fetch(`/api/ifc-qa/config/${sessionId}/regex`);
    if (!resp.ok) return;
    const data = await resp.json();
    setPatterns(data.patterns || []);
  }

  function openModal() {
    const modal = document.getElementById("regexModal");
    if (!modal) return;
    modal.hidden = false;
    const body = document.getElementById("regexModalBody");
    if (body) {
      body.innerHTML = patterns
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
  }

  function closeModal() {
    const modal = document.getElementById("regexModal");
    if (modal) modal.hidden = true;
  }

  function saveModal() {
    const updated = patterns.map((p, idx) => {
      const key = document.querySelector(`[data-regex-key="${idx}"]`)?.value || p.key;
      const pattern = document.querySelector(`[data-regex-pattern="${idx}"]`)?.value || p.pattern;
      const enabled = document.querySelector(`[data-regex-enabled="${idx}"]`)?.value || p.enabled;
      return { key, pattern, enabled };
    });
    setPatterns(updated);
    if (onApply) onApply(updated);
    closeModal();
  }

  function exportCsv() {
    const header = "key,pattern,enabled\n";
    const rows = patterns
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

  useEffect(() => {
    const saveBtn = document.getElementById("regexSave");
    const closeBtn = document.getElementById("regexClose");
    const exportBtn = document.getElementById("regexExport");
    if (saveBtn) saveBtn.onclick = saveModal;
    if (closeBtn) closeBtn.onclick = closeModal;
    if (exportBtn) exportBtn.onclick = exportCsv;
  }, [patterns]);

  return { patterns, setPatterns, loadPatterns, openModal };
}

function ExtractorPage() {
  const sessionId = useSession();
  const [useDefaults, setUseDefaults] = useState(true);
  const [showOverrides, setShowOverrides] = useState(false);
  const [jobId, setJobId] = useState(localStorage.getItem("ifc_qa_job_id") || "");
  const [progress, setProgress] = useState({ status: "", percent: 0, currentFile: "", currentStep: "", logs: [] });
  const [preview, setPreview] = useState([]);
  const { patterns, setPatterns, loadPatterns, openModal } = useRegexModal((updated) => setPatterns(updated));

  useEffect(() => {
    if (sessionId) loadPatterns(sessionId);
  }, [sessionId]);

  async function startExtraction() {
    const ifcInput = document.getElementById("qaIfcFiles");
    const ifcFiles = Array.from(ifcInput?.files || []);
    if (!ifcFiles.length) return alert("Upload IFC file(s).");

    const form = new FormData();
    ifcFiles.forEach((file) => form.append("files", file, file.name));
    form.append("session_id", sessionId || "");

    if (!useDefaults && showOverrides) {
      const overrideIds = [
        "qaRulesCsv",
        "qaPropertyCsv",
        "qaUnacceptableCsv",
        "qaRegexCsv",
        "qaExcludeCsv",
        "qaPsetCsv",
      ];
      const fields = [
        "qa_rules_csv",
        "qa_property_requirements_csv",
        "qa_unacceptable_values_csv",
        "regex_patterns_csv",
        "exclude_filter_csv",
        "pset_template_csv",
      ];
      overrideIds.forEach((id, idx) => {
        const file = document.getElementById(id)?.files?.[0];
        if (file) form.append(fields[idx], file, file.name);
      });
    }

    if (patterns.length) {
      const rows = patterns
        .map((p) => `${p.key || ""},${(p.pattern || "").replace(/"/g, '""')},${p.enabled || "true"}`)
        .join("\n");
      const csv = `key,pattern,enabled\n${rows}`;
      const blob = new Blob([csv], { type: "text/csv" });
      form.append("regex_patterns_csv", blob, "regex_patterns.csv");
    }

    setPreview([]);
    setProgress({ status: "running", percent: 0, currentFile: "", currentStep: "Starting", logs: [] });
    const resp = await fetch("/api/ifc-qa/extract", { method: "POST", body: form });
    if (!resp.ok) return alert("Failed to start extraction.");
    const data = await resp.json();
    setJobId(data.jobId);
    localStorage.setItem("ifc_qa_job_id", data.jobId);
    pollProgress(data.jobId);
  }

  async function pollProgress(id) {
    const resp = await fetch(`/api/ifc-qa/progress/${id}`);
    if (!resp.ok) return;
    const data = await resp.json();
    setProgress(data);
    if (data.status !== "complete" && data.status !== "failed") {
      setTimeout(() => pollProgress(id), 1200);
      return;
    }
    if (data.status === "complete") {
      const summaryResp = await fetch(`/api/ifc-qa/summary/${id}`);
      if (summaryResp.ok) {
        const summary = await summaryResp.json();
        setPreview(summary.failures?.slice(0, 10) || []);
      }
    }
  }

  return h("div", { className: "stack" }, [
    h("div", { className: "two-col" }, [
      h("div", null, [
        h("label", { htmlFor: "qaIfcFiles" }, "IFC files"),
        h("input", { id: "qaIfcFiles", type: "file", multiple: true, accept: ".ifc" }),
        h("div", { className: "inline", style: { marginTop: "8px" } }, [
          h("label", null, [
            h("input", {
              type: "checkbox",
              checked: useDefaults,
              onChange: (e) => setUseDefaults(e.target.checked),
            }),
            " Use default configs",
          ]),
          h("label", null, [
            h("input", {
              type: "checkbox",
              checked: showOverrides,
              onChange: (e) => setShowOverrides(e.target.checked),
              disabled: useDefaults,
            }),
            " Override configs",
          ]),
        ]),
        !useDefaults && showOverrides
          ? h("div", { className: "qa-grid", style: { marginTop: "12px" } }, [
              h("label", null, ["QA Rules CSV", h("input", { id: "qaRulesCsv", type: "file", accept: ".csv" })]),
              h("label", null, [
                "QA Property Requirements CSV",
                h("input", { id: "qaPropertyCsv", type: "file", accept: ".csv" }),
              ]),
              h("label", null, [
                "QA Unacceptable Values CSV",
                h("input", { id: "qaUnacceptableCsv", type: "file", accept: ".csv" }),
              ]),
              h("label", null, [
                "Regex Patterns CSV",
                h("input", { id: "qaRegexCsv", type: "file", accept: ".csv" }),
              ]),
              h("label", null, [
                "Exclude Filter CSV",
                h("input", { id: "qaExcludeCsv", type: "file", accept: ".csv" }),
              ]),
              h("label", null, [
                "Pset Template CSV",
                h("input", { id: "qaPsetCsv", type: "file", accept: ".csv" }),
              ]),
            ])
          : null,
      ]),
      h("div", null, [
        h("label", null, "Regex"),
        h(
          "button",
          { className: "btn secondary", onClick: openModal, type: "button" },
          "Configure Regex"
        ),
        h("p", { className: "muted" }, "Regex overrides are applied per session and can be exported as CSV."),
      ]),
    ]),
    h(
      "button",
      { className: "btn", onClick: startExtraction, style: { marginTop: "12px" } },
      "Start QA Extraction"
    ),
    h("div", { className: "progress-track", style: { marginTop: "16px" } }, [
      h("div", { className: "progress-fill", style: { width: `${progress.percent || 0}%` } }),
    ]),
    h(
      "div",
      { className: "muted" },
      `${progress.currentStep || ""} ${progress.currentFile ? `(${progress.currentFile})` : ""}`
    ),
    h("textarea", {
      className: "log-box",
      rows: 8,
      readOnly: true,
      value: (progress.logs || []).join("\n"),
    }),
    jobId && progress.status === "complete"
      ? h("a", { className: "btn secondary", href: `/api/ifc-qa/result/${jobId}` }, "Download ZIP")
      : null,
    preview.length
      ? h("div", { className: "card", style: { marginTop: "12px" } }, [
          h("h4", null, "Recent failures"),
          h(
            "table",
            { className: "data-table" },
            h(
              "tbody",
              null,
              preview.map((row, idx) =>
                h("tr", { key: idx }, [
                  h("td", null, row.page || ""),
                  h("td", null, row.rule_id || ""),
                  h("td", null, row.severity || ""),
                  h("td", null, row.message || ""),
                ])
              )
            )
          ),
        ])
      : null,
  ]);
}

function DashboardPage() {
  const sessionId = useSession();
  const [jobId, setJobId] = useState(localStorage.getItem("ifc_qa_job_id") || "");
  const [summary, setSummary] = useState(null);
  const [filters, setFilters] = useState({ page: "", severity: "" });

  async function loadSummary(targetJobId) {
    if (!targetJobId) return;
    const resp = await fetch(`/api/ifc-qa/summary/${targetJobId}`);
    if (!resp.ok) return;
    const data = await resp.json();
    setSummary(data);
  }

  useEffect(() => {
    if (jobId) loadSummary(jobId);
  }, [jobId, sessionId]);

  useEffect(() => {
    if (!summary) return;
    const ctx = document.getElementById("qaChart");
    const perPage = summary.per_page || {};
    if (ctx && window.Chart) {
      const existing = Chart.getChart(ctx);
      if (existing) existing.destroy();
      new Chart(ctx, {
        type: "bar",
        data: {
          labels: Object.keys(perPage),
          datasets: [
            {
              label: "Failures",
              data: Object.values(perPage).map((v) => v.fails || 0),
              backgroundColor: "#ff6b6b",
            },
          ],
        },
        options: { responsive: true, plugins: { legend: { display: false } } },
      });
    }
  }, [summary]);

  if (!summary) {
    return h("div", null, [
      h("p", { className: "muted" }, "Load a QA summary using the latest job ID."),
      h("input", {
        type: "text",
        value: jobId,
        onChange: (e) => setJobId(e.target.value),
        placeholder: "Job ID",
      }),
      h("button", { className: "btn", onClick: () => loadSummary(jobId) }, "Load"),
    ]);
  }

  const failures = summary.failures || [];
  const filtered = failures.filter((row) => {
    if (filters.page && row.page !== filters.page) return false;
    if (filters.severity && row.severity !== filters.severity) return false;
    return true;
  });

  return h("div", null, [
    h("div", { className: "qa-kpis" }, [
      h("div", { className: "qa-kpi" }, [
        h("div", { className: "label" }, "Files checked"),
        h("div", { className: "value" }, summary.overall?.files_checked ?? 0),
      ]),
      h("div", { className: "qa-kpi" }, [
        h("div", { className: "label" }, "Objects checked"),
        h("div", { className: "value" }, summary.overall?.objects_checked ?? 0),
      ]),
      h("div", { className: "qa-kpi" }, [
        h("div", { className: "label" }, "Pass %"),
        h("div", { className: "value" }, `${summary.overall?.pass_percent ?? 0}%`),
      ]),
      h("div", { className: "qa-kpi" }, [
        h("div", { className: "label" }, "Failures"),
        h("div", { className: "value" }, summary.overall?.total_failures ?? 0),
      ]),
    ]),
    h("div", { className: "qa-charts" }, [h("canvas", { id: "qaChart" })]),
    h("div", { className: "inline", style: { marginTop: "12px" } }, [
      h(
        "select",
        {
          value: filters.page,
          onChange: (e) => setFilters({ ...filters, page: e.target.value }),
        },
        [h("option", { value: "" }, "All pages")].concat(
          QA_PAGES.slice(1).map((p) => h("option", { key: p.key, value: p.key }, p.label))
        )
      ),
      h(
        "select",
        {
          value: filters.severity,
          onChange: (e) => setFilters({ ...filters, severity: e.target.value }),
        },
        [
          h("option", { value: "" }, "All severity"),
          h("option", { value: "low" }, "low"),
          h("option", { value: "medium" }, "medium"),
          h("option", { value: "high" }, "high"),
        ]
      ),
    ]),
    h(
      "table",
      { className: "data-table", style: { marginTop: "12px" } },
      h(
        "thead",
        null,
        h("tr", null, [
          h("th", null, "Page"),
          h("th", null, "Rule"),
          h("th", null, "Severity"),
          h("th", null, "Source file"),
          h("th", null, "IFC GlobalId"),
          h("th", null, "Message"),
        ])
      ),
      h(
        "tbody",
        null,
        filtered.map((row, idx) =>
          h("tr", { key: idx }, [
            h("td", null, row.page),
            h("td", null, row.rule_id),
            h("td", null, row.severity),
            h("td", null, row.source_file),
            h("td", null, row.ifc_globalid),
            h("td", null, row.message),
          ])
        )
      )
    ),
  ]);
}

function ConfigPage() {
  const sessionId = useSession();
  const [status, setStatus] = useState(null);

  async function refreshStatus() {
    if (!sessionId) return;
    const resp = await fetch(`/api/ifc-qa/config/${sessionId}`);
    if (!resp.ok) return;
    const data = await resp.json();
    setStatus(data);
  }

  async function uploadZip() {
    const file = document.getElementById("qaConfigZip")?.files?.[0];
    if (!file) return;
    const form = new FormData();
    form.append("config_zip", file, file.name);
    const resp = await fetch(`/api/ifc-qa/config/${sessionId}/upload`, { method: "POST", body: form });
    if (resp.ok) refreshStatus();
  }

  async function resetDefaults() {
    const resp = await fetch(`/api/ifc-qa/config/${sessionId}/reset`, { method: "POST" });
    if (resp.ok) refreshStatus();
  }

  useEffect(() => {
    refreshStatus();
  }, [sessionId]);

  return h("div", null, [
    h("p", { className: "muted" }, "Overrides apply only to the active session."),
    h("div", { className: "inline" }, [
      h("a", { className: "btn secondary", href: `/api/ifc-qa/config/${sessionId}/download` }, "Download configs"),
      h("button", { className: "btn ghost", onClick: resetDefaults }, "Reset to defaults"),
    ]),
    h("div", { style: { marginTop: "12px" } }, [
      h("label", null, ["Upload config ZIP", h("input", { id: "qaConfigZip", type: "file", accept: ".zip" })]),
      h("button", { className: "btn", onClick: uploadZip }, "Load ZIP"),
    ]),
    status
      ? h("div", { className: "card", style: { marginTop: "12px" } }, [
          h("h4", null, "Active overrides"),
          status.overrides?.length
            ? h("ul", null, status.overrides.map((o) => h("li", { key: o }, o)))
            : h("div", { className: "muted" }, "No overrides loaded."),
        ])
      : null,
  ]);
}

function IfcQaApp() {
  const root = document.getElementById("ifc-qa-root");
  const page = root?.dataset.qaPage || "extractor";
  if (page === "dashboard") return h(DashboardPage);
  if (page === "config") return h(ConfigPage);
  return h(ExtractorPage);
}

document.addEventListener("DOMContentLoaded", () => {
  const root = document.getElementById("ifc-qa-root");
  if (!root) return;
  ReactDOM.createRoot(root).render(h(IfcQaApp));
});
