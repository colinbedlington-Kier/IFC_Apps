(() => {
  const el = (id) => document.getElementById(id);
  if (!el("fsrRun")) return;

  const fields = {
    source: el("fsrSourceFile"),
    prefix: el("fsrOutputPrefix"),
    analyse: el("fsrAnalyse"),
    run: el("fsrRun"),
    refresh: el("fsrRefreshFiles"),
    exportZip: el("fsrExportZip"),
    removeSite: el("fsrRemoveSite"),
    purgeData: el("fsrPurgeData"),
    optimise: el("fsrOptimise"),
    splitStorey: el("fsrSplitStorey"),
    warningAck: el("fsrWarningAck"),
    analysisCards: el("fsrAnalysisCards"),
    analysisJson: el("fsrAnalysisJson"),
    riskText: el("fsrRiskText"),
    status: el("fsrStatus"),
    resultCards: el("fsrResultCards"),
    resultFiles: el("fsrResultFiles"),
    summaryJson: el("fsrSummaryJson"),
    log: el("fsrLog"),
  };

  let lastAnalysis = null;

  const currentMode = () => document.querySelector('input[name="fsrMode"]:checked')?.value || "conservative_viewer_copy";

  const getSessionId = () => localStorage.getItem("ifc_session_id");

  function updateModeUi() {
    const mode = currentMode();
    const aggressive = mode === "aggressive_viewer_copy";
    fields.purgeData.disabled = !aggressive;
    fields.optimise.disabled = !aggressive;
    if (!aggressive) {
      fields.purgeData.checked = false;
      fields.optimise.checked = false;
    }
    if (mode === "compress_only") {
      fields.riskText.textContent = "Best for safest filesize reduction: Compress Only (.ifczip).";
    } else if (mode === "conservative_viewer_copy") {
      fields.riskText.textContent = "Best for coordination/viewing: Conservative Viewer Copy.";
    } else if (mode === "aggressive_viewer_copy") {
      fields.riskText.textContent = "Use with caution: Aggressive Viewer Copy. PurgeData and Optimise are destructive.";
    } else {
      fields.riskText.textContent = "Split by Storey creates multiple derived files and a manifest.";
    }
    if (mode !== "split_by_storey") {
      fields.splitStorey.checked = false;
    }
  }

  function renderAnalysis(analysis) {
    const cards = [
      ["Schema", analysis.schema],
      ["Current size", `${(analysis.current_file_size_bytes / 1024 / 1024).toFixed(2)} MB`],
      ["Entity count", String(analysis.entity_count)],
      ["Product count", String(analysis.product_count)],
      ["Site representation", analysis.site?.has_representation ? "Yes" : "No"],
      ["Recommended mode", analysis.recommendation?.default_mode || "conservative_viewer_copy"],
    ];
    fields.analysisCards.innerHTML = cards
      .map(([k, v]) => `<div class="stat-card"><div class="stat-label">${k}</div><div class="stat-value">${v}</div></div>`)
      .join("");
    fields.analysisJson.textContent = JSON.stringify(analysis, null, 2);
  }

  async function analyse() {
    const sessionId = getSessionId();
    if (!sessionId || !fields.source.value) {
      fields.status.textContent = "Select a session IFC/IFCZIP file first.";
      return;
    }
    fields.status.textContent = "Running analysis…";
    const resp = await window.withProcessing("Analysing IFC file size opportunities…", async () => fetch("/api/ifc-tools/reduce-file-size/analyse", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ session_id: sessionId, source_file: fields.source.value }),
    }));
    const data = await resp.json();
    if (!resp.ok) {
      fields.status.textContent = `Analysis failed: ${data.detail || "Unknown error"}`;
      return;
    }
    lastAnalysis = data.analysis;
    renderAnalysis(lastAnalysis);
    const recommended = data.analysis?.recommendation?.default_mode;
    const target = document.querySelector(`input[name='fsrMode'][value='${recommended}']`);
    if (target) target.checked = true;
    updateModeUi();
    fields.status.textContent = "Analysis complete. Choose reduction options and run.";
  }

  function gatherPayload() {
    const mode = currentMode();
    return {
      session_id: getSessionId(),
      source_file: fields.source.value,
      output_prefix: fields.prefix.value || "reduced",
      mode,
      export_ifczip: !!fields.exportZip.checked,
      remove_site_representation: !!fields.removeSite.checked,
      purge_data: !!fields.purgeData.checked,
      optimise_model: !!fields.optimise.checked,
      split_by_storey: mode === "split_by_storey" || !!fields.splitStorey.checked,
      warning_acknowledged: !!fields.warningAck.checked,
    };
  }

  function renderResult(result) {
    const cards = [
      ["Original", `${(result.original_size_bytes / 1024 / 1024).toFixed(2)} MB`],
      ["Reduced", `${(result.reduced_size_bytes / 1024 / 1024).toFixed(2)} MB`],
      ["Reduction", `${result.percent_reduction}%`],
    ];
    fields.resultCards.innerHTML = cards
      .map(([k, v]) => `<div class="stat-card"><div class="stat-label">${k}</div><div class="stat-value">${v}</div></div>`)
      .join("");

    fields.resultFiles.innerHTML = (result.output_files || []).map((f) => {
      const sessionId = getSessionId();
      const href = `/api/session/${sessionId}/download?name=${encodeURIComponent(f.name)}`;
      return `<div class="file-pill"><div><div class="file-name">${f.name}</div><div class="muted">${(f.size / 1024).toFixed(1)} KB</div></div><a class="ghost" href="${href}">Download</a></div>`;
    }).join("");

    fields.summaryJson.textContent = JSON.stringify(result, null, 2);
    fields.log.textContent = JSON.stringify(result.log || [], null, 2);
    if (Number(result.percent_reduction || 0) < 5) {
      fields.status.textContent = "Reduction is below 5%. Consider scope reduction (split by storey) or IFCZIP packaging only.";
    } else {
      fields.status.textContent = "Reduction complete.";
    }
  }

  async function run() {
    const payload = gatherPayload();
    if (!payload.source_file) {
      fields.status.textContent = "Choose a session IFC/IFCZIP file first.";
      return;
    }
    if (payload.mode === "aggressive_viewer_copy" && (payload.purge_data || payload.optimise_model) && !payload.warning_acknowledged) {
      fields.status.textContent = "Acknowledge the warning before running aggressive options.";
      return;
    }
    fields.status.textContent = "Running reduction workflow…";
    const resp = await window.withProcessing("Reducing IFC file size…", async () => fetch("/api/ifc-tools/reduce-file-size/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }));
    const data = await resp.json();
    if (!resp.ok) {
      fields.status.textContent = `Run failed: ${typeof data.detail === "string" ? data.detail : "See log"}`;
      fields.log.textContent = JSON.stringify(data.detail, null, 2);
      return;
    }
    renderResult(data.result);
    if (typeof refreshFiles === "function") refreshFiles();
  }

  document.querySelectorAll("input[name='fsrMode']").forEach((node) => node.addEventListener("change", updateModeUi));
  fields.analyse.addEventListener("click", analyse);
  fields.run.addEventListener("click", run);
  fields.refresh.addEventListener("click", () => typeof refreshFiles === "function" && refreshFiles());

  updateModeUi();
})();
