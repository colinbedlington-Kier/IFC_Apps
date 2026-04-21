(function () {
  const form = document.getElementById("cobieqcForm");
  if (!form) return;

  const fileInput = document.getElementById("cobieqcFile");
  const runBtn = document.getElementById("cobieqcRun");
  const logsEl = document.getElementById("cobieqcLogs");
  const statusEl = document.getElementById("cobieqcStatus");
  const pctEl = document.getElementById("cobieqcProgressPct");
  const barEl = document.getElementById("cobieqcProgressBar");
  const resultWrap = document.getElementById("cobieqcResult");
  const previewEl = document.getElementById("cobieqcPreview");
  const downloadEl = document.getElementById("cobieqcDownload");
  const errorEl = document.getElementById("cobieqcError");
  const runtimeStatusEl = document.getElementById("cobieqcRuntimeStatus");

  const missingRuntimeMessage =
    "COBieQC runtime package is not installed or could not be restored from configured asset sources.";

  let pollTimer = null;

  function setProgress(value, message) {
    const percent = Math.max(0, Math.min(100, Math.round((value || 0) * 100)));
    pctEl.textContent = `${percent}%`;
    barEl.style.width = `${percent}%`;
    statusEl.textContent = message || "";
  }

  fileInput.addEventListener("change", () => {
    const file = fileInput.files && fileInput.files[0];
    runBtn.disabled = !file;
  });

  async function checkRuntimeAvailability() {
    try {
      const resp = await fetch("/health");
      if (!resp.ok) return;
      const payload = await resp.json();
      const qc = payload?.cobieqc || {};
      const missingFiles = Array.isArray(qc.missing_files) ? qc.missing_files : [];
      const warnings = Array.isArray(qc.warnings) ? qc.warnings : [];
      const sourceMode = qc.source_mode || "unknown";
      const resourcesReady = Boolean(qc.resources_ready);
      const jarReady = Boolean(qc.jar_ready);

      if (!resourcesReady || !jarReady || !payload?.cobieqc?.enabled) {
        runBtn.disabled = true;
        fileInput.disabled = !resourcesReady;
        errorEl.style.display = "block";
        errorEl.textContent = payload?.cobieqc?.last_error || missingRuntimeMessage;

        runtimeStatusEl.style.display = "block";
        const lines = [
          `<strong>COBieQC runtime status:</strong>`,
          `Source mode: <code>${sourceMode}</code>`,
          `JAR ready: <code>${jarReady}</code>`,
          `Resources ready: <code>${resourcesReady}</code>`,
        ];
        if (missingFiles.length) {
          lines.push(`Missing files: <code>${missingFiles.join(", ")}</code>`);
        }
        if (warnings.length) {
          lines.push(`Warnings: <code>${warnings.join(" | ")}</code>`);
        }
        if ((qc.errors || []).length) {
          lines.push(`Errors: <code>${qc.errors.join(" | ")}</code>`);
        }
        if (sourceMode === "unsupported_google_drive_folder") {
          lines.push("Google Drive folder URLs are unsupported. Configure COBIEQC_XML_FILE_URLS_JSON with direct file URLs.");
        }
        runtimeStatusEl.innerHTML = lines.join("<br/>");
      }
    } catch (_err) {
      // Best effort only.
    }
  }

  async function pollJob(jobId) {
    if (pollTimer) clearTimeout(pollTimer);
    try {
      const resp = await fetch(`/api/tools/cobieqc/jobs/${jobId}`);
      if (!resp.ok) throw new Error(`Status check failed (${resp.status})`);
      const data = await resp.json();
      setProgress(data.progress, data.message || data.status);
      logsEl.textContent = data.logs_tail || "(no logs yet)";

      if (data.status === "done") {
        await fetchResult(jobId);
        return;
      }
      if (data.status === "error") {
        errorEl.style.display = "block";
        errorEl.innerHTML = `<strong>COBieQC failed:</strong> ${data.message || "Unknown error"}`;
        return;
      }
      pollTimer = setTimeout(() => pollJob(jobId), 1500);
    } catch (err) {
      errorEl.style.display = "block";
      errorEl.textContent = err.message;
    }
  }

  async function fetchResult(jobId) {
    const resp = await fetch(`/api/tools/cobieqc/jobs/${jobId}/result`);
    if (!resp.ok) {
      throw new Error(`Result request failed (${resp.status})`);
    }
    const data = await resp.json();
    if (!data.ok) {
      errorEl.style.display = "block";
      errorEl.innerHTML = `<strong>Job ended with errors:</strong> ${data.message || "Unknown error"}`;
      return;
    }

    resultWrap.style.display = "block";
    previewEl.srcdoc = data.preview_html || "<html><body><p>No preview available.</p></body></html>";
    downloadEl.href = `/api/tools/cobieqc/jobs/${jobId}/download`;
  }

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (pollTimer) clearTimeout(pollTimer);
    errorEl.style.display = "none";
    resultWrap.style.display = "none";

    const file = fileInput.files && fileInput.files[0];
    if (!file) return;

    const stage = form.querySelector('input[name="cobieStage"]:checked')?.value || "D";
    const body = new FormData();
    body.append("file", file);
    body.append("stage", stage);

    runBtn.disabled = true;
    setProgress(0.02, "Uploading COBie workbook...");
    logsEl.textContent = "Starting job...";

    try {
      const resp = await fetch("/api/tools/cobieqc/run", { method: "POST", body });
      if (!resp.ok) {
        const payload = await resp.json().catch(() => ({}));
        throw new Error(payload.detail || `Run request failed (${resp.status})`);
      }
      const data = await resp.json();
      await pollJob(data.job_id);
    } catch (err) {
      errorEl.style.display = "block";
      errorEl.textContent = err.message;
    } finally {
      runBtn.disabled = false;
    }
  });

  checkRuntimeAvailability();
})();
