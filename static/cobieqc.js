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
})();
