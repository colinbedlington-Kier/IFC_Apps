const state = {
  sessionId: null,
  files: [],
  levels: [],
  selectedLevelId: null,
  selectedFiles: new Set(),
  uploadStatusEl: null,
  uploadProgressEl: null,
  uploadState: "idle",
  step2ifcJobId: null,
  pendingActions: [],
  presentationLayer: {
    rows: [],
    page: 1,
    pageSize: 20,
    filter: "all",
    allowedCsvText: "",
    allowedFullValues: [],
    allowedSet: new Set(),
  },
  proxyPredef: {
    rows: [],
    page: 1,
    pageSize: 20,
  },
  processingCount: 0,
  updatedIfcName: null,
  excelPreview: null,
  uploadLimits: {
    maxBytes: 1_200_000_000,
    maxDisplay: "1.2 GB",
  },
};


const el = (id) => document.getElementById(id);

async function fetchWithTimeout(url, options = {}, timeoutMs = 12000) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(url, { ...options, signal: controller.signal });
    return resp;
  } finally {
    clearTimeout(timeoutId);
  }
}

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

function setUploadState(nextState) {
  state.uploadState = nextState;
  console.info("[upload] state transition", { nextState });
}

function getBadgeSessionIdText() {
  const badge = document.querySelector("[data-session-badge]") || el("sessionStatus") || el("session-pill");
  return badge?.textContent || "";
}

function updateProcessingBar(active, message, percent = null) {
  const wrap = document.querySelector("[data-processing-bar]");
  const label = document.querySelector("[data-processing-label]");
  const percentEl = document.querySelector("[data-processing-percent]");
  if (!wrap || !label) return;
  wrap.classList.toggle("active", active);
  if (message) label.textContent = message;
  if (percentEl) {
    if (typeof percent === "number" && Number.isFinite(percent)) {
      const clamped = Math.max(0, Math.min(100, Math.round(percent)));
      percentEl.textContent = `${clamped}%`;
    } else if (!active) {
      percentEl.textContent = "0%";
    }
  }
}

function startProcessing(message) {
  state.processingCount += 1;
  updateProcessingBar(true, message || "Processing files…", 15);
}

function stopProcessing() {
  state.processingCount = Math.max(0, state.processingCount - 1);
  if (state.processingCount === 0) {
    updateProcessingBar(false, "Processing files…", 0);
  } else {
    updateProcessingBar(true, "Processing files…", 80);
  }
}

async function withProcessing(message, fn) {
  startProcessing(message);
  try {
    return await fn();
  } finally {
    stopProcessing();
  }
}

window.withProcessing = withProcessing;

async function ensureSession() {
  try {
    const shared = window.IFCSession;
    const resolved = shared ? await shared.ensureSession({ createIfMissing: true }) : "";
    state.sessionId = resolved || state.sessionId;
    if (!state.sessionId) throw new Error("No active session");
    if (shared) {
      shared.setCurrentSessionId(state.sessionId);
      setSessionBadge(`Session ready • ${shared.shortSessionId(state.sessionId)}`, true);
    } else {
      setSessionBadge(`Session ready • ${state.sessionId.slice(0, 8)}`, true);
    }
    await refreshFiles();
    if (state.uploadStatusEl) state.uploadStatusEl.textContent = "Session ready.";
  } catch (err) {
    setSessionBadge("Session error", false);
    if (state.uploadStatusEl) state.uploadStatusEl.textContent = "Session error. Reload to retry.";
    console.error(err);
  }
}

async function loadUploadLimits() {
  try {
    const resp = await fetchWithTimeout("/api/upload/limits", {}, 8000);
    if (resp.ok) {
      const data = await resp.json();
      if (Number.isFinite(Number(data.max_upload_bytes))) state.uploadLimits.maxBytes = Number(data.max_upload_bytes);
      if (data.max_upload_display) state.uploadLimits.maxDisplay = data.max_upload_display;
    }
  } catch (err) {
    console.warn("Unable to load upload limits", err);
  }
  const hint = el("upload-max-size");
  if (hint) hint.textContent = `Maximum file size: ${state.uploadLimits.maxDisplay}`;
}

function parseUploadFailure(payload, statusCode) {
  if (statusCode === 413 || payload?.code === "UPLOAD_TOO_LARGE" || payload?.detail?.code === "UPLOAD_TOO_LARGE") {
    return payload?.message || payload?.detail?.message || `File exceeds the maximum upload size of ${state.uploadLimits.maxDisplay}.`;
  }
  return payload?.detail || payload?.error || `Upload failed (HTTP ${statusCode})`;
}

async function refreshFiles() {
  if (!state.sessionId) return;
  try {
    if (window.IFCSession?.getSessionFiles) {
      state.files = await window.IFCSession.getSessionFiles(state.sessionId);
    } else {
      const resp = await fetchWithTimeout(`/api/session/${state.sessionId}/files`, {}, 8000);
      if (!resp.ok) throw new Error("Could not list files");
      const data = await resp.json();
      state.files = data.files || [];
    }
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
    const filter = sel.dataset.filesFilter;
    const extensions = filter ? filter.split(",").map((ext) => ext.trim().toLowerCase()) : null;
    sel.innerHTML = "";
    if (sel.dataset.allowEmpty) {
      const opt = document.createElement("option");
      opt.value = "";
      opt.textContent = sel.dataset.emptyLabel || "Select";
      sel.appendChild(opt);
    }
    state.files.forEach((f) => {
      if (extensions && !extensions.some((ext) => f.name.toLowerCase().endsWith(ext))) {
        return;
      }
      const opt = document.createElement("option");
      opt.value = f.name;
      opt.textContent = f.name;
      sel.appendChild(opt);
    });
  });
}

function resetUploadProgress() {
  const wrap = document.querySelector("[data-upload-progress-wrap]");
  const progressEl = document.querySelector("[data-upload-progress]");
  const bar = progressEl?.querySelector(".bar");
  const pct = document.querySelector("[data-upload-percent]");
  const status = document.querySelector("[data-upload-status]");
  const bytes = document.querySelector("[data-upload-bytes]");
  const speed = document.querySelector("[data-upload-speed]");
  const panelText = el("upload-progress-text");
  const sessionDebug = el("upload-session-debug");
  const fileList = el("upload-file-progress-list");
  if (wrap) wrap.classList.remove("visible", "done", "error");
  if (progressEl) progressEl.classList.add("hidden");
  if (progressEl) progressEl.classList.remove("indeterminate");
  if (bar) bar.style.width = "0%";
  if (pct) pct.textContent = "";
  if (bytes) bytes.textContent = "";
  if (speed) speed.textContent = "";
  if (panelText) panelText.textContent = "";
  if (sessionDebug) sessionDebug.textContent = "";
  if (fileList) fileList.innerHTML = "";
  if (status) status.textContent = "Waiting to start…";
  setUploadState("idle");
}

function renderUploadPanelFromState(uploadProgress) {
  const panelText = el("upload-progress-text");
  const panelFill = document.querySelector("#upload-progress-panel .upload-progress-fill");
  if (!panelText || !panelFill || !uploadProgress) return;
  const filename = uploadProgress.filename || "file";
  const percent = Math.max(0, Math.min(100, Number(uploadProgress.percent) || 0));
  const bytesText = `${formatBytes(uploadProgress.loaded)} / ${formatBytes(uploadProgress.total)}`;
  const speedText = uploadProgress.speedBytesPerSecond > 0 ? ` — ${formatTransferSpeed(uploadProgress.speedBytesPerSecond)}` : "";
  panelText.textContent = `${uploadProgress.statusText || "Preparing upload…"} ${filename} — ${percent}% — ${bytesText}${speedText}`;
  panelFill.style.width = `${percent}%`;
}

function formatBytes(bytes) {
  const value = Math.max(0, Number(bytes) || 0);
  if (value < 1024) return `${Math.round(value)} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let scaled = value;
  let index = -1;
  while (scaled >= 1024 && index < units.length - 1) {
    scaled /= 1024;
    index += 1;
  }
  const decimals = scaled >= 100 ? 0 : scaled >= 10 ? 1 : 2;
  return `${scaled.toFixed(decimals)} ${units[index]}`;
}

function formatTransferSpeed(bytesPerSecond) {
  const bps = Math.max(0, Number(bytesPerSecond) || 0);
  return `${(bps / (1024 * 1024)).toFixed(1)} MB/s`;
}

function calculateUploadPercent(loaded, total) {
  const safeTotal = Math.max(0, Number(total) || 0);
  if (!safeTotal) return 0;
  const ratio = (Math.max(0, Number(loaded) || 0) / safeTotal) * 100;
  return Math.max(0, Math.min(100, ratio));
}

function createRollingSpeedTracker(sampleWindowMs = 5000, maxSamples = 8) {
  const samples = [];
  return {
    push(bytesLoaded, timestamp = Date.now()) {
      samples.push({ bytesLoaded: Math.max(0, Number(bytesLoaded) || 0), timestamp: Number(timestamp) || Date.now() });
      while (samples.length > maxSamples) samples.shift();
      while (samples.length > 1 && (samples[samples.length - 1].timestamp - samples[0].timestamp) > sampleWindowMs) {
        samples.shift();
      }
    },
    bytesPerSecond(now = Date.now()) {
      if (samples.length < 2) return 0;
      const first = samples[0];
      const last = samples[samples.length - 1];
      const elapsedMs = Math.max(1, last.timestamp - first.timestamp);
      const instantaneous = Math.max(0, (last.bytesLoaded - first.bytesLoaded) / (elapsedMs / 1000));
      const idleMs = Math.max(0, (Number(now) || Date.now()) - last.timestamp);
      const decay = Math.max(0, 1 - (idleMs / 4000));
      return instantaneous * decay;
    },
  };
}

function buildPerFileProgress(files, loadedBytes) {
  let remaining = Math.max(0, Number(loadedBytes) || 0);
  return files.map((file) => {
    const size = Math.max(0, Number(file.size) || 0);
    const uploadedBytes = Math.max(0, Math.min(size, remaining));
    remaining = Math.max(0, remaining - size);
    return {
      name: file.name || "file",
      uploadedBytes,
      totalBytes: size,
      percent: calculateUploadPercent(uploadedBytes, size || 1),
    };
  });
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#39;");
}

function renderPerFileProgress(rows) {
  const container = el("upload-file-progress-list");
  if (!container) return;
  if (!rows?.length) {
    container.innerHTML = "";
    return;
  }
  container.innerHTML = rows.map((row) => {
    const pct = Math.round(Math.max(0, Math.min(100, Number(row.percent) || 0)));
    const rowSpeed = Number(row.speedBytesPerSecond) > 0 ? ` — ${formatTransferSpeed(row.speedBytesPerSecond)}` : "";
    return `
      <div class="upload-file-progress-row">
        <div class="upload-file-progress-meta">
          ${escapeHtml(row.name)} — ${pct}% — ${formatBytes(row.uploadedBytes)} / ${formatBytes(row.totalBytes)}${rowSpeed}
        </div>
        <div class="progress upload-file-progress-track" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="${pct}">
          <div class="bar upload-file-progress-fill" style="width:${pct}%"></div>
        </div>
      </div>
    `;
  }).join("");
}

function updateUploadStatus(text) {
  const message = String(text ?? "");
  if (state.uploadStatusEl) state.uploadStatusEl.textContent = message;
  const statusEl = document.querySelector("[data-upload-status]");
  if (statusEl) statusEl.textContent = message;
  const fallbackEl = document.getElementById("upload-status");
  if (fallbackEl && fallbackEl !== state.uploadStatusEl) fallbackEl.textContent = message;
  console.log("[upload-status]", message);
}

function updateUploadProgress({ percent, message, done = false, error = false, indeterminate = false, bytesText = "", speedText = "", fileRows = [] }) {
  const wrap = document.querySelector("[data-upload-progress-wrap]");
  const progressEl = document.querySelector("[data-upload-progress]");
  const bar = progressEl?.querySelector(".bar");
  const pct = document.querySelector("[data-upload-percent]");
  const status = document.querySelector("[data-upload-status]");
  const bytes = document.querySelector("[data-upload-bytes]");
  const speed = document.querySelector("[data-upload-speed]");
  if (!wrap || !progressEl || !bar || !status || !pct) return;
  wrap.classList.add("visible");
  wrap.classList.toggle("done", done);
  wrap.classList.toggle("error", error);
  progressEl.classList.remove("hidden");
  progressEl.classList.toggle("indeterminate", !!indeterminate);
  if (typeof percent === "number" && Number.isFinite(percent)) {
    const clamped = Math.max(0, Math.min(100, percent));
    bar.style.width = `${clamped}%`;
    pct.textContent = `${Math.round(clamped)}%`;
    progressEl.setAttribute("aria-valuenow", String(Math.round(clamped)));
  } else if (!percent) {
    pct.textContent = "";
  }
  if (bytes) bytes.textContent = bytesText;
  if (speed) speed.textContent = speedText;
  renderPerFileProgress(fileRows);
  status.textContent = message || "";
  setUploadState(error ? "failed" : done ? "complete" : indeterminate ? "preparing" : (Number(percent) >= 100 ? "processing" : "uploading"));
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
  const mappingFile = el("step2ifcMapping")?.value?.trim();
  try {
    const resp = await fetch(`/api/session/${state.sessionId}/step2ifc/auto`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        input_file: fileSelect.value,
        output_name: outputName || null,
        mapping_file: mappingFile || null,
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

function uploadWithProgress(url, form, options = {}) {
  const { onProgress, signal } = options;
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", url);
    if (signal) {
      signal.addEventListener("abort", () => xhr.abort(), { once: true });
    }
    xhr.upload.onprogress = (evt) => {
      if (typeof onProgress === "function") {
        onProgress({
          loaded: Number(evt.loaded) || 0,
          total: Number(evt.total) || 0,
          lengthComputable: !!evt.lengthComputable,
          timestamp: Date.now(),
        });
      }
    };
    xhr.onabort = () => reject(new Error(signal?.aborted ? "Upload cancelled." : "Upload interrupted."));
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
  const activeSessionId = String(
    window.IFCSession?.getActiveSessionId?.()
    || localStorage.getItem("ifc_toolkit_session_id")
    || state.sessionId
    || ""
  ).trim();
  if (!activeSessionId) {
    alert("Session not ready yet. Please wait a moment and retry.");
    return;
  }
  state.sessionId = activeSessionId;
  if (window.IFCSession?.setCurrentSessionId) window.IFCSession.setCurrentSessionId(activeSessionId);
  if (window.IFCSession?.shortSessionId) {
    setSessionBadge(`Session ready • ${window.IFCSession.shortSessionId(activeSessionId)}`, true);
  }
  const uploadSessionDebug = el("upload-session-debug");
  if (uploadSessionDebug) uploadSessionDebug.textContent = `Uploading to session: ${activeSessionId.slice(0, 8)}`;
  console.info("[upload] session alignment", {
    activeSessionId,
    badgeSessionText: getBadgeSessionIdText(),
  });
  resetUploadProgress();
  updateUploadStatus("Uploading...");
  setUploadState("preparing");
  updateUploadProgress({
    percent: 0,
    message: "Upload started, waiting for browser progress events...",
    indeterminate: false,
    bytesText: "",
    speedText: "",
  });
  const form = new FormData();
  const files = Array.from(input.files);
  const totalBytes = files.reduce((sum, file) => sum + (Number(file.size) || 0), 0);
  for (const f of input.files) {
    if (Number(f.size) > state.uploadLimits.maxBytes) {
      updateUploadStatus(`Upload failed: ${f.name} exceeds the maximum upload size of ${state.uploadLimits.maxDisplay}.`);
      updateUploadProgress({ percent: 0, message: `Failed — ${f.name} exceeds maximum size.`, error: true });
      return;
    }
    form.append("files", f);
  }
  const speedTracker = createRollingSpeedTracker();
  const uploadProgress = {
    status: "preparing",
    statusText: "Preparing upload…",
    filename: files[0]?.name || "file",
    loaded: 0,
    total: files[0]?.size || 0,
    percent: 0,
    speedBytesPerSecond: 0,
  };
  renderUploadPanelFromState(uploadProgress);
  let sawProgressEvent = false;
  const progressTimer = setInterval(() => {
    const speedNow = speedTracker.bytesPerSecond(Date.now());
    const label = speedNow > 0 ? formatTransferSpeed(speedNow) : "0.0 MB/s";
    const current = document.querySelector("[data-upload-speed]");
    if (current && current.textContent) current.textContent = label;
    if (!sawProgressEvent) {
      updateUploadProgress({
        percent: 0,
        message: "Upload started, waiting for browser progress events...",
        bytesText: "",
        speedText: label,
        fileRows: buildPerFileProgress(files, 0),
      });
    }
  }, 600);
  const targetNames = files.map((f) => f.name);
  const uploadUrl = `/api/session/${activeSessionId}/upload`;
  console.log("[upload] activeSessionId", activeSessionId);
  console.log("[upload] uploadUrl", uploadUrl);
  try {
    const uploadPayload = await uploadWithProgress(uploadUrl, form, {
      onProgress: ({ loaded, total, lengthComputable, timestamp }) => {
        sawProgressEvent = true;
        const progressTotal = lengthComputable && total > 0 ? total : totalBytes || (files[0]?.size || 0);
        const percent = Math.round((Math.max(0, loaded) / Math.max(1, progressTotal)) * 100);
        speedTracker.push(loaded, timestamp);
        const speedValue = speedTracker.bytesPerSecond(timestamp);
        const speedLabel = formatTransferSpeed(speedValue);
        const bytesLabel = `${formatBytes(loaded)} / ${formatBytes(progressTotal)}`;
        const perFile = buildPerFileProgress(files, loaded).map((row) => ({ ...row, speedBytesPerSecond: 0 }));
        const activeFile = perFile.find((row) => row.percent < 100) || perFile[perFile.length - 1];
        if (activeFile) activeFile.speedBytesPerSecond = speedTracker.bytesPerSecond(timestamp);
        const stateText = lengthComputable ? "uploading" : "preparing";
        uploadProgress.status = stateText;
        uploadProgress.statusText = stateText === "uploading" ? "Uploading…" : "Preparing upload…";
        uploadProgress.filename = activeFile?.name || files[0]?.name || "file";
        uploadProgress.loaded = Math.max(0, loaded);
        uploadProgress.total = progressTotal || files[0]?.size || 0;
        uploadProgress.percent = Math.max(0, Math.min(100, percent));
        uploadProgress.speedBytesPerSecond = speedValue;
        renderUploadPanelFromState(uploadProgress);
        console.info("[upload] progress event", {
          loaded,
          total,
          percent: Math.round(percent),
          lengthComputable,
        });
        console.debug("Upload progress", {
          filename: activeFile?.name || files[0]?.name || "unknown",
          loadedBytes: loaded,
          totalBytes: progressTotal,
          percent: Math.round(percent),
          speedMBps: Number((speedTracker.bytesPerSecond(timestamp) / (1024 * 1024)).toFixed(2)),
        });
        updateUploadProgress({
          percent: uploadProgress.percent,
          indeterminate: !lengthComputable,
          message: activeFile ? `Uploading ${activeFile.name} — ${Math.round(percent)}% — ${bytesLabel} — ${speedLabel}` : "Uploading files…",
          bytesText: bytesLabel,
          speedText: speedLabel,
          fileRows: perFile,
        });
        if (activeFile) {
          updateUploadStatus(`Uploading ${activeFile.name} — ${Math.round(percent)}% — ${bytesLabel}`);
        } else {
          updateUploadStatus(stateText === "uploading" ? "Uploading..." : "Preparing upload…");
        }
        if (Math.round(percent) >= 100) {
          updateUploadStatus("Saving file to session storage...");
        }
      },
    });
    if (uploadPayload?.files && Array.isArray(uploadPayload.files)) {
      state.files = uploadPayload.files;
    }
  } catch (err) {
    clearInterval(progressTimer);
    const message = String(err?.message || "");
    setUploadState("failed");
    updateUploadStatus(`Upload failed: ${message || "Upload failed."}`);
    updateUploadProgress({ percent: 100, message: `Failed — ${message || "Upload failed"}. Retry upload.`, error: true, fileRows: buildPerFileProgress(files, 0) });
    return;
  }
  clearInterval(progressTimer);
  uploadProgress.status = "processing";
  uploadProgress.statusText = "Saving file to session storage...";
  uploadProgress.loaded = totalBytes;
  uploadProgress.total = totalBytes;
  uploadProgress.percent = 100;
  uploadProgress.speedBytesPerSecond = 0;
  renderUploadPanelFromState(uploadProgress);
  setUploadState("processing");
  updateUploadStatus("Saving file to session storage...");
  updateUploadProgress({
    percent: 100,
    message: "Saving file to session storage...",
    bytesText: `${formatBytes(totalBytes)} / ${formatBytes(totalBytes)}`,
    speedText: "0.0 MB/s",
    fileRows: buildPerFileProgress(files, totalBytes),
  });
  input.value = "";
  await refreshFiles();
  const names = new Set((state.files || []).map((item) => item.name));
  if (!targetNames.every((name) => names.has(name))) {
    await refreshFiles();
  }
  setUploadState("complete");
  updateUploadStatus("Complete — added to session");
  updateUploadProgress({
    percent: 100,
    message: "Complete — added to session",
    done: true,
    bytesText: `${formatBytes(totalBytes)} / ${formatBytes(totalBytes)}`,
    speedText: "0.0 MB/s",
    fileRows: buildPerFileProgress(files, totalBytes),
  });
  console.info("[upload] completed refresh", { activeSessionId, badgeSessionText: getBadgeSessionIdText() });
}

async function endSession() {
  if (!state.sessionId) return;
  await fetch(`/api/session/${state.sessionId}`, { method: "DELETE" });
  if (window.IFCSession?.setCurrentSessionId) window.IFCSession.setCurrentSessionId("");
  else localStorage.removeItem("ifc_toolkit_session_id");
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
  return withProcessing("Cleaning IFC files…", async () => {
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
  });
}

async function extractExcel() {
  const file = el("excelIfc")?.value;
  if (!file) return alert("Select an IFC file.");
  const includeSheets = [];
  const sheetMap = [
    ["sheetProjectData", "ProjectData"],
    ["sheetElements", "Elements"],
    ["sheetProperties", "Properties"],
    ["sheetCobie", "COBieMapping"],
    ["sheetUniclassPr", "Uniclass_Pr"],
    ["sheetUniclassSs", "Uniclass_Ss"],
    ["sheetUniclassEf", "Uniclass_EF"],
  ];
  sheetMap.forEach(([id, name]) => {
    if (el(id)?.checked) includeSheets.push(name);
  });
  const selectedClasses = getSelectedMultiple(el("excelEntityClassFilter") || { selectedOptions: [] });
  const selectedPsets = getSelectedMultiple(el("excelPsetFilter") || { selectedOptions: [] });
  const payload = {
    ifc_file: file,
    plan: {
      include_sheets: includeSheets,
      entity_classes: selectedClasses,
      property_sets: selectedPsets,
      cobie_pairs: (state.excelPreview?.cobie_pairs || []).map((item) => `${item.pset}.${item.property}`),
      include_type_properties: !!el("excelIncludeTypeProps")?.checked,
      include_spatial_fields: !!el("excelIncludeSpatial")?.checked,
      include_classifications: !!el("excelIncludeClassifications")?.checked,
    },
  };
  return withProcessing("Extracting Excel workbook…", async () => {
    const resp = await fetch(`/api/session/${state.sessionId}/excel/extract`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await resp.json();
    if (data.excel) {
      const timings = data.timings_ms ? JSON.stringify(data.timings_ms) : "";
      const counts = data.counts ? JSON.stringify(data.counts) : "";
      const schemaMeta = data.schema_detected ? ` | schema: ${data.schema_detected}` : "";
      const warning = data.schema_warning ? ` | warning: ${data.schema_warning}` : "";
      el("excelStatus").textContent = `Excel ready: ${data.excel.name}${schemaMeta}${warning}${timings ? ` | timings(ms): ${timings}` : ""}${counts ? ` | counts: ${counts}` : ""}`;
      await refreshFiles();
    } else {
      el("excelStatus").textContent = JSON.stringify(data);
    }
  });
}

function renderExcelPreview(preview) {
  state.excelPreview = preview;
  const classSelect = el("excelEntityClassFilter");
  const psetSelect = el("excelPsetFilter");
  if (classSelect) {
    classSelect.innerHTML = "";
    (preview.available_classes || []).forEach((item) => {
      const opt = document.createElement("option");
      opt.value = item.name;
      opt.textContent = `${item.name} (${item.count})`;
      opt.selected = true;
      classSelect.appendChild(opt);
    });
  }
  if (psetSelect) {
    psetSelect.innerHTML = "";
    (preview.available_psets || []).forEach((item) => {
      const opt = document.createElement("option");
      opt.value = item.name;
      opt.textContent = `${item.name} (${item.count})`;
      psetSelect.appendChild(opt);
    });
  }
  if (el("excelPreviewStatus")) {
    el("excelPreviewStatus").textContent = `Scan complete for ${preview.schema || "unknown schema"} model. Elements: ${preview.model_info?.elements || 0}.`;
  }
  if (el("excelPreviewMeta")) {
    const summary = {
      schema: preview.schema,
      model_info: preview.model_info,
      timings_ms: preview.timings_ms,
      class_count: (preview.available_classes || []).length,
      pset_count: (preview.available_psets || []).length,
      quantity_set_count: Object.keys(preview.quantities_by_set || {}).length,
      classification_systems: preview.classification_systems || [],
    };
    el("excelPreviewMeta").textContent = JSON.stringify(summary, null, 2);
  }
}

async function scanExcelModel() {
  const file = el("excelIfc")?.value;
  if (!file) return alert("Select an IFC file.");
  return withProcessing("Scanning IFC model for preview…", async () => {
    const resp = await fetch(`/api/session/${state.sessionId}/excel/scan`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ifc_file: file }),
    });
    const data = await resp.json();
    if (data.preview) {
      renderExcelPreview(data.preview);
    } else {
      el("excelPreviewStatus").textContent = "Scan failed.";
      el("excelPreviewMeta").textContent = JSON.stringify(data, null, 2);
    }
  });
}

async function applyExcel() {
  const ifcFile = el("excelIfcUpdate")?.value;
  const xlsFile = el("excelEditedSelection")?.value;
  if (!ifcFile || !xlsFile) return alert("Select IFC and Excel files.");
  const payload = {
    ifc_file: ifcFile,
    excel_file: xlsFile,
    add_new: document.querySelector('input[name="addNew"]:checked')?.value || "no",
  };
  const downloadUpdatedBtn = el("downloadUpdatedIfc");
  if (downloadUpdatedBtn) downloadUpdatedBtn.classList.add("hidden");
  return withProcessing("Applying Excel updates…", async () => {
    const resp = await fetch(`/api/session/${state.sessionId}/excel/update`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await resp.json();
    if (data.ifc) {
      state.updatedIfcName = data.ifc.name;
      el("excelStatus").textContent = `Updated IFC: ${data.ifc.name}`;
      if (downloadUpdatedBtn) downloadUpdatedBtn.classList.remove("hidden");
      await refreshFiles();
    } else {
      state.updatedIfcName = null;
      el("excelStatus").textContent = JSON.stringify(data);
    }
  });
}

async function parseStoreyInfo() {
  const file = el("storeyIfc")?.value;
  if (!file) return alert("Select an IFC file.");
  const payload = { ifc_file: file };
  return withProcessing("Analyzing storey data…", async () => {
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
  });
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
  return withProcessing("Applying storey changes…", async () => {
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
  });
}

async function runProxyMapper() {
  const file = el("proxyIfc")?.value;
  if (!file) return alert("Select IFC file.");
  const payload = { ifc_file: file };
  return withProcessing("Mapping proxies to IFC classes…", async () => {
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
  });
}

// ------------------------------
// Presentation Layer Purge
// ------------------------------
function plpAllowedCsvText() {
  return state.presentationLayer.allowedCsvText || "";
}

function plpFilteredRows() {
  const mode = state.presentationLayer.filter || "all";
  const search = (el("plpSearch")?.value || "").toLowerCase();
  return (state.presentationLayer.rows || []).filter((row) => {
    if (search && !(row.existing_layer || "").toLowerCase().includes(search)) return false;
    if (mode === "changed" && row.final_layer === row.existing_layer) return false;
    if (mode !== "all" && mode !== "changed" && row.status !== mode) return false;
    return true;
  });
}

function renderPresentationRows() {
  const body = el("plpResultsBody");
  if (!body) return;
  const rows = plpFilteredRows();
  const datalistId = "plpAllowedList";
  body.innerHTML = "";
  rows.forEach((row, idx) => {
    const tr = document.createElement("tr");
    const allowedWarn = row.final_layer && !state.presentationLayer.allowedSet.has(row.final_layer);
    tr.innerHTML = `
      <td><input type="checkbox" data-plp-select="${row.existing_layer}" ${row.selected ? "checked" : ""} /></td>
      <td><input type="checkbox" data-plp-apply="${row.existing_layer}" ${row.apply_change ? "checked" : ""} /></td>
      <td>${row.existing_layer}</td>
      <td>${row.status}${row.exact_match ? " (exact)" : ""}</td>
      <td>${row.suggested_layer || "-"}</td>
      <td><input list="${datalistId}" data-plp-final="${row.existing_layer}" value="${row.final_layer || ""}" class="${allowedWarn ? "danger-text" : ""}" /></td>
      <td>${row.count || 0}</td>
      <td>${Number(row.suggested_confidence || 0).toFixed(2)}</td>
    `;
    body.appendChild(tr);
  });
  if (!document.getElementById(datalistId)) {
    const list = document.createElement("datalist");
    list.id = datalistId;
    document.body.appendChild(list);
  }
  const list = document.getElementById(datalistId);
  list.innerHTML = (state.presentationLayer.allowedFullValues || []).map((v) => `<option value="${v}"></option>`).join("");

  body.querySelectorAll("[data-plp-select]").forEach((input) => input.addEventListener("change", (e) => {
    const row = state.presentationLayer.rows.find((r) => r.existing_layer === e.target.dataset.plpSelect);
    if (row) row.selected = e.target.checked;
  }));
  body.querySelectorAll("[data-plp-apply]").forEach((input) => input.addEventListener("change", (e) => {
    const row = state.presentationLayer.rows.find((r) => r.existing_layer === e.target.dataset.plpApply);
    if (row) row.apply_change = e.target.checked;
  }));
  body.querySelectorAll("[data-plp-final]").forEach((input) => input.addEventListener("input", (e) => {
    const row = state.presentationLayer.rows.find((r) => r.existing_layer === e.target.dataset.plpFinal);
    if (row) {
      row.final_layer = e.target.value.trim();
      row.apply_change = row.final_layer && row.final_layer !== row.existing_layer;
    }
  }));
  if (el("plpResultsStatus")) el("plpResultsStatus").textContent = `${rows.length} rows shown`;
}

async function parseAllowedCsv() {
  const resp = await fetch(`/api/presentation-layers/allowed-layers/parse`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ csv_text: plpAllowedCsvText(), use_uploaded_only: el("plpUseUploadedOnly")?.checked ?? false }),
  });
  const data = await resp.json();
  state.presentationLayer.allowedFullValues = data.allowed_full_values || [];
  state.presentationLayer.allowedSet = new Set(state.presentationLayer.allowedFullValues);
  if (el("plpCsvStatus")) {
    const errs = (data.errors || []).map((e) => `row ${e.row}: ${e.message}`).join("; ");
    el("plpCsvStatus").textContent = `Allowed layers: ${data.count || 0}${errs ? ` · Warnings: ${errs}` : ""}`;
  }
}

async function extractPresentationLayers() {
  const file = el("plpIfc")?.value;
  if (!file) return alert("Select IFC file.");
  if (el("plpStats")) el("plpStats").textContent = "Extracting layers from model…";
  return withProcessing("Extracting presentation layers…", async () => {
    try {
      await parseAllowedCsv();
      const resp = await fetch(`/api/session/${state.sessionId}/presentation-layer/extract`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ifc_file: file,
          allowed_csv_text: plpAllowedCsvText(),
          use_uploaded_only: el("plpUseUploadedOnly")?.checked ?? false,
          confidence_threshold: 0.7,
        }),
      });
      const data = await resp.json();
      if (!resp.ok) {
        throw new Error(data.detail || "Extraction failed");
      }
      state.presentationLayer.rows = (data.rows || []).map((r) => ({ ...r, selected: false }));
      if (el("plpStats")) {
        const s = data.summary || {};
        const mode = s.source_mode === "uniclass_ss_fallback"
          ? "Uniclass Ss fallback"
          : (s.source_mode === "ifcclass_unassigned_fallback"
            ? "IfcClass unassigned fallback"
            : (s.source_mode === "presentation_layers" ? "Presentation layers" : "No source"));
        el("plpStats").textContent = `Mode: ${mode} · Found: ${s.layers_found || 0} · Exact: ${s.exact_matches || 0} · Suggested: ${s.suggested || 0} · Unmatched: ${s.unmatched || 0} · Classification candidates: ${s.classification_candidates || 0} · Unassigned groups: ${s.unassigned_ifcclass || 0}`;
      }
      renderPresentationRows();
    } catch (err) {
      state.presentationLayer.rows = [];
      renderPresentationRows();
      if (el("plpStats")) el("plpStats").textContent = `Extraction failed: ${err?.message || err}`;
      throw err;
    }
  });
}

function plpAcceptAllSuggestions() {
  state.presentationLayer.rows.forEach((row) => {
    if (row.suggested_layer) {
      row.final_layer = row.suggested_layer;
      row.apply_change = row.final_layer !== row.existing_layer;
    }
  });
  renderPresentationRows();
}

function plpAcceptHighConfidence() {
  state.presentationLayer.rows.forEach((row) => {
    if ((row.suggested_confidence || 0) >= 0.8 && row.suggested_layer) {
      row.final_layer = row.suggested_layer;
      row.apply_change = row.final_layer !== row.existing_layer;
    }
  });
  renderPresentationRows();
}

function plpBatchSetSelected() {
  const value = (el("plpBatchValue")?.value || "").trim();
  if (!value) return;
  state.presentationLayer.rows.forEach((row) => {
    if (row.selected) {
      row.final_layer = value;
      row.apply_change = row.final_layer !== row.existing_layer;
    }
  });
  renderPresentationRows();
}

function plpFillDown() {
  const selected = state.presentationLayer.rows.filter((row) => row.selected);
  if (!selected.length) return;
  const first = selected[0].final_layer || "";
  if (!first) return;
  selected.forEach((row) => {
    row.final_layer = first;
    row.apply_change = row.final_layer !== row.existing_layer;
  });
  renderPresentationRows();
}

async function applyPresentationLayers() {
  const file = el("plpIfc")?.value;
  if (!file) return alert("Select IFC file.");
  if (!state.presentationLayer.rows.length) return alert("Run extraction first.");
  const rows = state.presentationLayer.rows;
  const resp = await fetch(`/api/session/${state.sessionId}/presentation-layer/purge/apply`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ifc_file: file, rows, options: { update_both: true, remove_orphans: true } }),
  });
  const data = await resp.json();
  if (el("plpApplyStatus")) {
    const s = data.summary || {};
    el("plpApplyStatus").textContent = `Changed: ${s.changed || 0} · Unmatched: ${s.unmatched || 0}`;
  }
  await refreshFiles();
  const downloads = el("plpDownloads");
  if (!downloads) return;
  downloads.innerHTML = "";
  [data.ifc, data.log_json, data.log_csv].forEach((output) => {
    if (!output) return;
    const row = document.createElement("div");
    row.className = "file-row";
    row.innerHTML = `<span class="file-name">${output.name}</span>`;
    const btn = document.createElement("button");
    btn.className = "btn secondary sm";
    btn.textContent = "Download";
    btn.addEventListener("click", () => downloadFile(output.name));
    row.appendChild(btn);
    downloads.appendChild(row);
  });
}
// ------------------------------
// Proxy → PredefinedType fixer
// ------------------------------
function renderProxyPredefRows() {
  const body = el("proxyPredefBody");
  if (!body) return;
  const { page, pageSize } = state.proxyPredef;
  const rows = state.proxyPredef.rows;
  const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
  state.proxyPredef.page = Math.min(page, totalPages);
  const start = (state.proxyPredef.page - 1) * pageSize;
  const pageRows = rows.slice(start, start + pageSize);
  body.innerHTML = "";
  pageRows.forEach((row) => {
    const canApply = row.target_source !== "none" && row.proposed_predefined_type !== "";
    const checked = row.apply ?? row.apply_default;
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><input type="checkbox" data-proxy-predef-apply="${row.row_id}" ${checked ? "checked" : ""} ${canApply ? "" : "disabled"} /></td>
      <td>${row.globalid}</td>
      <td>${row.ifc_class}</td>
      <td>${row.type_name || "-"}</td>
      <td>${row.parsed_class || "-"}</td>
      <td>${row.resolved_type_class || "-"}</td>
      <td>${row.parsed_predef_token || "-"}</td>
      <td>${row.match_found ? "Yes" : "No"}</td>
      <td>${row.match_source || "none"}</td>
      <td>${row.matched_pset_name || "-"}</td>
      <td>${row.proposed_predefined_type}</td>
      <td>${row.target_source || "none"}</td>
      <td>${row.predef_reason || "-"}</td>
    `;
    body.appendChild(tr);
  });
  body.querySelectorAll("[data-proxy-predef-apply]").forEach((input) => {
    input.addEventListener("change", (e) => {
      const rowId = e.target.dataset.proxyPredefApply;
      const row = state.proxyPredef.rows.find((r) => r.row_id === rowId);
      if (row) row.apply = e.target.checked;
    });
  });
  if (el("proxyPredefPageInfo")) {
    el("proxyPredefPageInfo").textContent = `Page ${state.proxyPredef.page} of ${totalPages}`;
  }
}

async function loadProxyPredefClasses() {
  const file = el("proxyPredefIfc")?.value;
  const select = el("proxyPredefClasses");
  if (!file || !select) return;
  const resp = await fetch(`/api/session/${state.sessionId}/proxy/predefined/classes`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ifc_file: file }),
  });
  const data = await resp.json();
  select.innerHTML = "";
  (data.classes || []).forEach((cls) => {
    const opt = document.createElement("option");
    opt.value = cls;
    opt.textContent = cls;
    if (cls === "IfcBuildingElementProxy") opt.selected = true;
    select.appendChild(opt);
  });
}

async function scanProxyPredefined() {
  const file = el("proxyPredefIfc")?.value;
  if (!file) return alert("Select IFC file.");
  const select = el("proxyPredefClasses");
  const classes = Array.from(select?.selectedOptions || []).map((o) => o.value);
  if (el("proxyPredefStatus")) el("proxyPredefStatus").textContent = "Scanning…";
  return withProcessing("Scanning proxy predefined types…", async () => {
    const resp = await fetch(`/api/session/${state.sessionId}/proxy/predefined/scan`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ifc_file: file, classes }),
    });
    const data = await resp.json();
    state.proxyPredef.rows = (data.rows || []).map((row) => ({ ...row, apply: row.apply_default }));
    state.proxyPredef.page = 1;
    if (el("proxyPredefStatus")) {
      el("proxyPredefStatus").textContent = `Rows: ${state.proxyPredef.rows.length}`;
    }
    renderProxyPredefRows();
  });
}

async function applyProxyPredefined() {
  if (el("proxyPredefDryRun")?.checked) {
    if (el("proxyPredefApplyStatus")) el("proxyPredefApplyStatus").textContent = "Dry run enabled. No changes applied.";
    return;
  }
  if (!el("proxyPredefToggle")?.checked) {
    if (el("proxyPredefApplyStatus")) el("proxyPredefApplyStatus").textContent = "Toggle is off; no changes applied.";
    return;
  }
  const file = el("proxyPredefIfc")?.value;
  if (!file) return alert("Select IFC file.");
  const rows = state.proxyPredef.rows.filter((row) => row.apply);
  if (!rows.length) return alert("No rows selected.");
  if (el("proxyPredefApplyStatus")) el("proxyPredefApplyStatus").textContent = "Applying updates…";
  return withProcessing("Applying proxy predefined updates…", async () => {
    const resp = await fetch(`/api/session/${state.sessionId}/proxy/predefined/apply`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ifc_file: file, rows }),
    });
    const data = await resp.json();
    if (el("proxyPredefApplyStatus")) el("proxyPredefApplyStatus").textContent = "Export ready.";
    await refreshFiles();
    const downloads = el("proxyPredefDownloads");
    if (!downloads) return;
    downloads.innerHTML = "";
    [data.ifc, data.log_json, data.log_csv].forEach((output) => {
      if (!output) return;
      const row = document.createElement("div");
      row.className = "file-row";
      row.innerHTML = `<span class="file-name">${output.name}</span>`;
      const btn = document.createElement("button");
      btn.className = "btn secondary sm";
      btn.textContent = "Download";
      btn.addEventListener("click", () => downloadFile(output.name));
      row.appendChild(btn);
      downloads.appendChild(row);
    });
  });
}


async function uploadExcelSource() {
  const input = el("excelUploadInput");
  if (!input || !input.files?.length) return;
  if (!state.sessionId) return alert("Session not ready yet.");
  const form = new FormData();
  for (const f of input.files) form.append("files", f);
  const resp = await fetch(`/api/session/${state.sessionId}/upload`, { method: "POST", body: form });
  if (!resp.ok) {
    el("excelStatus").textContent = "Excel upload failed.";
    return;
  }
  await refreshFiles();
  const first = input.files[0]?.name;
  if (first && el("excelEditedSelection")) el("excelEditedSelection").value = first;
  el("excelStatus").textContent = "Excel uploaded to session files.";
  input.value = "";
}

async function downloadSelectedExcel() {
  const name = el("excelFileUpdate")?.value;
  if (!name) return alert("Select an Excel source file first.");
  await downloadFile(name);
}

function triggerExcelUpload() {
  el("excelUploadInput")?.click();
}

async function downloadUpdatedIfcFile() {
  if (!state.updatedIfcName) return alert("Run Apply Excel → IFC first.");
  await downloadFile(state.updatedIfcName);
}

async function downloadFile(name) {
  if (!name) return;
  const url = `/api/session/${state.sessionId}/download?name=${encodeURIComponent(name)}`;
  const link = document.createElement("a");
  link.href = url;
  link.download = name;
  link.rel = "noopener";
  document.body.appendChild(link);
  link.click();
  link.remove();
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
  return withProcessing("Saving level changes…", async () => {
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
  });
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

async function startDataExtraction() {
  const ifcInput = el("dataIfcFiles");
  if (!ifcInput) return;
  const ifcFiles = Array.from(ifcInput.files || []);
  if (!ifcFiles.length) return alert("Select at least one IFC file.");
  const tableValues = Array.from(document.querySelectorAll("#dataTables input:checked")).map((cb) => cb.value);
  if (!tableValues.length) return alert("Select at least one table.");

  const excludeFile = el("dataExcludeFilter")?.files?.[0] || null;
  const psetFile = el("dataPsetTemplate")?.files?.[0] || null;
  const form = new FormData();
  const uploadOrder = [];
  ifcFiles.forEach((file) => {
    form.append("files", file, file.name);
    uploadOrder.push(file);
  });
  if (excludeFile) {
    form.append("files", excludeFile, excludeFile.name);
    uploadOrder.push(excludeFile);
  }
  if (psetFile) {
    form.append("files", psetFile, psetFile.name);
    uploadOrder.push(psetFile);
  }

  el("dataExtractorStatus").textContent = "Uploading inputs…";
  const uploadResp = await fetch(`/api/session/${state.sessionId}/upload`, { method: "POST", body: form });
  if (!uploadResp.ok) {
    el("dataExtractorStatus").textContent = "Upload failed.";
    return;
  }
  const uploadData = await uploadResp.json();
  const saved = uploadData.files || [];
  const savedNames = saved.map((f) => f.name);
  const uploadMap = {};
  uploadOrder.forEach((file, index) => {
    uploadMap[file.name] = savedNames[index];
  });
  const excludeName = excludeFile ? uploadMap[excludeFile.name] : null;
  const psetName = psetFile ? uploadMap[psetFile.name] : null;

  const payload = {
    ifc_files: ifcFiles.map((f) => uploadMap[f.name] || f.name),
    exclude_filter: excludeName,
    pset_template: psetName,
    pset_template_default: el("dataPsetDefault")?.value || "GPA_Pset_Template.csv",
    tables: tableValues,
    regex_overrides: {
      regex_ifc_name: el("regex_ifc_name")?.value || "",
      regex_ifc_type: el("regex_ifc_type")?.value || "",
      regex_ifc_system: el("regex_ifc_system")?.value || "",
      regex_ifc_layer: el("regex_ifc_layer")?.value || "",
      regex_ifc_name_code: el("regex_ifc_name_code")?.value || "",
      regex_ifc_type_code: el("regex_ifc_type_code")?.value || "",
      regex_ifc_system_code: el("regex_ifc_system_code")?.value || "",
    },
  };

  el("dataExtractorLog").value = "";
  el("dataExtractorDownload").innerHTML = "";
  const previewTable = el("dataExtractorPreview");
  if (previewTable) previewTable.innerHTML = "";

  el("dataExtractorStatus").textContent = "Starting extraction…";
  const resp = await fetch(`/api/session/${state.sessionId}/data-extractor/start`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!resp.ok) {
    el("dataExtractorStatus").textContent = "Failed to start extraction.";
    return;
  }
  const data = await resp.json();
  pollDataExtraction(data.status_url, data.result_url);
}

function renderPreviewTable(preview) {
  const table = el("dataExtractorPreview");
  if (!table) return;
  table.innerHTML = "";
  if (!preview || !preview.columns || !preview.rows) {
    table.innerHTML = "<tr><td class=\"muted\">No preview available.</td></tr>";
    return;
  }
  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");
  preview.columns.forEach((col) => {
    const th = document.createElement("th");
    th.textContent = col;
    headRow.appendChild(th);
  });
  thead.appendChild(headRow);
  table.appendChild(thead);
  const tbody = document.createElement("tbody");
  preview.rows.forEach((row) => {
    const tr = document.createElement("tr");
    row.forEach((cell) => {
      const td = document.createElement("td");
      td.textContent = cell;
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
}

function pollDataExtraction(statusUrl, resultUrl = null) {
  if (!statusUrl) return;
  const progressBar = el("dataExtractorProgress");
  const statusEl = el("dataExtractorStatus");
  const logEl = el("dataExtractorLog");
  const downloadEl = el("dataExtractorDownload");
  const poll = async () => {
    const resp = await fetch(statusUrl);
    if (!resp.ok) {
      statusEl.textContent = "Status check failed.";
      return;
    }
    const data = await resp.json();
    if (progressBar) progressBar.style.width = `${data.progress || 0}%`;
    statusEl.textContent = data.message || "Processing…";
    if (logEl && data.logs) logEl.value = data.logs.join("\n");

    const terminal = data.done || ["done", "failed", "canceled"].includes(data.status);
    if (terminal) {
      if (data.error || data.status === "failed") {
        statusEl.textContent = data.error || data.message || "Extraction failed.";
        return;
      }
      const finalResultUrl = resultUrl || statusUrl.replace(/\/jobs\/([^/]+)$/, "/jobs/$1/result");
      const resultResp = await fetch(finalResultUrl);
      if (resultResp.ok) {
        const result = await resultResp.json();
        if (downloadEl && result.outputs && result.outputs.length) {
          const link = result.outputs[0];
          downloadEl.innerHTML = `<a href="${link.url}">Download ZIP (${link.name})</a>`;
        }
        if (result.preview) renderPreviewTable(result.preview);
      }
      return;
    }
    setTimeout(poll, 1200);
  };
  poll();
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
  const scanExcelBtn = el("scanExcelModel");
  if (scanExcelBtn) scanExcelBtn.addEventListener("click", scanExcelModel);

  const applyExcelBtn = el("applyExcel");
  if (applyExcelBtn) applyExcelBtn.addEventListener("click", applyExcel);

  const excelUploadInput = el("excelUploadInput");
  if (excelUploadInput) excelUploadInput.addEventListener("change", uploadExcelSource);

  const triggerExcelUploadBtn = el("triggerExcelUpload");
  if (triggerExcelUploadBtn) triggerExcelUploadBtn.addEventListener("click", triggerExcelUpload);

  const downloadExcelBtn = el("downloadExcelFile");
  if (downloadExcelBtn) downloadExcelBtn.addEventListener("click", downloadSelectedExcel);

  const downloadUpdatedIfcBtn = el("downloadUpdatedIfc");
  if (downloadUpdatedIfcBtn) downloadUpdatedIfcBtn.addEventListener("click", downloadUpdatedIfcFile);

  const parseStoreysBtn = el("parseStoreys");
  if (parseStoreysBtn) parseStoreysBtn.addEventListener("click", parseStoreyInfo);

  const applyStoreysBtn = el("applyStoreys");
  if (applyStoreysBtn) applyStoreysBtn.addEventListener("click", applyStoreyChanges);

  const proxyBtn = el("runProxy");
  if (proxyBtn) proxyBtn.addEventListener("click", runProxyMapper);

  const plpExtract = el("plpExtract");
  if (plpExtract) plpExtract.addEventListener("click", extractPresentationLayers);
  const plpExtractPrimary = el("plpExtractPrimary");
  if (plpExtractPrimary) plpExtractPrimary.addEventListener("click", extractPresentationLayers);
  const plpApply = el("plpApply");
  if (plpApply) plpApply.addEventListener("click", applyPresentationLayers);
  const plpAcceptAll = el("plpAcceptAll");
  if (plpAcceptAll) plpAcceptAll.addEventListener("click", plpAcceptAllSuggestions);
  const plpAcceptHigh = el("plpAcceptHigh");
  if (plpAcceptHigh) plpAcceptHigh.addEventListener("click", plpAcceptHighConfidence);
  const plpSetSelected = el("plpSetSelected");
  if (plpSetSelected) plpSetSelected.addEventListener("click", plpBatchSetSelected);
  const plpFillDownBtn = el("plpFillDown");
  if (plpFillDownBtn) plpFillDownBtn.addEventListener("click", plpFillDown);
  const plpSearch = el("plpSearch");
  if (plpSearch) plpSearch.addEventListener("input", renderPresentationRows);
  document.querySelectorAll("[data-plp-filter]").forEach((btn) => {
    btn.addEventListener("click", () => {
      state.presentationLayer.filter = btn.dataset.plpFilter || "all";
      renderPresentationRows();
    });
  });
  const allowedFileInput = el("plpAllowedFile");
  if (allowedFileInput) {
    allowedFileInput.addEventListener("change", (e) => {
      const file = e.target.files?.[0];
      if (!file) return;
      const reader = new FileReader();
      reader.onload = async () => {
        state.presentationLayer.allowedCsvText = String(reader.result || "");
        await parseAllowedCsv();
      };
      reader.readAsText(file);
    });
  }
  const plpUseUploadedOnly = el("plpUseUploadedOnly");
  if (plpUseUploadedOnly) plpUseUploadedOnly.addEventListener("change", parseAllowedCsv);
  const plpDownloadTemplate = el("plpDownloadTemplate");
  if (plpDownloadTemplate) {
    plpDownloadTemplate.addEventListener("click", () => {
      window.open(`/api/presentation-layers/template`, "_blank");
    });
  }

  const proxyPredefIfc = el("proxyPredefIfc");
  if (proxyPredefIfc) {
    proxyPredefIfc.addEventListener("change", loadProxyPredefClasses);
    if (proxyPredefIfc.value) loadProxyPredefClasses();
  }
  const proxyPredefScan = el("proxyPredefScan");
  if (proxyPredefScan) proxyPredefScan.addEventListener("click", scanProxyPredefined);
  const proxyPredefApply = el("proxyPredefApply");
  if (proxyPredefApply) proxyPredefApply.addEventListener("click", applyProxyPredefined);
  const proxyPredefPrev = el("proxyPredefPrev");
  if (proxyPredefPrev) {
    proxyPredefPrev.addEventListener("click", () => {
      state.proxyPredef.page = Math.max(1, state.proxyPredef.page - 1);
      renderProxyPredefRows();
    });
  }
  const proxyPredefNext = el("proxyPredefNext");
  if (proxyPredefNext) {
    proxyPredefNext.addEventListener("click", () => {
      state.proxyPredef.page += 1;
      renderProxyPredefRows();
    });
  }

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

  const dataExtractorBtn = el("dataExtractorStart");
  if (dataExtractorBtn) dataExtractorBtn.addEventListener("click", startDataExtraction);

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

  const step2ifcMapping = el("step2ifcMapping");
  if (step2ifcMapping) {
    step2ifcMapping.addEventListener("change", () => {
      if (step2ifcMapping.value) {
        updateStep2ifcProgress({ message: "Using selected mapping file." });
      }
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
  await loadUploadLimits();
  wireEvents();
  renderPendingChanges();
  ensureSession();
});
