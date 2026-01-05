const state = {
  sessionId: null,
  files: [],
  viewer: null,
  activeModelID: null,
};

let IFCViewerAPI = null;
let THREE = null;
const VIEWER_SOURCES = {
  viewer: [
    { label: "local", url: "/static/vendor/web-ifc-viewer/IFCViewerAPI.js" },
    { label: "jsdelivr", url: "https://cdn.jsdelivr.net/npm/web-ifc-viewer@1.0.172/dist/IFCViewerAPI.js" },
    { label: "unpkg", url: "https://unpkg.com/web-ifc-viewer@1.0.172/dist/IFCViewerAPI.js" },
  ],
  three: [
    { label: "local", url: "/static/vendor/three/three.module.js" },
    { label: "jsdelivr", url: "https://cdn.jsdelivr.net/npm/three@0.164.1/build/three.module.js" },
    { label: "unpkg", url: "https://unpkg.com/three@0.164.1/build/three.module.js" },
  ],
  wasm: {
    local: "/static/vendor/web-ifc/",
    cdn: [
      "https://cdn.jsdelivr.net/npm/web-ifc@0.0.50/",
      "https://unpkg.com/web-ifc@0.0.50/",
    ],
  },
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
    setStatus(state.files.length ? "Select an IFC to view." : "Session ready. Upload a file first.");
  } catch (err) {
    console.error(err);
    setSessionBadge("Session error", false);
    setStatus("Session error. Check connectivity.");
  }
}

async function refreshFiles() {
  if (!state.sessionId) return;
  try {
    const resp = await fetch(`/api/session/${state.sessionId}/files`);
    if (!resp.ok) throw new Error("Could not list files");
    const data = await resp.json();
    state.files = data.files || [];
    populateFileSelect();
    setStatus(state.files.length ? "Select an IFC to view." : "No session files yet.");
  } catch (err) {
    console.error(err);
    setStatus("Could not load files.");
  }
}

function populateFileSelect() {
  const select = el("viewer-files");
  if (!select) return;
  select.innerHTML = "";
  state.files.forEach((f) => {
    const opt = document.createElement("option");
    opt.value = f.name;
    opt.textContent = `${f.name} (${(f.size / 1024).toFixed(1)} KB)`;
    select.appendChild(opt);
  });
}

function setStatus(text) {
  const status = el("viewer-status");
  if (status) status.textContent = text;
}

async function importWithFallback(sources, label) {
  const errors = [];
  for (const src of sources) {
    try {
      const mod = await import(src.url);
      return { mod, source: src.label };
    } catch (err) {
      errors.push(`${src.label}: ${err?.message || err}`);
    }
  }
  const error = new Error(`Could not load ${label} from any source`);
  error.loadErrors = errors;
  throw error;
}

async function loadViewerLibs() {
  if (IFCViewerAPI && THREE) return;
  try {
    const [{ mod: viewerMod, source: viewerSource }, { mod: threeMod, source: threeSource }] = await Promise.all([
      importWithFallback(VIEWER_SOURCES.viewer, "web-ifc-viewer"),
      importWithFallback(VIEWER_SOURCES.three, "three.js"),
    ]);
    IFCViewerAPI = viewerMod.IFCViewerAPI;
    THREE = threeMod;
    console.info(`Viewer loaded from ${viewerSource}; three.js from ${threeSource}`);
  } catch (err) {
    console.error("Viewer dependencies failed to load", err);
    const detailList = Array.isArray(err.loadErrors) ? err.loadErrors : [];
    const message = `Could not load viewer dependencies: ${err?.message || err}`;
    setStatus(message);
    renderDiagnostics("Viewer dependencies failed", [
      "Provide local viewer assets under static/vendor or allow CDN access to jsdelivr/unpkg.",
      ...detailList,
    ].filter(Boolean));
    const dependencyError = new Error(message);
    dependencyError.dependencyFailure = true;
    throw dependencyError;
  }
}

async function createViewer() {
  await loadViewerLibs();
  const container = el("viewer-canvas");
  if (!container) return null;
  container.innerHTML = "";
  const viewer = new IFCViewerAPI({
    container,
    backgroundColor: new THREE.Color(0x0b0e16),
  });
  viewer.axes.setAxes();
  viewer.grid.setGrid(30, 30);
  viewer.IFC.setWasmPath(VIEWER_SOURCES.wasm.local);
  viewer.wasmSources = VIEWER_SOURCES.wasm;
  state.activeModelID = null;
  return viewer;
}

async function loadIfcWithFallback(file) {
  const errors = [];
  try {
    return await state.viewer.IFC.loadIfcFile(file, true);
  } catch (err) {
    errors.push(`local: ${err?.message || err}`);
    console.warn("Local WASM load failed, retrying with CDN", err);
    for (const src of VIEWER_SOURCES.wasm.cdn) {
      try {
        state.viewer.IFC.setWasmPath(src);
        return await state.viewer.IFC.loadIfcFile(file, true);
      } catch (cdnErr) {
        errors.push(`${src}: ${cdnErr?.message || cdnErr}`);
      }
    }
    const finalErr = new Error("All WASM sources failed");
    finalErr.dependencyFailure = true;
    finalErr.loadErrors = errors;
    throw finalErr;
  }
}

async function loadSelectedFile() {
  const select = el("viewer-files");
  if (!select || !select.value) {
    alert("Choose a session file to view.");
    return;
  }
  const fileName = select.value;
  setStatus(`Downloading ${fileName}…`);
  try {
    const resp = await fetch(
      `/api/session/${state.sessionId}/download?name=${encodeURIComponent(fileName)}`
    );
    if (!resp.ok) throw new Error("Download failed");
    const blob = await resp.blob();
    const file = new File([blob], fileName);

    state.viewer?.dispose?.();
    try {
      state.viewer = await createViewer();
    } catch (err) {
      renderDiagnostics("Viewer initialization failed", [
        "Could not start the IFC viewer.",
        err?.message ? `Error: ${err.message}` : null,
        "If this persists, verify CDN access for viewer assets.",
      ].filter(Boolean));
      throw err;
    }
    if (!state.viewer) throw new Error("Viewer could not start");
    let model;
    try {
      model = await loadIfcWithFallback(file);
    } catch (err) {
      renderDiagnostics("IFC parsing failed", [
        `File: ${fileName}`,
        err?.message ? `Error: ${err.message}` : null,
        "Ensure the IFC file is valid and supported.",
      ].filter(Boolean));
      throw new Error(`IFC parsing failed: ${err?.message || "Unknown error"}`);
    }
    state.activeModelID = model.modelID;
    togglePanel("layers-panel", el("toggle-layers")?.checked ?? true);
    togglePanel("properties-panel", el("toggle-properties")?.checked ?? true);
    toggleEdges(el("toggle-edges")?.checked ?? false);
    toggleSection(el("toggle-section")?.checked ?? false);
    setStatus(`Loaded ${fileName}. Click elements to see properties.`);
    await populateLayers();
    wireViewerInteractions();
  } catch (err) {
    console.error(err);
    const baseMessage = err?.message || "Unknown error";
    const dependencyHint = err?.dependencyFailure
      ? " Check CDN/network access for viewer assets."
      : "";
    const statusMessage = `Could not load IFC: ${baseMessage}${dependencyHint}`;
    setStatus(statusMessage);
    if (!el("properties-list")?.innerHTML) {
      renderDiagnostics("Viewer error", [
        err?.message ? `Error: ${err.message}` : "An unexpected error occurred.",
        ...(Array.isArray(err?.loadErrors) ? err.loadErrors : []),
      ]);
    }
  }
}

async function populateLayers() {
  const wrap = el("layers-list");
  if (!wrap) return;
  wrap.innerHTML = "";
  if (!state.viewer || state.activeModelID === null) {
    wrap.textContent = "Load an IFC to see its spatial tree.";
    return;
  }
  const tree = await state.viewer.IFC.getSpatialStructure(state.activeModelID);
  const ul = document.createElement("ul");
  ul.className = "tree";
  buildTree(tree, ul);
  wrap.appendChild(ul);
}

function buildTree(node, parent) {
  if (!node) return;
  const li = document.createElement("li");
  li.innerHTML = `<span>${node.type || "Item"} ${node.expressID ? `(#${node.expressID})` : ""}</span>`;
  parent.appendChild(li);
  if (node.children && node.children.length) {
    const childList = document.createElement("ul");
    node.children.forEach((child) => buildTree(child, childList));
    li.appendChild(childList);
  }
}

async function onSceneClick() {
  if (!state.viewer) return;
  const result = await state.viewer.IFC.selector.pickIfcItem(true);
  if (!result) return;
  const props = await state.viewer.IFC.loader.ifcManager.getItemProperties(
    result.modelID,
    result.id,
    true
  );
  renderProperties(props);
}

function renderProperties(props) {
  const wrap = el("properties-list");
  if (!wrap) return;
  wrap.innerHTML = "";
  if (!props) {
    wrap.textContent = "Select an element to see properties.";
    return;
  }
  const entries = Object.entries(props)
    .filter(([, v]) => v !== null && v !== undefined && v !== "")
    .map(([k, v]) => `<div class="prop-row"><span>${k}</span><strong>${v?.value || v}</strong></div>`);
  wrap.innerHTML = entries.join("") || "No properties available.";
}

function renderDiagnostics(title, details = []) {
  const wrap = el("properties-list");
  if (!wrap) return;
  wrap.innerHTML = "";
  const heading = document.createElement("div");
  heading.className = "prop-row";
  heading.innerHTML = `<strong>${title}</strong>`;
  wrap.appendChild(heading);
  if (details.length) {
    const list = document.createElement("ul");
    list.className = "diagnostics";
    details.forEach((text) => {
      const li = document.createElement("li");
      li.textContent = text;
      list.appendChild(li);
    });
    wrap.appendChild(list);
  }
}

function toggleEdges(enabled) {
  if (!state.viewer) return;
  if (typeof state.viewer.edges?.toggle === "function") {
    state.viewer.edges.toggle(enabled);
  } else if (enabled) {
    state.viewer.edges?.create?.();
  } else {
    state.viewer.edges?.delete?.();
  }
}

function toggleSection(enabled) {
  if (!state.viewer) return;
  if (enabled) {
    state.viewer.clipper.createPlane();
  } else {
    state.viewer.clipper.deleteAllPlanes();
  }
}

function togglePanel(id, show) {
  const panel = el(id);
  if (!panel) return;
  panel.classList.toggle("hidden", !show);
}

function wireViewerInteractions() {
  const canvas = state.viewer?.context?.renderer?.domElement;
  if (canvas) {
    canvas.addEventListener("click", onSceneClick);
  }
}

function bindUI() {
  el("refresh-session-files")?.addEventListener("click", refreshFiles);
  el("load-selection")?.addEventListener("click", loadSelectedFile);
  el("fit-scene")?.addEventListener("click", () => state.viewer?.context.fitToFrame());

  el("toggle-layers")?.addEventListener("change", (e) => togglePanel("layers-panel", e.target.checked));
  el("toggle-properties")?.addEventListener("change", (e) =>
    togglePanel("properties-panel", e.target.checked)
  );
  el("toggle-edges")?.addEventListener("change", (e) => toggleEdges(e.target.checked));
  el("toggle-section")?.addEventListener("change", (e) => toggleSection(e.target.checked));
}

document.addEventListener("DOMContentLoaded", async () => {
  bindUI();
  await ensureSession();
});
