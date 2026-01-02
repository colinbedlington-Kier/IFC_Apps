import { IFCViewerAPI } from "https://cdn.jsdelivr.net/npm/web-ifc-viewer@1.0.172/dist/IFCViewerAPI.js";
import * as THREE from "https://cdn.jsdelivr.net/npm/three@0.164.1/build/three.module.js";

const state = {
  sessionId: null,
  files: [],
  viewer: null,
  activeModelID: null,
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

function createViewer() {
  const container = el("viewer-canvas");
  if (!container) return null;
  container.innerHTML = "";
  const viewer = new IFCViewerAPI({
    container,
    backgroundColor: new THREE.Color(0x0b0e16),
  });
  viewer.axes.setAxes();
  viewer.grid.setGrid(30, 30);
  viewer.IFC.setWasmPath("https://cdn.jsdelivr.net/npm/web-ifc@0.0.50/");
  state.activeModelID = null;
  return viewer;
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
    state.viewer = createViewer();
    if (!state.viewer) throw new Error("Viewer could not start");
    const model = await state.viewer.IFC.loadIfcFile(file, true);
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
    setStatus("Could not load IFC. Check the console for details.");
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
