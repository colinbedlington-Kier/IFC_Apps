const mapState = {
  definitions: [],
  unmapped: [],
};

function renderMapList() {
  const target = document.getElementById("mapList");
  if (!target) return;
  if (!mapState.definitions.length) {
    target.innerHTML = '<div class="muted">No definitions loaded</div>';
    return;
  }
  target.innerHTML = mapState.definitions
    .map((d) => {
      const status = d.mapping_status || "unmapped";
      return `<div class="change-row">
        <div><strong>${d.check_id}</strong> · ${d.section}</div>
        <div class="muted">${d.info}</div>
        <div class="chip ${status === "mapped" ? "chip-ok" : "chip-warn"}">${status}</div>
      </div>`;
    })
    .join("");
}

function fillUnmappedSelect() {
  const select = document.getElementById("mapCheckSelect");
  if (!select) return;
  select.innerHTML = "";
  mapState.unmapped.forEach((d) => {
    const opt = document.createElement("option");
    opt.value = d.check_id;
    opt.textContent = `${d.check_id} — ${d.info}`;
    select.appendChild(opt);
  });
}

async function loadMappingData() {
  const resp = await fetch("/api/checks/definitions");
  const data = await resp.json();
  mapState.definitions = data.definitions || [];
  mapState.unmapped = mapState.definitions.filter((d) => d.mapping_status !== "mapped");
  fillUnmappedSelect();
  renderMapList();
}

async function saveMapping() {
  const checkId = document.getElementById("mapCheckSelect")?.value;
  if (!checkId) return alert("Choose a check to map.");
  const kind = document.getElementById("mapKind")?.value || "attribute";
  const mapping = {
    kind,
    attribute: document.getElementById("mapAttribute")?.value || undefined,
    pset: document.getElementById("mapPset")?.value || undefined,
    property: document.getElementById("mapProperty")?.value || undefined,
    qto: document.getElementById("mapPset")?.value || undefined,
    quantity: document.getElementById("mapProperty")?.value || undefined,
    classification_system: document.getElementById("mapClassification")?.value || undefined,
  };
  const expr = document.getElementById("mapExpression")?.value;
  const statusEl = document.getElementById("mapStatus");
  const resp = await fetch("/api/checks/mappings", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ check_id: checkId, mapping }),
  });
  const data = await resp.json();
  if (expr) {
    await fetch("/api/checks/expressions", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ check_id: checkId, expression: expr }),
    });
  }
  if (statusEl) statusEl.textContent = data.status === "ok" ? "Saved." : JSON.stringify(data);
  await loadMappingData();
}

function wireMappingEvents() {
  const saveBtn = document.getElementById("saveMapping");
  if (saveBtn) saveBtn.addEventListener("click", saveMapping);
}

document.addEventListener("DOMContentLoaded", async () => {
  if (!document.getElementById("mapCheckSelect")) return;
  await ensureSession();
  await loadMappingData();
  wireMappingEvents();
});
