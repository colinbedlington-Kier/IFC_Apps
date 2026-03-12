(() => {
  const el = (id) => document.getElementById(id);
  const pageRoot = el("mrCalculate");
  if (!pageRoot) return;

  const fields = {
    sourceFile: el("mrSourceFile"),
    uploadFile: el("mrUploadFile"),
    currentX: el("mrCurrentX"),
    currentY: el("mrCurrentY"),
    currentZ: el("mrCurrentZ"),
    targetX: el("mrTargetX"),
    targetY: el("mrTargetY"),
    targetZ: el("mrTargetZ"),
    mode: el("mrRotationMode"),
    explicit: el("mrRotationExplicit"),
    currentAngle: el("mrCurrentAngle"),
    targetAngle: el("mrTargetAngle"),
    deltaAngle: el("mrDeltaAngle"),
    computed: el("mrComputedRotation"),
    rotateGlobalZ: el("mrRotateGlobalZ"),
    preserveMetadata: el("mrPreserveMetadata"),
    outputSuffix: el("mrOutputSuffix"),
    tolerance: el("mrTolerance"),
    explicitWrap: el("mrExplicitWrap"),
    currentTargetWrap: el("mrCurrentTargetWrap"),
    preview: el("mrPreview"),
    result: el("mrResult"),
    status: el("mrStatus"),
    calculate: el("mrCalculate"),
    apply: el("mrApply"),
    reset: el("mrReset"),
    download: el("mrDownload"),
  };

  let lastCalculated = null;

  const n = (input) => {
    const value = Number(input?.value);
    return Number.isFinite(value) ? value : null;
  };

  function getRotation() {
    if (fields.mode.value === "explicit") {
      return n(fields.explicit);
    }
    const current = n(fields.currentAngle);
    const target = n(fields.targetAngle);
    if (current == null || target == null) return null;
    return target - current;
  }

  function updateModeUi() {
    const isExplicit = fields.mode.value === "explicit";
    fields.explicitWrap.style.display = isExplicit ? "grid" : "none";
    fields.currentTargetWrap.style.display = isExplicit ? "none" : "grid";
    const delta = getRotation();
    fields.deltaAngle.value = delta == null ? "" : String(delta);
    fields.computed.value = delta == null ? "" : String(delta);
    validate();
  }

  function gather() {
    const rotation = getRotation();
    return {
      sourceFile: fields.sourceFile.value || null,
      hasUpload: !!fields.uploadFile.files?.length,
      current_x: n(fields.currentX),
      current_y: n(fields.currentY),
      current_z: n(fields.currentZ),
      target_x: n(fields.targetX),
      target_y: n(fields.targetY),
      target_z: n(fields.targetZ),
      rotation_deg: rotation,
      tolerance: n(fields.tolerance),
      rotate_about_global_z: !!fields.rotateGlobalZ.checked,
      preserve_metadata: !!fields.preserveMetadata.checked,
      output_suffix: fields.outputSuffix.value || "_moved_rotated",
      mode: fields.mode.value,
      current_angle_deg: n(fields.currentAngle),
      target_angle_deg: n(fields.targetAngle),
    };
  }

  function validate() {
    const p = gather();
    const hasInputFile = p.sourceFile || p.hasUpload;
    const numericOk = [
      p.current_x,
      p.current_y,
      p.current_z,
      p.target_x,
      p.target_y,
      p.target_z,
      p.rotation_deg,
      p.tolerance,
    ].every((v) => v != null);
    const ok = hasInputFile && numericOk && p.tolerance > 0;
    fields.apply.disabled = !ok;
    if (!hasInputFile) {
      fields.status.textContent = "Select a session IFC or upload one.";
    } else if (!numericOk) {
      fields.status.textContent = "Enter valid numeric coordinate and rotation values.";
    } else if (!(p.tolerance > 0)) {
      fields.status.textContent = "Tolerance must be greater than zero.";
    } else {
      fields.status.textContent = "Inputs valid. Calculate transform, then apply.";
    }
    return ok;
  }

  function calcSummary() {
    const p = gather();
    if (!validate()) return null;
    const rad = (p.rotation_deg * Math.PI) / 180.0;
    const c = Math.cos(rad);
    const s = Math.sin(rad);
    const tx = p.target_x - (c * p.current_x - s * p.current_y);
    const ty = p.target_y - (s * p.current_x + c * p.current_y);
    const tz = p.target_z - p.current_z;
    const matrix = [
      [c, -s, 0, tx],
      [s, c, 0, ty],
      [0, 0, 1, tz],
      [0, 0, 0, 1],
    ];
    return { ...p, translation: [tx, ty, tz], matrix };
  }

  function renderPreview(summary) {
    if (!summary) {
      fields.preview.textContent = "No transform calculated yet.";
      return;
    }
    lastCalculated = summary;
    fields.preview.textContent = JSON.stringify(
      {
        transform_order: "T = T2 * Rz * T1",
        rotation_convention: "Positive is counter-clockwise when looking down +Z toward origin.",
        units_note: "Values interpreted in IFC project length units.",
        mode: summary.mode,
        current_xyz: [summary.current_x, summary.current_y, summary.current_z],
        target_xyz: [summary.target_x, summary.target_y, summary.target_z],
        rotation_applied_deg: summary.rotation_deg,
        translation_component: summary.translation,
        transform_matrix: summary.matrix,
      },
      null,
      2,
    );
  }

  async function applyTransform() {
    if (!validate()) return;
    const p = gather();
    const body = new FormData();
    if (p.sourceFile) body.append("source_file", p.sourceFile);
    if (fields.uploadFile.files?.length) body.append("upload_file", fields.uploadFile.files[0]);
    body.append("current_x", String(p.current_x));
    body.append("current_y", String(p.current_y));
    body.append("current_z", String(p.current_z));
    body.append("target_x", String(p.target_x));
    body.append("target_y", String(p.target_y));
    body.append("target_z", String(p.target_z));
    body.append("rotation_deg", String(p.rotation_deg));
    body.append("tolerance", String(p.tolerance));
    body.append("rotate_about_global_z", String(p.rotate_about_global_z));
    body.append("preserve_metadata", String(p.preserve_metadata));
    body.append("output_suffix", p.output_suffix);

    fields.apply.disabled = true;
    fields.status.textContent = "Applying transform…";
    fields.result.textContent = "Processing…";

    try {
      const sessionId = localStorage.getItem("ifc_session_id");
      const resp = await window.withProcessing("Applying IFC move/rotate…", async () => fetch(`/api/session/${sessionId}/ifc-move-rotate`, { method: "POST", body }));
      const data = await resp.json();
      if (!resp.ok) throw new Error(data.detail || "Transform request failed");
      fields.result.textContent = JSON.stringify(data.summary, null, 2);
      fields.status.textContent = "Transform complete.";
      if (fields.download) {
        fields.download.href = data.ifc.url;
        fields.download.download = data.ifc.name;
        fields.download.style.display = "inline-flex";
      }
    } catch (err) {
      fields.status.textContent = `Error: ${err.message}`;
      fields.result.textContent = `Error: ${err.message}`;
    } finally {
      fields.apply.disabled = !validate();
    }
  }

  function resetForm() {
    fields.currentX.value = "416338150";
    fields.currentY.value = "432650723";
    fields.currentZ.value = "106100";
    fields.targetX.value = "";
    fields.targetY.value = "";
    fields.targetZ.value = "";
    fields.mode.value = "explicit";
    fields.explicit.value = "0.0";
    fields.currentAngle.value = "0";
    fields.targetAngle.value = "0";
    fields.tolerance.value = "0.001";
    fields.outputSuffix.value = "_moved_rotated";
    fields.rotateGlobalZ.checked = true;
    fields.preserveMetadata.checked = true;
    fields.uploadFile.value = "";
    fields.result.textContent = "No output yet.";
    fields.download.style.display = "none";
    updateModeUi();
    renderPreview(null);
  }

  [
    fields.sourceFile,
    fields.uploadFile,
    fields.currentX,
    fields.currentY,
    fields.currentZ,
    fields.targetX,
    fields.targetY,
    fields.targetZ,
    fields.mode,
    fields.explicit,
    fields.currentAngle,
    fields.targetAngle,
    fields.tolerance,
    fields.outputSuffix,
    fields.rotateGlobalZ,
    fields.preserveMetadata,
  ].forEach((node) => node?.addEventListener("input", () => {
    if (node === fields.mode || node === fields.explicit || node === fields.currentAngle || node === fields.targetAngle) {
      updateModeUi();
    } else {
      validate();
    }
  }));

  fields.calculate.addEventListener("click", () => renderPreview(calcSummary()));
  fields.apply.addEventListener("click", applyTransform);
  fields.reset.addEventListener("click", resetForm);

  updateModeUi();
})();
