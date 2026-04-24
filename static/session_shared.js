(function initIfcSessionShared(global) {
  const STORAGE_KEY = "ifcToolkitSessionId";
  const LEGACY_STORAGE_KEYS = [
    "ifc_session_id",
    "sessionId",
    "ifcSessionId",
    "qaSessionId",
    "uploadSessionId",
  ];
  const SESSION_CHANGE_EVENT = "ifc-toolkit-session-changed";
  let currentSessionId = "";
  let sessionPromise = null;
  const listeners = new Set();

  function readStoredSessionId() {
    try {
      const canonicalId = localStorage.getItem(STORAGE_KEY) || sessionStorage.getItem(STORAGE_KEY) || "";
      const normalizedCanonical = String(canonicalId || "").trim();
      if (normalizedCanonical) return normalizedCanonical;

      for (const key of LEGACY_STORAGE_KEYS) {
        const legacyId = localStorage.getItem(key) || sessionStorage.getItem(key) || "";
        const normalizedLegacy = String(legacyId || "").trim();
        if (normalizedLegacy) {
          writeStoredSessionId(normalizedLegacy);
          return normalizedLegacy;
        }
      }
      return "";
    } catch (_) {
      return "";
    }
  }

  function writeStoredSessionId(sessionId) {
    const value = String(sessionId || "").trim();
    try {
      if (value) {
        localStorage.setItem(STORAGE_KEY, value);
        sessionStorage.setItem(STORAGE_KEY, value);
      } else {
        localStorage.removeItem(STORAGE_KEY);
        sessionStorage.removeItem(STORAGE_KEY);
      }
      LEGACY_STORAGE_KEYS.forEach((key) => {
        localStorage.removeItem(key);
        sessionStorage.removeItem(key);
      });
    } catch (_) {
      // no-op for private mode/storage-disabled environments
    }
  }

  function notifySessionChange(sessionId) {
    try {
      global.dispatchEvent(new CustomEvent(SESSION_CHANGE_EVENT, { detail: { sessionId } }));
    } catch (_) {
      // no-op if CustomEvent is unavailable
    }
    listeners.forEach((listener) => {
      try {
        listener(sessionId);
      } catch (err) {
        console.warn("Session listener failed", err);
      }
    });
  }

  function setCurrentSessionId(sessionId) {
    const normalized = String(sessionId || "").trim();
    if (normalized === currentSessionId) return currentSessionId;
    currentSessionId = normalized;
    writeStoredSessionId(normalized);
    notifySessionChange(normalized);
    return currentSessionId;
  }

  function getCurrentSessionId() {
    if (currentSessionId) return currentSessionId;
    currentSessionId = readStoredSessionId();
    return currentSessionId;
  }

  async function ensureSession(options = {}) {
    const { createIfMissing = true } = options;
    const existing = getCurrentSessionId();
    if (existing || !createIfMissing) return existing;
    if (!sessionPromise) {
      sessionPromise = fetch("/api/session", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ session_id: existing || "" }),
      })
        .then(async (resp) => {
          if (!resp.ok) throw new Error(`Session request failed (${resp.status})`);
          const data = await resp.json();
          const resolved = String(data?.session_id || "").trim();
          if (!resolved) throw new Error("Session response missing session_id");
          setCurrentSessionId(resolved);
          return resolved;
        })
        .finally(() => {
          sessionPromise = null;
        });
    }
    return sessionPromise;
  }

  function shortSessionId(sessionId, len = 8) {
    const value = String(sessionId || "").trim();
    return value ? value.slice(0, Math.max(1, len)) : "";
  }

  function normalizeSessionFile(record = {}) {
    const candidates = [record.name, record.filename, record.display_name, record.path]
      .map((value) => String(value || "").trim())
      .filter(Boolean);
    const rawName = candidates[0] || "";
    const basename = rawName.split(/[\\/]/).pop() || rawName;
    const size = Number(record.size ?? record.bytes ?? 0) || 0;
    return {
      ...record,
      name: basename,
      size,
      modified: record.modified || record.uploaded || record.uploaded_at || "",
    };
  }

  async function getSessionFiles(sessionId) {
    const sid = String(sessionId || "").trim();
    if (!sid) return [];
    const resp = await fetch(`/api/session/${sid}/files`);
    let data = null;
    let bodyText = "";
    try {
      data = await resp.json();
    } catch (_) {
      try {
        bodyText = await resp.text();
      } catch (_) {
        bodyText = "";
      }
    }
    if (!resp.ok) {
      const message = `Failed to list session files (HTTP ${resp.status})`;
      const err = new Error(message);
      err.status = resp.status;
      err.body = data ?? bodyText;
      throw err;
    }
    const records = Array.isArray(data)
      ? data
      : Array.isArray(data?.files)
        ? data.files
        : Array.isArray(data?.items)
          ? data.items
          : [];
    return records.map((record) => normalizeSessionFile(record));
  }

  function isIfcCandidate(file) {
    const name = String(file?.name || file?.filename || file?.display_name || file?.path || "").toLowerCase();
    return name.endsWith(".ifc") || name.endsWith(".ifczip") || name.endsWith(".ifcxml");
  }

  function subscribe(listener) {
    if (typeof listener !== "function") return () => {};
    listeners.add(listener);
    return () => listeners.delete(listener);
  }

  global.IFCSession = {
    storageKey: STORAGE_KEY,
    legacyStorageKeys: LEGACY_STORAGE_KEYS.slice(),
    sessionChangeEvent: SESSION_CHANGE_EVENT,
    getCurrentSessionId,
    setCurrentSessionId,
    ensureSession,
    shortSessionId,
    normalizeSessionFile,
    getSessionFiles,
    isIfcCandidate,
    subscribe,
  };
})(window);
