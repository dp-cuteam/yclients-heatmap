const startBtn = document.getElementById("startFull");
const statusEl = document.getElementById("etlStatus");
const progressEl = document.getElementById("etlProgress");
const timeEl = document.getElementById("etlTime");
const errorsEl = document.getElementById("etlErrors");
const histFileStatus = document.getElementById("histFileStatus");
const histFilePath = document.getElementById("histFilePath");
const histImportStatus = document.getElementById("histImportStatus");
const histImportMeta = document.getElementById("histImportMeta");
const histImportBtn = document.getElementById("histImport");
const histReimportBtn = document.getElementById("histReimport");
const histListFilesBtn = document.getElementById("histListFiles");
const histFiles = document.getElementById("histFiles");

async function fetchJSON(url, options = {}) {
  const res = await fetch(url, options);
  if (!res.ok) throw new Error(`Request failed: ${res.status}`);
  return res.json();
}

async function refreshStatus() {
  const data = await fetchJSON("/api/admin/etl/status");
  if (data.status === "none") {
    statusEl.textContent = "—";
    progressEl.textContent = "—";
    timeEl.textContent = "—";
    errorsEl.textContent = "";
    return;
  }
  const statusMap = {
    running: "Выполняется",
    success: "Успешно",
    failed: "Ошибка",
  };
  statusEl.textContent = statusMap[data.status] || data.status || "—";
  progressEl.textContent = data.progress || "—";
  timeEl.textContent = data.finished_at || data.started_at || "—";
  errorsEl.textContent = data.error_log || "";
}

function formatBytes(bytes) {
  if (!bytes && bytes !== 0) return "—";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}

async function refreshHistoricalStatus() {
  if (!histFileStatus) return;
  try {
    const data = await fetchJSON("/api/admin/historical/status");
    const file = data.file || {};
    const imp = data.import || {};
    histFileStatus.textContent = file.exists ? "Найден" : "Нет файла";
    const dbInfo = data.db_path ? `БД: ${data.db_path}` : "";
    histFilePath.textContent = file.path
      ? `${file.path} · ${formatBytes(file.size)}${dbInfo ? ` · ${dbInfo}` : ""}`
      : dbInfo || "—";
    if (!imp) {
      histImportStatus.textContent = "—";
      histImportMeta.textContent = "";
      return;
    }
    const statusMap = {
      running: "Выполняется",
      success: "Успешно",
      failed: "Ошибка",
    };
    histImportStatus.textContent = statusMap[imp.status] || imp.status || "—";
    const parts = [];
    if (imp.finished_at || imp.started_at) {
      parts.push(`время: ${imp.finished_at || imp.started_at}`);
    }
    if (imp.rows_count !== null && imp.rows_count !== undefined) {
      parts.push(`строк: ${imp.rows_count}`);
    }
    if (imp.error_log) {
      parts.push(`ошибка: ${imp.error_log.trim().split(\"\\n\").pop()}`);
    }
    histImportMeta.textContent = parts.join(" · ");
  } catch (err) {
    histFileStatus.textContent = "Ошибка";
    histFilePath.textContent = err.message || "—";
  }
}

async function startHistoricalImport(mode) {
  if (!histImportBtn) return;
  histImportBtn.disabled = true;
  histReimportBtn.disabled = true;
  try {
    await fetchJSON("/api/admin/historical/import", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode }),
    });
  } catch (err) {
    console.error(err);
  } finally {
    histImportBtn.disabled = false;
    histReimportBtn.disabled = false;
    await refreshHistoricalStatus();
  }
}

if (histImportBtn) {
  histImportBtn.addEventListener("click", () => startHistoricalImport("append"));
}
if (histReimportBtn) {
  histReimportBtn.addEventListener("click", () => startHistoricalImport("replace"));
}
if (histListFilesBtn) {
  histListFilesBtn.addEventListener("click", async () => {
    histListFilesBtn.disabled = true;
    try {
      const data = await fetchJSON("/api/admin/historical/files");
      histFiles.textContent = (data.files || []).join("\n") || "Файлы не найдены.";
    } catch (err) {
      histFiles.textContent = err.message || "Ошибка";
    } finally {
      histListFilesBtn.disabled = false;
    }
  });
}

startBtn.addEventListener("click", async () => {
  startBtn.disabled = true;
  try {
    await fetchJSON("/api/admin/etl/full_2025/start", { method: "POST" });
  } catch (err) {
    console.error(err);
  } finally {
    startBtn.disabled = false;
    await refreshStatus();
  }
});

setInterval(refreshStatus, 5000);
refreshStatus().catch((err) => console.error(err));
refreshHistoricalStatus().catch((err) => console.error(err));
setInterval(refreshHistoricalStatus, 10000);
