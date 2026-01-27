const startBtn = document.getElementById("startFull");
const dailyBtn = document.getElementById("startDaily");
const statusEl = document.getElementById("etlStatus");
const progressEl = document.getElementById("etlProgress");
const timeEl = document.getElementById("etlTime");
const errorsEl = document.getElementById("etlErrors");
const etlBranch = document.getElementById("etlBranch");
const etlBranchTable = document.getElementById("etlBranchTable");
const histFileStatus = document.getElementById("histFileStatus");
const histFilePath = document.getElementById("histFilePath");
const histImportStatus = document.getElementById("histImportStatus");
const histImportMeta = document.getElementById("histImportMeta");
const histImportBtn = document.getElementById("histImport");
const histReimportBtn = document.getElementById("histReimport");
const histListFilesBtn = document.getElementById("histListFiles");
const histFiles = document.getElementById("histFiles");
const paletteInputs = document.querySelectorAll('input[name="heatmapPalette"]');
const cuteamDbStatus = document.getElementById("cuteamDbStatus");
const cuteamDbMeta = document.getElementById("cuteamDbMeta");
const cuteamRange = document.getElementById("cuteamRange");
const cuteamRangeMeta = document.getElementById("cuteamRangeMeta");
const cuteamSyncStatus = document.getElementById("cuteamSyncStatus");
const cuteamSyncMeta = document.getElementById("cuteamSyncMeta");
const cuteamEnvMeta = document.getElementById("cuteamEnvMeta");
const cuteamOutput = document.getElementById("cuteamOutput");
const cuteamSheetNames = document.getElementById("cuteamSheetNames");
const cuteamSyncBtn = document.getElementById("cuteamSync");
const cuteamSyncDryBtn = document.getElementById("cuteamSyncDry");
const cuteamImportFiles = document.getElementById("cuteamImportFiles");
const cuteamImportFilesMeta = document.getElementById("cuteamImportFilesMeta");
const cuteamImportStatus = document.getElementById("cuteamImportStatus");
const cuteamImportMeta = document.getElementById("cuteamImportMeta");
const cuteamImportOutput = document.getElementById("cuteamImportOutput");
const cuteamImportBtn = document.getElementById("cuteamImportBtn");

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

async function loadEtlBranches() {
  if (!etlBranch) return;
  etlBranch.innerHTML = "";
  try {
    const data = await fetchJSON("/api/branches");
    (data.branches || []).forEach((b) => {
      const opt = document.createElement("option");
      opt.value = b.branch_id;
      opt.textContent = b.display_name;
      etlBranch.appendChild(opt);
    });
  } catch (err) {
    console.warn("Failed to load ETL branches", err);
  }
  if (!etlBranch.value && etlBranch.options.length) {
    etlBranch.value = etlBranch.options[0].value;
  }
}

async function refreshFullBranchStatus() {
  if (!etlBranchTable) return;
  try {
    const data = await fetchJSON("/api/admin/etl/full/last");
    const rows = data.branches || [];
    etlBranchTable.innerHTML = "";
    if (!rows.length) {
      etlBranchTable.innerHTML = "<tr><td colspan=\"3\">Данные отсутствуют.</td></tr>";
      return;
    }
    rows.forEach((row) => {
      const tr = document.createElement("tr");
      const nameCell = document.createElement("td");
      nameCell.textContent = row.display_name || row.branch_id;
      const statusCell = document.createElement("td");
      const statusMap = {
        running: "Выполняется",
        success: "Успешно",
        failed: "Ошибка",
      };
      statusCell.textContent = statusMap[row.last_status] || row.last_status || "—";
      const lastCell = document.createElement("td");
      lastCell.textContent = row.last_full ? new Date(row.last_full).toLocaleString("ru-RU") : "—";
      tr.appendChild(nameCell);
      tr.appendChild(statusCell);
      tr.appendChild(lastCell);
      etlBranchTable.appendChild(tr);
    });
  } catch (err) {
    etlBranchTable.innerHTML = "<tr><td colspan=\"3\">Ошибка загрузки.</td></tr>";
  }
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
      parts.push("время: " + (imp.finished_at || imp.started_at));
    }
    if (imp.rows_count !== null && imp.rows_count !== undefined) {
      parts.push("строк: " + imp.rows_count);
    }
    if (imp.error_log) {
      parts.push("ошибка: " + String(imp.error_log).trim().split("\n").pop());
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
  if (dailyBtn) dailyBtn.disabled = true;
  try {
    const payload = {};
    if (etlBranch && etlBranch.value) {
      payload.branch_id = etlBranch.value;
    }
    await fetchJSON("/api/admin/etl/full_2025/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
  } catch (err) {
    console.error(err);
  } finally {
    startBtn.disabled = false;
    if (dailyBtn) dailyBtn.disabled = false;
    await refreshStatus();
    await refreshFullBranchStatus();
  }
});

if (dailyBtn) {
  dailyBtn.addEventListener("click", async () => {
    dailyBtn.disabled = true;
    startBtn.disabled = true;
    try {
      await fetchJSON("/api/admin/etl/daily/start", { method: "POST" });
    } catch (err) {
      console.error(err);
    } finally {
      dailyBtn.disabled = false;
      startBtn.disabled = false;
      await refreshStatus();
    }
  });
}

setInterval(refreshStatus, 5000);
refreshStatus().catch((err) => console.error(err));
refreshHistoricalStatus().catch((err) => console.error(err));
setInterval(refreshHistoricalStatus, 10000);
loadEtlBranches().then(refreshFullBranchStatus);
setInterval(refreshFullBranchStatus, 15000);

function loadPaletteSetting() {
  const current = localStorage.getItem("heatmapPalette") || "perceptual";
  paletteInputs.forEach((input) => {
    input.checked = input.value === current;
  });
}

paletteInputs.forEach((input) => {
  input.addEventListener("change", () => {
    localStorage.setItem("heatmapPalette", input.value);
  });
});

loadPaletteSetting();


async function refreshCuteamStatus() {
  if (!cuteamDbStatus) return;
  try {
    const data = await fetchJSON("/api/admin/cuteam/status");
    const dbSource = data.db_source || "SQLite";
    if (!data.db_exists) {
      cuteamDbStatus.textContent = "Нет базы";
    } else {
      cuteamDbStatus.textContent = dbSource;
    }
    const dbMetaParts = [];
    if (data.db_path) dbMetaParts.push(data.db_path);
    if (data.db_size !== null && data.db_size !== undefined) {
      dbMetaParts.push(formatBytes(data.db_size));
    }
    cuteamDbMeta.textContent = dbMetaParts.join(" · ") || "—";

    if (data.date_min || data.date_max) {
      cuteamRange.textContent = `${data.date_min || "?"} — ${data.date_max || "?"}`;
    } else {
      cuteamRange.textContent = "—";
    }
    const rangeParts = [];
    if (data.rows !== null && data.rows !== undefined) {
      rangeParts.push(`строк: ${data.rows}`);
    }
    if (data.branches !== null && data.branches !== undefined) {
      rangeParts.push(`филиалов: ${data.branches}`);
    }
    if (data.updated_at) {
      rangeParts.push(`обновлено: ${data.updated_at}`);
    }
    cuteamRangeMeta.textContent = rangeParts.join(" · ") || "—";

    const sync = data.sync || {};
    const syncStatusMap = {
      idle: "Ожидание",
      running: "Выполняется",
      success: "Успешно",
      error: "Ошибка",
    };
    cuteamSyncStatus.textContent = syncStatusMap[sync.status] || sync.status || "—";
    const syncParts = [];
    if (sync.started_at) syncParts.push(`старт: ${sync.started_at}`);
    if (sync.finished_at) syncParts.push(`финиш: ${sync.finished_at}`);
    if (sync.last_error) syncParts.push(`ошибка: ${sync.last_error}`);
    if (sync.last_sheets && sync.last_sheets.length) {
      syncParts.push(`листы: ${sync.last_sheets.join(", ")}`);
    }
    if (sync.dry_run) syncParts.push("dry-run");
    cuteamSyncMeta.textContent = syncParts.join(" · ") || "—";

    if (cuteamOutput) {
      cuteamOutput.textContent = sync.last_output || "—";
    }

    const env = data.env || {};
    const envParts = [];
    if (env.sheet_name) envParts.push(`лист: ${env.sheet_name}`);
    if (env.sheet_id) envParts.push(`sheet_id: ${env.sheet_id}`);
    if (env.has_sa_json || env.has_sa_json_b64) {
      envParts.push("service account: ok");
    } else {
      envParts.push("service account: нет");
    }
    if (env.db_env) envParts.push(`db env: ${env.db_env}`);
    if (env.db_source) envParts.push(`db: ${env.db_source}`);
    if (cuteamEnvMeta) {
      cuteamEnvMeta.textContent = envParts.join(" · ") || "—";
    }
    if (cuteamSheetNames && !cuteamSheetNames.value && env.sheet_name) {
      cuteamSheetNames.value = env.sheet_name;
    }

    const imports = data.imports || {};
    const importState = imports.state || {};
    const importFiles = imports.files || {};
    const importSheets = imports.sheets || {};
    if (cuteamImportFiles) {
      const plans = importFiles.plans || {};
      const checks = importFiles.checks || {};
      const ok = plans.exists && checks.exists;
      cuteamImportFiles.textContent = ok ? "Файлы найдены" : "Файлы не найдены";
      const fileParts = [];
      if (plans.path) {
        const size =
          plans.size !== undefined && plans.size !== null ? ` · ${formatBytes(plans.size)}` : "";
        fileParts.push(`Планы: ${plans.path}${size}`);
      }
      if (checks.path) {
        const size =
          checks.size !== undefined && checks.size !== null ? ` · ${formatBytes(checks.size)}` : "";
        fileParts.push(`Чеки: ${checks.path}${size}`);
      }
      if (cuteamImportFilesMeta) {
        cuteamImportFilesMeta.textContent = fileParts.join(" · ") || "—";
      }
    }
    if (cuteamImportStatus) {
      const importStatusMap = {
        idle: "Ожидание",
        running: "Выполняется",
        success: "Успешно",
        error: "Ошибка",
      };
      cuteamImportStatus.textContent =
        importStatusMap[importState.status] || importState.status || "—";
    }
    if (cuteamImportMeta) {
      const metaParts = [];
      if (importState.started_at) metaParts.push(`старт: ${importState.started_at}`);
      if (importState.finished_at) metaParts.push(`финиш: ${importState.finished_at}`);
      if (importState.last_error) metaParts.push(`ошибка: ${importState.last_error}`);
      if (importSheets.plans) metaParts.push(`лист планов: ${importSheets.plans}`);
      if (importSheets.checks) metaParts.push(`лист чеков: ${importSheets.checks}`);
      cuteamImportMeta.textContent = metaParts.join(" · ") || "—";
    }
    if (cuteamImportOutput) {
      cuteamImportOutput.textContent = importState.last_output || "—";
    }
  } catch (err) {
    cuteamDbStatus.textContent = "Ошибка";
    cuteamDbMeta.textContent = err.message || "—";
  }
}

async function startCuteamSync(dryRun) {
  if (!cuteamSyncBtn) return;
  cuteamSyncBtn.disabled = true;
  if (cuteamSyncDryBtn) cuteamSyncDryBtn.disabled = true;
  try {
    const raw = (cuteamSheetNames?.value || "").trim();
    const sheetNames = raw
      ? raw.split(",").map((name) => name.trim()).filter(Boolean)
      : [];
    await fetchJSON("/api/admin/cuteam/sync", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sheet_names: sheetNames, dry_run: !!dryRun }),
    });
  } catch (err) {
    if (cuteamOutput) cuteamOutput.textContent = err.message || "Ошибка";
  } finally {
    cuteamSyncBtn.disabled = false;
    if (cuteamSyncDryBtn) cuteamSyncDryBtn.disabled = false;
    await refreshCuteamStatus();
  }
}

if (cuteamSyncBtn) {
  cuteamSyncBtn.addEventListener("click", () => startCuteamSync(false));
}
if (cuteamSyncDryBtn) {
  cuteamSyncDryBtn.addEventListener("click", () => startCuteamSync(true));
}
if (cuteamImportBtn) {
  cuteamImportBtn.addEventListener("click", async () => {
    cuteamImportBtn.disabled = true;
    try {
      await fetchJSON("/api/admin/cuteam/import-plans-checks", { method: "POST" });
    } catch (err) {
      if (cuteamImportOutput) {
        cuteamImportOutput.textContent = err.message || "Ошибка";
      }
    } finally {
      cuteamImportBtn.disabled = false;
      await refreshCuteamStatus();
    }
  });
}
if (cuteamDbStatus) {
  refreshCuteamStatus().catch((err) => console.error(err));
  setInterval(refreshCuteamStatus, 10000);
}

