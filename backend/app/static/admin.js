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
const goodsBranch = document.getElementById("goodsBranch");
const goodsTerm = document.getElementById("goodsTerm");
const goodsCheckBtn = document.getElementById("goodsCheck");
const goodsOutput = document.getElementById("goodsOutput");

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

async function loadGoodsBranches() {
  if (!goodsBranch) return;
  goodsBranch.innerHTML = "";
  try {
    const data = await fetchJSON("/api/mini/branches");
    (data.branches || []).forEach((b) => {
      const opt = document.createElement("option");
      opt.value = b.branch_id;
      opt.textContent = b.display_name;
      goodsBranch.appendChild(opt);
    });
  } catch (err) {
    console.warn("Failed to load goods branches", err);
  }
  if (!goodsBranch.value && goodsBranch.options.length) {
    goodsBranch.value = goodsBranch.options[0].value;
  }
}

async function checkGoodsApi() {
  if (!goodsCheckBtn || !goodsBranch.value) return;
  goodsCheckBtn.disabled = true;
  goodsOutput.textContent = "";
  try {
    const payload = {
      branch_id: goodsBranch.value,
      term: goodsTerm.value || "",
    };
    const data = await fetchJSON("/api/admin/goods/check", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    goodsOutput.textContent = JSON.stringify(data, null, 2);
  } catch (err) {
    goodsOutput.textContent = err.message || "Ошибка";
  } finally {
    goodsCheckBtn.disabled = false;
  }
}

if (goodsCheckBtn) {
  goodsCheckBtn.addEventListener("click", checkGoodsApi);
}

loadGoodsBranches();
