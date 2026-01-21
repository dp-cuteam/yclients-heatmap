const monthSelect = document.getElementById("monthSelect");
const branchSelect = document.getElementById("branchSelect");
const monthContainer = document.getElementById("monthContainer");
const statusMonth = document.getElementById("dataMonth");
const statusUpdated = document.getElementById("dataUpdated");
const statusSource = document.getElementById("dataSource");
const statusCount = document.getElementById("dataCount");
const statusNote = document.getElementById("dataNote");
const statusRefresh = document.getElementById("dataRefresh");

const PALETTES = {
  perceptual: [
    "#f7fbff",
    "#deebf7",
    "#c6dbef",
    "#9ecae1",
    "#6baed6",
    "#41ab5d",
    "#fee08b",
    "#fdae61",
    "#f46d43",
    "#d73027",
  ],
  bgyor: [
    "#edf8fb",
    "#ccece6",
    "#99d8c9",
    "#66c2a4",
    "#41ae76",
    "#ffffcc",
    "#fed976",
    "#feb24c",
    "#fd8d3c",
    "#f03b20",
  ],
};

const paletteInputs = document.querySelectorAll('input[name="heatmapPalette"]');

function colorFor(pct) {
  return getHeatColor(pct, getPaletteName());
}

function formatPct(value) {
  return `${value.toFixed(1)}%`;
}

function formatValue(value) {
  return `${Math.round(value || 0)}`;
}

function getPaletteName() {
  return localStorage.getItem("heatmapPalette") || "perceptual";
}

function getHeatColor(percent, paletteName) {
  const palette = PALETTES[paletteName] || PALETTES.perceptual;
  const pct = Math.max(0, Math.min(100, Number(percent) || 0));
  const bucket = Math.min(9, Math.floor(pct / 10));
  return palette[bucket];
}

function updateLegend(paletteName) {
  const palette = PALETTES[paletteName] || PALETTES.perceptual;
  const stops = palette.map((c) => c).join(", ");
  document.querySelectorAll(".legend-bar").forEach((bar) => {
    bar.style.background = `linear-gradient(90deg, ${stops})`;
  });
}

function applyPalette(container, paletteName) {
  if (!container) return;
  container.querySelectorAll(".cell").forEach((cell) => {
    const pct = cell.dataset.pct || 0;
    cell.style.background = getHeatColor(pct, paletteName);
  });
  updateLegend(paletteName);
}

function initPaletteControls() {
  const current = getPaletteName();
  paletteInputs.forEach((input) => {
    input.checked = input.value === current;
    input.addEventListener("change", () => {
      localStorage.setItem("heatmapPalette", input.value);
      applyPalette(monthContainer, input.value);
    });
  });
  window.addEventListener("storage", (evt) => {
    if (evt.key === "heatmapPalette") {
      applyPalette(monthContainer, getPaletteName());
      paletteInputs.forEach((input) => {
        input.checked = input.value === getPaletteName();
      });
    }
  });
  updateLegend(current);
}

function buildMonthsLocal() {
  monthSelect.innerHTML = "";
  const startValue = window.BRANCH_START_DATE || "2025-01-01";
  const startDate = new Date(startValue);
  const start = Number.isNaN(startDate.getTime()) ? new Date(2025, 0, 1) : new Date(startDate.getFullYear(), startDate.getMonth(), 1);
  const now = new Date();
  const end = new Date(now.getFullYear(), now.getMonth(), 1);
  const months = [];
  const cursor = new Date(start.getTime());
  while (cursor <= end) {
    const value = `${cursor.getFullYear()}-${String(cursor.getMonth() + 1).padStart(2, "0")}`;
    let label = cursor.toLocaleDateString("ru-RU", { month: "long", year: "numeric" });
    label = label.charAt(0).toUpperCase() + label.slice(1);
    months.push({ value, label });
    cursor.setMonth(cursor.getMonth() + 1);
  }
  months.sort((a, b) => b.value.localeCompare(a.value));
  months.forEach((m) => {
    const opt = document.createElement("option");
    opt.value = m.value;
    opt.textContent = m.label;
    monthSelect.appendChild(opt);
  });
  if (months.length) {
    monthSelect.value = months[0].value;
  }
}

async function loadMonths() {
  monthSelect.innerHTML = "";
  try {
    const data = await fetchJSON("/api/months");
    const months = (data.months || []).slice().sort((a, b) => b.localeCompare(a));
    months.forEach((value) => {
      const [year, month] = value.split("-").map((part) => Number(part));
      const date = new Date(year, Math.max(0, month - 1), 1);
      let label = date.toLocaleDateString("ru-RU", { month: "long", year: "numeric" });
      label = label.charAt(0).toUpperCase() + label.slice(1);
      const opt = document.createElement("option");
      opt.value = value;
      opt.textContent = label;
      monthSelect.appendChild(opt);
    });
    if (months.length) {
      monthSelect.value = months[0];
      return;
    }
  } catch (err) {
    console.warn("Failed to load months from API, using local fallback.", err);
  }
  buildMonthsLocal();
}

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Ошибка запроса: ${res.status}`);
  return res.json();
}

async function loadBranches() {
  const data = await fetchJSON("/api/branches");
  branchSelect.innerHTML = "";
  data.branches.forEach((b) => {
    const opt = document.createElement("option");
    opt.value = b.branch_id;
    opt.textContent = b.display_name;
    branchSelect.appendChild(opt);
  });
  if (data.branches.length) {
    branchSelect.value = data.branches[0].branch_id;
  }
}

async function loadGroups() {
  const branchId = branchSelect.value;
  if (!branchId) return;
  const data = await fetchJSON(`/api/branches/${branchId}/groups`);
  return data.groups || [];
}

function flattenDays(weeks) {
  const days = [];
  weeks.forEach((week) => {
    week.days.forEach((day) => days.push(day));
  });
  return days;
}

function formatDayHeader(value) {
  const date = new Date(value);
  return date.toLocaleDateString("ru-RU", { day: "2-digit" });
}

function renderGroupMonth(group, data) {
  const block = document.createElement("div");
  block.className = "group-block";

  const header = document.createElement("div");
  header.className = "group-header";
  const groupName = group?.name || "Группа";
  header.innerHTML = `
    <div class="group-title">${groupName}</div>
    <div class="group-meta">ID ${group?.group_id || "—"} · средняя за месяц: ${formatValue(data.month_avg || 0)}</div>
  `;
  block.appendChild(header);

  const days = flattenDays(data.weeks || []);
  const hours = data.hours || [];

  if (!days.length || !hours.length) {
    const empty = document.createElement("div");
    empty.className = "group-empty";
    empty.textContent = "Нет данных за выбранный месяц.";
    block.appendChild(empty);
    monthContainer.appendChild(block);
    return;
  }

  const scroll = document.createElement("div");
  scroll.className = "month-wide";

  const grid = document.createElement("div");
  grid.className = "month-grid-wide";
  grid.style.gridTemplateColumns = `72px repeat(${days.length}, var(--heatmap-cell))`;

  const weekCorner = document.createElement("div");
  weekCorner.className = "cell-head row-head corner week-head";
  weekCorner.textContent = "";
  grid.appendChild(weekCorner);

  const weeks = data.weeks || [];
  let dayOffset = 0;
  if (weeks.length) {
    weeks.forEach((week) => {
      const span = week.days ? week.days.length : 0;
      if (!span) return;
      const weekCell = document.createElement("div");
      weekCell.className = "cell-head week-head";
      weekCell.style.gridColumn = `span ${span}`;
      weekCell.textContent = formatValue(week.week_avg || 0);
      if (dayOffset > 0 && week.days[0] && week.days[0].dow === 1) {
        weekCell.classList.add("week-sep");
      }
      grid.appendChild(weekCell);
      dayOffset += span;
    });
  } else {
    const weekCell = document.createElement("div");
    weekCell.className = "cell-head week-head";
    weekCell.style.gridColumn = `span ${days.length}`;
    grid.appendChild(weekCell);
  }

  const corner = document.createElement("div");
  corner.className = "cell-head row-head corner day-head";
  corner.textContent = "Время";
  grid.appendChild(corner);

  days.forEach((day, idx) => {
    const head = document.createElement("div");
    head.className = "cell-head day-head";
    if (day.dow === 6 || day.dow === 7) {
      head.classList.add("weekend");
    }
    if (day.dow === 1 && idx > 0) {
      head.classList.add("week-sep");
    }
    head.innerHTML = formatDayHeader(day.date);
    grid.appendChild(head);
  });

  hours.forEach((hour, hourIdx) => {
    const rowHead = document.createElement("div");
    rowHead.className = "cell-head row-head";
    if (hour === 9) {
      rowHead.classList.add("hour-sep");
    }
    if (hour === 22) {
      rowHead.classList.add("hour-sep-top");
    }
    rowHead.textContent = `${hour}:00`;
    grid.appendChild(rowHead);

    days.forEach((day, dayIdx) => {
      const cellData = day.cells?.[hourIdx] || { load_pct: 0, busy_count: 0, staff_total: 0 };
      const cell = document.createElement("div");
      cell.className = "cell";
      if (day.dow === 1 && dayIdx > 0) {
        cell.classList.add("week-sep");
      }
      if (hour === 9) {
        cell.classList.add("hour-sep");
      }
      if (hour === 22) {
        cell.classList.add("hour-sep-top");
      }
      cell.dataset.pct = cellData.load_pct || 0;
      cell.style.background = colorFor(cellData.load_pct || 0);
      cell.textContent = formatValue(cellData.load_pct || 0);
      cell.title = `${day.date} ${hour}:00 • ${formatPct(cellData.load_pct || 0)} (${cellData.busy_count}/${cellData.staff_total})`;
      grid.appendChild(cell);
    });
  });

  const avgLabel = document.createElement("div");
  avgLabel.className = "cell-head row-head";
  avgLabel.textContent = "Итог дня";
  grid.appendChild(avgLabel);

  days.forEach((day, dayIdx) => {
    const cell = document.createElement("div");
    cell.className = "cell-total";
    if (day.dow === 1 && dayIdx > 0) {
      cell.classList.add("week-sep");
    }
    cell.textContent = formatValue(day.day_avg || 0);
    grid.appendChild(cell);
  });

  scroll.appendChild(grid);
  block.appendChild(scroll);
  monthContainer.appendChild(block);
}

function renderGroupError(group, err) {
  const block = document.createElement("div");
  block.className = "group-block";
  const header = document.createElement("div");
  header.className = "group-header";
  header.innerHTML = `
    <div class="group-title">${group?.name || "Группа"}</div>
    <div class="group-meta">ID ${group?.group_id || "—"}</div>
  `;
  block.appendChild(header);
  const body = document.createElement("div");
  body.className = "group-empty";
  const message = err?.message || err || "Нет данных";
  if (message.toLowerCase().includes("нет данных")) {
    body.textContent = message;
  } else {
    body.textContent = message.startsWith("Ошибка") ? message : `Ошибка загрузки: ${message}`;
  }
  block.appendChild(body);
  monthContainer.appendChild(block);
}

async function refresh() {
  const branchId = branchSelect.value;
  const month = monthSelect.value;
  if (!branchId || !month) return;
  monthContainer.innerHTML = '<div class="group-empty">Загрузка данных…</div>';
  let statusData = null;
  try {
    statusData = await fetchJSON(`/api/heatmap/status?branch_id=${branchId}&month=${month}`);
    updateStatus(statusData);
  } catch (err) {
    updateStatus(null, err);
  }
  const groups = await loadGroups();
  monthContainer.innerHTML = "";
  if (!groups.length) {
    const empty = document.createElement("div");
    empty.className = "group-empty";
    empty.textContent = "Для этого филиала не настроены группы.";
    monthContainer.appendChild(empty);
    return;
  }
  const countMap = new Map();
  if (statusData && statusData.group_counts) {
    statusData.group_counts.forEach((g) => {
      countMap.set(g.group_id, g.count);
    });
  }
  for (const group of groups) {
    const cnt = countMap.has(group.group_id) ? countMap.get(group.group_id) : null;
    if (cnt === 0) {
      renderGroupError(group, { message: "Нет данных за выбранный месяц." });
      continue;
    }
    try {
      const data = await fetchJSON(
        `/api/heatmap/month?branch_id=${branchId}&group_id=${group.group_id}&month=${month}`
      );
      renderGroupMonth(group, data);
    } catch (err) {
      renderGroupError(group, err);
    }
  }
  applyPalette(monthContainer, getPaletteName());
}

async function init() {
  await loadMonths();
  await loadBranches();
  initPaletteControls();
  await refresh();
}

monthSelect.addEventListener("change", refresh);
branchSelect.addEventListener("change", async () => {
  await refresh();
});
if (statusRefresh) {
  statusRefresh.addEventListener("click", refresh);
}

init().catch((err) => {
  console.error(err);
});

function updateStatus(data, error) {
  if (!statusMonth) return;
  statusMonth.textContent = monthSelect.value || "—";
  if (error) {
    statusUpdated.textContent = "—";
    statusSource.textContent = "—";
    statusCount.textContent = "—";
    statusNote.textContent = "Ошибка получения статуса данных.";
    statusNote.className = "data-note";
    return;
  }
  const last = data?.last_updated ? new Date(data.last_updated).toLocaleString("ru-RU") : "—";
  statusUpdated.textContent = last;
  statusSource.textContent = data?.source || "—";
  statusCount.textContent = data?.total_rows ?? "—";
  if (data && data.total_rows > 0) {
    statusNote.textContent = "Данные загружены.";
    statusNote.className = "data-note ok";
  } else if (data && data.db_exists === false) {
    statusNote.textContent = "База данных не найдена. Проверьте путь к БД.";
    statusNote.className = "data-note";
  } else {
    statusNote.textContent =
      "Данных нет (возможна первая загрузка или сервер перезапущен). Нажмите «Обновить».";
    statusNote.className = "data-note";
  }
}
