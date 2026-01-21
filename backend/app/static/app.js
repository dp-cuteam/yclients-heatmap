const monthSelect = document.getElementById("monthSelect");
const branchSelect = document.getElementById("branchSelect");
const monthContainer = document.getElementById("monthContainer");
const statusMonth = document.getElementById("dataMonth");
const statusUpdated = document.getElementById("dataUpdated");
const statusSource = document.getElementById("dataSource");
const statusCount = document.getElementById("dataCount");
const statusNote = document.getElementById("dataNote");
const statusRefresh = document.getElementById("dataRefresh");

const colors = [
  "#fff5f5",
  "#ffe9e9",
  "#ffdddd",
  "#ffd1d1",
  "#ffc5c5",
  "#ffb9b9",
  "#ffadad",
  "#ffa1a1",
  "#ff9595",
  "#ff8989",
  "#ff7d7d",
  "#ff6f6f",
  "#ff6161",
  "#ff5353",
  "#ff4545",
  "#ff3737",
  "#ff2929",
  "#ff1b1b",
  "#f11212",
  "#d90429",
];

const SHOW_EDGE_LABELS = false;

function colorFor(pct) {
  const idx = Math.max(0, Math.min(colors.length - 1, Math.floor(pct / 5)));
  return colors[idx];
}

function formatPct(value) {
  return `${value.toFixed(1)}%`;
}

function buildMonths() {
  const names = [
    "Январь",
    "Февраль",
    "Март",
    "Апрель",
    "Май",
    "Июнь",
    "Июль",
    "Август",
    "Сентябрь",
    "Октябрь",
    "Ноябрь",
    "Декабрь",
  ];
  monthSelect.innerHTML = "";
  const startValue = window.BRANCH_START_DATE || "2025-01-01";
  const startDate = new Date(startValue);
  const startMonth = Number.isNaN(startDate.getTime()) ? 1 : startDate.getMonth() + 1;
  for (let idx = startMonth; idx <= 12; idx += 1) {
    const name = names[idx - 1];
    const value = `2025-${String(idx).padStart(2, "0")}`;
    const opt = document.createElement("option");
    opt.value = value;
    opt.textContent = `${name} 2025`;
    monthSelect.appendChild(opt);
  }
  monthSelect.value = `2025-${String(startMonth).padStart(2, "0")}`;
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
  const weekday = date.toLocaleDateString("ru-RU", { weekday: "short" });
  const day = date.toLocaleDateString("ru-RU", { day: "2-digit", month: "2-digit" });
  return `${weekday}<br/>${day}`;
}

function renderGroupMonth(group, data) {
  const block = document.createElement("div");
  block.className = "group-block";

  const header = document.createElement("div");
  header.className = "group-header";
  const groupName = group?.name || "Группа";
  header.innerHTML = `
    <div class="group-title">${groupName}</div>
    <div class="group-meta">ID ${group?.group_id || "—"} · средняя за месяц: ${formatPct(data.month_avg || 0)}</div>
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

  const corner = document.createElement("div");
  corner.className = "cell-head row-head corner";
  corner.textContent = "Время";
  grid.appendChild(corner);

  days.forEach((day) => {
    const head = document.createElement("div");
    head.className = "cell-head day-head";
    if (day.dow === 6 || day.dow === 7) {
      head.classList.add("weekend");
    }
    head.innerHTML = formatDayHeader(day.date);
    grid.appendChild(head);
  });

  hours.forEach((hour, hourIdx) => {
    const rowHead = document.createElement("div");
    rowHead.className = "cell-head row-head";
    rowHead.textContent = `${hour}:00`;
    grid.appendChild(rowHead);

    days.forEach((day) => {
      const cellData = day.cells?.[hourIdx] || { load_pct: 0, busy_count: 0, staff_total: 0 };
      const cell = document.createElement("div");
      cell.className = "cell";
      cell.style.background = colorFor(cellData.load_pct || 0);
      const rounded = Math.round(cellData.load_pct || 0);
      if (SHOW_EDGE_LABELS && (rounded === 10 || rounded === 90)) {
        cell.textContent = `${rounded}%`;
      }
      cell.title = `${day.date} ${hour}:00 • ${formatPct(cellData.load_pct || 0)} (${cellData.busy_count}/${cellData.staff_total})`;
      grid.appendChild(cell);
    });
  });

  const avgLabel = document.createElement("div");
  avgLabel.className = "cell-head row-head";
  avgLabel.textContent = "Итог дня";
  grid.appendChild(avgLabel);

  days.forEach((day) => {
    const cell = document.createElement("div");
    cell.className = "cell-total";
    cell.textContent = formatPct(day.day_avg || 0);
    grid.appendChild(cell);
  });

  const grayLabel = document.createElement("div");
  grayLabel.className = "cell-head row-head";
  grayLabel.textContent = "Вне окна";
  grid.appendChild(grayLabel);

  days.forEach((day) => {
    const gray = document.createElement("div");
    gray.className = "cell-gray";
    const early = day.gray?.early ? "●" : "—";
    const late = day.gray?.late ? "●" : "—";
    gray.innerHTML = `<span class="gray-dot-mini">${early}</span><span class="gray-dot-mini">${late}</span>`;
    grid.appendChild(gray);
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
}

async function init() {
  buildMonths();
  await loadBranches();
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
