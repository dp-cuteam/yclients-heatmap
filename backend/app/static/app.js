const monthSelect = document.getElementById("monthSelect");
const branchSelect = document.getElementById("branchSelect");
const monthContainer = document.getElementById("monthContainer");

const colors = [
  "#2f5aa8",
  "#326bb0",
  "#3381b3",
  "#3496ac",
  "#35a695",
  "#3bb77e",
  "#69b864",
  "#93b44f",
  "#c58a3e",
  "#e85f3f",
];

function colorFor(pct) {
  const idx = Math.max(0, Math.min(colors.length - 1, Math.floor(pct / 10)));
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
  names.forEach((name, idx) => {
    const value = `2025-${String(idx + 1).padStart(2, "0")}`;
    const opt = document.createElement("option");
    opt.value = value;
    opt.textContent = `${name} 2025`;
    monthSelect.appendChild(opt);
  });
  monthSelect.value = "2025-01";
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
  grid.style.gridTemplateColumns = `110px repeat(${days.length}, minmax(48px, 1fr))`;

  const corner = document.createElement("div");
  corner.className = "cell-head row-head";
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
      cell.textContent = `${Math.round(cellData.load_pct || 0)}%`;
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
  body.textContent = `Ошибка загрузки: ${err?.message || err}`;
  block.appendChild(body);
  monthContainer.appendChild(block);
}

async function refresh() {
  const branchId = branchSelect.value;
  const month = monthSelect.value;
  if (!branchId || !month) return;
  monthContainer.innerHTML = '<div class="group-empty">Загрузка данных…</div>';
  const groups = await loadGroups();
  monthContainer.innerHTML = "";
  if (!groups.length) {
    const empty = document.createElement("div");
    empty.className = "group-empty";
    empty.textContent = "Для этого филиала не настроены группы.";
    monthContainer.appendChild(empty);
    return;
  }
  for (const group of groups) {
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

init().catch((err) => {
  console.error(err);
});
