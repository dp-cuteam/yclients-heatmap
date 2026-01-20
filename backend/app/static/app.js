const monthSelect = document.getElementById("monthSelect");
const branchSelect = document.getElementById("branchSelect");
const groupSelect = document.getElementById("groupSelect");
const monthContainer = document.getElementById("monthContainer");
const avgMonthEl = document.getElementById("avgMonth");
const avgMonthBottom = document.getElementById("avgMonthBottom");

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

function formatDate(value) {
  const date = new Date(value);
  return date.toLocaleDateString("ru-RU", { day: "2-digit", month: "2-digit" });
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
  groupSelect.innerHTML = "";
  data.groups.forEach((g) => {
    const opt = document.createElement("option");
    opt.value = g.group_id;
    opt.textContent = g.name;
    groupSelect.appendChild(opt);
  });
  if (data.groups.length) {
    groupSelect.value = data.groups[0].group_id;
  }
}

function renderMonth(data) {
  monthContainer.innerHTML = "";
  const hours = data.hours;

  data.weeks.forEach((week) => {
    const block = document.createElement("div");
    block.className = "week-block";

    const header = document.createElement("div");
    header.className = "week-header";
    header.textContent = `Неделя ${formatDate(week.week_start)} – ${formatDate(week.week_end)}`;
    block.appendChild(header);

    const grid = document.createElement("div");
    grid.className = "week-grid";

    const headLabel = document.createElement("div");
    headLabel.className = "cell-head";
    headLabel.textContent = "День";
    grid.appendChild(headLabel);

    hours.forEach((h) => {
      const cell = document.createElement("div");
      cell.className = "cell-head";
      cell.textContent = `${h}:00`;
      grid.appendChild(cell);
    });

    const totalCell = document.createElement("div");
    totalCell.className = "cell-head";
    totalCell.textContent = "Итог дня";
    grid.appendChild(totalCell);

    const grayCell = document.createElement("div");
    grayCell.className = "cell-head";
    grayCell.textContent = "Вне окна";
    grid.appendChild(grayCell);

    week.days.forEach((day) => {
      const dayCell = document.createElement("div");
      dayCell.className = "cell-day";
      const date = new Date(day.date);
      dayCell.textContent = date.toLocaleDateString("ru-RU", {
        weekday: "short",
        day: "2-digit",
        month: "2-digit",
      });
      grid.appendChild(dayCell);

      day.cells.forEach((cellData, idx) => {
        const cell = document.createElement("div");
        cell.className = "cell";
        cell.style.background = colorFor(cellData.load_pct);
        cell.textContent = `${Math.round(cellData.load_pct)}%`;
        const hour = hours[idx];
        cell.title = `${day.date} ${hour}:00 • ${formatPct(cellData.load_pct)} (${cellData.busy_count}/${cellData.staff_total})`;
        grid.appendChild(cell);
      });

      const dayTotal = document.createElement("div");
      dayTotal.className = "cell-total";
      dayTotal.textContent = formatPct(day.day_avg || 0);
      grid.appendChild(dayTotal);

      const gray = document.createElement("div");
      gray.className = "cell-gray";
      const early = day.gray?.early ? "●" : "—";
      const late = day.gray?.late ? "●" : "—";
      gray.innerHTML = `<span class="gray-dot-mini">${early}</span><span class="gray-dot-mini">${late}</span>`;
      grid.appendChild(gray);
    });

    block.appendChild(grid);

    const weekFooter = document.createElement("div");
    weekFooter.className = "week-footer";
    weekFooter.textContent = `Средняя загрузка недели: ${formatPct(week.week_avg || 0)}`;
    block.appendChild(weekFooter);

    monthContainer.appendChild(block);
  });

  avgMonthEl.textContent = formatPct(data.month_avg || 0);
  if (avgMonthBottom) {
    avgMonthBottom.textContent = formatPct(data.month_avg || 0);
  }
}

async function refresh() {
  const branchId = branchSelect.value;
  const groupId = groupSelect.value;
  const month = monthSelect.value;
  if (!branchId || !groupId || !month) return;
  const data = await fetchJSON(
    `/api/heatmap/month?branch_id=${branchId}&group_id=${groupId}&month=${month}`
  );
  renderMonth(data);
}

async function init() {
  buildMonths();
  await loadBranches();
  await loadGroups();
  await refresh();
}

monthSelect.addEventListener("change", refresh);
branchSelect.addEventListener("change", async () => {
  await loadGroups();
  await refresh();
});
groupSelect.addEventListener("change", refresh);

init().catch((err) => {
  console.error(err);
});
