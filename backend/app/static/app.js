const monthSelect = document.getElementById("monthSelect");
const branchSelect = document.getElementById("branchSelect");
const groupSelect = document.getElementById("groupSelect");
const weekSelect = document.getElementById("weekSelect");
const heatmap = document.getElementById("heatmap");
const grayEarly = document.getElementById("grayEarly");
const grayLate = document.getElementById("grayLate");

const avgDayEl = document.getElementById("avgDay");
const avgWeekEl = document.getElementById("avgWeek");
const avgMonthEl = document.getElementById("avgMonth");

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
  if (!res.ok) throw new Error(`Request failed: ${res.status}`);
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

async function loadWeeks() {
  const month = monthSelect.value;
  const data = await fetchJSON(`/api/months/${month}/weeks`);
  weekSelect.innerHTML = "";
  data.weeks.forEach((w) => {
    const opt = document.createElement("option");
    opt.value = w;
    opt.textContent = w;
    weekSelect.appendChild(opt);
  });
  if (data.weeks.length) {
    weekSelect.value = data.weeks[0];
  }
}

function renderHeatmap(data) {
  heatmap.innerHTML = "";
  const hours = data.hours;
  const days = data.days;

  const headLabel = document.createElement("div");
  headLabel.className = "cell-head";
  headLabel.textContent = "День";
  heatmap.appendChild(headLabel);

  hours.forEach((h) => {
    const cell = document.createElement("div");
    cell.className = "cell-head";
    cell.textContent = `${h}:00`;
    heatmap.appendChild(cell);
  });

  const grayEarlyRow = [];
  const grayLateRow = [];

  days.forEach((day) => {
    const dayCell = document.createElement("div");
    dayCell.className = "cell-day";
    const date = new Date(day.date);
    dayCell.textContent = date.toLocaleDateString("ru-RU", {
      weekday: "short",
      day: "2-digit",
      month: "2-digit",
    });
    heatmap.appendChild(dayCell);

    day.cells.forEach((cellData, idx) => {
      const cell = document.createElement("div");
      cell.className = "cell";
      cell.style.background = colorFor(cellData.load_pct);
      cell.textContent = `${Math.round(cellData.load_pct)}%`;
      const hour = hours[idx];
      cell.title = `${day.date} ${hour}:00 • ${formatPct(cellData.load_pct)} (${cellData.busy_count}/${cellData.staff_total})`;
      heatmap.appendChild(cell);
    });

    grayEarlyRow.push(day.gray.early);
    grayLateRow.push(day.gray.late);
  });

  grayEarly.innerHTML = "";
  grayLate.innerHTML = "";
  grayEarlyRow.forEach((active) => {
    const dot = document.createElement("div");
    dot.className = "gray-dot" + (active ? " active" : "");
    dot.textContent = active ? "●" : "—";
    grayEarly.appendChild(dot);
  });
  grayLateRow.forEach((active) => {
    const dot = document.createElement("div");
    dot.className = "gray-dot" + (active ? " active" : "");
    dot.textContent = active ? "●" : "—";
    grayLate.appendChild(dot);
  });

  // summary for week
  const allValues = days.flatMap((d) => d.cells.map((c) => c.load_pct));
  const weekAvg = allValues.length
    ? allValues.reduce((a, b) => a + b, 0) / allValues.length
    : 0;
  const dailyAvgs = days.map((d) => {
    const vals = d.cells.map((c) => c.load_pct);
    return vals.reduce((a, b) => a + b, 0) / vals.length;
  });
  const dayAvg =
    dailyAvgs.length > 0
      ? dailyAvgs.reduce((a, b) => a + b, 0) / dailyAvgs.length
      : 0;
  avgDayEl.textContent = formatPct(dayAvg);
  avgWeekEl.textContent = formatPct(weekAvg);
}

async function loadSummary() {
  const branchId = branchSelect.value;
  const groupId = groupSelect.value;
  const month = monthSelect.value;
  if (!branchId || !groupId || !month) return;
  const data = await fetchJSON(`/api/summary/month?branch_id=${branchId}&group_id=${groupId}&month=${month}`);
  avgMonthEl.textContent = formatPct(data.avg_month || 0);
}

async function refresh() {
  const branchId = branchSelect.value;
  const groupId = groupSelect.value;
  const weekStart = weekSelect.value;
  if (!branchId || !groupId || !weekStart) return;
  const data = await fetchJSON(
    `/api/heatmap?branch_id=${branchId}&group_id=${groupId}&week_start=${weekStart}`
  );
  renderHeatmap(data);
  await loadSummary();
}

async function init() {
  buildMonths();
  await loadBranches();
  await loadGroups();
  await loadWeeks();
  await refresh();
}

monthSelect.addEventListener("change", async () => {
  await loadWeeks();
  await refresh();
});

branchSelect.addEventListener("change", async () => {
  await loadGroups();
  await refresh();
});

groupSelect.addEventListener("change", refresh);
weekSelect.addEventListener("change", refresh);

init().catch((err) => {
  console.error(err);
});
