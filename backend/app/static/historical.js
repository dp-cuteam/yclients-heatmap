const branchSelect = document.getElementById("histBranch");
const monthSelect = document.getElementById("histMonth");
const container = document.getElementById("histContainer");

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

async function fetchJSON(url, options = {}) {
  const res = await fetch(url, options);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Ошибка запроса: ${res.status}`);
  }
  return res.json();
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

function renderTypeMonth(type, hours) {
  const block = document.createElement("div");
  block.className = "group-block";

  const header = document.createElement("div");
  header.className = "group-header";
  header.innerHTML = `
    <div class="group-title">${type.name || "Тип"}</div>
    <div class="group-meta">Средняя за месяц: ${formatPct(type.month_avg || 0)}</div>
  `;
  block.appendChild(header);

  const days = flattenDays(type.weeks || []);
  if (!days.length || !hours.length) {
    const empty = document.createElement("div");
    empty.className = "group-empty";
    empty.textContent = "Нет данных за выбранный месяц.";
    block.appendChild(empty);
    container.appendChild(block);
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
      cell.title = `${day.date} ${hour}:00 • ${formatPct(cellData.load_pct || 0)}`;
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
  container.appendChild(block);
}

async function loadBranches() {
  const data = await fetchJSON("/api/historical/branches");
  branchSelect.innerHTML = "";
  data.branches.forEach((b) => {
    const opt = document.createElement("option");
    opt.value = b.branch_id;
    opt.textContent = b.code ? `${b.code} (${b.branch_id})` : b.branch_id;
    branchSelect.appendChild(opt);
  });
  if (data.branches.length) {
    branchSelect.value = data.branches[0].branch_id;
  }
}

async function loadMonths(branchId) {
  const data = await fetchJSON(`/api/historical/branches/${branchId}/months`);
  monthSelect.innerHTML = "";
  data.months.forEach((m) => {
    const opt = document.createElement("option");
    opt.value = m;
    opt.textContent = m;
    monthSelect.appendChild(opt);
  });
  if (data.months.length) {
    monthSelect.value = data.months[data.months.length - 1];
  }
}

async function refresh() {
  const branchId = branchSelect.value;
  const month = monthSelect.value;
  if (!branchId || !month) return;
  container.innerHTML = '<div class="group-empty">Загрузка данных…</div>';
  try {
    const data = await fetchJSON(`/api/historical/month?branch_id=${branchId}&month=${month}`);
    container.innerHTML = "";
    if (!data.types || !data.types.length) {
      container.innerHTML = '<div class="group-empty">Нет данных для выбранного месяца.</div>';
      return;
    }
    data.types.forEach((type) => renderTypeMonth(type, data.hours || []));
  } catch (err) {
    container.innerHTML = `<div class=\"group-empty\">Ошибка: ${err.message}</div>`;
  }
}

branchSelect.addEventListener("change", async () => {
  await loadMonths(branchSelect.value);
  await refresh();
});
monthSelect.addEventListener("change", refresh);

async function init() {
  await loadBranches();
  await loadMonths(branchSelect.value);
  await refresh();
}

init().catch((err) => {
  container.innerHTML = `<div class=\"group-empty\">Ошибка: ${err.message}</div>`;
});
