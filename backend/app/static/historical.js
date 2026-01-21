const branchSelect = document.getElementById("histBranch");
const monthSelect = document.getElementById("histMonth");
const container = document.getElementById("histContainer");

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

function applyPalette(containerEl, paletteName) {
  if (!containerEl) return;
  containerEl.querySelectorAll(".cell").forEach((cell) => {
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
      applyPalette(container, input.value);
    });
  });
  window.addEventListener("storage", (evt) => {
    if (evt.key === "heatmapPalette") {
      applyPalette(container, getPaletteName());
      paletteInputs.forEach((input) => {
        input.checked = input.value === getPaletteName();
      });
    }
  });
  updateLegend(current);
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
  return date.toLocaleDateString("ru-RU", { day: "2-digit" });
}

function renderTypeMonth(type, hours) {
  const block = document.createElement("div");
  block.className = "group-block";

  const header = document.createElement("div");
  header.className = "group-header";
  header.innerHTML = `
    <div class="group-title">${type.name || "Тип"}</div>
    <div class="group-meta">Средняя за месяц: ${formatValue(type.month_avg || 0)}</div>
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
  grid.style.gridTemplateColumns = `72px repeat(${days.length}, var(--heatmap-cell))`;

  const weekCorner = document.createElement("div");
  weekCorner.className = "cell-head row-head corner week-head";
  weekCorner.textContent = "";
  grid.appendChild(weekCorner);

  const weeks = type.weeks || [];
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
      cell.title = `${day.date} ${hour}:00 • ${formatPct(cellData.load_pct || 0)}`;
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
  if (!data.branches.length) {
    container.innerHTML = '<div class="group-empty">Нет импортированных данных. Выполните импорт в админке.</div>';
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
  if (!data.months.length) {
    container.innerHTML = '<div class="group-empty">Нет данных за выбранный филиал.</div>';
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
    applyPalette(container, getPaletteName());
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
  initPaletteControls();
  await loadBranches();
  if (!branchSelect.value) return;
  await loadMonths(branchSelect.value);
  await refresh();
}

init().catch((err) => {
  container.innerHTML = `<div class=\"group-empty\">Ошибка: ${err.message}</div>`;
});
