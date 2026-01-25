const summaryContainer = document.getElementById("summaryContainer");
const summaryToc = document.getElementById("summaryToc");
const paletteInputs = document.querySelectorAll('input[name="heatmapPalette"]');

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
  container.querySelectorAll(".summary-cell[data-pct]").forEach((cell) => {
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
      applyPalette(summaryContainer, input.value);
    });
  });
  window.addEventListener("storage", (evt) => {
    if (evt.key === "heatmapPalette") {
      applyPalette(summaryContainer, getPaletteName());
      paletteInputs.forEach((input) => {
        input.checked = input.value === getPaletteName();
      });
    }
  });
  updateLegend(current);
}

function formatValue(value) {
  if (value === null || value === undefined) return "";
  return `${Math.round(value)}`;
}

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Ошибка запроса: ${res.status}`);
  return res.json();
}

function renderSummary(data) {
  summaryContainer.innerHTML = "";
  const years = data.years || [];
  const months = data.months || [];
  const branches = data.branches || [];

  if (!branches.length) {
    if (summaryToc) summaryToc.innerHTML = "";
    const empty = document.createElement("div");
    empty.className = "summary-empty";
    empty.textContent = "Нет данных для отображения.";
    summaryContainer.appendChild(empty);
    return;
  }

  if (summaryToc) {
    summaryToc.innerHTML = "";
    branches.forEach((branch) => {
      const link = document.createElement("a");
      const anchorId = `branch-${branch.branch_id}`;
      link.href = `#${anchorId}`;
      link.textContent = branch.display_name || branch.branch_id;
      summaryToc.appendChild(link);
    });
  }

  branches.forEach((branch) => {
    const branchBlock = document.createElement("div");
    branchBlock.className = "summary-branch";
    branchBlock.id = `branch-${branch.branch_id}`;

    const branchHeader = document.createElement("div");
    branchHeader.className = "summary-branch-header";
    branchHeader.innerHTML = `
      <div class="summary-branch-title">${branch.display_name || branch.branch_id}</div>
      <div class="summary-branch-meta">Филиал #${branch.branch_id}</div>
    `;
    branchBlock.appendChild(branchHeader);

    const groups = branch.groups || [];
    if (!groups.length) {
      const none = document.createElement("div");
      none.className = "summary-empty";
      none.textContent = "Нет ресурсов для филиала.";
      branchBlock.appendChild(none);
    }

    groups.forEach((group) => {
      const groupBlock = document.createElement("div");
      groupBlock.className = "summary-group";

      const groupHeader = document.createElement("div");
      groupHeader.className = "summary-group-header";
      groupHeader.innerHTML = `
        <div class="summary-group-title">${group.name || "Ресурс"}</div>
        <div class="summary-group-meta">ID ${group.group_id || "—"}</div>
      `;
      groupBlock.appendChild(groupHeader);

      const scroll = document.createElement("div");
      scroll.className = "summary-scroll";

      const table = document.createElement("table");
      table.className = "summary-table";

      const thead = document.createElement("thead");
      const headRow = document.createElement("tr");
      const monthHead = document.createElement("th");
      monthHead.textContent = "Месяц";
      headRow.appendChild(monthHead);
      years.forEach((year) => {
        const th = document.createElement("th");
        th.textContent = year;
        headRow.appendChild(th);
      });
      thead.appendChild(headRow);
      table.appendChild(thead);

      const tbody = document.createElement("tbody");
      const values = group.values || {};
      months.forEach((month) => {
        const row = document.createElement("tr");
        const label = document.createElement("th");
        label.textContent = month.label || month.short || month.num;
        row.appendChild(label);
        years.forEach((year) => {
          const key = `${year}-${String(month.num).padStart(2, "0")}`;
          const value = values[key];
          const cell = document.createElement("td");
          cell.className = "summary-cell";
          if (value === null || value === undefined) {
            cell.classList.add("is-empty");
            cell.textContent = "";
          } else {
            cell.dataset.pct = value;
            cell.textContent = formatValue(value);
          }
          row.appendChild(cell);
        });
        tbody.appendChild(row);
      });

      table.appendChild(tbody);
      scroll.appendChild(table);
      groupBlock.appendChild(scroll);
      branchBlock.appendChild(groupBlock);
    });

    summaryContainer.appendChild(branchBlock);
  });

  applyPalette(summaryContainer, getPaletteName());
}

async function loadSummary() {
  summaryContainer.innerHTML = "<div class=\"summary-loading\">Загрузка…</div>";
  try {
    const data = await fetchJSON("/api/heatmap/summary?start_year=2024");
    renderSummary(data);
  } catch (err) {
    console.error(err);
    summaryContainer.innerHTML = "<div class=\"summary-empty\">Не удалось загрузить свод.</div>";
  }
}

initPaletteControls();
loadSummary();
