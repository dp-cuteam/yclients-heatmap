const branchSelect = document.getElementById("staffBranch");
const refreshBtn = document.getElementById("staffRefresh");
const statusEl = document.getElementById("staffStatus");
const summaryEl = document.getElementById("staffSummary");
const blocksEl = document.getElementById("staffTypeBlocks");

async function fetchJSON(url, options = {}) {
  const res = await fetch(url, options);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Ошибка запроса: ${res.status}`);
  }
  return res.json();
}

function setStatus(text, kind = "") {
  statusEl.textContent = text;
  statusEl.className = `audit-status ${kind}`.trim();
}

function renderTypeBlock(typeName, staffList) {
  const card = document.createElement("div");
  card.className = "type-card";

  const header = document.createElement("div");
  header.className = "type-header";
  header.innerHTML = `
    <span>${typeName}</span>
    <span class="type-count">${staffList.length}</span>
  `;
  card.appendChild(header);

  const list = document.createElement("div");
  list.className = "type-list";
  if (!staffList.length) {
    const empty = document.createElement("div");
    empty.className = "type-row empty";
    empty.textContent = "Нет сотрудников";
    list.appendChild(empty);
  } else {
    staffList.forEach((staff) => {
      const row = document.createElement("div");
      row.className = "type-row";
      const role = staff.position || staff.specialization || "—";
      row.innerHTML = `
        <div class="type-name">${staff.name || "Без имени"}</div>
        <div class="type-meta">ID ${staff.id} · ${role}</div>
      `;
      list.appendChild(row);
    });
  }
  card.appendChild(list);
  return card;
}

function renderStaff(data) {
  blocksEl.innerHTML = "";
  const grouped = {};
  data.staff.forEach((staff) => {
    const type = staff.type || "Не классифицирован";
    grouped[type] = grouped[type] || [];
    grouped[type].push(staff);
  });

  const total = data.staff.length;
  const unknown = (grouped["Не классифицирован"] || []).length;
  summaryEl.textContent = `Филиал ${data.branch_id} · сотрудников ${total} · не классифицировано ${unknown}`;

  const orderedTypes = [...(data.types || [])];
  if (!orderedTypes.includes("Не классифицирован") && grouped["Не классифицирован"]) {
    orderedTypes.push("Не классифицирован");
  }
  const remaining = Object.keys(grouped).filter((t) => !orderedTypes.includes(t));
  orderedTypes.push(...remaining.sort());

  orderedTypes.forEach((typeName) => {
    const list = grouped[typeName] || [];
    blocksEl.appendChild(renderTypeBlock(typeName, list));
  });
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

async function refresh() {
  const branchId = branchSelect.value;
  if (!branchId) return;
  setStatus("Загрузка…", "warn");
  summaryEl.textContent = "Загрузка списка сотрудников…";
  blocksEl.innerHTML = "";
  try {
    const data = await fetchJSON(`/api/branches/${branchId}/staff-types`);
    setStatus("Готово", "ok");
    renderStaff(data);
  } catch (err) {
    setStatus("Ошибка", "err");
    summaryEl.textContent = err.message || "Ошибка загрузки";
  }
}

refreshBtn.addEventListener("click", refresh);
branchSelect.addEventListener("change", refresh);

async function init() {
  await loadBranches();
  await refresh();
}

init().catch((err) => {
  setStatus("Ошибка", "err");
  summaryEl.textContent = err.message || "Ошибка загрузки";
});
