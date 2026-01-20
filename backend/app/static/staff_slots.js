const runBtn = document.getElementById("auditRun");
const branchInput = document.getElementById("auditBranch");
const dateInput = document.getElementById("auditDate");
const statusEl = document.getElementById("auditStatus");
const summaryEl = document.getElementById("auditSummary");
const resultsEl = document.getElementById("auditResults");

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

function formatSlot(slot) {
  const start = slot.start_time || "—";
  const end = slot.end_time || "—";
  const recId = slot.record_id ?? "—";
  const attendance = slot.attendance ?? "—";
  return `${start}–${end} | запись #${recId} | attendance=${attendance}`;
}

function renderStaffCard(staff, title) {
  const card = document.createElement("div");
  card.className = "staff-card";
  const name = staff.name || "Без имени";
  const slotCount = staff.slot_count ?? (staff.slots ? staff.slots.length : 0);
  const header = document.createElement("div");
  header.className = "staff-header";
  header.innerHTML = `
    <div>${title || name}</div>
    <div class="staff-meta">ID ${staff.id} · слотов: ${slotCount}</div>
  `;
  card.appendChild(header);
  const pre = document.createElement("pre");
  pre.className = "staff-slots";
  if (!staff.slots || staff.slots.length === 0) {
    pre.textContent = "Нет завершённых записей";
  } else {
    pre.textContent = staff.slots.map(formatSlot).join("\n");
  }
  card.appendChild(pre);
  return card;
}

function renderResults(data) {
  resultsEl.innerHTML = "";
  const summary = `Филиал ${data.branch_id} · дата ${data.date} (${data.timezone}) · сотрудников ${data.staff_count} · завершённых записей ${data.records_fact} из ${data.records_total} · учёт attendance=1/2`;
  summaryEl.textContent = summary;

  if (data.staff && data.staff.length) {
    data.staff.forEach((staff) => {
      resultsEl.appendChild(renderStaffCard(staff));
    });
  }

  if (data.unknown_staff && data.unknown_staff.length) {
    const note = document.createElement("div");
    note.className = "audit-note";
    note.textContent = "Записи с неизвестными сотрудниками:";
    resultsEl.appendChild(note);
    data.unknown_staff.forEach((staff) => {
      resultsEl.appendChild(renderStaffCard(staff, staff.name));
    });
  }
}

async function runAudit() {
  const branchId = (branchInput.value || "").trim();
  if (!branchId) {
    setStatus("Введите ID филиала", "err");
    return;
  }
  runBtn.disabled = true;
  setStatus("Запрос…", "warn");
  summaryEl.textContent = "Идёт загрузка данных…";
  resultsEl.innerHTML = "";
  try {
    const payload = {
      branch_id: branchId,
      date: dateInput.value || null,
    };
    const data = await fetchJSON("/api/admin/staff-slots/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    setStatus("Готово", "ok");
    renderResults(data);
  } catch (err) {
    setStatus("Ошибка", "err");
    summaryEl.textContent = err.message || "Ошибка загрузки";
  } finally {
    runBtn.disabled = false;
  }
}

runBtn.addEventListener("click", runAudit);

// init default date = yesterday
const d = new Date();
d.setDate(d.getDate() - 1);
dateInput.value = d.toISOString().slice(0, 10);
