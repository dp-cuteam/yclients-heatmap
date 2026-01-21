const runBtn = document.getElementById("runDiag");
const branchSelect = document.getElementById("diagBranch");
const dateInput = document.getElementById("diagDate");
const staffInput = document.getElementById("diagStaff");

const cfgPartner = document.getElementById("cfgPartner");
const cfgUser = document.getElementById("cfgUser");
const cfgTz = document.getElementById("cfgTz");
const cfgEnv = document.getElementById("cfgEnv");
const cfgLast = document.getElementById("cfgLast");

const tableBody = document.getElementById("diagTableBody");
const logPath = document.getElementById("logPath");
const logSize = document.getElementById("logSize");
const logTailBtn = document.getElementById("logTail");
const logOutput = document.getElementById("logOutput");

const supportBtn = document.getElementById("supportPacketRun");
const supportStatus = document.getElementById("supportPacketStatus");
const supportOutput = document.getElementById("supportPacketOutput");

async function fetchJSON(url, options = {}) {
  const res = await fetch(url, options);
  if (!res.ok) throw new Error(`Ошибка запроса: ${res.status}`);
  return res.json();
}

function formatStatus(status) {
  const cls = status === "ОК" ? "ok" : status === "Предупреждение" ? "warn" : "err";
  return `<span class="diag-status ${cls}">${status}</span>`;
}

function renderTable(tests) {
  tableBody.innerHTML = "";
  tests.forEach((t) => {
    const row = document.createElement("tr");
    row.innerHTML = `
      <td>${t.name || "—"}</td>
      <td>${formatStatus(t.status || "—")}</td>
      <td>${t.http_code ?? "—"}</td>
      <td>${t.latency_ms ?? "—"}</td>
      <td>${t.message || "—"}</td>
      <td>
        <details>
          <summary>Показать</summary>
          <pre>${JSON.stringify(t.details || {}, null, 2)}</pre>
        </details>
      </td>
    `;
    tableBody.appendChild(row);
  });
}

function updateConfig(cfg) {
  cfgPartner.textContent = cfg.partner_token || "не задан";
  cfgUser.textContent = cfg.user_token || "не задан";
  cfgTz.textContent = cfg.timezone || "—";
  cfgEnv.textContent = cfg.environment || "—";
  cfgLast.textContent = cfg.last_attempt ? new Date(cfg.last_attempt).toLocaleString("ru-RU") : "—";
}

function updateLog(info) {
  logPath.textContent = info.path || "—";
  logSize.textContent = info.size ? `${(info.size / 1024).toFixed(1)} KB` : "0 KB";
}

async function runDiagnostics() {
  runBtn.disabled = true;
  logOutput.textContent = "";
  try {
    const payload = {
      branch_id: branchSelect.value || null,
      date: dateInput.value || null,
      staff_id: staffInput.value || null,
    };
    const data = await fetchJSON("/api/admin/diagnostics/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    updateConfig(data.config || {});
    renderTable(data.tests || []);
    updateLog(data.log || {});
    if (data.branches && data.branches.length && !branchSelect.value) {
      branchSelect.innerHTML = "";
      data.branches.forEach((b) => {
        const opt = document.createElement("option");
        opt.value = b.id;
        opt.textContent = `${b.id} — ${b.title || "Без названия"}`;
        branchSelect.appendChild(opt);
      });
    }
  } catch (err) {
    logOutput.textContent = `Ошибка: ${err.message}`;
  } finally {
    runBtn.disabled = false;
  }
}

logTailBtn.addEventListener("click", async () => {
  const res = await fetch("/api/admin/diagnostics/log/tail?lines=200");
  const text = await res.text();
  logOutput.textContent = text || "Лог пуст.";
});

runBtn.addEventListener("click", runDiagnostics);

async function runSupportPacket() {
  if (!supportBtn) return;
  supportBtn.disabled = true;
  if (supportStatus) supportStatus.textContent = "running...";
  if (supportOutput) supportOutput.textContent = "";
  try {
    const payload = {
      date: dateInput.value || null,
    };
    const data = await fetchJSON("/api/admin/diagnostics/support-packet", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (supportStatus) supportStatus.textContent = "ok";
    if (supportOutput) supportOutput.textContent = JSON.stringify(data.meta || data, null, 2);
  } catch (err) {
    if (supportStatus) supportStatus.textContent = "error";
    if (supportOutput) supportOutput.textContent = err.message || String(err);
  } finally {
    supportBtn.disabled = false;
  }
}

if (supportBtn) {
  supportBtn.addEventListener("click", runSupportPacket);
}

// Init
dateInput.value = new Date().toISOString().slice(0, 10);
runDiagnostics().catch((err) => {
  logOutput.textContent = `Ошибка: ${err.message}`;
});
