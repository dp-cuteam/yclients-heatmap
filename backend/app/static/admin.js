const startBtn = document.getElementById("startFull");
const statusEl = document.getElementById("etlStatus");
const progressEl = document.getElementById("etlProgress");
const timeEl = document.getElementById("etlTime");
const errorsEl = document.getElementById("etlErrors");

async function fetchJSON(url, options = {}) {
  const res = await fetch(url, options);
  if (!res.ok) throw new Error(`Request failed: ${res.status}`);
  return res.json();
}

async function refreshStatus() {
  const data = await fetchJSON("/api/admin/etl/status");
  if (data.status === "none") {
    statusEl.textContent = "—";
    progressEl.textContent = "—";
    timeEl.textContent = "—";
    errorsEl.textContent = "";
    return;
  }
  statusEl.textContent = data.status || "—";
  progressEl.textContent = data.progress || "—";
  timeEl.textContent = data.finished_at || data.started_at || "—";
  errorsEl.textContent = data.error_log || "";
}

startBtn.addEventListener("click", async () => {
  startBtn.disabled = true;
  try {
    await fetchJSON("/api/admin/etl/full_2025/start", { method: "POST" });
  } catch (err) {
    console.error(err);
  } finally {
    startBtn.disabled = false;
    await refreshStatus();
  }
});

setInterval(refreshStatus, 5000);
refreshStatus().catch((err) => console.error(err));
