const state = {
  branchId: null,
  mode: "now",
  records: [],
  selectedRecord: null,
  selectedServiceId: null,
  storageId: null,
  selectedGood: null,
  sessionAdds: [],
  sendMode: "goods_only",
  tgUser: null,
};

const branchSelect = document.getElementById("miniBranchSelect");
const recordSearch = document.getElementById("miniRecordSearch");
const recordList = document.getElementById("recordList");
const recordEmpty = document.getElementById("recordEmpty");
const recordScreen = document.getElementById("screenRecords");
const goodsScreen = document.getElementById("screenGoods");
const toggleButtons = document.querySelectorAll(".mini-toggle-btn");

const backButton = document.getElementById("miniBack");
const recordTitle = document.getElementById("miniRecordTitle");
const recordCard = document.getElementById("miniRecordCard");
const goodsSearch = document.getElementById("miniGoodsSearch");
const recentGoods = document.getElementById("miniRecentGoods");
const goodsResults = document.getElementById("goodsResults");
const selectedGoodTitle = document.getElementById("selectedGoodTitle");
const selectedGoodMeta = document.getElementById("selectedGoodMeta");
const goodAmount = document.getElementById("goodAmount");
const goodQuick = document.getElementById("goodQuick");
const addGoodBtn = document.getElementById("addGoodBtn");
const basketList = document.getElementById("basketList");
const undoGoodBtn = document.getElementById("undoGoodBtn");
const toast = document.getElementById("miniToast");
const sendModeButtons = document.querySelectorAll("#miniSendMode .mini-toggle-btn");

const RECENT_KEY = "miniRecentGoods";
const SEND_MODE_KEY = "miniSendMode";

const rawBase = document.body?.dataset?.appBase || "";
const APP_BASE = rawBase.endsWith("/") ? rawBase.slice(0, -1) : rawBase;

function withBase(path) {
  if (!path) return APP_BASE || "";
  if (!path.startsWith("/")) {
    return `${APP_BASE}/${path}`;
  }
  return `${APP_BASE}${path}`;
}



function showScreen(name) {
  if (name === "records") {
    recordScreen.classList.add("active");
    goodsScreen.classList.remove("active");
  } else {
    recordScreen.classList.remove("active");
    goodsScreen.classList.add("active");
  }
}

function showToast(message, tone = "info") {
  if (!toast) return;
  toast.textContent = message;
  toast.className = `mini-toast ${tone}`;
  toast.style.opacity = "1";
  clearTimeout(showToast._timer);
  showToast._timer = setTimeout(() => {
    toast.style.opacity = "0";
  }, 2500);
}

async function fetchJSON(url) {
  const res = await fetch(url);
  if (res.status === 401) {
    window.location = `${withBase("/login")}?next=${encodeURIComponent(withBase("/mini"))}`;
    throw new Error("Не авторизован");
  }
  if (!res.ok) {
    throw new Error(`Ошибка запроса: ${res.status}`);
  }
  return res.json();
}

async function postJSON(url, payload) {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (res.status === 401) {
    window.location = `${withBase("/login")}?next=${encodeURIComponent(withBase("/mini"))}`;
    throw new Error("Не авторизован");
  }
  if (!res.ok) {
    let message = `Ошибка запроса: ${res.status}`;
    try {
      const data = await res.json();
      if (data && data.detail) message = data.detail;
    } catch (_) {
      // ignore
    }
    throw new Error(message);
  }
  return res.json();
}

function formatPrice(price, unit) {
  if (price == null || Number.isNaN(Number(price))) return "—";
  const value = Number(price);
  const formatted = value.toLocaleString("ru-RU", { maximumFractionDigits: 2 });
  const suffix = unit ? `₽/${unit}` : "₽";
  return `${formatted} ${suffix}`;
}

function setMode(mode) {
  state.mode = mode;
  localStorage.setItem("miniMode", mode);
  toggleButtons.forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.mode === mode);
  });
}

function setSendMode(mode) {
  state.sendMode = mode;
  localStorage.setItem(SEND_MODE_KEY, mode);
  sendModeButtons.forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.send === mode);
  });
}

function setBranch(branchId) {
  state.branchId = branchId;
  localStorage.setItem("miniBranchId", String(branchId || ""));
}

function renderRecords() {
  recordList.innerHTML = "";
  if (!state.records.length) {
    recordEmpty.style.display = "block";
    return;
  }
  recordEmpty.style.display = "none";
  state.records.forEach((record) => {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "record-card";
    const statusClass = record.status === "В процессе" ? "live" : record.status === "Ожидает" ? "wait" : "done";
    btn.innerHTML = `
      <div class="record-top">
        <div class="record-time">${record.time || "—"}</div>
        <div class="record-status ${statusClass}">${record.status || ""}</div>
      </div>
      <div class="record-staff">${record.staff_name || "—"}</div>
      <div class="record-client">${record.client_name || ""}</div>
    `;
    btn.addEventListener("click", () => openRecord(record));
    recordList.appendChild(btn);
  });
}

async function loadRecords() {
  if (!state.branchId) return;
  recordList.innerHTML = "";
  recordEmpty.style.display = "block";
  recordEmpty.textContent = "Загружаем записи…";
  const params = new URLSearchParams({
    branch_id: state.branchId,
    mode: state.mode,
    q: recordSearch.value.trim(),
  });
  try {
    const data = await fetchJSON(`${withBase("/api/mini/records")}?${params}`);
    state.records = data.records || [];
    renderRecords();
  } catch (err) {
    recordEmpty.style.display = "block";
    recordEmpty.textContent = "Не удалось загрузить записи.";
    console.warn(err);
  }
}

async function loadBranches() {
  const data = await fetchJSON(withBase("/api/mini/branches"));
  branchSelect.innerHTML = "";
  (data.branches || []).forEach((branch) => {
    const opt = document.createElement("option");
    opt.value = branch.branch_id;
    opt.textContent = branch.display_name;
    branchSelect.appendChild(opt);
  });
  const saved = localStorage.getItem("miniBranchId");
  if (saved && [...branchSelect.options].some((opt) => opt.value === saved)) {
    branchSelect.value = saved;
  }
  if (!branchSelect.value && branchSelect.options.length) {
    branchSelect.value = branchSelect.options[0].value;
  }
  setBranch(branchSelect.value);
}

function updateRecordHeader(record) {
  recordTitle.textContent = `${record.time || ""} · ${record.staff_name || ""}`.trim() || "Запись";
  recordCard.innerHTML = `
    <div class="record-top">
      <div class="record-time">${record.time || "—"}</div>
      <div class="record-status">${record.status || ""}</div>
    </div>
    <div class="record-staff">${record.staff_name || "—"}</div>
    <div class="record-client">${record.client_name || ""}</div>
  `;
}

async function openRecord(record) {
  state.selectedRecord = record;
  state.selectedServiceId = null;
  state.storageId = null;
  state.selectedGood = null;
  state.sessionAdds = [];
  renderBasket();
  try {
    const params = new URLSearchParams({ branch_id: state.branchId });
    const data = await fetchJSON(`${withBase("/api/mini/records")}/${record.record_id}?${params}`);
    const detail = data.record || record;
    state.selectedServiceId = data.service_id || null;
    state.storageId = data.storage_id || null;
    state.selectedRecord = { ...record, ...detail };
    updateRecordHeader(state.selectedRecord);
  } catch (err) {
    console.warn(err);
    updateRecordHeader(record);
  }
  showScreen("goods");
  setTimeout(() => goodsSearch.focus(), 100);
  renderSelectedGood();
  renderRecent();
}

function renderSelectedGood() {
  if (!state.selectedGood) {
    selectedGoodTitle.textContent = "Товар не выбран";
    selectedGoodMeta.textContent = "—";
    goodAmount.value = "";
    addGoodBtn.disabled = true;
    return;
  }
  selectedGoodTitle.textContent = state.selectedGood.title || "Товар";
  selectedGoodMeta.textContent = formatPrice(state.selectedGood.price, state.selectedGood.unit);
  if (!goodAmount.value) {
    goodAmount.value = "10";
  }
  addGoodBtn.disabled = false;
}

function renderGoods(items) {
  goodsResults.innerHTML = "";
  if (!items.length) {
    const empty = document.createElement("div");
    empty.className = "mini-empty";
    empty.textContent = "Ничего не найдено.";
    goodsResults.appendChild(empty);
    return;
  }
  items.forEach((item) => {
    const card = document.createElement("button");
    card.type = "button";
    card.className = "goods-card";
    card.innerHTML = `
      <div class="goods-title">${item.title}</div>
      <div class="goods-meta">${formatPrice(item.price, item.unit)}</div>
    `;
    card.addEventListener("click", () => {
      state.selectedGood = item;
      pushRecent(item);
      renderSelectedGood();
      renderRecent();
    });
    goodsResults.appendChild(card);
  });
}

let goodsSearchTimer = null;
async function runGoodsSearch() {
  const term = goodsSearch.value.trim();
  if (term.length < 2) {
    goodsResults.innerHTML = "";
    return;
  }
  const params = new URLSearchParams({ branch_id: state.branchId, term });
  try {
    const data = await fetchJSON(`${withBase("/api/mini/goods/search")}?${params}`);
    renderGoods(data.items || []);
  } catch (err) {
    console.warn(err);
    showToast("Ошибка поиска товаров", "err");
  }
}

function loadRecent() {
  try {
    const raw = localStorage.getItem(RECENT_KEY);
    return raw ? JSON.parse(raw) : [];
  } catch (_) {
    return [];
  }
}

function saveRecent(items) {
  localStorage.setItem(RECENT_KEY, JSON.stringify(items.slice(0, 10)));
}

function pushRecent(item) {
  const items = loadRecent().filter((g) => g.good_id !== item.good_id);
  items.unshift(item);
  saveRecent(items);
}

function renderRecent() {
  const items = loadRecent();
  recentGoods.innerHTML = "";
  if (!items.length) return;
  items.forEach((item) => {
    const chip = document.createElement("button");
    chip.type = "button";
    chip.className = "chip";
    chip.textContent = item.title;
    chip.addEventListener("click", () => {
      state.selectedGood = item;
      renderSelectedGood();
    });
    recentGoods.appendChild(chip);
  });
}

function renderBasket() {
  basketList.innerHTML = "";
  if (!state.sessionAdds.length) {
    basketList.innerHTML = '<div class="mini-empty">Пока ничего не добавлено.</div>';
    return;
  }
  state.sessionAdds.forEach((item) => {
    const row = document.createElement("div");
    row.className = "basket-row";
    const modeLabel = item.mode ? ` · ${item.mode}` : "";
    row.innerHTML = `
      <div>
        <div class="basket-title">${item.title}</div>
        <div class="basket-meta">${item.amount} ${item.unit || ""} · ${formatPrice(item.price, item.unit)}${modeLabel}</div>
      </div>
      <div class="basket-time">${item.time || ""}</div>
    `;
    basketList.appendChild(row);
  });
}

async function addGoodToRecord() {
  if (!state.selectedRecord || !state.selectedGood) return;
  const amount = Number(goodAmount.value);
  if (!amount || amount <= 0) {
    showToast("Укажите граммы", "warn");
    return;
  }
  addGoodBtn.disabled = true;
  try {
    const payload = {
      branch_id: state.branchId,
      good_id: state.selectedGood.good_id,
      amount,
      service_id: state.selectedServiceId,
      storage_id: state.storageId,
      tg_user: state.tgUser,
      mode: state.sendMode || "goods_only",
    };
    const data = await postJSON(`${withBase("/api/mini/records")}/${state.selectedRecord.record_id}/goods`, payload);
    const added = data.added || {};
    state.sessionAdds.unshift({
      good_id: state.selectedGood.good_id,
      title: state.selectedGood.title,
      amount,
      unit: state.selectedGood.unit,
      price: added.price ?? state.selectedGood.price,
      goods_transaction_id: added.goods_transaction_id || null,
      mode: state.sendMode || "goods_only",
      time: new Date().toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit" }),
    });
    renderBasket();
    showToast("Товар добавлен", "ok");
  } catch (err) {
    console.warn(err);
    showToast(err.message || "Ошибка добавления", "err");
  } finally {
    addGoodBtn.disabled = false;
  }
}

async function undoLast() {
  if (!state.sessionAdds.length) return;
  const last = state.sessionAdds[0];
  if (!last.goods_transaction_id) {
    showToast("Нет данных для отмены", "warn");
    return;
  }
  try {
    await postJSON(`${withBase("/api/mini/records")}/${state.selectedRecord.record_id}/goods/undo`, {
      branch_id: state.branchId,
      service_id: state.selectedServiceId,
      goods_transaction_id: last.goods_transaction_id,
      tg_user: state.tgUser,
    });
    state.sessionAdds.shift();
    renderBasket();
    showToast("Последняя позиция удалена", "ok");
  } catch (err) {
    console.warn(err);
    showToast(err.message || "Ошибка отмены", "err");
  }
}

function initTelegram() {
  const tg = window.Telegram?.WebApp;
  if (tg) {
    tg.ready();
    tg.expand();
    state.tgUser = tg.initDataUnsafe?.user || null;
  }
}

let recordSearchTimer = null;
recordSearch.addEventListener("input", () => {
  clearTimeout(recordSearchTimer);
  recordSearchTimer = setTimeout(loadRecords, 250);
});

branchSelect.addEventListener("change", async () => {
  setBranch(branchSelect.value);
  await loadRecords();
});

toggleButtons.forEach((btn) => {
  btn.addEventListener("click", async () => {
    setMode(btn.dataset.mode);
    await loadRecords();
  });
});

sendModeButtons.forEach((btn) => {
  btn.addEventListener("click", () => {
    setSendMode(btn.dataset.send);
  });
});

backButton.addEventListener("click", () => showScreen("records"));

goodsSearch.addEventListener("input", () => {
  clearTimeout(goodsSearchTimer);
  goodsSearchTimer = setTimeout(runGoodsSearch, 250);
});

goodQuick.addEventListener("click", (evt) => {
  const btn = evt.target.closest("button");
  if (!btn) return;
  const add = Number(btn.dataset.add);
  if (!add) return;
  const current = Number(goodAmount.value || 0);
  goodAmount.value = (current + add).toString();
});

addGoodBtn.addEventListener("click", addGoodToRecord);
undoGoodBtn.addEventListener("click", undoLast);

async function init() {
  initTelegram();
  setMode(localStorage.getItem("miniMode") || "now");
  setSendMode(localStorage.getItem(SEND_MODE_KEY) || "goods_only");
  await loadBranches();
  await loadRecords();
  renderBasket();
}

init().catch((err) => console.error(err));
