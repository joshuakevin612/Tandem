import { api, auth, useAuth, setUnauthorizedHandler, ApiError } from "./api.js";
import { $, $$, esc, applyVars, STATUSES, statusOf, shortId, ago, stamp, duration, clock, highlightJSON, copy, icon } from "./util.js";
import { initMotion, countTo, movePill, lockScroll, scrollToY } from "./motion.js";
import { emblemSVG } from "./emblem.js";
import { initRewards, celebrate, pulse, bump } from "./reward.js";

const POLL_MS = 3000;
const STAT_CAP = 200;

const state = {
  route: null,
  live: true,
  tenant: null,
  filter: { status: "all", queue: "" },
  jobs: [],
  cursor: null,
  loadingMore: false,
  stats: {},
  schedules: [],
  selected: null,
  lastSync: 0,
  highlight: null,
};

const app = $("#app");
const drawer = $(".drawer");
const sheet = $(".sheet");
const scrim = $(".scrim");
let pollTimer = null;
let tickTimer = null;
let listAbort = null;
let drawerTimer = null;
let drawerJob = null;
let lastFocus = null;
let refetchPending = false;

const STAT_ORDER = ["queued", "running", "succeeded", "dead"];
const LABEL = { all: "All", queued: "Queued", running: "Running", succeeded: "Succeeded", dead: "Dead", cancelled: "Cancelled" };
const TEMPLATES = {
  sleep: { ms: 2000 },
  flaky: { fail_rate: 0.5 },
  http: { url: "https://example.com", method: "GET", timeout_ms: 5000 },
};

/* =============================== boot =============================== */

initMotion();
initRewards();
setUnauthorizedHandler((msg) => { auth.clear(); toast(msg, true); showGate(); });
addEventListener("hashchange", route);
document.addEventListener("visibilitychange", () => { if (!document.hidden) poll(true); });
document.addEventListener("keydown", onKey);
$(".live").addEventListener("click", toggleLive);
$(".account").addEventListener("click", openConnection);
$('[data-action="new"]').addEventListener("click", () => (state.route === "schedules" ? openScheduleForm() : openJobForm()));
scrim.addEventListener("click", closeOverlays);
addEventListener("resize", syncIndicators);
document.fonts?.ready.then(syncIndicators); // indicators are measured from text width
drawer.inert = true;
sheet.inert = true;

function syncIndicators() {
  syncNavPill();
  $$(".seg").forEach((s) => movePill($(".seg__pill", s), $('[aria-pressed="true"]', s)));
}

function start() {
  useAuth(auth.get());
  $(".nav").hidden = false;
  $(".bar__right").hidden = false;
  paintTenant();
  route();
}


// The tenant name is no longer shown anywhere in the console.
function paintTenant() {}

/* =============================== routing =============================== */

function parseHash() {
  const [, view = "jobs", id] = location.hash.replace(/^#/, "").split("/");
  return { view: ["schedules", "home"].includes(view) ? view : "jobs", id };
}

function route() {
  if (!auth.get()) return showGate();
  const { view, id } = parseHash();
  if (view === "home") {
    if (state.route !== "home") { window.scrollTo({ top: 0, behavior: "instant" }); state.route = "home"; closeOverlays(); renderHome(); syncNav(view); }
    return;
  }
  if (view !== state.route) {
    window.scrollTo({ top: 0, behavior: "instant" });
    state.route = view;
    setLanding(false);
    view === "jobs" ? renderJobs() : renderSchedules();
    syncNav(view);
  }
  if (view === "jobs" && id) openJob(id); else if (drawer.classList.contains("is-open")) closeDrawer(false);
}

function syncNav(view) {
  for (const a of $$(".nav__link")) {
    if (a.dataset.route === view) a.setAttribute("aria-current", "page"); else a.removeAttribute("aria-current");
  }
  $("[data-new-label]").textContent = view === "schedules" ? "New schedule" : "New job";
  $('[data-action="new"]').setAttribute("aria-label", view === "schedules" ? "New schedule" : "New job");
  requestAnimationFrame(syncNavPill);
}
function syncNavPill() {
  const active = $('.nav__link[aria-current="page"]');
  if (active) movePill($(".nav__pill"), active); else $(".nav__pill").style.width = "0px";
}

/* =============================== connect =============================== */

const FACTS = [
  { c: "succeeded", t: "No one left on the floor", p: "At-least-once delivery. A 202 means the job runs, even if the worker holding it is killed mid-run." },
  { c: "running", t: "One lead per step", p: "Every lease carries a fresh fencing token, so a worker that lost its job can't write over the new owner's result." },
  { c: "queued", t: "Once on every beat", p: "Cron schedules advance by compare-and-swap. Two schedulers can never fire the same tick." },
];

function landingHTML(action) {
  return `
    <section class="hero">
      <div class="hero__copy">
        <h1 class="brand" aria-label="tandem">${[..."tandem"].map((ch, i) => `<span aria-hidden="true" data-i="${i}">${ch}</span>`).join("")}</h1>
        <p class="hero__lede">A distributed job queue on Postgres, where jobs and workers move in perfect step.</p>
        <p class="hero__body">Each worker takes a job on a lease and leads it through to the last note. If a partner stumbles, the lease lapses and the reaper guides the job back to the front of the floor. Fencing tokens let only one partner lead each step, and schedules fire once on every beat. Whatever happens in the dark, an accepted job always finishes the dance.</p>
        <div class="hero__action">${action}</div>
      </div>
      <div class="hero__art">${emblemSVG()}</div>
    </section>
    <section class="facts" aria-label="Guarantees">${FACTS.map((f) => `<div class="fact"><h2><span class="dot" data-c="${f.c}"></span>${esc(f.t)}</h2><p>${esc(f.p)}</p></div>`).join("")}</section>`;
}

// Size the wordmark so it spans the content width exactly, whatever the font metrics.
function fitBrand() {
  const el = $(".brand", app);
  if (!el) return;
  el.style.fontSize = "";
  const word = [...el.children].reduce((w, s) => w + s.getBoundingClientRect().width, 0);
  const avail = el.clientWidth;
  if (!word || !avail) return;
  const size = parseFloat(getComputedStyle(el).fontSize) * (avail / word) * 0.995;
  el.style.fontSize = `${Math.min(size, 240)}px`;
}
addEventListener("resize", fitBrand);
document.fonts?.ready.then(fitBrand);

function setLanding(on) {
  document.body.classList.toggle("is-landing", on);
  $(".nav").hidden = on && !auth.get();
  $(".bar__right").hidden = !auth.get();
}

// Connected visitors who open the landing page get a way back in, not a form.
function renderHome() {
  stopPolling();
  setLanding(true);
  app.innerHTML = landingHTML(`<a class="btn btn--pop btn--lg" href="#/jobs">Open console</a>`);
  applyVars(app); fitBrand();
}

function showGate() {
  stopPolling();
  state.route = null;
  closeOverlays();
  setLanding(true);
  let mode = "apikey";
  app.innerHTML = landingHTML(`
      <form class="connect" novalidate>
        <label class="sr-only" for="connect-value" data-label>API key</label>
        <div class="connect__row">
          <input id="connect-value" class="input mono" name="value" autocomplete="off" spellcheck="false" placeholder="Paste an API key, td_…">
          <button class="btn btn--pop btn--lg" type="submit">Open console</button>
        </div>
        <small class="field__error" data-error></small>
        <button type="button" class="linkish" data-mode-toggle>Server in header mode? Use a tenant ID</button>
      </form>`);
  applyVars(app); fitBrand();
  const form = $("form", app), input = $("input", form), err = $("[data-error]", form);
  const toggle = $("[data-mode-toggle]", form);
  toggle.addEventListener("click", () => {
    mode = mode === "apikey" ? "header" : "apikey";
    $("[data-label]", form).textContent = mode === "apikey" ? "API key" : "Tenant ID";
    input.placeholder = mode === "apikey" ? "Paste an API key, td_…" : "Tenant ID, or leave blank for default";
    toggle.textContent = mode === "apikey" ? "Server in header mode? Use a tenant ID" : "Use an API key instead";
    input.value = ""; input.classList.remove("is-invalid"); err.textContent = "";
    input.focus();
  });
  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const value = input.value.trim();
    if (mode === "apikey" && !value) { input.classList.add("is-invalid"); err.textContent = "Paste an API key to connect."; return; }
    const creds = mode === "apikey" ? { mode, key: value } : { mode, tenant: value || "default" };
    const btn = $("button[type=submit]", form);
    btn.disabled = true; btn.classList.add("is-busy");
    try {
      const res = await api.probe(creds);
      creds.tenantLabel = mode === "header" ? creds.tenant : res?.jobs?.[0]?.tenant_id || null;
      auth.set(creds);
      history.replaceState(null, "", "#/jobs");
      start();
    } catch (ex) {
      input.classList.add("is-invalid");
      err.textContent = ex instanceof ApiError && ex.status === 401 ? "That key isn't valid. Check it and try again." : (ex.message || "Couldn't connect.");
    } finally { btn.disabled = false; btn.classList.remove("is-busy"); }
  });
}

/* =============================== jobs =============================== */

function renderJobs() {
  app.innerHTML = `
    <section class="slab" aria-labelledby="jobs-title">
      <div class="slab__head">
        <div>
          <h1 class="slab__title" id="jobs-title">Jobs</h1>
        </div>
        <div class="slab__meta" data-synced>Connecting</div>
      </div>
      <div>
        <div class="stats">
          ${STAT_ORDER.map((s) => `
            <button type="button" class="stat" data-status="${s}" aria-pressed="false">
              <span class="stat__label"><span class="swatch"></span>${LABEL[s]}${s === "dead" ? `<span class="stat__flag">needs you</span>` : ""}</span>
              <span class="stat__value">–</span>
            </button>`).join("")}
        </div>
      </div>
      <div class="mix" role="img" aria-label="Share of jobs by status">${STAT_ORDER.map((s) => `<i data-k="${s}"></i>`).join("")}</div>
    </section>

    <section class="section" aria-labelledby="list-title">
      <div class="section__head">
        <h2 class="section__title" id="list-title">Recent jobs <span class="count" data-count></span></h2>
        <div class="toolbar">
          <div class="seg" role="group" aria-label="Filter by status">
            <span class="seg__pill" aria-hidden="true"></span>
            ${["all", ...STATUSES].map((s) => `<button type="button" class="seg__opt" data-filter="${s}" aria-pressed="${state.filter.status === s}">${LABEL[s]}</button>`).join("")}
          </div>
          <label class="field-inline">${icon.search}<span class="sr-only">Filter by queue</span><input data-queue placeholder="Queue" value="${esc(state.filter.queue)}" spellcheck="false" autocomplete="off"></label>
        </div>
      </div>
      <div class="table" role="table" aria-label="Jobs">
        <div class="thead" role="row">
          <span role="columnheader">Status</span><span role="columnheader">Type</span><span role="columnheader" class="col-queue">Queue</span>
          <span role="columnheader">Attempts</span><span role="columnheader" class="cell--right">Created</span><span role="columnheader" class="col-id cell--right">ID</span>
        </div>
        <div class="tbody" role="rowgroup">${'<div class="skeleton"></div>'.repeat(6)}</div>
      </div>
      <div class="more"></div>
    </section>`;

  const seg = $(".seg", app);
  requestAnimationFrame(() => movePill($(".seg__pill", seg), $('[aria-pressed="true"]', seg)));
  seg.addEventListener("click", (e) => { const b = e.target.closest("[data-filter]"); if (b) setFilter(b.dataset.filter); });
  $$(".stat", app).forEach((b) => b.addEventListener("click", () => {
    const next = state.filter.status === b.dataset.status ? "all" : b.dataset.status;
    setFilter(next);
    if (next !== "all") scrollToY($(".section", app).getBoundingClientRect().top + scrollY - 72);
  }));
  let qt;
  $("[data-queue]", app).addEventListener("input", (e) => { clearTimeout(qt); qt = setTimeout(() => { state.filter.queue = e.target.value.trim(); reloadList(); }, 250); });
  $(".tbody", app).addEventListener("click", (e) => { const r = e.target.closest(".row"); if (r) location.hash = `#/jobs/${r.dataset.id}`; });
  paintStatActive();

  state.jobs = []; state.cursor = null;
  startPolling();
  reloadList();
}

function setFilter(status) {
  if (state.filter.status === status) return;
  state.filter.status = status;
  const seg = $(".seg", app);
  $$("[data-filter]", seg).forEach((b) => b.setAttribute("aria-pressed", String(b.dataset.filter === status)));
  movePill($(".seg__pill", seg), $('[aria-pressed="true"]', seg));
  paintStatActive();
  reloadList();
}

function paintStatActive() {
  $$(".stat", app).forEach((s) => {
    const on = s.dataset.status === state.filter.status;
    s.classList.toggle("is-active", on);
    s.setAttribute("aria-pressed", String(on));
  });
}

function listQuery(extra = {}) {
  return { status: state.filter.status === "all" ? "" : state.filter.status, queue: state.filter.queue, limit: 50, ...extra };
}

async function reloadList() {
  listAbort?.abort();
  listAbort = new AbortController();
  const tbody = $(".tbody", app);
  if (!tbody) return;
  try {
    const res = await api.listJobs(listQuery(), listAbort.signal);
    state.jobs = res.jobs; state.cursor = res.next_cursor;
    tbody.innerHTML = "";
    paintRows(true);
  } catch (e) { if (e.name !== "AbortError") listError(e); }
}

async function refreshList() {
  if (state.jobs.length > 200) return; // past one API page we stop live-refreshing what's paged in
  const n = Math.min(Math.max(50, state.jobs.length), 200);
  const res = await api.listJobs(listQuery({ limit: n }));
  if (state.jobs.length <= 50) state.cursor = res.next_cursor;
  state.jobs = res.jobs;
  paintRows(false);
}

async function loadMore(btn) {
  if (!state.cursor || state.loadingMore) return;
  state.loadingMore = true; btn.disabled = true; btn.classList.add("is-busy");
  try {
    const res = await api.listJobs(listQuery({ cursor: state.cursor }));
    const known = new Set(state.jobs.map((j) => j.id));
    state.jobs.push(...res.jobs.filter((j) => !known.has(j.id)));
    state.cursor = res.next_cursor;
    paintRows(false);
  } catch (e) { toast(e.message, true); }
  finally { state.loadingMore = false; }
}

function listError(e) {
  const tbody = $(".tbody", app);
  if (tbody) tbody.innerHTML = `<div class="empty"><strong>Jobs didn't load</strong><span>${esc(e.message)}. Retrying every few seconds.</span></div>`;
}

function statusHTML(s, chip = false) {
  return `<span class="status${chip ? " status--chip" : ""}" data-k="${s}" data-c="${s}"><span class="dot"></span>${esc(s)}</span>`;
}

function rowHTML(j) {
  const s = statusOf(j.status);
  return `
    <span class="cell col-status" role="cell">${statusHTML(s)}</span>
    <span class="cell cell--type col-type" role="cell" title="${esc(j.type)}">${esc(j.type)}</span>
    <span class="cell cell--muted col-queue" role="cell">${esc(j.queue)}</span>
    <span class="cell col-attempts" role="cell"><span class="attempts${s === "dead" ? " is-fail" : ""}">${j.attempts} of ${j.max_attempts}</span></span>
    <span class="cell cell--muted cell--right col-created num" role="cell" data-ago="${esc(j.created_at)}" title="${esc(stamp(j.created_at))}">${esc(ago(j.created_at))}</span>
    <span class="cell cell--id cell--right col-id" role="cell">${esc(shortId(j.id))}</span>`;
}

function paintRows(initial) {
  const tbody = $(".tbody", app);
  if (!tbody) return;
  const count = $("[data-count]", app);
  if (count) count.textContent = state.jobs.length ? `${state.jobs.length}${state.cursor ? "+" : ""}` : "";

  if (!state.jobs.length) {
    const what = state.filter.status === "all" ? "jobs" : `${state.filter.status} jobs`;
    const where = state.filter.queue ? ` in ${esc(state.filter.queue)}` : "";
    tbody.innerHTML = `<div class="empty"><strong>No ${esc(what)}${where}</strong>${state.filter.status === "all" && !state.filter.queue ? `<span>Submitted jobs show up here as soon as they're accepted.</span><button type="button" class="btn btn--pop" data-empty-new>New job</button>` : ""}</div>`;
    $("[data-empty-new]", tbody)?.addEventListener("click", openJobForm);
    paintMore();
    return;
  }
  $(".empty", tbody)?.remove();
  $$(".skeleton", tbody).forEach((s) => s.remove());

  const existing = new Map($$(".row", tbody).map((r) => [r.dataset.id, r]));
  let prev = null;
  for (const j of state.jobs) {
    let row = existing.get(j.id);
    const sig = `${j.status}|${j.attempts}|${j.updated_at}`;
    if (!row) {
      row = document.createElement("button");
      row.type = "button";
      row.className = "row";
      row.setAttribute("role", "row");
      row.dataset.id = j.id;
      row.innerHTML = rowHTML(j);
      row.dataset.sig = sig;
      row.addEventListener("animationend", () => row.classList.remove("is-flash"));
      if (!initial) row.classList.add("is-flash");
    } else if (row.dataset.sig !== sig) {
      const changed = row.dataset.sig.split("|")[0] !== j.status;
      row.innerHTML = rowHTML(j);
      row.dataset.sig = sig;
      if (changed) { row.classList.remove("is-flash"); void row.offsetWidth; row.classList.add("is-flash"); }
    }
    existing.delete(j.id);
    row.classList.toggle("is-selected", j.id === state.selected);
    applyVars(row);
    const want = prev ? prev.nextElementSibling : tbody.firstElementChild;
    if (want !== row) tbody.insertBefore(row, want);
    prev = row;
  }
  for (const r of existing.values()) r.remove();
  paintMore();
}

function paintMore() {
  const more = $(".more", app);
  if (!more) return;
  if (state.cursor && state.jobs.length) {
    const b = $("button", more);
    if (!b) {
      more.innerHTML = `<button type="button" class="btn btn--ghost">Load more</button>`;
      $("button", more).addEventListener("click", (e) => loadMore(e.currentTarget));
    } else { b.disabled = false; b.classList.remove("is-busy"); }
  } else more.innerHTML = "";
}

async function refreshStats() {
  const results = await Promise.all(STAT_ORDER.map((s) =>
    api.listJobs({ status: s, limit: STAT_CAP }).then((r) => [s, r]).catch(() => [s, null])));
  if (results.every(([, r]) => r === null)) throw new Error("stats unavailable");
  for (const [s, r] of results) {
    if (!r) continue;
    state.stats[s] = { n: r.jobs.length, more: !!r.next_cursor };
    if (!state.tenant && r.jobs[0]) {
      state.tenant = r.jobs[0].tenant_id;
      const a = auth.get();
      if (a?.mode === "apikey" && !a.tenantLabel) { a.tenantLabel = state.tenant; auth.set(a); paintTenant(); }
    }
  }
  const total = STAT_ORDER.reduce((n, s) => n + (state.stats[s]?.n || 0), 0);
  for (const el of $$(".stat", app)) {
    const st = state.stats[el.dataset.status];
    if (!st) continue;
    const v = $(".stat__value", el);
    const before = v.dataset.seen ? Number(v.dataset.value) : null;
    countTo(v, st.n, { suffix: st.more ? "+" : "" });
    // finished work ticking up is the console's quiet reward
    if (before !== null && st.n > before && el.dataset.status === "succeeded") bump(v);
    v.classList.toggle("has-value", st.n > 0);
    if (el.dataset.status === "dead") el.classList.toggle("has-dead", st.n > 0);
  }
  for (const i of $$(".mix i", app)) {
    const n = state.stats[i.dataset.k]?.n || 0;
    i.style.setProperty("--w", total ? `${((n / total) * 100).toFixed(2)}%` : "0%");
  }
}

/* =============================== polling =============================== */

function startPolling() { stopPolling(); poll(true); tickTimer = setInterval(tick, 1000); }
function stopPolling() { clearTimeout(pollTimer); clearInterval(tickTimer); pollTimer = tickTimer = null; }

async function poll(force = false) {
  clearTimeout(pollTimer);
  if (!auth.get() || !state.route) return;
  if (state.live || force) {
    try {
      if (state.route === "jobs") await Promise.all([refreshStats(), state.jobs.length ? refreshList() : null]);
      else await refreshSchedules();
      state.lastSync = Date.now();
      markLive(true);
      tick();
    } catch (e) {
      if (!(e instanceof ApiError && e.status === 401)) markLive(false);
    }
  }
  if (!document.hidden) pollTimer = setTimeout(() => poll(), POLL_MS);
}

function markLive(ok) {
  const b = $(".live");
  b.classList.toggle("is-on", ok && state.live);
  b.classList.toggle("is-error", !ok);
  $(".live__text").textContent = !ok ? "Can't reach API" : state.live ? "Live" : "Paused";
}

function toggleLive() {
  state.live = !state.live;
  markLive(true);
  if (state.live) poll(true);
  pulse($(".live"));
  toast(state.live ? "Live updates resumed" : "Live updates paused");
}

function tick() {
  const s = $("[data-synced]", app);
  if (s && state.lastSync) s.textContent = state.live ? `Updated ${ago(state.lastSync)}` : `Paused, last updated ${ago(state.lastSync)}`;
  for (const el of $$("[data-ago]", app)) el.textContent = ago(el.dataset.ago);
  if (state.route === "schedules") tickSchedules();
}

/* =============================== job panel =============================== */

async function openJob(id) {
  state.selected = id;
  $$(".row").forEach((r) => r.classList.toggle("is-selected", r.dataset.id === id));
  if (!drawer.classList.contains("is-open")) {
    drawer.innerHTML = `<div class="drawer__head"><div><h2 class="drawer__title" id="drawer-title">Loading job</h2><div class="drawer__id">${esc(id)}</div></div></div>`;
    openOverlay(drawer);
  }
  await loadJob(id, true);
}

async function loadJob(id, first) {
  clearTimeout(drawerTimer);
  if (state.selected !== id) return;
  try {
    const j = await api.getJob(id);
    if (state.selected !== id) return;
    const changed = !drawerJob || drawerJob.id !== j.id || drawerJob.updated_at !== j.updated_at;
    drawerJob = j;
    if (first || changed) paintDrawer(j);
    if (["queued", "running"].includes(j.status)) drawerTimer = setTimeout(() => loadJob(id, false), 2000);
  } catch (e) {
    if (!first) return;
    drawer.innerHTML = `<div class="drawer__head"><div><h2 class="drawer__title" id="drawer-title">${e.status === 404 ? "Job not found" : "Job didn't load"}</h2><div class="drawer__id">${esc(id)}</div></div><button class="btn btn--ghost btn--icon" data-close aria-label="Close">${icon.close}</button></div>`;
    $("[data-close]", drawer).addEventListener("click", () => closeDrawer());
  }
}

function lifecycle(j) {
  const s = statusOf(j.status);
  const items = [{ c: "queued", t: "Submitted", at: j.created_at }];
  const delayed = new Date(j.run_at) - new Date(j.created_at) > 1500;
  // a requeue resets attempts and sets run_at to now, but keeps last_error
  const requeued = delayed && j.attempts === 0 && j.last_error && Math.abs(new Date(j.run_at) - new Date(j.updated_at)) < 2000;
  if (delayed) items.push({ c: "queued", t: requeued ? "Requeued" : new Date(j.run_at) > Date.now() ? "Scheduled to run" : "Became runnable", at: j.run_at });
  if (j.status === "running") items.push({ c: "running", t: "Running", at: null, worker: j.worker_id, sub: j.locked_until ? `Lease held until ${stamp(j.locked_until)}` : "" });
  if (j.attempts > 0 && j.status === "queued") items.push({ c: "dead", t: `Attempt ${j.attempts} failed, will retry`, at: j.run_at, err: j.last_error });
  if (["succeeded", "dead", "failed", "cancelled"].includes(j.status)) {
    const t = { succeeded: "Succeeded", dead: "Moved to dead letter", failed: "Failed", cancelled: "Cancelled" }[j.status];
    items.push({ c: s, t, at: j.finished_at, err: s === "dead" ? j.last_error : null });
  }
  return `<ol class="timeline">${items.map((it) => `
    <li><span class="dot" data-c="${it.c}"></span>
      <div class="timeline__row"><strong>${esc(it.t)}</strong><span>${esc(it.at ? stamp(it.at) : "Now")}</span></div>
      ${it.worker ? `<div class="timeline__worker">${esc(it.sub || "")}</div>` : ""}
      ${it.err ? `<div class="timeline__err">${esc(it.err)}</div>` : ""}
    </li>`).join("")}</ol>`;
}

function paintDrawer(j) {
  const s = statusOf(j.status);
  const ran = j.finished_at ? new Date(j.finished_at) - new Date(j.run_at) : j.status === "running" ? Date.now() - new Date(j.run_at) : null;
  let payload;
  try { payload = typeof j.payload === "string" ? JSON.parse(j.payload) : j.payload; } catch { payload = j.payload; }
  const scrollTop = $(".drawer__body", drawer)?.scrollTop || 0;
  const timeLabel = j.finished_at ? "Duration" : j.status === "running" ? "Running for" : "Waiting for";
  const timeValue = j.status === "queued" ? duration(Math.max(0, Date.now() - new Date(j.run_at))) : duration(ran);
  drawer.innerHTML = `
    <div class="drawer__head">
      <div>
        ${statusHTML(s, true)}
        <h2 class="drawer__title" id="drawer-title">${esc(j.type)}</h2>
        <div class="drawer__id">${esc(j.id)}<button class="copy" data-copy="${esc(j.id)}" aria-label="Copy job ID">${icon.copy}</button></div>
      </div>
      <button class="btn btn--ghost btn--icon" data-close aria-label="Close">${icon.close}</button>
    </div>
    <div class="drawer__body">
      <dl class="kv">
        <div><dt>Queue</dt><dd>${esc(j.queue)}</dd></div>
        <div><dt>Priority</dt><dd>${j.priority}</dd></div>
        <div><dt>Attempts</dt><dd>${j.attempts} of ${j.max_attempts}</dd></div>
        <div><dt>${timeLabel}</dt><dd>${esc(timeValue)}</dd></div>
        <div><dt>Created</dt><dd>${esc(stamp(j.created_at))}</dd></div>
        <div><dt>Last updated</dt><dd>${esc(stamp(j.updated_at))}</dd></div>
        ${j.worker_id ? `<div class="kv__wide"><dt>${j.status === "running" ? "Worker" : "Last worker"}</dt><dd class="mono">${esc(j.worker_id)}</dd></div>` : ""}
        ${j.idempotency_key ? `<div class="kv__wide"><dt>Idempotency key</dt><dd class="mono">${esc(j.idempotency_key)}</dd></div>` : ""}
      </dl>
      <div><div class="block__label">History</div>${lifecycle(j)}</div>
      ${j.last_error && s !== "dead" && j.status !== "queued" ? `<div><div class="block__label">Last error</div><pre class="code code--error">${esc(j.last_error)}</pre></div>` : ""}
      <div><div class="block__label">Payload</div><pre class="code">${highlightJSON(payload)}</pre></div>
    </div>
    <div class="drawer__foot">
      ${["queued", "running"].includes(j.status) ? `<button class="btn btn--danger" data-act="cancel">Cancel job</button>` : ""}
      ${["dead", "failed"].includes(j.status) ? `<button class="btn btn--pop" data-act="requeue">${icon.retry}Requeue</button>` : ""}
    </div>`;
  if (!$(".drawer__foot button", drawer)) $(".drawer__foot", drawer).remove();
  applyVars(drawer);
  $(".drawer__body", drawer).scrollTop = scrollTop;
  $("[data-close]", drawer).addEventListener("click", () => closeDrawer());
  $("[data-copy]", drawer).addEventListener("click", async (e) => {
    const b = e.currentTarget;
    if (!(await copy(b.dataset.copy))) return;
    b.innerHTML = icon.check; b.classList.add("is-done");
    celebrate(b, { count: 10, spread: 0.6 });
    setTimeout(() => { if (b.isConnected) { b.innerHTML = icon.copy; b.classList.remove("is-done"); } }, 1400);
    toast("Job ID copied");
  });
  $('[data-act="cancel"]', drawer)?.addEventListener("click", (e) => jobAction(e.currentTarget, () => api.cancelJob(j.id), "Job cancelled"));
  $('[data-act="requeue"]', drawer)?.addEventListener("click", (e) => jobAction(e.currentTarget, () => api.requeueJob(j.id), "Job requeued, back in line", true));
}

async function jobAction(btn, fn, msg, party = false) {
  btn.disabled = true; btn.classList.add("is-busy");
  try {
    const j = await fn();
    if (party) celebrate(btn); else pulse(btn);
    toast(msg);
    drawerJob = j; paintDrawer(j);
    if (["queued", "running"].includes(j.status)) drawerTimer = setTimeout(() => loadJob(j.id, false), 2000);
    poll(true);
  } catch (e) { toast(e.message, true); btn.disabled = false; btn.classList.remove("is-busy"); }
}

function closeDrawer(updateHash = true) {
  clearTimeout(drawerTimer);
  state.selected = null; drawerJob = null;
  $$(".row.is-selected").forEach((r) => r.classList.remove("is-selected"));
  closeOverlay(drawer);
  if (updateHash && location.hash.startsWith("#/jobs/")) history.replaceState(null, "", "#/jobs");
}

/* =============================== overlays =============================== */

function openOverlay(el) {
  if (!el.classList.contains("is-open")) lastFocus = document.activeElement;
  el.classList.add("is-open");
  el.setAttribute("aria-hidden", "false");
  el.inert = false;
  scrim.classList.add("is-open");
  lockScroll(true);
  setTimeout(() => (el.querySelector("[autofocus]") || el.querySelector("input, textarea") || el.querySelector("button") || el).focus({ preventScroll: true }), 40);
}
function closeOverlay(el) {
  if (!el.classList.contains("is-open")) return;
  el.classList.remove("is-open");
  el.setAttribute("aria-hidden", "true");
  el.inert = true;
  if (!$(".drawer.is-open, .sheet.is-open")) { scrim.classList.remove("is-open"); lockScroll(false); }
  lastFocus?.focus?.({ preventScroll: true });
}
function closeOverlays() {
  if (sheet.classList.contains("is-open")) return closeOverlay(sheet);
  if (drawer.classList.contains("is-open")) closeDrawer();
}

function onKey(e) {
  if (e.key === "Escape") { closeOverlays(); return; }
  const typing = /^(INPUT|TEXTAREA|SELECT)$/.test(document.activeElement?.tagName);
  if (typing || e.metaKey || e.ctrlKey || e.altKey || !auth.get() || !["jobs", "schedules"].includes(state.route)) return;
  if (sheet.classList.contains("is-open")) return;
  if (e.key === "n") { e.preventDefault(); state.route === "schedules" ? openScheduleForm() : openJobForm(); }
  else if (e.key === "g") location.hash = state.route === "jobs" ? "#/schedules" : "#/jobs";
  else if (e.key === "l") toggleLive();
}

/* =============================== forms =============================== */

function typeChips(selected) {
  return `<div class="chips" role="group" aria-label="Built-in job types">${Object.keys(TEMPLATES).map((t) => `<button type="button" class="chip mono" data-type="${t}" aria-pressed="${t === selected}">${t}</button>`).join("")}</div>`;
}

function wireTypeChips(form) {
  const typeInput = $('[name="type"]', form), payload = $('[name="payload"]', form);
  const sync = () => $$("[data-type]", form).forEach((x) => x.setAttribute("aria-pressed", String(x.dataset.type === typeInput.value.trim())));
  $$("[data-type]", form).forEach((b) => b.addEventListener("click", () => {
    typeInput.value = b.dataset.type;
    payload.value = JSON.stringify(TEMPLATES[b.dataset.type], null, 2);
    sync();
  }));
  typeInput.addEventListener("input", sync);
}

function readPayload(form) {
  const el = $('[name="payload"]', form);
  const raw = el.value.trim();
  if (!raw) return {};
  try { return JSON.parse(raw); } catch (e) { fieldError(el, "Payload isn't valid JSON."); throw e; }
}

function fieldError(el, msg) {
  el.classList.add("is-invalid");
  const err = el.closest(".field")?.querySelector(".field__error");
  if (err) err.textContent = msg;
  el.focus();
}

function clearErrors(form) {
  $$(".is-invalid", form).forEach((x) => x.classList.remove("is-invalid"));
  $$(".field__error", form).forEach((x) => (x.textContent = ""));
}

function openSheet(html, onSubmit) {
  sheet.innerHTML = html;
  applyVars(sheet);
  const form = $("form", sheet);
  $("[data-close]", sheet).addEventListener("click", () => closeOverlay(sheet));
  form.addEventListener("input", (e) => { e.target.classList?.remove("is-invalid"); const er = e.target.closest(".field")?.querySelector(".field__error"); if (er) er.textContent = ""; });
  form.addEventListener("keydown", (e) => { if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) { e.preventDefault(); form.requestSubmit(); } });
  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    clearErrors(form);
    const btn = $('button[type="submit"]', sheet);
    btn.disabled = true; btn.classList.add("is-busy");
    try { await onSubmit(form); }
    catch (ex) {
      if (ex instanceof ApiError) {
        const m = ex.message;
        const field = /cron|never fires/i.test(m) ? "cron" : /timezone|location/i.test(m) ? "timezone" : /name/i.test(m) ? "name" : /type/i.test(m) ? "type" : /priority/.test(m) ? "priority" : /max_attempts/.test(m) ? "max_attempts" : /JSON/.test(m) ? "payload" : null;
        const el = field && $(`[name="${field}"]`, form);
        if (el) fieldError(el, m.charAt(0).toUpperCase() + m.slice(1) + "."); else toast(m, true);
      }
    } finally { btn.disabled = false; btn.classList.remove("is-busy"); }
  });
  openOverlay(sheet);
}

const sheetHead = (title, sub) => `
  <div class="sheet__head">
    <div><h2 class="sheet__title" id="sheet-title">${esc(title)}</h2>${sub ? `<p class="sheet__sub">${esc(sub)}</p>` : ""}</div>
    <button class="btn btn--ghost btn--icon" type="button" data-close aria-label="Close">${icon.close}</button>
  </div>`;
const sheetFoot = (label) => `
  <div class="sheet__foot">
    <span class="sheet__hint"><kbd>Ctrl</kbd> <kbd>Enter</kbd> to submit</span>
    <button class="btn btn--pop" type="submit" form="sheet-form">${esc(label)}</button>
  </div>`;

function openJobForm() {
  openSheet(`
    ${sheetHead("New job", "Runs as soon as a worker is free, unless you set a delay.")}
    <form class="sheet__body" id="sheet-form" novalidate>
      <label class="field"><span>Type</span><input class="input mono" name="type" value="sleep" spellcheck="false" autocomplete="off"><small class="field__error"></small></label>
      ${typeChips("sleep")}
      <label class="field"><span>Payload <em>JSON</em></span><textarea class="textarea" name="payload" spellcheck="false">${esc(JSON.stringify(TEMPLATES.sleep, null, 2))}</textarea><small class="field__error"></small></label>
      <div class="grid-2">
        <label class="field"><span>Queue</span><input class="input" name="queue" placeholder="default" spellcheck="false" autocomplete="off"></label>
        <label class="field"><span>Delay <em>seconds</em></span><input class="input" name="delay" type="number" min="0" step="1" placeholder="0"></label>
      </div>
      <details class="advanced">
        <summary>${icon.chevron}Retries and delivery</summary>
        <div class="advanced__body">
          <div class="grid-2">
            <label class="field"><span>Priority <em>−100 to 100</em></span><input class="input" name="priority" type="number" min="-100" max="100" placeholder="0"><small class="field__error"></small></label>
            <label class="field"><span>Max attempts <em>1 to 25</em></span><input class="input" name="max_attempts" type="number" min="1" max="25" placeholder="5"><small class="field__error"></small></label>
          </div>
          <label class="field"><span>Idempotency key <em>a repeat returns the first job</em></span><input class="input mono" name="idem" spellcheck="false" autocomplete="off"></label>
          <label class="field"><span>Copies <em>submit several at once</em></span><input class="input" name="count" type="number" min="1" max="500" placeholder="1"></label>
        </div>
      </details>
    </form>
    ${sheetFoot("Submit job")}`,
  async (form) => {
    const f = Object.fromEntries(new FormData(form));
    if (!f.type.trim()) { fieldError($('[name="type"]', form), "Enter a job type."); return; }
    const body = { type: f.type.trim(), payload: readPayload(form) };
    if (f.queue.trim()) body.queue = f.queue.trim();
    if (f.priority !== "") body.priority = Number(f.priority);
    if (f.max_attempts !== "") body.max_attempts = Number(f.max_attempts);
    if (Number(f.delay) > 0) body.run_at = new Date(Date.now() + Number(f.delay) * 1000).toISOString();
    const n = Math.min(Math.max(1, Number(f.count) || 1), 500);
    const idem = f.idem.trim();
    let first;
    if (n === 1) first = await api.createJob(body, idem || null);
    else {
      // small batches so the API's rate limiter isn't hammered
      for (let i = 0; i < n; i += 20) {
        const batch = await Promise.all(Array.from({ length: Math.min(20, n - i) }, (_, k) => api.createJob(body, idem ? `${idem}-${i + k}` : null)));
        first ??= batch[0];
      }
    }
    celebrate($('button[type="submit"]', sheet), { count: Math.min(14 + n * 2, 40), spread: n > 1 ? 1.4 : 1 });
    closeOverlay(sheet);
    toast(n === 1 ? `Job ${shortId(first.id)} is on its way` : `${n} jobs on their way`);
    if (state.route !== "jobs") location.hash = "#/jobs"; else poll(true);
  });
  wireTypeChips($("form", sheet));
}

function openScheduleForm() {
  const tz = Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
  const presets = ["@every 30s", "*/5 * * * *", "@hourly", "0 9 * * 1-5"];
  openSheet(`
    ${sheetHead("New schedule", "Submits a job every time the cron expression fires.")}
    <form class="sheet__body" id="sheet-form" novalidate>
      <label class="field"><span>Name</span><input class="input" name="name" autocomplete="off" placeholder="nightly-report"><small class="field__error"></small></label>
      <div class="grid-2">
        <label class="field"><span>Cron</span><input class="input mono" name="cron" value="@every 30s" spellcheck="false" autocomplete="off"><small class="field__error"></small></label>
        <label class="field"><span>Timezone</span><input class="input" name="timezone" value="${esc(tz)}" spellcheck="false" autocomplete="off"><small class="field__error"></small></label>
      </div>
      <div class="chips" role="group" aria-label="Common cron expressions">${presets.map((p) => `<button type="button" class="chip mono" data-cron="${esc(p)}" aria-pressed="${p === "@every 30s"}">${esc(p)}</button>`).join("")}</div>
      <label class="field"><span>Job type</span><input class="input mono" name="type" value="sleep" spellcheck="false" autocomplete="off"><small class="field__error"></small></label>
      ${typeChips("sleep")}
      <label class="field"><span>Payload <em>JSON</em></span><textarea class="textarea" name="payload" spellcheck="false">${esc(JSON.stringify(TEMPLATES.sleep, null, 2))}</textarea><small class="field__error"></small></label>
      <details class="advanced">
        <summary>${icon.chevron}Retries and delivery</summary>
        <div class="advanced__body grid-3">
          <label class="field"><span>Queue</span><input class="input" name="queue" placeholder="default" spellcheck="false" autocomplete="off"></label>
          <label class="field"><span>Priority</span><input class="input" name="priority" type="number" min="-100" max="100" placeholder="0"><small class="field__error"></small></label>
          <label class="field"><span>Max attempts</span><input class="input" name="max_attempts" type="number" min="1" max="25" placeholder="5"><small class="field__error"></small></label>
        </div>
      </details>
    </form>
    ${sheetFoot("Create schedule")}`,
  async (form) => {
    const f = Object.fromEntries(new FormData(form));
    if (!f.name.trim()) { fieldError($('[name="name"]', form), "Give the schedule a name."); return; }
    if (!f.cron.trim()) { fieldError($('[name="cron"]', form), "Enter a cron expression."); return; }
    const body = { name: f.name.trim(), cron: f.cron.trim(), type: f.type.trim(), payload: readPayload(form) };
    if (f.timezone.trim()) body.timezone = f.timezone.trim();
    if (f.queue.trim()) body.queue = f.queue.trim();
    if (f.priority !== "") body.priority = Number(f.priority);
    if (f.max_attempts !== "") body.max_attempts = Number(f.max_attempts);
    const s = await api.createSchedule(body);
    celebrate($('button[type="submit"]', sheet), { count: 24 });
    closeOverlay(sheet);
    toast(`Schedule ${s.name} created`);
    state.highlight = s.id;
    if (state.route !== "schedules") location.hash = "#/schedules"; else poll(true);
  });
  const form = $("form", sheet);
  wireTypeChips(form);
  const cron = $('[name="cron"]', form);
  const sync = () => $$("[data-cron]", form).forEach((x) => x.setAttribute("aria-pressed", String(x.dataset.cron === cron.value.trim())));
  $$("[data-cron]", form).forEach((b) => b.addEventListener("click", () => { cron.value = b.dataset.cron; sync(); cron.classList.remove("is-invalid"); }));
  cron.addEventListener("input", sync);
}

function openConnection() {
  const a = auth.get();
  openSheet(`
    ${sheetHead("Connection", a?.mode === "header" ? "Signed in with a tenant ID header." : "Signed in with an API key.")}
    <form class="sheet__body" id="sheet-form">
      <dl class="kv">
        <div><dt>Credentials</dt><dd>${a?.mode === "header" ? "Tenant ID header" : "API key"}, hidden</dd></div>
        <div><dt>Server</dt><dd>${esc(location.host)}</dd></div>
        <div><dt>Shortcuts</dt><dd>N new, G switch view, L pause live updates</dd></div>
      </dl>
    </form>
    <div class="sheet__foot"><span></span><button class="btn btn--danger" type="submit" form="sheet-form">Disconnect</button></div>`,
  async () => { closeOverlay(sheet); auth.clear(); state.tenant = null; history.replaceState(null, "", "#/jobs"); showGate(); });
}

/* =============================== schedules =============================== */

function renderSchedules() {
  app.innerHTML = `
    <section class="slab" aria-labelledby="sched-title">
      <div class="slab__head">
        <div>
          <h1 class="slab__title" id="sched-title">Schedules</h1>
        </div>
        <div class="slab__meta" data-synced>Connecting</div>
      </div>
      <div class="stats">
        <div class="stat"><span class="stat__label">Active</span><span class="stat__value" data-s="active">–</span></div>
        <div class="stat"><span class="stat__label">Paused</span><span class="stat__value" data-s="paused">–</span></div>
        <div class="stat"><span class="stat__label" data-s="next-label">Next run</span><span class="stat__value num" data-s="next">–</span></div>
        <div class="stat"><span class="stat__label">Ran this hour</span><span class="stat__value" data-s="recent">–</span></div>
      </div>
    </section>
    <section class="section" aria-labelledby="sched-list-title">
      <div class="section__head">
        <h2 class="section__title" id="sched-list-title">All schedules <span class="count" data-count></span></h2>
      </div>
      <div class="sched">${'<div class="skeleton"></div>'.repeat(3)}</div>
    </section>`;
  $(".sched", app).addEventListener("click", onScheduleClick);
  startPolling();
}

async function refreshSchedules() {
  let res;
  try { res = await api.listSchedules(); }
  catch (e) {
    const list = $(".sched", app);
    if (list && !state.schedules.length) list.innerHTML = `<div class="empty"><strong>Schedules didn't load</strong><span>${esc(e.message)}. Retrying every few seconds.</span></div>`;
    throw e;
  }
  state.schedules = res.schedules.sort((a, b) => (b.enabled - a.enabled) || new Date(a.next_fire_at) - new Date(b.next_fire_at));
  paintSchedules();
}

function cronParts(c) {
  const parts = c.trim().split(/\s+/);
  return parts.length >= 5 && !c.startsWith("@") ? parts : [c];
}

function schedHTML(s) {
  return `
    <div class="sched__main">
      <h3 class="sched__name">${esc(s.name)}</h3>
      <div class="sched__sub"><span class="mono">${esc(s.type)}</span> on ${esc(s.queue)}${s.last_fire_at ? `, last ran <span data-ago="${esc(s.last_fire_at)}">${esc(ago(s.last_fire_at))}</span>` : ", hasn't run yet"}</div>
    </div>
    <div class="cron" aria-label="Cron expression">${cronParts(s.cron).map((p) => `<span>${esc(p)}</span>`).join("")}${s.timezone !== "UTC" ? `<span class="tz">${esc(s.timezone)}</span>` : ""}</div>
    <div class="countdown">
      <span class="countdown__value">${s.enabled ? clock(new Date(s.next_fire_at) - Date.now()) : "Paused"}</span>
      <div class="countdown__bar"><i></i></div>
    </div>
    <button type="button" class="toggle" role="switch" aria-checked="${s.enabled}" aria-label="${s.enabled ? "Pause" : "Resume"} ${esc(s.name)}" data-act="toggle"></button>
    <button type="button" class="btn btn--ghost btn--sm btn--icon sched__del" data-act="delete" aria-label="Delete ${esc(s.name)}">${icon.trash}</button>`;
}

function paintSchedules() {
  const list = $(".sched", app);
  if (!list) return;
  const active = state.schedules.filter((s) => s.enabled);
  const count = $("[data-count]", app);
  if (count) count.textContent = state.schedules.length || "";
  const set = (k, v) => { const el = $(`[data-s="${k}"]`, app); if (el) countTo(el, v); };
  set("active", active.length);
  set("paused", state.schedules.length - active.length);
  const hourAgo = Date.now() - 3600e3;
  set("recent", state.schedules.filter((s) => s.last_fire_at && new Date(s.last_fire_at) > hourAgo).length);

  if (!state.schedules.length) {
    list.innerHTML = `<div class="empty"><strong>No schedules yet</strong><span>A schedule submits a job on a cron expression, like every 5 minutes or every weekday at 9.</span><button type="button" class="btn btn--pop" data-act="create">New schedule</button></div>`;
    tickSchedules();
    return;
  }
  $$(".empty, .skeleton", list).forEach((c) => c.remove());
  const existing = new Map($$(".sched__row", list).map((c) => [c.dataset.id, c]));
  let prev = null;
  for (const s of state.schedules) {
    let row = existing.get(s.id);
    const sig = `${s.enabled}|${s.next_fire_at}|${s.last_fire_at}|${s.updated_at}`;
    if (!row) {
      row = document.createElement("article");
      row.className = "sched__row";
      row.dataset.id = s.id;
      row.innerHTML = schedHTML(s);
      row.dataset.sig = sig;
      row.addEventListener("animationend", () => row.classList.remove("is-flash"));
    } else if (row.dataset.sig !== sig) {
      row.innerHTML = schedHTML(s);
      row.dataset.sig = sig;
    }
    row.classList.toggle("is-paused", !s.enabled);
    existing.delete(s.id);
    const want = prev ? prev.nextElementSibling : list.firstElementChild;
    if (want !== row) list.insertBefore(row, want);
    prev = row;
    if (state.highlight === s.id) { row.classList.add("is-flash"); state.highlight = null; }
  }
  for (const c of existing.values()) c.remove();
  tickSchedules();
}

function tickSchedules() {
  const now = Date.now();
  let due = false;
  for (const row of $$(".sched__row", app)) {
    const s = state.schedules.find((x) => x.id === row.dataset.id);
    if (!s) continue;
    const v = $(".countdown__value", row), bar = $(".countdown__bar i", row);
    if (!s.enabled) { bar.style.setProperty("--f", 0); continue; }
    const next = new Date(s.next_fire_at).getTime();
    const from = new Date(s.last_fire_at || s.updated_at || s.created_at).getTime();
    const left = next - now;
    v.textContent = left > 0 ? clock(left) : "Running";
    v.classList.toggle("is-soon", left < 10000);
    bar.style.setProperty("--f", Math.min(1, Math.max(0, (now - from) / Math.max(1, next - from))).toFixed(4));
    if (left < -1500) due = true;
  }
  const soonest = state.schedules.filter((s) => s.enabled).map((s) => new Date(s.next_fire_at).getTime()).sort((a, b) => a - b)[0];
  const nextEl = $('[data-s="next"]', app);
  if (nextEl) nextEl.textContent = soonest ? clock(Math.max(0, soonest - now)) : "None";
  if (due && !refetchPending) { refetchPending = true; refreshSchedules().catch(() => {}).finally(() => { refetchPending = false; }); }
}

async function onScheduleClick(e) {
  const btn = e.target.closest("[data-act]");
  if (!btn) return;
  if (btn.dataset.act === "create") return openScheduleForm();
  const row = btn.closest(".sched__row");
  const s = state.schedules.find((x) => x.id === row?.dataset.id);
  if (!s) return;
  if (btn.dataset.act === "toggle") {
    btn.setAttribute("aria-checked", String(!s.enabled));
    try {
      const u = s.enabled ? await api.pauseSchedule(s.id) : await api.resumeSchedule(s.id);
      Object.assign(s, u);
      if (u.enabled) celebrate(btn, { count: 12, spread: 0.7 });
      toast(u.enabled ? `${u.name} resumed` : `${u.name} paused`);
      paintSchedules();
      pulse($(`.sched__row[data-id="${s.id}"] .toggle`, app));
    } catch (ex) { btn.setAttribute("aria-checked", String(s.enabled)); toast(ex.message, true); }
  } else if (btn.dataset.act === "delete") {
    if (btn.dataset.armed !== "1") {
      // two-step delete: the first click asks, the second deletes
      btn.dataset.armed = "1";
      btn.classList.replace("btn--ghost", "btn--danger");
      btn.classList.remove("btn--icon");
      btn.textContent = "Delete";
      setTimeout(() => {
        if (btn.isConnected && btn.dataset.armed === "1") { btn.dataset.armed = ""; btn.classList.replace("btn--danger", "btn--ghost"); btn.classList.add("btn--icon"); btn.innerHTML = icon.trash; }
      }, 3000);
      return;
    }
    btn.disabled = true;
    try {
      await api.deleteSchedule(s.id);
      row.classList.add("is-leaving");
      await new Promise((r) => { row.addEventListener("animationend", r, { once: true }); setTimeout(r, 400); });
      state.schedules = state.schedules.filter((x) => x.id !== s.id);
      row.remove();
      paintSchedules();
      toast(`${s.name} deleted`);
    } catch (ex) { btn.disabled = false; toast(ex.message, true); }
  }
}

/* =============================== toasts =============================== */

function toast(msg, isError = false) {
  const host = $(".toasts");
  const el = document.createElement("div");
  el.className = `toast${isError ? " is-error" : ""}`;
  el.innerHTML = isError ? `<span class="dot"></span><span></span>` : `<span class="toast__check">${icon.tick}</span><span></span>`;
  el.lastChild.textContent = msg;
  host.append(el);
  while (host.children.length > 3) host.firstElementChild.remove();
  setTimeout(() => { el.classList.add("is-out"); el.addEventListener("animationend", () => el.remove(), { once: true }); }, isError ? 4200 : 2400);
}

/* =============================== go =============================== */
// Last, so every module-level binding above is initialised before first render.
if (auth.get()) start(); else showGate();
