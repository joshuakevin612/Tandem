// Small helpers: escaping, time formatting, JSON highlighting, icons.

export const $ = (sel, root = document) => root.querySelector(sel);
export const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];

export function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

// CSP forbids inline style attributes, so per-element custom properties are
// declared as data-* in markup and applied through the CSSOM here.
export function applyVars(root) {
  for (const el of root.querySelectorAll("[data-c],[data-i]")) {
    if (el.dataset.c) el.style.setProperty("--c", `var(--s-${el.dataset.c}, var(--ink-3))`);
    if (el.dataset.i) el.style.setProperty("--i", el.dataset.i);
  }
}

export const STATUSES = ["queued", "running", "succeeded", "dead", "cancelled"];

export function statusOf(s) { return s === "failed" ? "dead" : s; }

export function shortId(id) { return String(id).slice(0, 8); }

export function ago(ts, now = Date.now()) {
  if (!ts) return "—";
  const d = (new Date(ts).getTime() - now) / 1000;
  const a = Math.abs(d);
  if (a < 5) return d > 0 ? "in a moment" : "just now";
  const n = a < 60 ? `${Math.round(a)}s` : a < 3600 ? `${Math.round(a / 60)}m` : a < 86400 ? `${Math.round(a / 3600)}h` : `${Math.round(a / 86400)}d`;
  return d > 0 ? `in ${n}` : `${n} ago`;
}

const dtf = new Intl.DateTimeFormat("en", { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
export function stamp(ts) { return ts ? dtf.format(new Date(ts)) : "—"; }

export function duration(ms) {
  if (ms == null || !isFinite(ms)) return "—";
  if (ms < 0) ms = 0;
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60), r = s % 60;
  if (m < 60) return `${m}m ${String(r).padStart(2, "0")}s`;
  const h = Math.floor(m / 60);
  if (h < 48) return `${h}h ${String(m % 60).padStart(2, "0")}m`;
  return `${Math.floor(h / 24)}d ${h % 24}h`;
}

export function clock(ms) {
  if (ms <= 0) return "00:00";
  const s = Math.floor(ms / 1000);
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), r = s % 60;
  const mm = String(m).padStart(2, "0"), ss = String(r).padStart(2, "0");
  if (h >= 24) return `${Math.floor(h / 24)}d ${String(h % 24).padStart(2, "0")}h`;
  return h ? `${h}:${mm}:${ss}` : `${mm}:${ss}`;
}

export function highlightJSON(value) {
  let text;
  try { text = JSON.stringify(value, null, 2); } catch { text = String(value); }
  if (text === undefined) text = "null";
  return esc(text).replace(
    /(&quot;(?:\\.|[^\\])*?&quot;)(\s*:)?|\b(true|false|null)\b|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)/g,
    (m, str, colon, bool, num) => {
      if (str) return colon ? `<span class="k">${str}</span>${colon}` : `<span class="s">${str}</span>`;
      if (bool) return `<span class="b">${bool}</span>`;
      return `<span class="n">${num}</span>`;
    },
  );
}

export function uuid() {
  if (crypto.randomUUID) return crypto.randomUUID();
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    return (c === "x" ? r : (r & 0x3) | 0x8).toString(16);
  });
}

export async function copy(text) {
  try { await navigator.clipboard.writeText(text); return true; } catch { return false; }
}

const S = 'viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"';
export const icon = {
  close: `<svg ${S}><path d="M4 4l8 8M12 4l-8 8"/></svg>`,
  copy: `<svg ${S}><rect x="5.5" y="5.5" width="8" height="8" rx="2"/><path d="M10.5 5.5V4a1.5 1.5 0 0 0-1.5-1.5H4A1.5 1.5 0 0 0 2.5 4v5A1.5 1.5 0 0 0 4 10.5h1.5"/></svg>`,
  search: `<svg ${S}><circle cx="7" cy="7" r="4.5"/><path d="M10.5 10.5L14 14"/></svg>`,
  chevron: `<svg ${S}><path d="M6 3.5L10.5 8 6 12.5"/></svg>`,
  stop: `<svg ${S}><rect x="4" y="4" width="8" height="8" rx="1.5"/></svg>`,
  retry: `<svg ${S}><path d="M13 8a5 5 0 1 1-1.6-3.7"/><path d="M13 2.5v3h-3"/></svg>`,
  trash: `<svg ${S}><path d="M3 4.5h10M6.5 4.5V3h3v1.5M4.5 4.5l.6 8a1.5 1.5 0 0 0 1.5 1.4h2.8a1.5 1.5 0 0 0 1.5-1.4l.6-8"/></svg>`,
  inbox: `<svg ${S}><path d="M2.5 9.5l1.8-5.2A1.5 1.5 0 0 1 5.7 3.3h4.6a1.5 1.5 0 0 1 1.4 1l1.8 5.2v2.7a1.5 1.5 0 0 1-1.5 1.5h-8A1.5 1.5 0 0 1 2.5 12.2z"/><path d="M2.5 9.5h3l1 1.5h3l1-1.5h3"/></svg>`,
  clock: `<svg ${S}><circle cx="8" cy="8" r="5.5"/><path d="M8 5v3l2 1.5"/></svg>`,
  check: `<svg ${S}><path d="M3.5 8.5l3 3 6-7"/></svg>`,
  tick: `<svg viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M2.5 6.2l2.4 2.4 4.6-5"/></svg>`,
  logout: `<svg ${S}><path d="M6 13.5H4A1.5 1.5 0 0 1 2.5 12V4A1.5 1.5 0 0 1 4 2.5h2M10.5 11L13.5 8l-3-3M13.5 8H6"/></svg>`,
};
