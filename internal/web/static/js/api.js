// Thin client for the tandem /v1 API. Credentials live in localStorage so a
// reload keeps the session; nothing is sent anywhere but this origin.

const KEY = "tandem.console.auth";

export class ApiError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

export const auth = {
  get() {
    try {
      const v = JSON.parse(localStorage.getItem(KEY) || "null");
      if (v && (v.mode === "apikey" || v.mode === "header")) return v;
    } catch { /* storage unavailable */ }
    return null;
  },
  set(v) {
    try { localStorage.setItem(KEY, JSON.stringify(v)); } catch { /* ignore */ }
    current = v;
  },
  clear() {
    try { localStorage.removeItem(KEY); } catch { /* ignore */ }
    current = null;
  },
};

let current = auth.get();
let onUnauthorized = () => {};
export function setUnauthorizedHandler(fn) { onUnauthorized = fn; }
export function useAuth(v) { current = v; }

function headers(extra = {}, creds = current) {
  const h = { Accept: "application/json", ...extra };
  if (creds?.mode === "apikey" && creds.key) h.Authorization = `Bearer ${creds.key}`;
  if (creds?.mode === "header" && creds.tenant) h["X-Tenant-ID"] = creds.tenant;
  return h;
}

async function request(method, path, { body, query, extraHeaders, creds, signal } = {}) {
  const url = new URL(path, location.origin);
  if (query) for (const [k, v] of Object.entries(query)) if (v !== undefined && v !== null && v !== "") url.searchParams.set(k, v);
  const init = { method, headers: headers(extraHeaders, creds), signal };
  if (body !== undefined) {
    init.body = JSON.stringify(body);
    init.headers["Content-Type"] = "application/json";
  }
  let res;
  try {
    res = await fetch(url, init);
  } catch (e) {
    if (e.name === "AbortError") throw e;
    throw new ApiError(0, "Can't reach the API");
  }
  if (res.status === 204) return null;
  let data = null;
  const text = await res.text();
  if (text) { try { data = JSON.parse(text); } catch { data = null; } }
  if (!res.ok) {
    const msg = data?.error || (res.status === 429 ? "Rate limit exceeded" : `Request failed (${res.status})`);
    if (res.status === 401 && !creds) onUnauthorized(msg);
    throw new ApiError(res.status, msg);
  }
  return data;
}

export const api = {
  probe: (creds) => request("GET", "/v1/jobs", { query: { limit: 1 }, creds }),
  listJobs: (q, signal) => request("GET", "/v1/jobs", { query: q, signal }),
  getJob: (id) => request("GET", `/v1/jobs/${id}`),
  createJob: (body, idemKey) => request("POST", "/v1/jobs", { body, extraHeaders: idemKey ? { "Idempotency-Key": idemKey } : {} }),
  cancelJob: (id) => request("POST", `/v1/jobs/${id}/cancel`),
  requeueJob: (id) => request("POST", `/v1/jobs/${id}/requeue`),
  listSchedules: () => request("GET", "/v1/schedules"),
  createSchedule: (body) => request("POST", "/v1/schedules", { body }),
  deleteSchedule: (id) => request("DELETE", `/v1/schedules/${id}`),
  pauseSchedule: (id) => request("POST", `/v1/schedules/${id}/pause`),
  resumeSchedule: (id) => request("POST", `/v1/schedules/${id}/resume`),
};
