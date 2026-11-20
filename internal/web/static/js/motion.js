// Motion is reserved for two things: one settle-in of the live counts on load,
// and feedback for the user's own actions. Scrolling stays native.

const reduce = matchMedia("(prefers-reduced-motion: reduce)");
export const prefersReduced = () => reduce.matches;

const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));

/* ---------- overlay scroll lock ---------- */
// Swallow wheel/touch outside the open panel instead of toggling overflow,
// which would shift layout and unstick the header.
let locked = false;
function insideScroller(el, dy) {
  for (; el && el !== document.body; el = el.parentElement) {
    if (el.hasAttribute?.("data-native-scroll")) return true;
    const s = getComputedStyle(el);
    if (/(auto|scroll)/.test(s.overflowY) && el.scrollHeight > el.clientHeight + 1) {
      if (dy < 0 && el.scrollTop > 0) return true;
      if (dy > 0 && el.scrollTop + el.clientHeight < el.scrollHeight - 1) return true;
    }
  }
  return false;
}
export function lockScroll(on) { locked = on; }

/* ---------- numbers ---------- */
const tweens = new WeakMap();
export function countTo(el, to, { suffix = "" } = {}) {
  const from = Number(el.dataset.value || 0);
  const first = !el.dataset.seen;
  el.dataset.value = to;
  el.dataset.seen = "1";
  const render = (v) => { el.textContent = `${Math.round(v).toLocaleString("en")}${suffix}`; };
  // only the very first paint animates; later polls just swap the value
  if (reduce.matches || !first || to === 0) { render(to); return; }
  const start = performance.now(), dur = 900;
  const id = {};
  tweens.set(el, id);
  const step = (now) => {
    if (tweens.get(el) !== id) return;
    const p = clamp((now - start) / dur, 0, 1);
    render(from + (to - from) * (1 - Math.pow(1 - p, 3)));
    if (p < 1) requestAnimationFrame(step);
  };
  requestAnimationFrame(step);
}

/* ---------- segmented indicator ---------- */
export function movePill(pill, active) {
  if (!pill || !active) return;
  const parent = pill.parentElement.getBoundingClientRect();
  const r = active.getBoundingClientRect();
  pill.style.width = `${r.width}px`;
  pill.style.transform = `translateX(${r.left - parent.left + pill.parentElement.scrollLeft}px)`;
}

export function scrollToY(y) {
  window.scrollTo({ top: y, behavior: reduce.matches ? "auto" : "smooth" });
}

export function initMotion() {
  const bar = document.querySelector(".bar");
  const onScroll = () => bar?.classList.toggle("is-scrolled", scrollY > 4);
  addEventListener("scroll", onScroll, { passive: true });
  onScroll();
  addEventListener("wheel", (e) => { if (locked && !insideScroller(e.target, e.deltaY)) e.preventDefault(); }, { passive: false });
  addEventListener("touchmove", (e) => { if (locked && !e.target.closest?.("[data-native-scroll]")) e.preventDefault(); }, { passive: false });
}
