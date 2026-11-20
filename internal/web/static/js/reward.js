// Feedback for the user's own actions: every press answers with a spring and a
// ripple from the finger, and actions that land get a small celebration.
// Nothing here runs on the landing page, and reduced motion keeps only the
// colour changes.

import { prefersReduced } from "./motion.js";

const RIPPLE = ".btn, .chip, .seg__opt, .row, .live, .tenant";
const COLORS = ["#fff0dc", "#f5bf4f", "#9fcca6", "#d9b48c", "#ffab9b"];

const onLanding = () => document.body.classList.contains("is-landing");

let layer = null;
function burstLayer() {
  if (!layer || !layer.isConnected) {
    layer = document.createElement("div");
    layer.className = "burst";
    layer.setAttribute("aria-hidden", "true");
    document.body.append(layer);
  }
  return layer;
}

export function buzz(ms = 8) {
  try { if (matchMedia("(pointer: coarse)").matches) navigator.vibrate?.(ms); } catch { /* unsupported */ }
}

function ripple(el, e) {
  const r = el.getBoundingClientRect();
  const size = Math.max(r.width, r.height) * 2.2;
  const dot = document.createElement("span");
  dot.className = "ripple";
  dot.setAttribute("aria-hidden", "true");
  dot.style.setProperty("--x", `${e.clientX - r.left - size / 2}px`);
  dot.style.setProperty("--y", `${e.clientY - r.top - size / 2}px`);
  dot.style.setProperty("--d", `${size}px`);
  // stop here so the host's own animationend handlers (row flash, reward pulse) don't fire early
  dot.addEventListener("animationend", (ev) => { ev.stopPropagation(); dot.remove(); }, { once: true });
  el.append(dot);
}

export function initRewards() {
  addEventListener("pointerdown", (e) => {
    if (e.button !== 0 || onLanding()) return;
    const el = e.target.closest(RIPPLE);
    if (!el || el.disabled) return;
    if (!prefersReduced()) ripple(el, e);
    buzz(6);
  }, { passive: true });
}

// A burst of confetti from the element's centre, drawn on a fixed layer so it
// survives the sheet or drawer closing underneath it.
export function celebrate(el, { count = 18, spread = 1 } = {}) {
  if (!el || onLanding()) return;
  buzz(14);
  pulse(el);
  if (prefersReduced()) return;
  const r = el.getBoundingClientRect();
  const cx = r.left + r.width / 2, cy = r.top + r.height / 2;
  const host = burstLayer();
  for (let i = 0; i < count; i++) {
    const p = document.createElement("i");
    const round = i % 3 === 0;
    p.className = round ? "burst__dot" : "burst__bit";
    p.style.setProperty("--c", COLORS[i % COLORS.length]);
    p.style.left = `${cx}px`;
    p.style.top = `${cy}px`;
    host.append(p);
    const a = (Math.PI * 2 * i) / count + (Math.random() - 0.5) * 0.6;
    const v = (60 + Math.random() * 70) * spread;
    const dx = Math.cos(a) * v, dy = Math.sin(a) * v - 30;
    const spin = (Math.random() - 0.5) * 720;
    p.animate([
      { transform: "translate(-50%, -50%) scale(.4) rotate(0deg)", opacity: 1 },
      { transform: `translate(calc(-50% + ${dx}px), calc(-50% + ${dy}px)) scale(1) rotate(${spin / 2}deg)`, opacity: 1, offset: 0.55 },
      { transform: `translate(calc(-50% + ${dx * 1.15}px), calc(-50% + ${dy + 70}px)) scale(.7) rotate(${spin}deg)`, opacity: 0 },
    ], { duration: 820 + Math.random() * 360, easing: "cubic-bezier(.2,.7,.3,1)", fill: "forwards" }).finished.then(() => p.remove(), () => p.remove());
  }
}

// A glow ring and a springy bump: the "that worked" beat on its own.
export function pulse(el) {
  if (!el) return;
  el.classList.remove("is-rewarded");
  void el.offsetWidth;
  el.classList.add("is-rewarded");
  el.addEventListener("animationend", () => el.classList.remove("is-rewarded"), { once: true });
}

// Numbers that go up deserve a moment.
export function bump(el) {
  if (!el) return;
  el.classList.remove("is-bump");
  void el.offsetWidth;
  el.classList.add("is-bump");
  el.addEventListener("animationend", () => el.classList.remove("is-bump"), { once: true });
}
