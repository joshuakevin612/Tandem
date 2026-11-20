// The tandem emblem: two dancers waltzing under a full moon, caught in a spotlight.
//
// Built as stacked layers rather than one SVG so the motion stays on the GPU:
// the sky and moon are painted once, while the spotlight and the couple are
// separate layers that CSS moves with transforms only (no repaint, no filter
// re-rasterising each frame). That is what keeps the dance free of jitter.

const IVORY = "#fff0dc";
const WINE = "#1c0407";

// Silhouettes are drawn on a 400×400 canvas; the floor line is y = 340.
// She is rigged in nested groups so each move has its own pivot:
//   her-dip    rotates her back from her feet for the lean-back
//   her-twirl  spins her body in place under the joined hands
//   her-reach  swings her raised arm from the shoulder during the dip
//   her-raised stretches that arm across her as she turns, so the joined
//              hands never come apart
// His raised arm and the arm round her back follow her through both moves.
const HER = `
  <g class="her-dip">
    <g class="her-twirl">
      <g class="dancer dancer--her">
        <g fill="${WINE}" stroke="none">
          <circle cx="161" cy="160" r="8"/>
          <circle cx="171" cy="170" r="13"/>
          <path d="M166 182 h10 l12 6 l-3 46 h-24 l-7-46 z"/>
          <g class="dancer__skirt">
            <path d="M161 230 h28 c6 34 14 70 22 110 c-30 7-66 6-98-2 c14-34 32-70 48-108 z"/>
            <path d="M154 340 c-26-2-44-8-52-14 c18 2 36 2 52 0 z" opacity=".85"/>
          </g>
        </g>
        <path class="limb" d="M168 194 c10 8 30 0 52-18" stroke-width="6"/>
      </g>
    </g>
    <g class="her-reach"><path class="limb her-raised" d="M186 190 c6-26 10-48 18-70" stroke-width="7"/></g>
  </g>`;
const HIM = `
  <g class="dancer dancer--him">
    <g fill="${WINE}" stroke="none">
      <circle cx="233" cy="146" r="14"/>
      <path d="M226 158 h14 l18 10 l-6 84 h-36 l-4-84 z"/>
      <path d="M218 248 h18 l-4 92 h-15 z"/>
      <path d="M236 248 h16 l12 88 l-14 4 l-14-84 z"/>
      <path d="M214 337 h20 v5 h-24 z M250 337 h18 l2 5 h-22 z"/>
    </g>
    <path class="limb him-raised" d="M222 170 c-6-18-10-34-16-50" stroke-width="8"/>
    <path class="limb him-hold" d="M256 174 c6 28-6 50-62 56" stroke-width="8"/>
  </g>`;
const HANDS = `<circle class="hands" cx="205" cy="117" r="6" fill="${WINE}"/>`;

const STARS = [
  [72, 96, 1.6], [104, 58, 1], [318, 70, 1.4], [346, 128, 1], [62, 196, 1], [338, 236, 1.6],
  [92, 268, 1.1], [300, 30, 0.9], [136, 30, 1.2], [360, 180, 0.9], [48, 140, 0.8],
];
const sparkle = ([x, y, s], i) =>
  `<path class="star" data-i="${i}" d="M${x} ${y - 6 * s} L${x + 1.2 * s} ${y - 1.2 * s} L${x + 6 * s} ${y} L${x + 1.2 * s} ${y + 1.2 * s} L${x} ${y + 6 * s} L${x - 1.2 * s} ${y + 1.2 * s} L${x - 6 * s} ${y} L${x - 1.2 * s} ${y - 1.2 * s} Z" fill="${IVORY}" opacity="${(0.45 + s * 0.25).toFixed(2)}"/>`;

const layer = (cls, body, defs = "") =>
  `<svg class="emblem__layer ${cls}" viewBox="0 0 400 400" aria-hidden="true" focusable="false">${defs ? `<defs>${defs}</defs>` : ""}${body}</svg>`;

export function emblemSVG() {
  const sky = layer("emblem__sky", `
    <rect width="400" height="400" fill="url(#em-sky)"/>
    <circle cx="200" cy="178" r="150" fill="url(#em-halo)"/>
    <circle cx="200" cy="178" r="118" fill="url(#em-moon)"/>
    <g fill="#e2bd92" opacity=".55">
      <circle cx="160" cy="132" r="14"/><circle cx="246" cy="118" r="9"/><circle cx="262" cy="206" r="17"/>
      <circle cx="138" cy="214" r="8"/><circle cx="212" cy="246" r="11"/><circle cx="188" cy="90" r="6"/>
    </g>
    <rect y="340" width="400" height="60" fill="#140205"/>`, `
    <radialGradient id="em-sky" cx="50%" cy="42%" r="62%">
      <stop offset="0" stop-color="#5a0c15"/><stop offset=".55" stop-color="#33070d"/><stop offset="1" stop-color="#1c0407"/>
    </radialGradient>
    <radialGradient id="em-moon" cx="40%" cy="36%" r="70%">
      <stop offset="0" stop-color="#fff8ee"/><stop offset=".7" stop-color="#fbe6c8"/><stop offset="1" stop-color="#ecc9a0"/>
    </radialGradient>
    <radialGradient id="em-halo" cx="50%" cy="50%" r="50%">
      <stop offset=".55" stop-color="${IVORY}" stop-opacity=".22"/><stop offset="1" stop-color="${IVORY}" stop-opacity="0"/>
    </radialGradient>`);

  const stars = layer("emblem__stars", STARS.map(sparkle).join(""));

  const beam = layer("emblem__beam", `
    <path d="M176 -40 L96 340 L292 340 Z" fill="url(#em-beam)"/>`, `
    <linearGradient id="em-beam" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0" stop-color="${IVORY}" stop-opacity=".5"/><stop offset=".55" stop-color="${IVORY}" stop-opacity=".16"/><stop offset="1" stop-color="${IVORY}" stop-opacity=".08"/>
    </linearGradient>`);

  // the pool of light follows the couple, so it lives on its own layer too
  const pool = layer("emblem__pool", `<ellipse cx="200" cy="341" rx="112" ry="13" fill="url(#em-pool)"/>`, `
    <radialGradient id="em-pool" cx="50%" cy="50%" r="50%">
      <stop offset="0" stop-color="${IVORY}" stop-opacity=".55"/><stop offset="1" stop-color="${IVORY}" stop-opacity="0"/>
    </radialGradient>`);

  const couple = layer("emblem__couple", `<g stroke="${WINE}" fill="none" stroke-linecap="round" stroke-linejoin="round">${HIM}${HER}${HANDS}</g>`);

  const ring = layer("emblem__ring", `<circle cx="200" cy="200" r="195" fill="none" stroke="${IVORY}" stroke-opacity=".28" stroke-width="2"/>`);

  return `
<div class="emblem" role="img" aria-label="Two dancers waltzing under a full moon, lit by a spotlight">
  <div class="emblem__glow" aria-hidden="true"></div>
  <div class="emblem__clip">
    ${sky}${stars}${beam}${pool}
    <div class="emblem__floor">${couple}</div>
  </div>
  ${ring}
</div>`;
}
