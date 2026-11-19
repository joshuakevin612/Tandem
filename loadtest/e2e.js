// End to end: submit a job, poll GET until succeeded, record submit-to-done.
// Env: BASE_URL, API_KEY, RATE (new jobs/s, default 20), DURATION (s, default 50)
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://nginx:80';
const KEY = __ENV.API_KEY;
const RATE = Number(__ENV.RATE || 20);
const DUR = Number(__ENV.DURATION || 50);
const submitToDone = new Trend('submit_to_done_ms', true);
const timeouts = new Counter('poll_timeouts');
const H = { headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${KEY}` } };

export const options = {
  scenarios: {
    e2e: {
      executor: 'constant-arrival-rate', rate: RATE, timeUnit: '1s', duration: DUR + 's',
      preAllocatedVUs: 50, maxVUs: 300,
    },
  },
  thresholds: {
    submit_to_done_ms: ['p(95)<2000'],
    poll_timeouts: ['count==0'],
    'http_req_failed': ['rate<0.01'],
  },
};

export default function () {
  const t0 = Date.now();
  const res = http.post(`${BASE}/v1/jobs`, JSON.stringify({ type: 'sleep', payload: { ms: 50 } }), H);
  if (!check(res, { 'submit 202': (r) => r.status === 202 })) return;
  const id = res.json('id');
  for (let i = 0; i < 200; i++) { // 200 * 50ms = 10s cap
    const g = http.get(`${BASE}/v1/jobs/${id}`, H);
    if (g.status === 200 && g.json('status') === 'succeeded') {
      submitToDone.add(Date.now() - t0);
      return;
    }
    sleep(0.05);
  }
  timeouts.add(1);
}
