// Submit throughput: ramping arrival rate of short sleep jobs.
// Env: BASE_URL, API_KEY, RATE (target arrivals/s, default 200), DURATION (s of steady load, default 40)
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://nginx:80';
const KEY = __ENV.API_KEY;
const RATE = Number(__ENV.RATE || 200);
const HOLD = Number(__ENV.DURATION || 40);
const limited = new Counter('rate_limited_429');

export const options = {
  scenarios: {
    submit: {
      executor: 'ramping-arrival-rate',
      startRate: 10, timeUnit: '1s',
      preAllocatedVUs: 50, maxVUs: 400,
      stages: [
        { target: RATE, duration: '10s' },
        { target: RATE, duration: HOLD + 's' },
        { target: 0, duration: '5s' },
      ],
    },
  },
  // 429 is a correct, expected answer when the limiter is on, so it is not an
  // error: only 5xx/transport failures count. Run with the loadtest compose
  // override (limiter off) to measure raw throughput.
  thresholds: {
    'http_req_failed{expected_response:true}': ['rate<0.01'],
    http_req_duration: ['p(95)<250'],
    checks: ['rate>0.99'],
  },
};

http.setResponseCallback(http.expectedStatuses(202, 429));

export default function () {
  const res = http.post(`${BASE}/v1/jobs`,
    JSON.stringify({ type: 'sleep', payload: { ms: 20 } }),
    { headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${KEY}` } });
  if (res.status === 429) limited.add(1);
  check(res, { 'accepted or limited': (r) => r.status === 202 || r.status === 429 });
}
