import http from 'k6/http';
import { Counter } from 'k6/metrics';
import { sleep } from 'k6';

// F3 proof: end-to-end latency through nginx stays under the documented
// thresholds (p95 < 10ms, p99 < 20ms) under sustained below-limit load.
//
// The client is provisioned with effectively unlimited headroom (rate
// 1,000,000/hour, burst 100,000,000) so no 429s enter the latency sample —
// a saturated bucket is a different behavior and must not pollute this test.
// Server-side p99 < 5ms is measured separately via each replica's Prometheus
// histogram; k6 can't observe that number through the load balancer.

const ADMIN = 'http://control-plane:8081';
const CHECK = 'http://nginx:80';
const TOKEN = __ENV.ADMIN_BEARER_TOKEN;

const rateLimited = new Counter('rate_limited');

export const options = {
  scenarios: {
    latency: { executor: 'constant-vus', vus: 100, duration: '30s', exec: 'latencyLoad' },
  },
  thresholds: {
    http_req_duration: ['p(95) < 10', 'p(99) < 20'],
    http_req_failed: ['rate < 0.01'],
    rate_limited: ['count == 0'],
  },
};

export function setup() {
  const headers = { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' };
  const created = http.post(`${ADMIN}/v1/admin/clients`, null, { headers });
  if (created.status !== 200) {
    throw new Error(`create client failed: ${created.status} ${created.body}`);
  }
  const { client_id, api_key } = created.json();
  const put = http.put(
    `${ADMIN}/v1/admin/clients/${client_id}/limits`,
    JSON.stringify({ rate: 1000000, period: 'hour', burst: 100000000 }),
    { headers },
  );
  if (put.status !== 200) {
    throw new Error(`put limits failed: ${put.status} ${put.body}`);
  }
  return { client_id, api_key };
}

export function teardown(data) {
  http.del(`${ADMIN}/v1/admin/clients/${data.client_id}`, null, {
    headers: { Authorization: `Bearer ${TOKEN}` },
  });
}

export function latencyLoad(data) {
  const res = http.post(`${CHECK}/v1/check`, JSON.stringify({ cost: 1 }), {
    headers: { 'X-Api-Key': data.api_key, 'Content-Type': 'application/json' },
  });
  if (res.status === 429) {
    rateLimited.add(1);
  }
  sleep(0.02);
}
