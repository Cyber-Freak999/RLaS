import http from 'k6/http';
import { Counter } from 'k6/metrics';
import { sleep } from 'k6';

// M6 proof: fail-open under a Redis outage (AGENTS.md constraint 4).
//
// Load starts first so every replica's fallback cache is seeded — limits and
// the key->client map refresh on each successful round trip. The bash wrapper
// (chaos-runner.sh) then stops redis-master mid-run. Sentinel promotion takes
// a few seconds, during which /check hits 3 consecutive Redis failures per
// replica and trips the local circuit breaker; from then on each replica
// serves a degraded 200 from its cached token bucket, carrying
// X-RateLimit-Degraded: true. No 503 is allowed while Redis is down.
//
// k6 has no Docker access (orchestration stays in bash), and it cannot wait
// for the failover to settle, so recovery — degraded header gone, no service
// restart — is asserted in chaos-runner.sh, not here.

const ADMIN = 'http://control-plane:8081';
const CHECK = 'http://nginx:80';
const TOKEN = __ENV.ADMIN_BEARER_TOKEN;

// 429 is an expected answer (an exhausted bucket), not a failure. Any other
// status — a 503 from the hot path, or a 401 from a replica whose fallback
// cache never saw this key — fails the gate.
http.setResponseCallback(http.expectedStatuses(200, 429));

const degraded = new Counter('degraded_responses');
const unexpected = new Counter('unexpected_status');

// Go's net/http canonicalizes header names (X-RateLimit-Degraded is written
// on the wire as X-Ratelimit-Degraded), and k6 exposes res.headers with those
// canonical keys. A strict lookup like headers['X-RateLimit-Degraded'] misses
// every degraded response, so match case-insensitively.
function isDegraded(res) {
  const keys = Object.keys(res.headers);
  for (let i = 0; i < keys.length; i++) {
    if (keys[i].toLowerCase() === 'x-ratelimit-degraded') {
      return res.headers[keys[i]] === 'true';
    }
  }
  return false;
}

export const options = {
  scenarios: {
    chaos: { executor: 'constant-vus', vus: 100, duration: '40s', exec: 'chaosLoad' },
  },
  thresholds: {
    // Every response during the outage must be a degraded 200/429, never an
    // error. >0 degraded responses proves the circuit breaker engaged.
    http_req_failed: ['rate < 0.01'],
    unexpected_status: ['count == 0'],
    degraded_responses: ['count > 0'],
  },
};

export function setup() {
  const headers = { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' };
  const created = http.post(`${ADMIN}/v1/admin/clients`, null, { headers });
  if (created.status !== 200) {
    throw new Error(`create client failed: ${created.status} ${created.body}`);
  }
  const { client_id, api_key } = created.json();
  // Generous bucket so every outage request is a 200: a saturated bucket would
  // answer a healthy-looking 429 and muddy the fail-open proof.
  const put = http.put(
    `${ADMIN}/v1/admin/clients/${client_id}/limits`,
    JSON.stringify({ rate: 1000000, period: 'hour', burst: 100000000 }),
    { headers },
  );
  if (put.status !== 200) {
    throw new Error(`put limits failed: ${put.status} ${put.body}`);
  }
  // chaos-runner.sh starts k6 in the background but the container takes ~10-20s
  // to come up (docker compose run), so the runner cannot rely on wall-clock
  // sleeps to place the kill mid-load. This line is the "load is about to start"
  // signal the runner greps for before starting its WARMUP countdown.
  console.log('LOAD_STARTED');
  return { client_id, api_key };
}

export function teardown(data) {
  http.del(`${ADMIN}/v1/admin/clients/${data.client_id}`, null, {
    headers: { Authorization: `Bearer ${TOKEN}` },
  });
}

export function chaosLoad(data) {
  const res = http.post(`${CHECK}/v1/check`, JSON.stringify({ cost: 1 }), {
    headers: { 'X-Api-Key': data.api_key, 'Content-Type': 'application/json' },
  });
  if (res.status !== 200 && res.status !== 429) {
    unexpected.add(1);
  }
  if (isDegraded(res)) {
    degraded.add(1);
  }
  sleep(0.02);
}
