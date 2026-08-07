import http from 'k6/http';
import { Counter } from 'k6/metrics';

// F2 proof: exact per-client quota enforcement across 3 replicas behind nginx.
//
// Three named clients run concurrently with *different* limits (arch doc §5),
// each firing a tight volley with period="hour". Refill is 1 token per 3600s,
// so any sub-minute run refills less than a token — the allowed/429 split is
// bit-exact, not approximate. Per-client exactness lives here (k6 counters,
// one per client per status); the fleet-wide cross-check happens in
// run-correctness.sh against each replica's /metrics.

const ADMIN = 'http://control-plane:8081';
const CHECK = 'http://nginx:80';
const TOKEN = __ENV.ADMIN_BEARER_TOKEN;

const cA200 = new Counter('client_a_200');
const cA429 = new Counter('client_a_429');
const cB200 = new Counter('client_b_200');
const cB429 = new Counter('client_b_429');
const cC200 = new Counter('client_c_200');
const cC429 = new Counter('client_c_429');
const unexpected = new Counter('unexpected_status');

export const options = {
  scenarios: {
    client_a: { executor: 'shared-iterations', vus: 500, iterations: 2000, exec: 'clientA' },
    client_b: { executor: 'shared-iterations', vus: 100, iterations: 400, exec: 'clientB' },
    client_c: { executor: 'shared-iterations', vus: 50, iterations: 400, exec: 'clientC' },
  },
  thresholds: {
    // A: burst 1000 / cost 1 -> exactly 1000 x 200, 1000 x 429
    client_a_200: ['count == 1000'],
    client_a_429: ['count == 1000'],
    // B: burst 200 / cost 1 -> exactly 200 x 200, 200 x 429
    client_b_200: ['count == 200'],
    client_b_429: ['count == 200'],
    // C: burst 100 / cost 5 -> exactly 20 x 200, 380 x 429
    client_c_200: ['count == 20'],
    client_c_429: ['count == 380'],
    unexpected_status: ['count == 0'],
    http_req_failed: ['rate < 0.01'],
  },
};

// Provisions a fresh client and sets its limits. Called once per run.
export function setup() {
  const headers = { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' };

  function provision(limits) {
    const created = http.post(`${ADMIN}/v1/admin/clients`, null, { headers });
    if (created.status !== 200) {
      throw new Error(`create client failed: ${created.status} ${created.body}`);
    }
    const { client_id, api_key } = created.json();
    const put = http.put(`${ADMIN}/v1/admin/clients/${client_id}/limits`, JSON.stringify(limits), { headers });
    if (put.status !== 200) {
      throw new Error(`put limits failed: ${put.status} ${put.body}`);
    }
    return { client_id, api_key };
  }

  return {
    a: provision({ rate: 1, period: 'hour', burst: 1000 }),
    b: provision({ rate: 1, period: 'hour', burst: 200 }),
    c: provision({ rate: 1, period: 'hour', burst: 100 }),
  };
}

function hit(apiKey, cost) {
  const res = http.post(`${CHECK}/v1/check`, JSON.stringify({ cost }), {
    headers: { 'X-Api-Key': apiKey, 'Content-Type': 'application/json' },
  });
  if (res.status === 200) {
    return true;
  }
  if (res.status !== 429) {
    unexpected.add(1);
  }
  return false;
}

export function clientA(data) {
  if (hit(data.a.api_key, 1)) {
    cA200.add(1);
  } else {
    cA429.add(1);
  }
}

export function clientB(data) {
  if (hit(data.b.api_key, 1)) {
    cB200.add(1);
  } else {
    cB429.add(1);
  }
}

export function clientC(data) {
  if (hit(data.c.api_key, 5)) {
    cC200.add(1);
  } else {
    cC429.add(1);
  }
}
