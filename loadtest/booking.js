// k6 load test for the booking path (plan 02.1-04, mentor review D2).
//
// One script, four profiles chosen with the PROFILE environment variable:
//   smoke       5 booking requests per second for 30 s: checks plumbing and thresholds
//   sustained   NFR-01: sustained_rps for sustained_seconds
//   burst       NFR-02: burst_rps for burst_seconds
//   contention  NFR-07: contention_requests concurrent requests for ONE slot
//
// Every number that comes from the NFRs is read from nfr.json (the single source;
// tests/loadtest/test_nfr_consistency.py keeps the PRD table equal to it and
// tests/loadtest/test_loadtest_assets.py fails if a number is copied in here).
// The world (slots, patients, front-desk login, contention slot) comes from
// data/plan.json, written by scripts/loadtest_seed.py.
//
// Run it with loadtest/run.sh <profile>. LOCAL STACK ONLY: see loadtest/README.md.
// No token or password is ever logged; failures print status codes and ids only.

import http from 'k6/http';
import exec from 'k6/execution';
import { check, fail, sleep } from 'k6';
import { SharedArray } from 'k6/data';
import { Counter, Trend } from 'k6/metrics';

const NFR = JSON.parse(open('./nfr.json'));

const PROFILE = __ENV.PROFILE || 'smoke';
const PROFILES = ['smoke', 'sustained', 'burst', 'contention'];
if (PROFILES.indexOf(PROFILE) === -1) {
  throw new Error(`unknown PROFILE '${PROFILE}'; use one of ${PROFILES.join(', ')}`);
}
const BASE_URL = (__ENV.GATEWAY_URL || 'http://localhost:8000').replace(/\/+$/, '');

// The smoke profile is deliberately small and is not an NFR target.
const SMOKE_RPS = 5;
const SMOKE_SECONDS = 30;

// The booking plan is consumed in order, one entry per iteration, and a booked slot
// cannot be booked again. So each profile starts after the entries the profiles
// before it use (smoke, then sustained, then burst), and one seed serves one run of
// each. PLAN_OFFSET overrides the start (reseed instead when the plan is spent).
const DEFAULT_OFFSETS = {
  smoke: 0,
  sustained: SMOKE_RPS * SMOKE_SECONDS,
  burst: SMOKE_RPS * SMOKE_SECONDS + NFR.sustained_rps * NFR.sustained_seconds,
  contention: 0,
};
const PLAN_OFFSET = __ENV.PLAN_OFFSET !== undefined ? parseInt(__ENV.PLAN_OFFSET, 10) : DEFAULT_OFFSETS[PROFILE];

// Virtual-user headroom for the arrival-rate profiles (a VU that is polling a booking is busy).
const MIN_PREALLOCATED_VUS = 20;
const MIN_MAX_VUS = 120;

const CONFIRM_SAMPLE = 0.1; // share of iterations that poll the booking to a final status
const READ_SAMPLE = 0.2; // share of iterations that also read a slot list and the booking
const CONFIRM_POLL_SECONDS = 0.5;
const CONFIRM_GIVE_UP_SECONDS = 30;
const ACTIVE_STATUSES = ['pending', 'confirmed', 'checked_in', 'completed', 'no_show'];

const PLAN = new SharedArray('plan', function () {
  return JSON.parse(open('./data/plan.json')).plan;
});
const META = new SharedArray('meta', function () {
  const data = JSON.parse(open('./data/plan.json'));
  return [{ front_desk: data.front_desk, contention: data.contention }];
});

const bookingConfirmSeconds = new Trend('booking_confirm_seconds'); // plain numbers: the unit is the name, not milliseconds
const bookingConfirmTimeouts = new Counter('booking_confirm_timeouts');
const bookingSagaFailed = new Counter('booking_saga_failed'); // informational
const contentionAccepted = new Counter('contention_accepted'); // informational
const contentionViolations = new Counter('contention_violations');

const BOOK_FAILED = 'http_req_failed{endpoint:book}';

function bookingThresholds() {
  const thresholds = {};
  thresholds['http_req_duration{endpoint:book}'] = [`p(95)<=${NFR.book_p95_ms}`, `p(99)<=${NFR.book_p99_ms}`];
  thresholds['http_req_duration{endpoint:read_appointment}'] = [`p(95)<=${NFR.read_p95_ms}`, `p(99)<=${NFR.read_p99_ms}`];
  thresholds['http_req_duration{endpoint:read_slots}'] = [`p(95)<=${NFR.read_p95_ms}`, `p(99)<=${NFR.read_p99_ms}`];
  thresholds['booking_confirm_seconds'] = [`p(95)<=${NFR.confirm_p95_s}`, `p(99)<=${NFR.confirm_p99_s}`];
  thresholds['booking_confirm_timeouts'] = ['count==0'];
  thresholds[BOOK_FAILED] = [`rate<${NFR.error_rate_max}`];
  // An arrival-rate run that cannot start iterations on time is not applying the load it names.
  thresholds['dropped_iterations'] = ['count==0'];
  return thresholds;
}

function contentionThresholds() {
  const thresholds = {};
  thresholds[BOOK_FAILED] = [`rate<${NFR.error_rate_max}`]; // 202 and 409 are the expected answers
  thresholds['contention_violations'] = ['count==0'];
  return thresholds;
}

function arrivalScenario(rate, seconds) {
  return {
    executor: 'constant-arrival-rate',
    exec: 'booking',
    rate: rate,
    timeUnit: '1s',
    duration: `${seconds}s`,
    preAllocatedVUs: Math.max(MIN_PREALLOCATED_VUS, rate * 2),
    maxVUs: Math.max(MIN_MAX_VUS, rate * 10),
  };
}

const SCENARIOS = {
  smoke: arrivalScenario(SMOKE_RPS, SMOKE_SECONDS),
  sustained: arrivalScenario(NFR.sustained_rps, NFR.sustained_seconds),
  burst: arrivalScenario(NFR.burst_rps, NFR.burst_seconds),
  contention: {
    executor: 'per-vu-iterations',
    exec: 'contention',
    vus: NFR.contention_requests,
    iterations: 1,
    maxDuration: '2m',
  },
};

export const options = {
  scenarios: { [PROFILE]: SCENARIOS[PROFILE] },
  thresholds: PROFILE === 'contention' ? contentionThresholds() : bookingThresholds(),
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  tags: { profile: PROFILE },
};

function authHeaders(token) {
  return { Authorization: `Bearer ${token}` };
}

function jsonHeaders(token, extra) {
  return Object.assign({ 'Content-Type': 'application/json' }, authHeaders(token), extra || {});
}

function freshKey(prefix, index) {
  return `${prefix}-${index}-${Math.random().toString(36).slice(2, 12)}-${Date.now().toString(36)}`;
}

export function setup() {
  const login = META[0].front_desk;
  if (!login || !login.email || !login.password) {
    fail('data/plan.json has no front-desk login; run scripts/loadtest_seed.py first');
  }
  if (PROFILE === 'contention' && META[0].contention.patient_ids.length < NFR.contention_requests) {
    fail('data/plan.json reserves fewer contention patients than the profile needs; reseed');
  }
  const response = http.post(
    `${BASE_URL}/api/auth/login`,
    JSON.stringify({ email: login.email, password: login.password }),
    { headers: { 'Content-Type': 'application/json' }, tags: { endpoint: 'login' } },
  );
  if (response.status !== 200) {
    // The status only; never the body or the credentials.
    fail(`front-desk login failed with status ${response.status}`);
  }
  // The access token lives 15 minutes: keep every profile run well under that.
  return { token: response.json('access_token') };
}

function pollUntilSettled(appointmentId, token) {
  const started = Date.now();
  while ((Date.now() - started) / 1000 < CONFIRM_GIVE_UP_SECONDS) {
    const response = http.get(`${BASE_URL}/api/appointments/${appointmentId}`, {
      headers: authHeaders(token),
      tags: { endpoint: 'read_appointment' },
    });
    if (response.status === 200) {
      const status = response.json('status');
      if (status !== 'pending') {
        bookingConfirmSeconds.add((Date.now() - started) / 1000);
        if (status === 'failed') {
          bookingSagaFailed.add(1);
        }
        return;
      }
    }
    sleep(CONFIRM_POLL_SECONDS);
  }
  bookingConfirmSeconds.add(CONFIRM_GIVE_UP_SECONDS);
  bookingConfirmTimeouts.add(1);
}

function readSlots(token) {
  const contention = META[0].contention;
  const end = Date.parse(contention.slot_start);
  const start = end - 24 * 60 * 60 * 1000;
  const query =
    `provider_id=${contention.provider_id}` +
    `&start=${encodeURIComponent(new Date(start).toISOString())}` +
    `&end=${encodeURIComponent(new Date(end).toISOString())}`;
  const response = http.get(`${BASE_URL}/api/slots?${query}`, {
    headers: authHeaders(token),
    tags: { endpoint: 'read_slots' },
  });
  check(response, { 'slot list is 200': (r) => r.status === 200 });
}

export function booking(data) {
  const index = PLAN_OFFSET + exec.scenario.iterationInTest;
  const entry = PLAN[index];
  if (!entry) {
    fail(`booking plan exhausted at entry ${index}; reseed with scripts/loadtest_seed.py or lower PLAN_OFFSET`);
  }

  const response = http.post(
    `${BASE_URL}/api/appointments`,
    JSON.stringify({ slot_id: entry.slot_id, patient_id: entry.patient_id }),
    {
      headers: jsonHeaders(data.token, { 'Idempotency-Key': freshKey('k6', index) }),
      tags: { endpoint: 'book' },
      responseCallback: http.expectedStatuses(202),
    },
  );
  const accepted = check(response, { 'booking accepted (202)': (r) => r.status === 202 });
  if (!accepted) {
    return;
  }
  const appointmentId = response.json('appointment_id');

  if (Math.random() < CONFIRM_SAMPLE) {
    pollUntilSettled(appointmentId, data.token);
  }
  if (Math.random() < READ_SAMPLE) {
    readSlots(data.token);
    const read = http.get(`${BASE_URL}/api/appointments/${appointmentId}`, {
      headers: authHeaders(data.token),
      tags: { endpoint: 'read_appointment' },
    });
    check(read, { 'appointment read is 200': (r) => r.status === 200 });
  }
}

export function contention(data) {
  const target = META[0].contention;
  const n = exec.scenario.iterationInTest;
  const body = JSON.stringify({ slot_id: target.slot_id, patient_id: target.patient_ids[n] });
  const params = {
    headers: jsonHeaders(data.token, { 'Idempotency-Key': freshKey('k6-contend', n) }),
    tags: { endpoint: 'book' },
    responseCallback: http.expectedStatuses(202, 409),
  };

  const response = http.post(`${BASE_URL}/api/appointments`, body, params);
  check(response, { 'accepted or slot taken (202 or 409)': (r) => r.status === 202 || r.status === 409 });

  if (response.status === 202) {
    contentionAccepted.add(1);
    // A retry with the same key and body must answer with the SAME appointment, never a second one.
    const retry = http.post(`${BASE_URL}/api/appointments`, body, params);
    check(retry, {
      'retry returns the same appointment': (r) =>
        r.status === 202 && r.json('appointment_id') === response.json('appointment_id'),
    });
  }
}

export function teardown(data) {
  if (PROFILE !== 'contention') {
    return;
  }
  const target = META[0].contention;
  const slot = Date.parse(target.slot_start);
  const query =
    `provider_id=${target.provider_id}` +
    `&start=${encodeURIComponent(new Date(slot - 60 * 1000).toISOString())}` +
    `&end=${encodeURIComponent(new Date(slot + 60 * 1000).toISOString())}` +
    `&limit=${NFR.contention_requests}`;
  const response = http.get(`${BASE_URL}/api/appointments?${query}`, {
    headers: authHeaders(data.token),
    tags: { endpoint: 'read_appointment_list' },
  });
  if (response.status !== 200) {
    contentionViolations.add(1);
    console.error(`contention check could not read the appointments (status ${response.status})`);
    return;
  }
  const active = response
    .json()
    .filter((a) => a.slot_id === target.slot_id && ACTIVE_STATUSES.indexOf(a.status) !== -1);
  if (active.length !== 1) {
    contentionViolations.add(1);
    console.error(`NFR-07 violated: ${active.length} active appointments on the contention slot, expected exactly 1`);
  } else {
    console.log(`NFR-07 holds: exactly 1 active appointment on slot ${target.slot_id}`);
  }
}
