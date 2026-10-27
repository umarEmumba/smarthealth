/* Runtime smoke of tools/validation-ui in Node with a tiny fake DOM and a
 * stub fetch (no browser, no network, no dependency). It proves the four
 * scripts load together, every panel builds, the safe builder refuses unsafe
 * attributes, and a login / refresh / replay / list flow produces the expected
 * log entries and PASS badges. It is NOT a visual check of the real page.
 *
 * usage: node fake_dom_smoke.js <ui-dir>   (prints "smoke ok" on success) */
"use strict";
const fs = require("fs");
const path = require("path");
const vm = require("vm");
const assert = require("assert");

const uiDir = process.argv[2];

class TextNode {
  constructor(text) { this.nodeType = 3; this.text = text; this.parentNode = null; }
  get textContent() { return this.text; }
}

class FakeEl {
  constructor(tag) {
    this.nodeType = 1; this.tag = tag; this.attrs = {}; this.listeners = {}; this.kids = [];
    this.parentNode = null; this.value = ""; this.checked = false; this.disabled = false;
  }
  get childNodes() { return this.kids; }
  get firstChild() { return this.kids[0] || null; }
  get lastChild() { return this.kids[this.kids.length - 1] || null; }
  get className() { return this.attrs["class"] || ""; }
  set className(v) { this.attrs["class"] = v; }
  get id() { return this.attrs.id || ""; }
  get textContent() { return this.kids.map((k) => k.textContent).join(""); }
  set textContent(v) { this.kids = []; if (v !== "") this.appendChild(new TextNode(String(v))); }
  appendChild(n) { n.parentNode = this; this.kids.push(n); return n; }
  removeChild(n) { this.kids = this.kids.filter((k) => k !== n); n.parentNode = null; return n; }
  insertBefore(n, ref) {
    n.parentNode = this;
    const i = ref ? this.kids.indexOf(ref) : -1;
    if (i < 0) this.kids.push(n); else this.kids.splice(i, 0, n);
    return n;
  }
  setAttribute(k, v) { this.attrs[k] = String(v); if (k === "disabled") this.disabled = true; if (k === "checked") this.checked = true; }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  removeAttribute(k) { delete this.attrs[k]; }
  addEventListener(n, fn) { (this.listeners[n] = this.listeners[n] || []).push(fn); }
  click() { (this.listeners.click || []).forEach((fn) => fn({ target: this })); }
  walk(fn) { fn(this); this.kids.forEach((k) => k.walk && k.walk(fn)); }
}

const root = new FakeEl("body");
function mk(tag, attrs, parent) {
  const e = new FakeEl(tag);
  Object.entries(attrs || {}).forEach(([k, v]) => e.setAttribute(k, v));
  (parent || root).appendChild(e);
  return e;
}
["gateway-url", "act-as", "acting-identity", "context-fields", "context-json", "context-load", "clear-session",
  "log-count", "log-clear", "log-entries"].forEach((id) => mk(id === "act-as" ? "select" : "div", { id }));
["auth", "me", "lists", "booking", "visit", "provider", "schedule"].forEach((p) => mk("div", { "data-panel": p }));

function find(pred) {
  const out = [];
  root.walk((n) => { if (pred(n)) out.push(n); });
  return out;
}
const byId = (id) => find((n) => n.attrs && n.attrs.id === id)[0] || null;
const buttonByText = (t) => {
  const hit = find((n) => n.tag === "button" && n.textContent === t)[0];
  assert.ok(hit, "button not found: " + t);
  return hit;
};

const docListeners = {};
const document = {
  readyState: "interactive",
  createElement: (t) => new FakeEl(t),
  createTextNode: (t) => new TextNode(t),
  getElementById: byId,
  querySelector: (sel) => {
    const m = /^\[data-panel="([a-z]+)"\]$/.exec(sel);
    return m ? find((n) => n.attrs && n.attrs["data-panel"] === m[1])[0] || null : null;
  },
  addEventListener: (n, fn) => { docListeners[n] = fn; },
};

function b64(o) { return Buffer.from(JSON.stringify(o)).toString("base64url"); }
function jwt(claims) { return b64({ alg: "none" }) + "." + b64(claims) + ".sig"; }
let counter = 0;
function tokenPair(role) {
  counter += 1;
  const now = Math.floor(Date.now() / 1000);
  return {
    access_token: jwt({ sub: "u1", role, jti: "a" + counter, rjti: "r" + counter, type: "access", exp: now + 900 }),
    refresh_token: jwt({ sub: "u1", jti: "r" + counter, type: "refresh", exp: now + 86400 }),
    token_type: "bearer",
  };
}

const calls = [];
const usedRefresh = new Set();
function reply(status, body, headers) {
  const text = body === undefined ? "" : typeof body === "string" ? body : JSON.stringify(body);
  return Promise.resolve({
    status, ok: status >= 200 && status < 300, headers: new Headers(headers || {}),
    text: () => Promise.resolve(text),
    json: () => Promise.resolve(JSON.parse(text)),
  });
}
function fetchStub(url, init) {
  const method = (init && init.method) || "GET";
  const body = init && init.body ? JSON.parse(init.body) : undefined;
  calls.push({ method, url, body, headers: (init && init.headers) || {} });
  if (url === "/_ui/config") return reply(200, { gateway_url: "http://localhost:8000" });
  if (url === "/api/auth/login") return reply(200, tokenPair("patient"));
  if (url === "/api/auth/refresh") {
    if (usedRefresh.has(body.refresh_token)) return reply(401, { detail: "refresh token already used or revoked" });
    usedRefresh.add(body.refresh_token);
    return reply(200, tokenPair("patient"));
  }
  if (url === "/api/patients/me") return reply(200, { id: "p1", is_active: true });
  if (url.indexOf("/api/clinics") === 0) {
    return reply(200, [{ id: "c1", name: "A", timezone: "UTC", is_active: true }], { "X-Total-Count": "42" });
  }
  return reply(404, { detail: "not found" });
}

const store = new Map();
const sandbox = {
  document, fetch: fetchStub, Headers, TextDecoder, crypto: require("crypto").webcrypto, console, Date, Math, JSON, Promise,
  atob: (s) => Buffer.from(s, "base64").toString("binary"),
  setTimeout, clearTimeout, setInterval: () => 0, clearInterval: () => {},
  confirm: () => false,
  sessionStorage: {
    getItem: (k) => (store.has(k) ? store.get(k) : null),
    setItem: (k, v) => { store.set(k, String(v)); },
    removeItem: (k) => { store.delete(k); },
  },
};
const ctx = vm.createContext(sandbox);
["lib.js", "core.js", "panels_booking.js", "panels_ops.js"].forEach((f) => {
  vm.runInContext(fs.readFileSync(path.join(uiDir, f), "utf8"), ctx, { filename: f });
});
const V = ctx.VUI;
const tick = () => new Promise((r) => setImmediate(r));
const settle = async () => { for (let i = 0; i < 8; i++) await tick(); };

(async () => {
  // the safe builder refuses everything outside its allowlist
  for (const bad of [{ onclick: "x" }, { href: "x" }, { src: "x" }, { style: "x" }, { formaction: "x" }]) {
    assert.throws(() => V.el("div", bad), /not allowed/);
  }
  assert.throws(() => V.el("script", {}), /not allowed/);
  const cell = V.el("td", { text: "<img src=x onerror=alert(1)>" });
  assert.strictEqual(cell.textContent, "<img src=x onerror=alert(1)>");
  assert.strictEqual(cell.kids.length, 1);
  assert.strictEqual(cell.kids[0].nodeType, 3);

  docListeners.DOMContentLoaded();
  await settle();
  assert.strictEqual(byId("gateway-url").textContent, "http://localhost:8000");
  ["auth", "me", "lists", "booking", "visit", "provider", "schedule"].forEach((p) => {
    assert.ok(document.querySelector('[data-panel="' + p + '"]').kids.length > 0, "panel not built: " + p);
  });
  assert.strictEqual(byId("context-fields").kids.length, 10);

  // login: stores a session, clears the password, redacts it in the log
  const email = find((n) => n.tag === "input" && n.attrs.placeholder === "email")[0];
  const pw = find((n) => n.tag === "input" && n.attrs.type === "password")[0];
  email.value = "Demo@Example.com"; pw.value = "not-a-real-password";
  buttonByText("Login").click();
  await settle();
  assert.strictEqual(JSON.stringify(V.sessions.list()), JSON.stringify(["patient"]));
  assert.strictEqual(V.sessions.active(), "patient");
  assert.strictEqual(pw.value, "");
  const loginLog = byId("log-entries").firstChild;
  assert.ok(loginLog.textContent.indexOf("PASS") !== -1, "login should show PASS");
  assert.ok(loginLog.textContent.indexOf("(hidden)") !== -1, "password must be redacted in the log");
  assert.ok(loginLog.textContent.indexOf("not-a-real-password") === -1, "password must not be logged");

  // upper-case helper sends the upper-cased email
  pw.value = "not-a-real-password";
  buttonByText("Login with email in different case").click();
  await settle();
  assert.strictEqual(calls.filter((c) => c.url === "/api/auth/login")[1].body.email, "DEMO@EXAMPLE.COM");

  // refresh rotates and keeps the old token; replay shows 401 + PASS
  const before = V.sessions.activeRecord().refresh;
  buttonByText("Refresh").click();
  await settle();
  const rec = V.sessions.activeRecord();
  assert.strictEqual(rec.previousRefresh, before);
  assert.notStrictEqual(rec.refresh, before);
  buttonByText("Replay old refresh token (must be 401)").click();
  await settle();
  const replayLog = byId("log-entries").firstChild.textContent;
  assert.ok(replayLog.indexOf("-> 401") !== -1 && replayLog.indexOf("PASS") !== -1, replayLog.slice(0, 200));
  assert.ok(V.sessions.activeRecord().previousRefresh, "previous refresh kept");

  // Me: patient 200
  buttonByText("GET /api/patients/me (active session)").click();
  await settle();
  assert.ok(byId("log-entries").firstChild.textContent.indexOf("-> 200") !== -1);

  // Lists: X-Total-Count visible, limit sent exactly as typed
  V.ctx.set("clinic_id", "c1");
  const limits = find((n) => n.tag === "input" && n.attrs.size === "5");
  limits[0].value = "500";
  buttonByText("Load").click();
  await settle();
  const clinicCall = calls.filter((c) => c.url.indexOf("/api/clinics?") === 0).pop();
  assert.ok(clinicCall.url.indexOf("limit=500") !== -1, clinicCall.url);
  assert.ok(find((n) => n.tag === "p" && /X-Total-Count: 42 \| returned: 1/.test(n.textContent)).length === 1);

  // log is newest first and Clear empties it
  assert.ok(Number(byId("log-count").textContent) > 5);
  byId("log-clear").click();
  assert.strictEqual(byId("log-entries").kids.length, 0);

  // context loader: ids only, unknown keys ignored
  byId("context-json").value = 'CONTEXT_JSON: {"provider_id":"pp","evil":"x","slot_id":5}';
  byId("context-load").click();
  assert.strictEqual(V.ctx.get("provider_id"), "pp");
  assert.strictEqual(V.ctx.get("slot_id"), "");
  console.log("smoke ok");
})().catch((e) => { console.error(e && e.stack ? e.stack : e); process.exit(1); });
