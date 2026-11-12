/* Pure helpers for the Phase 2.1 validation UI.
 *
 * Classic script: no modules and no DOM access at load time, so Node can load
 * it in a vm context for the stack-free tests. Everything is attached to
 * globalThis.VUI. */
(function (g) {
  "use strict";

  var VUI = g.VUI || (g.VUI = {});

  function hasCrypto() {
    return typeof g.crypto === "object" && g.crypto !== null && typeof g.crypto.getRandomValues === "function";
  }

  function randomBytes(n) {
    var out = new Uint8Array(n);
    if (hasCrypto()) {
      g.crypto.getRandomValues(out);
    } else {
      for (var i = 0; i < n; i++) {
        out[i] = Math.floor(Math.random() * 256);
      }
    }
    return out;
  }

  function toHex(bytes) {
    var s = "";
    for (var i = 0; i < bytes.length; i++) {
      s += (bytes[i] < 16 ? "0" : "") + bytes[i].toString(16);
    }
    return s;
  }

  /* base64url -> UTF-8 string (padding added). Throws on malformed input;
   * decodeJwtClaims is the non-throwing wrapper. */
  function b64urlToString(s) {
    var b64 = String(s).replace(/-/g, "+").replace(/_/g, "/");
    while (b64.length % 4 !== 0) {
      b64 += "=";
    }
    var binary = g.atob(b64);
    var bytes = new Uint8Array(binary.length);
    for (var i = 0; i < binary.length; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    return new g.TextDecoder("utf-8").decode(bytes);
  }

  /* JWT payload object, or null for anything malformed. Never throws. The
   * token is NOT verified (the browser has no key); this only reads claims. */
  function decodeJwtClaims(token) {
    try {
      if (typeof token !== "string") {
        return null;
      }
      var parts = token.split(".");
      if (parts.length !== 3 || !parts[1]) {
        return null;
      }
      var payload = JSON.parse(b64urlToString(parts[1]));
      if (payload === null || typeof payload !== "object" || Array.isArray(payload)) {
        return null;
      }
      return payload;
    } catch (e) {
      return null;
    }
  }

  function describeExp(exp, nowMs) {
    if (typeof exp !== "number" || !isFinite(exp)) {
      return { iso: null, secondsRemaining: null };
    }
    var now = typeof nowMs === "number" ? nowMs : Date.now();
    return { iso: new Date(exp * 1000).toISOString(), secondsRemaining: Math.round(exp - now / 1000) };
  }

  /* Matches the booking API's rule ^[A-Za-z0-9._:-]{1,128}$ by construction. */
  function newIdempotencyKey() {
    return "ui-" + Date.now().toString(36) + "-" + toHex(randomBytes(16));
  }

  function newUuid() {
    if (g.crypto && typeof g.crypto.randomUUID === "function") {
      return g.crypto.randomUUID();
    }
    var b = randomBytes(16);
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    var h = toHex(b);
    return h.slice(0, 8) + "-" + h.slice(8, 12) + "-" + h.slice(12, 16) + "-" + h.slice(16, 20) + "-" + h.slice(20);
  }

  function isUuid(s) {
    return typeof s === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(s);
  }

  /* key=value pairs joined with &; undefined, null and "" are skipped; order
   * is preserved; no leading "?". */
  function buildQuery(params) {
    var pairs = [];
    var keys = Object.keys(params || {});
    for (var i = 0; i < keys.length; i++) {
      var value = params[keys[i]];
      if (value === undefined || value === null || value === "") {
        continue;
      }
      pairs.push(encodeURIComponent(keys[i]) + "=" + encodeURIComponent(String(value)));
    }
    return pairs.join("&");
  }

  /* The server trims and lowercases the login email (C3); sending it upper
   * case is the proof. */
  function uppercaseEmail(email) {
    return String(email).toUpperCase();
  }

  function sameResponse(a, b) {
    return !!a && !!b && a.status === b.status && a.text === b.text;
  }

  function pageState(offset, limit, returned, total) {
    var hasNext =
      typeof total === "number" && isFinite(total) ? offset + returned < total : returned === limit;
    return {
      from: returned ? offset + 1 : 0,
      to: offset + returned,
      hasPrev: offset > 0,
      hasNext: hasNext,
    };
  }

  function pad2(n) {
    return (n < 10 ? "0" : "") + n;
  }

  /* YYYY-MM-DD from the LOCAL calendar fields (not toISOString, which is UTC). */
  function localDateString(date) {
    var y = String(date.getFullYear());
    while (y.length < 4) {
      y = "0" + y;
    }
    return y + "-" + pad2(date.getMonth() + 1) + "-" + pad2(date.getDate());
  }

  function isoOffset(ms, nowMs) {
    var now = typeof nowMs === "number" ? nowMs : Date.now();
    return new Date(now + ms).toISOString();
  }

  VUI.b64urlToString = b64urlToString;
  VUI.decodeJwtClaims = decodeJwtClaims;
  VUI.describeExp = describeExp;
  VUI.newIdempotencyKey = newIdempotencyKey;
  VUI.newUuid = newUuid;
  VUI.isUuid = isUuid;
  VUI.buildQuery = buildQuery;
  VUI.uppercaseEmail = uppercaseEmail;
  VUI.sameResponse = sameResponse;
  VUI.pageState = pageState;
  VUI.localDateString = localDateString;
  VUI.isoOffset = isoOffset;
})(globalThis);
