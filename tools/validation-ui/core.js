/* Core of the Phase 2.1 validation UI: safe DOM builder, sessionStorage state,
 * shared log pane, api() with raw logging and PASS/FAIL, panel registry and the
 * Auth panel (panel 1).
 *
 * Every piece of text that came from a response is written with textContent
 * through VUI.el; there is no other DOM construction path in this tool. */
(function (g) {
  "use strict";

  var VUI = g.VUI;
  var doc = g.document;

  /* ---------------------------------------------------------------- el() */

  var ALLOWED_ATTRS = {
    "class": 1, id: 1, type: 1, value: 1, title: 1, name: 1, "for": 1, rows: 1, cols: 1,
    min: 1, max: 1, step: 1, colspan: 1, disabled: 1, checked: 1, hidden: 1,
    // harmless extras beyond the minimum list (the password field must be able
    // to say autocomplete=off, and inputs benefit from a placeholder)
    placeholder: 1, autocomplete: 1, spellcheck: 1, size: 1, maxlength: 1,
  };
  var BOOLEAN_ATTRS = { disabled: 1, checked: 1, hidden: 1 };
  var REFUSED_TAGS = { script: 1, iframe: 1, object: 1, embed: 1, link: 1, style: 1, base: 1, meta: 1, form: 1 };

  function appendChildren(node, children) {
    for (var i = 0; i < children.length; i++) {
      var child = children[i];
      if (child === null || child === undefined || child === false) {
        continue;
      }
      if (Array.isArray(child)) {
        appendChildren(node, child);
      } else if (typeof child === "string" || typeof child === "number" || typeof child === "boolean") {
        node.appendChild(doc.createTextNode(String(child)));
      } else {
        node.appendChild(child);
      }
    }
  }

  /* The ONLY way to create DOM. Attributes outside the allowlist (anything
   * starting with "on", href, src, style, formaction, ...) throw. */
  function el(tag, attrs) {
    if (typeof tag !== "string" || !/^[a-z][a-z0-9]*$/.test(tag) || REFUSED_TAGS[tag]) {
      throw new Error("el: tag not allowed: " + tag);
    }
    var node = doc.createElement(tag);
    var pendingValue;
    var hasValue = false;
    var listeners = null;
    var keys = Object.keys(attrs || {});
    for (var i = 0; i < keys.length; i++) {
      var key = keys[i];
      var val = attrs[key];
      if (key === "text") {
        node.textContent = val === null || val === undefined ? "" : String(val);
      } else if (key === "on") {
        listeners = val;
      } else if (key === "value") {
        pendingValue = val;
        hasValue = true;
      } else if (/^data-[a-z0-9-]+$/.test(key)) {
        node.setAttribute(key, String(val));
      } else if (Object.prototype.hasOwnProperty.call(ALLOWED_ATTRS, key)) {
        if (BOOLEAN_ATTRS[key]) {
          if (val) {
            node.setAttribute(key, "");
          }
        } else if (val !== null && val !== undefined) {
          node.setAttribute(key, String(val));
        }
      } else {
        throw new Error("el: attribute not allowed: " + key);
      }
    }
    appendChildren(node, Array.prototype.slice.call(arguments, 2));
    if (hasValue && pendingValue !== null && pendingValue !== undefined) {
      node.value = String(pendingValue); // after children so a select finds its options
    }
    if (listeners) {
      Object.keys(listeners).forEach(function (name) {
        node.addEventListener(name, listeners[name]);
      });
    }
    return node;
  }
  VUI.el = el;

  function clear(node) {
    while (node.firstChild) {
      node.removeChild(node.firstChild);
    }
  }
  VUI.clear = clear;

  /* --------------------------------------------------------------- store */

  var KEY_SESSIONS = "vui.sessions";
  var KEY_ACTIVE = "vui.active";
  var KEY_CTX = "vui.ctx";

  VUI.store = {
    get: function (key, fallback) {
      try {
        var raw = g.sessionStorage.getItem(key);
        return raw === null ? fallback : JSON.parse(raw);
      } catch (e) {
        return fallback;
      }
    },
    set: function (key, value) {
      try {
        g.sessionStorage.setItem(key, JSON.stringify(value));
      } catch (e) {
        /* storage unavailable: the page still works for this load */
      }
    },
    remove: function (key) {
      try {
        g.sessionStorage.removeItem(key);
      } catch (e) {
        /* ignore */
      }
    },
  };

  /* ------------------------------------------------------------- context */

  var CTX_FIELDS = [
    "clinic_id", "department_id", "provider_id", "patient_id", "slot_id", "new_slot_id",
    "appointment_id", "visit_id", "other_appointment_id", "template_id",
  ];
  var ctxState = {};
  var ctxInputs = {};
  var ctxListeners = [];

  function notifyCtx(name) {
    ctxListeners.forEach(function (fn) {
      try {
        fn(name);
      } catch (e) {
        /* a listener error must not break the others */
      }
    });
  }

  function loadCtxState() {
    var stored = VUI.store.get(KEY_CTX, {});
    ctxState = {};
    CTX_FIELDS.forEach(function (name) {
      ctxState[name] = stored && typeof stored[name] === "string" ? stored[name] : "";
    });
  }
  loadCtxState();

  VUI.ctx = {
    fields: CTX_FIELDS,
    get: function (name) {
      return (ctxState[name] || "").trim();
    },
    set: function (name, value) {
      if (CTX_FIELDS.indexOf(name) === -1) {
        return;
      }
      ctxState[name] = value === null || value === undefined ? "" : String(value);
      VUI.store.set(KEY_CTX, ctxState);
      if (ctxInputs[name] && ctxInputs[name].value !== ctxState[name]) {
        ctxInputs[name].value = ctxState[name];
      }
      notifyCtx(name);
    },
    onChange: function (fn) {
      ctxListeners.push(fn);
    },
  };

  function buildContext() {
    var host = doc.getElementById("context-fields");
    if (host) {
      clear(host);
      ctxInputs = {};
      CTX_FIELDS.forEach(function (name) {
        var input = el("input", {
          type: "text", "data-ctx": name, value: ctxState[name], spellcheck: "false", autocomplete: "off", size: 38,
          on: {
            input: function () {
              ctxState[name] = input.value;
              VUI.store.set(KEY_CTX, ctxState);
              notifyCtx(name);
            },
          },
        });
        ctxInputs[name] = input;
        host.appendChild(el("label", { "class": "ctx-field" }, el("span", { text: name }), input));
      });
    }
    var loadButton = doc.getElementById("context-load");
    if (loadButton && !loadButton.getAttribute("data-bound")) {
      loadButton.setAttribute("data-bound", "1");
      loadButton.addEventListener("click", loadContextJson);
    }
    var clearButton = doc.getElementById("clear-session");
    if (clearButton && !clearButton.getAttribute("data-bound")) {
      clearButton.setAttribute("data-bound", "1");
      clearButton.addEventListener("click", function () {
        VUI.store.remove(KEY_SESSIONS);
        VUI.store.remove(KEY_ACTIVE);
        VUI.store.remove(KEY_CTX);
        loadCtxState();
        buildContext();
        VUI.sessions.changed();
        VUI.log.note("session data cleared (sessions, active identity, context)");
      });
    }
  }

  function loadContextJson() {
    var area = doc.getElementById("context-json");
    if (!area) {
      return;
    }
    var text = area.value.trim().replace(/^CONTEXT_JSON:\s*/, "");
    var parsed;
    try {
      parsed = JSON.parse(text);
    } catch (e) {
      VUI.log.note("context JSON not loaded: not valid JSON (" + e.message + ")");
      return;
    }
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
      VUI.log.note("context JSON not loaded: expected a JSON object");
      return;
    }
    var loaded = [];
    CTX_FIELDS.forEach(function (name) {
      if (Object.prototype.hasOwnProperty.call(parsed, name) && typeof parsed[name] === "string") {
        VUI.ctx.set(name, parsed[name]);
        loaded.push(name);
      }
    });
    VUI.log.note("context loaded, keys: " + (loaded.length ? loaded.join(", ") : "(none matched)"));
  }

  /* ------------------------------------------------------------ sessions */

  var sessionListeners = [];

  function readSessions() {
    var s = VUI.store.get(KEY_SESSIONS, {});
    return s && typeof s === "object" && !Array.isArray(s) ? s : {};
  }

  function defaultRecord() {
    return {
      email: "", access: "", refresh: "", previousRefresh: null,
      loggedOut: false, loggedOutAccess: "", loggedOutRefresh: "",
    };
  }

  VUI.sessions = {
    list: function () {
      return Object.keys(readSessions());
    },
    get: function (label) {
      var rec = readSessions()[label];
      return rec ? rec : null;
    },
    save: function (label, record) {
      var all = readSessions();
      var merged = defaultRecord();
      Object.keys(record).forEach(function (k) {
        merged[k] = record[k];
      });
      all[label] = merged;
      VUI.store.set(KEY_SESSIONS, all);
      VUI.sessions.changed();
    },
    remove: function (label) {
      var all = readSessions();
      delete all[label];
      VUI.store.set(KEY_SESSIONS, all);
      if (VUI.sessions.active() === label) {
        VUI.store.remove(KEY_ACTIVE);
      }
      VUI.sessions.changed();
    },
    active: function () {
      var label = VUI.store.get(KEY_ACTIVE, null);
      return typeof label === "string" && readSessions()[label] ? label : null;
    },
    setActive: function (label) {
      if (label && readSessions()[label]) {
        VUI.store.set(KEY_ACTIVE, label);
      } else {
        VUI.store.remove(KEY_ACTIVE);
      }
      VUI.sessions.changed();
    },
    activeRecord: function () {
      var label = VUI.sessions.active();
      return label ? VUI.sessions.get(label) : null;
    },
    roleOf: function (label) {
      var rec = VUI.sessions.get(label);
      var claims = rec ? VUI.decodeJwtClaims(rec.access) : null;
      return claims && typeof claims.role === "string" ? claims.role : "unknown";
    },
    onChange: function (fn) {
      sessionListeners.push(fn);
    },
    changed: function () {
      renderActAs();
      sessionListeners.forEach(function (fn) {
        try {
          fn();
        } catch (e) {
          /* a panel render error must not break the others */
        }
      });
    },
  };

  function describeLabel(label) {
    var rec = VUI.sessions.get(label);
    return label + " (" + VUI.sessions.roleOf(label) + ")" + (rec && rec.loggedOut ? " - logged out" : "");
  }

  function renderActAs() {
    var select = doc.getElementById("act-as");
    var identity = doc.getElementById("acting-identity");
    var labels = VUI.sessions.list();
    var active = VUI.sessions.active();
    if (select) {
      clear(select);
      select.appendChild(el("option", { value: "", text: labels.length ? "(none)" : "(not logged in)" }));
      labels.forEach(function (label) {
        select.appendChild(el("option", { value: label, text: describeLabel(label) }));
      });
      select.value = active || "";
    }
    if (identity) {
      identity.textContent = active ? "acting as " + describeLabel(active) : "not logged in";
    }
  }

  /* ----------------------------------------------------------------- log */

  var LOG_CAP = 200;

  function pad2(n) {
    return (n < 10 ? "0" : "") + n;
  }
  function clockTime() {
    var d = new Date();
    return pad2(d.getHours()) + ":" + pad2(d.getMinutes()) + ":" + pad2(d.getSeconds());
  }
  function pretty(text) {
    try {
      return JSON.stringify(JSON.parse(text), null, 2);
    } catch (e) {
      return text;
    }
  }
  function statusClass(status) {
    if (!status) {
      return "s0";
    }
    return "s" + Math.floor(status / 100) + "xx";
  }

  function updateLogCount() {
    var box = doc.getElementById("log-entries");
    var counter = doc.getElementById("log-count");
    if (box && counter) {
      counter.textContent = String(box.childNodes.length);
    }
  }

  function prependLogNode(node) {
    var box = doc.getElementById("log-entries");
    if (!box) {
      return;
    }
    box.insertBefore(node, box.firstChild);
    while (box.childNodes.length > LOG_CAP) {
      box.removeChild(box.lastChild);
    }
    updateLogCount();
  }

  function pre(label, text) {
    return el("div", { "class": "log-block" }, el("div", { "class": "log-label", text: label }), el("pre", { text: text }));
  }

  VUI.log = {
    note: function (text) {
      prependLogNode(el("div", { "class": "log-note" }, el("span", { "class": "log-time", text: clockTime() }), " ", text));
    },
    clear: function () {
      var box = doc.getElementById("log-entries");
      if (box) {
        clear(box);
      }
      updateLogCount();
    },
    /* entry: {method, url, requestHeaders, requestBody, status, responseHeaders, responseBody, ms}
     * Returns {setVerdict(pass, text)}. */
    add: function (entry) {
      var badge = el("span", { "class": "badge hidden-badge", hidden: true });
      var summary = el(
        "summary", { "class": statusClass(entry.status) },
        el("span", { "class": "log-time", text: clockTime() }), " ",
        el("strong", { text: entry.method }), " ",
        el("code", { text: entry.url }), " ",
        el("span", { "class": "log-status", text: "-> " + (entry.status || "network error") }), " ",
        el("span", { "class": "log-ms", text: entry.ms + " ms" }), " ",
        badge
      );
      var node = el(
        "details", { "class": "log-entry" },
        summary,
        pre("Request headers", entry.requestHeaders),
        pre("Request body", entry.requestBody),
        pre("Response headers", entry.responseHeaders),
        pre("Response body", entry.responseBody)
      );
      node.setAttribute("open", "");
      prependLogNode(node);
      return {
        setVerdict: function (pass, text) {
          badge.removeAttribute("hidden");
          badge.className = "badge " + (pass ? "pass" : "fail");
          badge.textContent = text || (pass ? "PASS" : "FAIL");
        },
      };
    },
  };

  /* ----------------------------------------------------------------- api */

  function resolveToken(spec) {
    if (typeof spec.token === "string" && spec.token) {
      return spec.token;
    }
    if (spec.auth === false) {
      return "";
    }
    var rec = typeof spec.auth === "string" ? VUI.sessions.get(spec.auth) : VUI.sessions.activeRecord();
    return rec && rec.access ? rec.access : "";
  }

  function headerLines(headersObj) {
    var lines = [];
    Object.keys(headersObj).forEach(function (k) {
      lines.push(k + ": " + headersObj[k]);
    });
    return lines.length ? lines.join("\n") : "(none)";
  }

  /* Display-only: the typed password is never echoed into the log pane. */
  function displayBody(body) {
    if (body === undefined) {
      return "(none)";
    }
    if (body !== null && typeof body === "object" && !Array.isArray(body) && "password" in body) {
      var copy = {};
      Object.keys(body).forEach(function (k) {
        copy[k] = k === "password" ? "(hidden)" : body[k];
      });
      return JSON.stringify(copy, null, 2);
    }
    return JSON.stringify(body, null, 2);
  }

  function failedResult(message) {
    return {
      status: 0, ok: false, headers: new g.Headers(), text: "", json: null, ms: 0,
      networkError: message, pass: null, setVerdict: function () {},
    };
  }

  VUI.api = function (spec) {
    var method = (spec.method || "GET").toUpperCase();
    if (typeof spec.path !== "string" || spec.path.indexOf("/api/") !== 0) {
      VUI.log.note("refused: path must start with /api/ (got " + String(spec.path) + ")");
      return Promise.resolve(failedResult("path must start with /api/"));
    }
    var query = spec.query ? VUI.buildQuery(spec.query) : "";
    var url = spec.path + (query ? "?" + query : "");
    var headers = { Accept: "application/json" };
    var hasBody = spec.body !== undefined;
    if (hasBody) {
      headers["Content-Type"] = "application/json";
    }
    var token = resolveToken(spec);
    if (token) {
      headers.Authorization = "Bearer " + token;
    }
    Object.keys(spec.headers || {}).forEach(function (k) {
      headers[k] = spec.headers[k];
    });
    var expected = spec.expect === undefined ? null : Array.isArray(spec.expect) ? spec.expect : [spec.expect];
    var started = Date.now();
    var shouldLog = spec.log !== false;

    return g
      .fetch(url, {
        method: method,
        headers: headers,
        body: hasBody ? JSON.stringify(spec.body) : undefined,
        cache: "no-store",
        credentials: "omit",
      })
      .then(
        function (resp) {
          return resp.text().then(function (text) {
            var json = null;
            try {
              json = text ? JSON.parse(text) : null;
            } catch (e) {
              json = null;
            }
            var lines = [];
            resp.headers.forEach(function (value, name) {
              lines.push(name + ": " + value);
            });
            return { resp: resp, text: text, json: json, headerText: lines.length ? lines.join("\n") : "(none)" };
          });
        },
        function (err) {
          return { error: err };
        }
      )
      .then(function (got) {
        var ms = Date.now() - started;
        var result;
        if (got.error) {
          var message = got.error && got.error.message ? got.error.message : String(got.error);
          result = failedResult(message);
          result.ms = ms;
          if (shouldLog) {
            var failEntry = VUI.log.add({
              method: method, url: url, requestHeaders: headerLines(headers), requestBody: displayBody(spec.body),
              status: 0, responseHeaders: "(none)", responseBody: "NETWORK ERROR: " + message, ms: ms,
            });
            result.setVerdict = failEntry.setVerdict;
            if (expected) {
              result.pass = false;
              failEntry.setVerdict(false, "FAIL - expected " + expected.join("/"));
            }
            VUI.log.note("NETWORK ERROR: " + message + " (is serve.py still running and the stack up?)");
          }
          return result;
        }
        var resp = got.resp;
        result = {
          status: resp.status, ok: resp.ok, headers: resp.headers, text: got.text, json: got.json, ms: ms,
          networkError: null, pass: null, setVerdict: function () {},
        };
        if (shouldLog) {
          var entry = VUI.log.add({
            method: method, url: url, requestHeaders: headerLines(headers), requestBody: displayBody(spec.body),
            status: resp.status, responseHeaders: got.headerText, responseBody: pretty(got.text) || "(empty)", ms: ms,
          });
          result.setVerdict = function (pass, text) {
            result.pass = !!pass;
            entry.setVerdict(pass, text);
          };
          if (expected) {
            var pass = expected.indexOf(resp.status) !== -1;
            result.setVerdict(pass, pass ? "PASS" : "FAIL - expected " + expected.join("/"));
          }
          if (resp.status === 429) {
            VUI.log.note(
              "429 rate limited, Retry-After: " + (resp.headers.get("Retry-After") || "(not sent)") +
                ". Raise the gateway limits as described in README.md (Prerequisites)."
            );
          }
        } else {
          result.setVerdict = function (pass) {
            result.pass = !!pass;
          };
          if (expected) {
            result.pass = expected.indexOf(resp.status) !== -1;
          }
        }
        return result;
      });
  };

  /* ---------------------------------------------------------- ui helpers */

  function cellText(value) {
    if (value === null || value === undefined) {
      return "";
    }
    if (typeof value === "object") {
      return JSON.stringify(value);
    }
    return String(value);
  }

  VUI.ui = {
    button: function (text, onClick, opts) {
      var o = opts || {};
      return el("button", { type: "button", text: text, "class": o.class, title: o.title, disabled: o.disabled, on: { click: onClick } });
    },
    field: function (labelText, inputEl) {
      return el("label", { "class": "field" }, el("span", { text: labelText }), inputEl);
    },
    input: function (attrs) {
      var a = {};
      Object.keys(attrs || {}).forEach(function (k) {
        a[k] = attrs[k];
      });
      if (!a.type) {
        a.type = "text";
      }
      return el("input", a);
    },
    select: function (options, attrs) {
      return el("select", attrs || {}, options.map(function (pair) {
        return el("option", { value: pair[0], text: pair[1] });
      }));
    },
    row: function () {
      return el("div", { "class": "row" }, Array.prototype.slice.call(arguments));
    },
    kv: function (pairs) {
      return el("div", { "class": "kv" }, pairs.map(function (pair) {
        return el("div", { "class": "kv-line" }, el("span", { "class": "kv-key", text: pair[0] }), " ", el("span", { "class": "kv-val", text: cellText(pair[1]) }));
      }));
    },
    table: function (columns, rows, actions) {
      if (!rows || !rows.length) {
        return el("p", { "class": "muted", text: "no rows" });
      }
      var head = el("tr", {}, columns.map(function (c) {
        return el("th", { text: c.label });
      }), actions && actions.length ? el("th", { text: "" }) : null);
      var body = rows.map(function (row) {
        var cells = columns.map(function (c) {
          return el("td", { text: cellText(c.get ? c.get(row) : row[c.key]) });
        });
        if (actions && actions.length) {
          cells.push(el("td", { "class": "actions" }, actions.map(function (a) {
            return VUI.ui.button(a.label, function () {
              a.onClick(row);
            });
          })));
        }
        return el("tr", {}, cells);
      });
      return el("div", { "class": "table-wrap" }, el("table", {}, el("thead", {}, head), el("tbody", {}, body)));
    },
    statusLine: function () {
      var node = el("p", { "class": "status info", text: "" });
      return {
        el: node,
        set: function (text, kind) {
          node.className = "status " + (kind || "info");
          node.textContent = text;
        },
      };
    },
  };

  /* ------------------------------------------------------------ pollUntil */

  VUI.pollUntil = function (fn, opts) {
    var o = opts || {};
    var intervalMs = o.intervalMs || 1500;
    var timeoutMs = o.timeoutMs || 90000;
    var cancelled = false;
    var timer = null;
    var settle = null;
    var done = false;
    var started = Date.now();
    var attempt = 0;
    var promise = new Promise(function (resolve) {
      settle = function (value) {
        if (!done) {
          done = true;
          resolve(value);
        }
      };
      function tick() {
        if (cancelled) {
          settle(null);
          return;
        }
        attempt += 1;
        Promise.resolve()
          .then(fn)
          .then(
            function (result) {
              if (cancelled) {
                settle(null);
                return;
              }
              if (o.onTick) {
                try {
                  o.onTick(result, attempt, Math.round((Date.now() - started) / 1000));
                } catch (e) {
                  /* a tick callback must not stop the poll */
                }
              }
              if (result) {
                settle(result);
              } else if (Date.now() - started >= timeoutMs) {
                settle(null);
              } else {
                timer = g.setTimeout(tick, intervalMs);
              }
            },
            function () {
              settle(null);
            }
          );
      }
      tick();
    });
    return {
      promise: promise,
      cancel: function () {
        cancelled = true;
        if (timer) {
          g.clearTimeout(timer);
        }
        settle(null);
      },
    };
  };

  /* ------------------------------------------------------ panel registry */

  var panelBuilders = [];

  VUI.registerPanel = function (id, build) {
    panelBuilders.push({ id: id, build: build });
  };

  function bootstrap() {
    var gatewayLabel = doc.getElementById("gateway-url");
    g.fetch("/_ui/config", { cache: "no-store", credentials: "omit" })
      .then(function (r) {
        return r.json();
      })
      .then(
        function (cfg) {
          if (gatewayLabel) {
            gatewayLabel.textContent = cfg && typeof cfg.gateway_url === "string" ? cfg.gateway_url : "unknown";
          }
        },
        function () {
          if (gatewayLabel) {
            gatewayLabel.textContent = "unknown";
          }
        }
      );
    buildContext();
    var select = doc.getElementById("act-as");
    if (select) {
      select.addEventListener("change", function () {
        VUI.sessions.setActive(select.value || null);
      });
    }
    var clearLog = doc.getElementById("log-clear");
    if (clearLog) {
      clearLog.addEventListener("click", function () {
        VUI.log.clear();
      });
    }
    renderActAs();
    updateLogCount();
    panelBuilders.forEach(function (p) {
      var body = doc.querySelector('[data-panel="' + p.id + '"]');
      if (body) {
        try {
          p.build(body);
        } catch (e) {
          VUI.log.note("panel " + p.id + " failed to build: " + (e && e.message ? e.message : String(e)));
        }
      }
    });
  }

  // Deferred scripts run before DOMContentLoaded (readyState "interactive"),
  // so every panel script has registered by the time bootstrap runs.
  if (doc.readyState === "complete") {
    g.setTimeout(bootstrap, 0);
  } else {
    doc.addEventListener("DOMContentLoaded", bootstrap);
  }

  /* ---------------------------------------------------------- Auth panel */

  function claimsBlock(title, token) {
    var claims = token ? VUI.decodeJwtClaims(token) : null;
    var children = [el("h4", { text: title })];
    if (!token) {
      children.push(el("p", { "class": "muted", text: "(none)" }));
      return el("div", { "class": "claims" }, children);
    }
    if (!claims) {
      children.push(el("p", { "class": "status bad", text: "token is not a decodable JWT" }));
      return el("div", { "class": "claims" }, children);
    }
    var exp = VUI.describeExp(claims.exp);
    children.push(
      VUI.ui.kv([
        ["jti", claims.jti], ["rjti", claims.rjti], ["role", claims.role], ["clinic_id", claims.clinic_id],
        ["sub", claims.sub], ["type", claims.type],
        ["exp", exp.iso ? exp.iso + " (" + exp.secondsRemaining + " s remaining)" : "n/a"],
      ])
    );
    children.push(el("pre", { text: JSON.stringify(claims, null, 2) }));
    return el("div", { "class": "claims" }, children);
  }

  function buildAuthPanel(body) {
    var email = VUI.ui.input({ type: "text", autocomplete: "off", spellcheck: "false", placeholder: "email", size: 30 });
    var password = VUI.ui.input({ type: "password", autocomplete: "off", placeholder: "password (never stored)", size: 24 });
    var label = VUI.ui.input({ type: "text", autocomplete: "off", placeholder: "default: the role claim", size: 18 });
    var status = VUI.ui.statusLine();
    var sessionsBox = el("div", {});
    var claimsBox = el("div", { "class": "claims-grid" });
    var pairingBox = el("div", {});
    var replayButton;

    function afterLogin(result, upper) {
      var access = result.json && result.json.access_token;
      var refresh = result.json && result.json.refresh_token;
      if (result.status !== 200 || !access || !refresh) {
        status.set("login failed with status " + result.status + "; nothing stored", "bad");
        return;
      }
      var claims = VUI.decodeJwtClaims(access) || {};
      var name = label.value.trim() || (typeof claims.role === "string" ? claims.role : "session");
      VUI.sessions.save(name, {
        email: email.value.trim(), access: access, refresh: refresh, previousRefresh: null,
        loggedOut: false, loggedOutAccess: "", loggedOutRefresh: "",
      });
      VUI.sessions.setActive(name);
      password.value = "";
      status.set("logged in as " + name + " (role " + claims.role + ")" + (upper ? " using the upper-cased email" : ""), "ok");
    }

    function doLogin(upper) {
      var typedEmail = email.value.trim();
      if (!typedEmail || !password.value) {
        status.set("enter an email and a password first", "warn");
        return Promise.resolve();
      }
      return VUI.api({
        method: "POST", path: "/api/auth/login", auth: false, expect: 200,
        body: { email: upper ? VUI.uppercaseEmail(typedEmail) : typedEmail, password: password.value },
      }).then(function (r) {
        afterLogin(r, upper);
      });
    }

    function doRefresh() {
      var rec = VUI.sessions.activeRecord();
      if (!rec || !rec.refresh) {
        status.set("no active session to refresh", "warn");
        return;
      }
      var name = VUI.sessions.active();
      VUI.api({ method: "POST", path: "/api/auth/refresh", auth: false, expect: 200, body: { refresh_token: rec.refresh } }).then(function (r) {
        if (r.status === 200 && r.json && r.json.access_token && r.json.refresh_token) {
          var next = {};
          Object.keys(rec).forEach(function (k) {
            next[k] = rec[k];
          });
          next.previousRefresh = rec.refresh;
          next.access = r.json.access_token;
          next.refresh = r.json.refresh_token;
          VUI.sessions.save(name, next);
          status.set("refreshed: the NEW refresh token is stored, the old one is kept for the replay button", "ok");
        } else {
          status.set("refresh returned " + r.status, "bad");
        }
      });
    }

    function doReplay() {
      var rec = VUI.sessions.activeRecord();
      if (!rec || !rec.previousRefresh) {
        status.set("refresh first so there is an old refresh token to replay", "warn");
        return;
      }
      VUI.api({ method: "POST", path: "/api/auth/refresh", auth: false, expect: 401, body: { refresh_token: rec.previousRefresh } }).then(function (r) {
        status.set(r.status === 401 ? "replay refused with 401, as required (single-use rotation)" : "replay returned " + r.status + ", expected 401", r.status === 401 ? "ok" : "bad");
      });
    }

    function doLogout() {
      var rec = VUI.sessions.activeRecord();
      if (!rec || !rec.access) {
        status.set("no active session to log out", "warn");
        return;
      }
      var name = VUI.sessions.active();
      VUI.api({ method: "POST", path: "/api/auth/logout", expect: 204 }).then(function (r) {
        if (r.status === 204) {
          var next = {};
          Object.keys(rec).forEach(function (k) {
            next[k] = rec[k];
          });
          next.loggedOut = true;
          next.loggedOutAccess = rec.access;
          next.loggedOutRefresh = rec.refresh;
          VUI.sessions.save(name, next);
          status.set("logged out: the access jti and the paired refresh jti are now revoked", "ok");
        } else {
          status.set("logout returned " + r.status, "bad");
        }
      });
    }

    function doLoggedOutRefresh() {
      var rec = VUI.sessions.activeRecord();
      if (!rec || !rec.loggedOut) {
        VUI.log.note("log out first: this check needs a logged-out session (use Logout, then this button)");
        status.set("log out first", "warn");
        return;
      }
      VUI.api({ method: "POST", path: "/api/auth/refresh", auth: false, expect: 401, body: { refresh_token: rec.loggedOutRefresh } }).then(function (r) {
        status.set(r.status === 401 ? "logged-out refresh token refused with 401, as required" : "returned " + r.status + ", expected 401", r.status === 401 ? "ok" : "bad");
      });
    }

    function doLoggedOutAccess() {
      var rec = VUI.sessions.activeRecord();
      if (!rec || !rec.loggedOut) {
        VUI.log.note("log out first: this check needs a logged-out session (use Logout, then this button)");
        status.set("log out first", "warn");
        return;
      }
      VUI.api({ method: "GET", path: "/api/appointments", token: rec.loggedOutAccess, expect: 401 }).then(function (r) {
        status.set(r.status === 401 ? "revoked access token refused with 401, as required" : "returned " + r.status + ", expected 401", r.status === 401 ? "ok" : "bad");
      });
    }

    replayButton = VUI.ui.button("Replay old refresh token (must be 401)", doReplay, { disabled: true });

    function render() {
      var labels = VUI.sessions.list();
      var rows = labels.map(function (name) {
        var rec = VUI.sessions.get(name);
        var claims = VUI.decodeJwtClaims(rec.access) || {};
        var exp = VUI.describeExp(claims.exp);
        return {
          label: name + (name === VUI.sessions.active() ? " *" : ""),
          email: rec.email, role: claims.role || "unknown",
          seconds: exp.secondsRemaining === null ? "n/a" : exp.secondsRemaining,
          state: rec.loggedOut ? "logged out" : "active",
        };
      });
      clear(sessionsBox);
      sessionsBox.appendChild(el("h3", { text: "Sessions (* = acting identity)" }));
      sessionsBox.appendChild(
        VUI.ui.table(
          [
            { label: "label", key: "label" }, { label: "email", key: "email" }, { label: "role", key: "role" },
            { label: "access s remaining", key: "seconds" }, { label: "state", key: "state" },
          ],
          rows
        )
      );
      var rec = VUI.sessions.activeRecord();
      clear(claimsBox);
      clear(pairingBox);
      if (rec) {
        claimsBox.appendChild(claimsBlock("Access token claims", rec.access));
        claimsBox.appendChild(claimsBlock("Refresh token claims", rec.refresh));
        claimsBox.appendChild(claimsBlock("Previous refresh token claims", rec.previousRefresh || ""));
        var a = VUI.decodeJwtClaims(rec.access) || {};
        var r = VUI.decodeJwtClaims(rec.refresh) || {};
        var p = rec.previousRefresh ? VUI.decodeJwtClaims(rec.previousRefresh) || {} : null;
        pairingBox.appendChild(
          VUI.ui.kv([
            ["access.rjti equals refresh.jti", a.rjti !== undefined && a.rjti === r.jti ? "yes" : "no"],
            ["previous refresh jti differs from current", p ? (p.jti !== r.jti ? "yes" : "no") : "n/a (refresh first)"],
          ])
        );
      }
      replayButton.disabled = !(rec && rec.previousRefresh);
    }

    body.appendChild(
      el("div", {},
        VUI.ui.row(VUI.ui.field("email", email), VUI.ui.field("password", password), VUI.ui.field("label (optional)", label)),
        VUI.ui.row(
          VUI.ui.button("Login", function () { doLogin(false); }),
          VUI.ui.button("Login with email in different case", function () { doLogin(true); }, { title: "sends the email upper-cased; the server lowercases it, so this must be 200" }),
          VUI.ui.button("Refresh", doRefresh),
          replayButton,
          VUI.ui.button("Logout", doLogout)
        ),
        VUI.ui.row(
          VUI.ui.button("Refresh with logged-out token (must be 401)", doLoggedOutRefresh),
          VUI.ui.button("Call API with logged-out access token (must be 401)", doLoggedOutAccess)
        ),
        status.el, sessionsBox, pairingBox, claimsBox
      )
    );
    VUI.sessions.onChange(render);
    render();
    g.setInterval(render, 5000);
  }

  VUI.registerPanel("auth", buildAuthPanel);
})(globalThis);
