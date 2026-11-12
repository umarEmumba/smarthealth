/* Panels 2-4 of the Phase 2.1 validation UI: Me, Lists + pagination, Booking.
 *
 * Built only through VUI.el / VUI.ui (core.js). Every path literal below is a
 * public gateway route; nothing here references an internal route. */
(function (g) {
  "use strict";

  var VUI = g.VUI;
  var el = VUI.el;
  var ui = VUI.ui;
  var DAY_MS = 24 * 60 * 60 * 1000;

  function detailOf(r) {
    if (r && r.json && r.json.detail !== undefined) {
      return typeof r.json.detail === "string" ? r.json.detail : JSON.stringify(r.json.detail);
    }
    return r && r.networkError ? r.networkError : "";
  }

  function activeRole() {
    var label = VUI.sessions.active();
    return label ? VUI.sessions.roleOf(label) : "none";
  }

  function num(value, fallback) {
    var n = Number(value);
    return isFinite(n) && String(value).trim() !== "" ? n : fallback;
  }

  /* Poll GET /api/appointments/{id} (not logged per call) until terminal(status)
   * is true, the route stops answering 200, or 90 s pass. The appointment and
   * billing fan-out are asynchronous, so a single read right after an action is
   * not enough. Shared with the Visit panel (panels_ops.js loads after this). */
  VUI.watchAppointment = function (appointmentId, opts) {
    var last = null;
    var line = opts.line;
    var handle = VUI.pollUntil(
      function () {
        return VUI.api({ path: "/api/appointments/" + encodeURIComponent(appointmentId), log: false }).then(function (r) {
          if (r.status === 401 || r.status === 403 || r.status === 404) {
            return { stop: r.status };
          }
          if (r.status === 200 && r.json) {
            var status = r.json.status;
            if (status !== last) {
              VUI.log.note("appointment " + appointmentId + " status: " + (last === null ? "" : last + " -> ") + status);
              last = status;
            }
            if (opts.terminal(status)) {
              return { appointment: r.json };
            }
          }
          return null;
        });
      },
      {
        intervalMs: 1500,
        timeoutMs: 90000,
        onTick: function (result, attempt, elapsed) {
          if (!result) {
            line.set("polling: status " + (last || "?") + " (attempt " + attempt + ", " + elapsed + " s)", "info");
          }
        },
      }
    );
    handle.promise.then(function (result) {
      if (result && result.appointment) {
        var a = result.appointment;
        var text = "final status: " + a.status + " (version " + a.version + ")";
        if (a.status === "failed" && a.failure_reason) {
          text += ", failure_reason: " + a.failure_reason;
        }
        line.set(text, a.status === "failed" ? "bad" : "ok");
        VUI.log.note("polling finished: " + text);
      } else if (result && result.stop) {
        line.set("polling stopped: HTTP " + result.stop, "warn");
        VUI.log.note("polling stopped: HTTP " + result.stop);
      } else {
        line.set("polling ended without a terminal status (timeout or stopped); last seen: " + (last || "none"), "warn");
        VUI.log.note("polling ended without a terminal status; last seen: " + (last || "none"));
      }
    });
    return handle;
  };

  function renderHistory(box, history) {
    VUI.clear(box);
    var rows = (history || []).map(function (h) {
      return { change: (h.from_status || "(none)") + " -> " + h.to_status, reason: h.reason || "", at: h.created_at };
    });
    box.appendChild(ui.table([{ label: "change", key: "change" }, { label: "reason", key: "reason" }, { label: "created_at", key: "at" }], rows));
  }
  VUI.renderHistory = renderHistory;

  /* ---------------------------------------------------------- 2. Me */

  VUI.registerPanel("me", function (body) {
    var status = ui.statusLine();
    var table = el("div", {});

    function explain(r) {
      if (r.status === 200 && r.json) {
        status.set("200: patient profile " + r.json.id + ", is_active=" + r.json.is_active, "ok");
      } else if (r.status === 403) {
        status.set("403: this route is patient-only", "warn");
      } else if (r.status === 404) {
        status.set("404: no linked active profile in this clinic, or no clinic claim in the token", "warn");
      } else if (r.status === 401) {
        status.set("401: token invalid, expired or logged out", "bad");
      } else {
        status.set("status " + r.status + " " + detailOf(r), "bad");
      }
    }

    function tryAll() {
      var labels = VUI.sessions.list();
      var rows = [];
      return labels
        .reduce(function (chain, label) {
          return chain.then(function () {
            return VUI.api({ path: "/api/patients/me", auth: label }).then(function (r) {
              rows.push({ label: label, role: VUI.sessions.roleOf(label), status: r.status });
            });
          });
        }, Promise.resolve())
        .then(function () {
          VUI.clear(table);
          table.appendChild(ui.table([{ label: "session", key: "label" }, { label: "role", key: "role" }, { label: "status", key: "status" }], rows));
        });
    }

    body.appendChild(
      el("div", {},
        el("p", { "class": "hint", text: "Act as a patient: 200. Every other role: 403. A patient with no linked profile (or a token without a clinic claim): 404." }),
        ui.row(
          ui.button("GET /api/patients/me (active session)", function () {
            VUI.api({ path: "/api/patients/me" }).then(explain);
          }),
          ui.button("Try with every logged-in session", tryAll)
        ),
        status.el,
        table
      )
    );
  });

  /* ------------------------------------------------------- 3. Lists */

  function listBlock(cfg) {
    var limit = ui.input({ type: "text", value: "5", size: 5 });
    var offset = ui.input({ type: "text", value: "0", size: 5 });
    var info = ui.statusLine();
    var tableBox = el("div", {});
    var prev = ui.button("Prev", function () {
      offset.value = String(Math.max(0, num(offset.value, 0) - num(limit.value, 0)));
      load();
    }, { disabled: true });
    var next = ui.button("Next", function () {
      offset.value = String(num(offset.value, 0) + num(limit.value, 0));
      load();
    }, { disabled: true });

    function load() {
      // limit and offset go out exactly as typed: limit 0 or 500 must reach
      // the server so its 422 is visible (never clamped client-side).
      var query = cfg.query();
      query.limit = limit.value.trim();
      query.offset = offset.value.trim();
      return VUI.api({ path: cfg.path, query: query }).then(function (r) {
        VUI.clear(tableBox);
        if (r.status === 200 && Array.isArray(r.json)) {
          var rawTotal = r.headers.get("X-Total-Count");
          var total = rawTotal === null ? null : Number(rawTotal);
          var page = VUI.pageState(num(offset.value, 0), num(limit.value, 0), r.json.length, total);
          info.set(
            "X-Total-Count: " + (rawTotal === null ? "(absent)" : rawTotal) + " | returned: " + r.json.length + " | showing " + page.from + "-" + page.to,
            "ok"
          );
          prev.disabled = !page.hasPrev;
          next.disabled = !page.hasNext;
          tableBox.appendChild(ui.table(cfg.columns, r.json, cfg.actions));
        } else {
          info.set("status " + r.status + (detailOf(r) ? ": " + detailOf(r) : ""), r.status === 422 ? "warn" : "bad");
          prev.disabled = true;
          next.disabled = true;
        }
      });
    }

    return el("div", { "class": "sub-block" },
      el("h3", { text: cfg.title }),
      el("p", { "class": "hint", text: cfg.hint }),
      ui.row(ui.field("limit", limit), ui.field("offset", offset), ui.button("Load", load), prev, next),
      info.el,
      tableBox,
      cfg.extra || null
    );
  }

  VUI.registerPanel("lists", function (body) {
    var deptStatus = ui.statusLine();
    var deptTable = el("div", {});

    function loadDepartments() {
      var clinicId = VUI.ctx.get("clinic_id");
      if (!clinicId) {
        deptStatus.set("set clinic_id in the context first", "warn");
        return;
      }
      VUI.api({ path: "/api/clinics/" + encodeURIComponent(clinicId) + "/departments" }).then(function (r) {
        VUI.clear(deptTable);
        if (r.status === 200 && Array.isArray(r.json)) {
          deptStatus.set(r.json.length + " departments (not paginated)", "ok");
          deptTable.appendChild(
            ui.table(
              [{ label: "id", key: "id" }, { label: "name", key: "name" }, { label: "is_active", key: "is_active" }],
              r.json,
              [{ label: "use", onClick: function (row) { VUI.ctx.set("department_id", row.id); } }]
            )
          );
        } else {
          deptStatus.set("status " + r.status + (detailOf(r) ? ": " + detailOf(r) : ""), "bad");
        }
      });
    }

    body.appendChild(
      el("div", {},
        listBlock({
          title: "Clinics (admin)", path: "/api/clinics", hint: "Act as admin. limit 1..200, offset >= 0; out-of-range values are 422, never clamped.",
          query: function () { return {}; },
          columns: [{ label: "id", key: "id" }, { label: "name", key: "name" }, { label: "timezone", key: "timezone" }, { label: "is_active", key: "is_active" }],
          actions: [{ label: "use", onClick: function (row) { VUI.ctx.set("clinic_id", row.id); } }],
        }),
        listBlock({
          title: "Providers (admin)", path: "/api/providers", hint: "Act as admin. department_id (context) is required; leave it empty to see the 422.",
          query: function () { return { department_id: VUI.ctx.get("department_id") }; },
          columns: [{ label: "id", key: "id" }, { label: "full_name", key: "full_name" }, { label: "department_id", key: "department_id" }, { label: "is_active", key: "is_active" }],
          actions: [{ label: "use", onClick: function (row) { VUI.ctx.set("provider_id", row.id); } }],
          extra: el("div", {}, ui.button("Departments of clinic", loadDepartments), deptStatus.el, deptTable),
        }),
        listBlock({
          title: "Patients (front_desk own clinic / admin)", path: "/api/patients", hint: "Act as front_desk or admin. clinic_id (context) is required; a front_desk asking for another clinic gets 403.",
          query: function () { return { clinic_id: VUI.ctx.get("clinic_id") }; },
          columns: [{ label: "id", key: "id" }, { label: "full_name", key: "full_name" }, { label: "clinic_id", key: "clinic_id" }, { label: "is_active", key: "is_active" }],
          actions: [{ label: "use", onClick: function (row) { VUI.ctx.set("patient_id", row.id); } }],
        })
      )
    );
  });

  /* ----------------------------------------------------- 4. Booking */

  VUI.registerPanel("booking", function (body) {
    /* a. Find slots */
    var startInput = ui.input({ type: "text", size: 26 });
    var endInput = ui.input({ type: "text", size: 26 });
    var statusSelect = ui.select(
      [["available", "available"], ["any", "any"], ["held", "held"], ["booked", "booked"], ["cancelled", "cancelled"]],
      { value: "available" }
    );
    var slotsStatus = ui.statusLine();
    var slotsTable = el("div", {});

    function fill(fromMs, toMs) {
      startInput.value = VUI.isoOffset(fromMs);
      endInput.value = VUI.isoOffset(toMs);
    }
    fill(0, 3 * DAY_MS);

    function slotColumns() {
      return [
        { label: "id", key: "id" },
        { label: "start (local)", get: function (s) { return new Date(s.start_time).toLocaleString(); } },
        { label: "start (UTC)", get: function (s) { return s.start_time; } },
        { label: "end", key: "end_time" },
        { label: "status", key: "status" },
      ];
    }

    function loadSlots() {
      return VUI.api({
        path: "/api/slots",
        query: { provider_id: VUI.ctx.get("provider_id"), start: startInput.value.trim(), end: endInput.value.trim(), status: statusSelect.value },
      }).then(function (r) {
        VUI.clear(slotsTable);
        if (r.status === 200 && Array.isArray(r.json)) {
          slotsStatus.set(r.json.length + " slots", "ok");
          slotsTable.appendChild(
            ui.table(slotColumns(), r.json, [
              { label: "pick for booking", onClick: function (s) { VUI.ctx.set("slot_id", s.id); } },
              { label: "use as reschedule target", onClick: function (s) { VUI.ctx.set("new_slot_id", s.id); } },
            ])
          );
        } else {
          slotsStatus.set("status " + r.status + (detailOf(r) ? ": " + detailOf(r) : ""), r.status === 422 ? "warn" : "bad");
        }
      });
    }

    /* b. Book */
    var slotInput = ui.input({ type: "text", size: 38, value: VUI.ctx.get("slot_id"), spellcheck: "false" });
    var patientInput = ui.input({ type: "text", size: 38, value: VUI.ctx.get("patient_id"), spellcheck: "false" });
    var coverageInput = ui.input({ type: "text", size: 20, placeholder: "optional, <= 64 chars" });
    var keyInput = ui.input({ type: "text", size: 44, value: VUI.newIdempotencyKey(), spellcheck: "false" });
    var bookStatus = ui.statusLine();
    var pollStatus = ui.statusLine();
    var historyBox = el("div", {});
    var lastBooking = null;
    var poll = null;

    // The visible inputs and the shared context stay two-way bound.
    slotInput.addEventListener("input", function () { VUI.ctx.set("slot_id", slotInput.value); });
    patientInput.addEventListener("input", function () { VUI.ctx.set("patient_id", patientInput.value); });
    VUI.ctx.onChange(function (name) {
      if (name === "slot_id" && slotInput.value !== VUI.ctx.get("slot_id")) {
        slotInput.value = VUI.ctx.get("slot_id");
      }
      if (name === "patient_id" && patientInput.value !== VUI.ctx.get("patient_id")) {
        patientInput.value = VUI.ctx.get("patient_id");
      }
    });

    function bookingBody(slotId) {
      var payload = { slot_id: slotId };
      // staff must send patient_id; a patient must not (it is resolved from the login)
      if (activeRole() !== "patient" && patientInput.value.trim()) {
        payload.patient_id = patientInput.value.trim();
      }
      if (coverageInput.value.trim()) {
        payload.coverage_ref = coverageInput.value.trim();
      }
      return payload;
    }

    function startPolling(appointmentId) {
      if (poll) {
        poll.cancel();
      }
      pollStatus.set("polling appointment " + appointmentId + " ...", "info");
      poll = VUI.watchAppointment(appointmentId, {
        line: pollStatus,
        terminal: function (status) { return status !== "pending"; },
      });
    }

    function book() {
      var slotId = slotInput.value.trim() || VUI.ctx.get("slot_id");
      if (!slotId) {
        bookStatus.set("pick or paste a slot_id first", "warn");
        return;
      }
      var payload = bookingBody(slotId);
      var key = keyInput.value.trim();
      VUI.api({ method: "POST", path: "/api/appointments", headers: { "Idempotency-Key": key }, body: payload, expect: 202 }).then(function (r) {
        if (r.status === 202 && r.json && r.json.appointment_id) {
          lastBooking = { key: key, body: payload, appointment_id: r.json.appointment_id };
          VUI.ctx.set("appointment_id", r.json.appointment_id);
          keyInput.value = VUI.newIdempotencyKey();
          bookStatus.set("202 accepted: appointment " + r.json.appointment_id + " (key used: " + key + "; a fresh key is ready)", "ok");
          startPolling(r.json.appointment_id);
        } else {
          bookStatus.set("status " + r.status + (detailOf(r) ? ": " + detailOf(r) : ""), r.status === 409 || r.status === 422 ? "warn" : "bad");
        }
      });
    }

    function replay() {
      if (!lastBooking) {
        bookStatus.set("book first so there is a request to replay", "warn");
        return;
      }
      VUI.api({
        method: "POST", path: "/api/appointments", headers: { "Idempotency-Key": lastBooking.key }, body: lastBooking.body, expect: 202,
      }).then(function (r) {
        var same = r.status === 202 && r.json && r.json.appointment_id === lastBooking.appointment_id;
        r.setVerdict(same, same ? "PASS - same appointment id on replay" : "FAIL - replay did not return the same appointment id");
        bookStatus.set(same ? "replay returned the SAME appointment id (idempotent)" : "replay: status " + r.status + " " + detailOf(r), same ? "ok" : "bad");
      });
    }

    function showHistory() {
      var id = VUI.ctx.get("appointment_id");
      if (!id) {
        bookStatus.set("no appointment_id yet", "warn");
        return;
      }
      VUI.api({ path: "/api/appointments/" + encodeURIComponent(id) }).then(function (r) {
        if (r.status === 200 && r.json) {
          renderHistory(historyBox, r.json.history);
        } else {
          bookStatus.set("status " + r.status + " " + detailOf(r), "bad");
        }
      });
    }

    /* c. Past slot (S6) */
    var daysBack = ui.input({ type: "text", value: "2", size: 4 });
    var pastStatus = ui.statusLine();
    var pastTable = el("div", {});

    function pastDays() {
      return Math.max(1, Math.floor(num(daysBack.value, 2)));
    }

    function generatePast() {
      var n = pastDays();
      var today = new Date();
      var from = new Date(today.getTime() - n * DAY_MS);
      var yesterday = new Date(today.getTime() - DAY_MS);
      VUI.api({
        method: "POST", path: "/api/slots/generate",
        body: { provider_id: VUI.ctx.get("provider_id"), from_date: VUI.localDateString(from), to_date: VUI.localDateString(yesterday) },
      }).then(function (r) {
        pastStatus.set(
          r.status === 200 ? "generated: " + JSON.stringify(r.json) : "status " + r.status + (detailOf(r) ? ": " + detailOf(r) : "") + (r.status === 403 ? " (a patient cannot generate)" : ""),
          r.status === 200 ? "ok" : "warn"
        );
      });
    }

    function loadPast() {
      var n = pastDays();
      var now = Date.now();
      return VUI.api({
        path: "/api/slots",
        query: { provider_id: VUI.ctx.get("provider_id"), start: VUI.isoOffset(-n * DAY_MS, now), end: VUI.isoOffset(0, now), status: "any" },
      }).then(function (r) {
        VUI.clear(pastTable);
        if (r.status === 200 && Array.isArray(r.json)) {
          var past = r.json.filter(function (s) { return new Date(s.start_time).getTime() <= now; });
          pastStatus.set(past.length + " past slots", past.length ? "ok" : "warn");
          pastTable.appendChild(ui.table(slotColumns(), past, [{ label: "try booking this past slot", onClick: tryPast }]));
        } else {
          pastStatus.set("status " + r.status + (detailOf(r) ? ": " + detailOf(r) : ""), "bad");
        }
      });
    }

    function tryPast(slot) {
      VUI.api({
        method: "POST", path: "/api/appointments", headers: { "Idempotency-Key": VUI.newIdempotencyKey() }, body: bookingBody(slot.id), expect: 422,
      }).then(function (r) {
        var ok = r.status === 422 && detailOf(r).indexOf("slot has already started") !== -1;
        r.setVerdict(ok, ok ? "PASS - 422 slot has already started" : "FAIL - expected 422 'slot has already started', got " + r.status + " " + detailOf(r));
        pastStatus.set(ok ? "refused as required: 422 slot has already started" : "status " + r.status + " " + detailOf(r), ok ? "ok" : "bad");
      });
    }

    body.appendChild(
      el("div", {},
        el("div", { "class": "sub-block" },
          el("h3", { text: "a. Find slots" }),
          el("p", { "class": "hint", text: "provider_id comes from the context. start/end must be tz-aware ISO (UTC with offset), else 422; span <= 31 days." }),
          ui.row(ui.field("start", startInput), ui.field("end", endInput), ui.field("status", statusSelect)),
          ui.row(
            ui.button("next 3 days", function () { fill(0, 3 * DAY_MS); }),
            ui.button("rest of today", function () {
              var endOfDay = new Date();
              endOfDay.setHours(23, 59, 59, 999);
              fill(0, endOfDay.getTime() - Date.now());
            }),
            ui.button("past 2 days", function () { fill(-2 * DAY_MS, 0); }),
            ui.button("Load", loadSlots)
          ),
          slotsStatus.el,
          slotsTable
        ),
        el("div", { "class": "sub-block" },
          el("h3", { text: "b. Book (Idempotency-Key)" }),
          el("p", { "class": "hint", text: "Patients omit patient_id; staff must supply it. A coverage_ref starting DENY- makes the billing pre-check reject: the appointment ends failed with reason BillingRejected. Expected: 202, then pending -> confirmed." }),
          ui.row(ui.field("slot_id", slotInput), ui.field("patient_id (staff only)", patientInput)),
          ui.row(
            ui.field("coverage_ref", coverageInput),
            ui.field("Idempotency-Key", keyInput),
            ui.button("new key", function () { keyInput.value = VUI.newIdempotencyKey(); })
          ),
          ui.row(
            ui.button("Book", book),
            ui.button("Replay last request (same key + same body)", replay),
            ui.button("Stop polling", function () { if (poll) { poll.cancel(); } }),
            ui.button("Show history", showHistory)
          ),
          bookStatus.el,
          pollStatus.el,
          historyBox
        ),
        el("div", { "class": "sub-block" },
          el("h3", { text: "c. Past slot (S6, must be 422)" }),
          el("p", { "class": "hint", text: "Step 1 is staff only (front_desk/admin); a patient gets 403 and can skip it when the seeder already generated past days. Step 2 lists past slots; the row action tries to book one: 422 'slot has already started'." }),
          ui.row(
            ui.field("days back", daysBack),
            ui.button("1. Generate past slots (front_desk/admin only)", generatePast),
            ui.button("2. Load past slots", loadPast)
          ),
          pastStatus.el,
          pastTable
        )
      )
    );
  });
})(globalThis);
