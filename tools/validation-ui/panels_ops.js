/* Panels 5-7 of the Phase 2.1 validation UI: Visit state machine, Provider
 * view, Schedule change. Built only through VUI.el / VUI.ui; every path is a
 * public gateway route (check-in is POST /api/visits). */
(function (g) {
  "use strict";

  var VUI = g.VUI;
  var el = VUI.el;
  var ui = VUI.ui;

  function detailOf(r) {
    if (r && r.json && r.json.detail !== undefined) {
      return typeof r.json.detail === "string" ? r.json.detail : JSON.stringify(r.json.detail);
    }
    return r && r.networkError ? r.networkError : "";
  }

  function ctxInput(name, size) {
    var input = ui.input({ type: "text", size: size || 38, value: VUI.ctx.get(name), spellcheck: "false" });
    input.addEventListener("input", function () { VUI.ctx.set(name, input.value); });
    VUI.ctx.onChange(function (changed) {
      if (changed === name && input.value !== VUI.ctx.get(name)) {
        input.value = VUI.ctx.get(name);
      }
    });
    return input;
  }

  /* ----------------------------------------------------- 5. Visit */

  VUI.registerPanel("visit", function (body) {
    var appointmentInput = ctxInput("appointment_id");
    var visitInput = ctxInput("visit_id");
    var clinicInput = ctxInput("clinic_id");
    var newSlotInput = ctxInput("new_slot_id");
    var apptStatus = ui.statusLine();
    var visitStatus = ui.statusLine();
    var actionStatus = ui.statusLine();
    var historyBox = el("div", {});
    var visitHistoryBox = el("div", {});
    var cancelExpect = ui.input({ type: "checkbox", checked: true });
    var rescheduleExpect = ui.input({ type: "checkbox", checked: true });
    var poll = null;

    function appointmentId() { return appointmentInput.value.trim(); }
    function visitId() { return visitInput.value.trim(); }

    function refreshAppointment() {
      var id = appointmentId();
      if (!id) {
        apptStatus.set("set appointment_id first", "warn");
        return Promise.resolve();
      }
      return VUI.api({ path: "/api/appointments/" + encodeURIComponent(id) }).then(function (r) {
        if (r.status === 200 && r.json) {
          apptStatus.set("appointment status: " + r.json.status + " (version " + r.json.version + ")", "ok");
          VUI.renderHistory(historyBox, r.json.history);
        } else {
          apptStatus.set("status " + r.status + " " + detailOf(r), "bad");
        }
      });
    }

    function checkIn() {
      var id = appointmentId();
      if (!id) {
        actionStatus.set("set appointment_id first", "warn");
        return;
      }
      var payload = { appointment_id: id };
      if (clinicInput.value.trim()) {
        payload.clinic_id = clinicInput.value.trim();
      }
      VUI.api({ method: "POST", path: "/api/visits", body: payload, expect: [200, 201] }).then(function (r) {
        if ((r.status === 201 || r.status === 200) && r.json && r.json.id) {
          VUI.ctx.set("visit_id", r.json.id);
          actionStatus.set(r.status + ": visit " + r.json.id + " is " + r.json.status, "ok");
          return refreshAppointment();
        }
        actionStatus.set(
          "status " + r.status + (detailOf(r) ? ": " + detailOf(r) : "") +
            (r.status === 422 ? " (an admin must send clinic_id)" : ""),
          r.status === 409 ? "warn" : "bad"
        );
        return null;
      });
    }

    function stateChange(name, path, checkbox, bodyFn) {
      var id = appointmentId();
      if (!id) {
        actionStatus.set("set appointment_id first", "warn");
        return;
      }
      var spec = { method: "POST", path: "/api/appointments/" + encodeURIComponent(id) + path, body: bodyFn() };
      if (checkbox.checked) {
        spec.expect = [409];
      }
      VUI.api(spec).then(function (r) {
        actionStatus.set(name + ": status " + r.status + (detailOf(r) ? " " + detailOf(r) : ""), r.status === 409 ? "warn" : r.ok ? "ok" : "bad");
        return refreshAppointment();
      });
    }

    function visitAction(name, verb) {
      var id = visitId();
      if (!id) {
        actionStatus.set("set visit_id first (check in)", "warn");
        return;
      }
      VUI.api({ method: "POST", path: "/api/visits/" + encodeURIComponent(id) + verb, expect: 200 }).then(function (r) {
        if (r.status === 200 && r.json) {
          visitStatus.set("visit status: " + r.json.status, "ok");
          if (verb === "/complete" && appointmentId()) {
            if (poll) { poll.cancel(); }
            actionStatus.set("visit completed; waiting for the asynchronous fan-out to complete the appointment ...", "info");
            poll = VUI.watchAppointment(appointmentId(), {
              line: apptStatus,
              terminal: function (status) { return status === "completed"; },
            });
          }
        } else {
          visitStatus.set(name + ": status " + r.status + (detailOf(r) ? " " + detailOf(r) : ""), r.status === 409 ? "warn" : "bad");
        }
      });
    }

    function getVisit() {
      var id = visitId();
      if (!id) {
        visitStatus.set("set visit_id first", "warn");
        return;
      }
      VUI.api({ path: "/api/visits/" + encodeURIComponent(id) }).then(function (r) {
        if (r.status === 200 && r.json) {
          visitStatus.set("visit status: " + r.json.status, "ok");
          VUI.clear(visitHistoryBox);
          var rows = (r.json.history || []).map(function (h) {
            return { change: (h.from_status || "(none)") + " -> " + h.to_status, at: h.created_at };
          });
          visitHistoryBox.appendChild(ui.table([{ label: "change", key: "change" }, { label: "created_at", key: "at" }], rows));
        } else {
          visitStatus.set("status " + r.status + " " + detailOf(r), "bad");
        }
      });
    }

    body.appendChild(
      el("div", {},
        el("p", { "class": "hint", text: "Act as front_desk (an admin must also fill clinic_id). Book the seeder's window slot as the patient first. Check-in 409s: 'appointment not confirmed', 'check-in window not open', 'check-in window closed'; the window opens 60 minutes before the slot start and closes at the slot end, so use the seeder's window slot. After check-in, Cancel and Reschedule must both be 409." }),
        ui.row(ui.field("appointment_id", appointmentInput), ui.field("visit_id", visitInput)),
        ui.row(ui.field("clinic_id (admin)", clinicInput), ui.field("new_slot_id", newSlotInput)),
        ui.row(
          ui.button("Check in", checkIn),
          ui.button("Refresh appointment + history", refreshAppointment)
        ),
        ui.row(
          ui.button("Cancel", function () { stateChange("cancel", "/cancel", cancelExpect, function () { return {}; }); }),
          el("label", { "class": "inline" }, cancelExpect, " expect 409 (already checked in)")
        ),
        ui.row(
          ui.button("Reschedule", function () {
            stateChange("reschedule", "/reschedule", rescheduleExpect, function () {
              return { new_slot_id: newSlotInput.value.trim() || VUI.newUuid() };
            });
          }),
          el("label", { "class": "inline" }, rescheduleExpect, " expect 409 (already checked in)")
        ),
        el("p", { "class": "hint", text: "The state check precedes the new-slot lookup, so an empty new_slot_id (a random UUID is sent) still proves the post-check-in 409." }),
        ui.row(
          ui.button("Start visit", function () { visitAction("start", "/start"); }),
          ui.button("Complete visit", function () { visitAction("complete", "/complete"); }),
          ui.button("Get visit", getVisit)
        ),
        actionStatus.el,
        apptStatus.el,
        historyBox,
        visitStatus.el,
        visitHistoryBox
      )
    );
  });

  /* --------------------------------------------------- 6. Provider */

  VUI.registerPanel("provider", function (body) {
    var otherInput = ctxInput("other_appointment_id");
    var statusSelect = ui.select(
      [["", "(any status)"], ["pending", "pending"], ["confirmed", "confirmed"], ["checked_in", "checked_in"], ["completed", "completed"], ["cancelled", "cancelled"], ["failed", "failed"], ["no_show", "no_show"]],
      { value: "" }
    );
    var limitInput = ui.input({ type: "text", value: "100", size: 5 });
    var listStatus = ui.statusLine();
    var listTable = el("div", {});
    var compareStatus = ui.statusLine();

    function listMine() {
      VUI.api({ path: "/api/appointments", query: { status: statusSelect.value, limit: limitInput.value.trim() } }).then(function (r) {
        VUI.clear(listTable);
        if (r.status === 200 && Array.isArray(r.json)) {
          var ids = {};
          r.json.forEach(function (a) { ids[a.provider_id] = true; });
          var distinct = Object.keys(ids);
          var expected = VUI.ctx.get("provider_id");
          var pass = expected ? distinct.every(function (id) { return id === expected; }) : distinct.length <= 1;
          r.setVerdict(pass, pass ? "PASS - own appointments only" : "FAIL - rows from " + distinct.length + " providers");
          listStatus.set(
            r.json.length + " appointments, " + distinct.length + " distinct provider_id" + (expected ? " (expected " + expected + ")" : ""),
            pass ? "ok" : "bad"
          );
          listTable.appendChild(
            ui.table(
              [{ label: "id", key: "id" }, { label: "provider_id", key: "provider_id" }, { label: "status", key: "status" }, { label: "slot_start", key: "slot_start" }],
              r.json
            )
          );
        } else {
          listStatus.set("status " + r.status + " " + detailOf(r), "bad");
        }
      });
    }

    function getOther() {
      var id = otherInput.value.trim();
      if (!id) {
        compareStatus.set("set other_appointment_id first", "warn");
        return Promise.resolve(null);
      }
      return VUI.api({ path: "/api/appointments/" + encodeURIComponent(id), expect: 404 });
    }

    function getUnknown() {
      return VUI.api({ path: "/api/appointments/" + encodeURIComponent(VUI.newUuid()), expect: 404 });
    }

    function compare() {
      return getOther().then(function (a) {
        if (!a) { return null; }
        return getUnknown().then(function (b) {
          var same = VUI.sameResponse(a, b);
          var pass = a.status === 404 && b.status === 404 && same;
          b.setVerdict(pass, pass ? "PASS - identical 404s" : "FAIL - statuses " + a.status + "/" + b.status + ", identical=" + same);
          compareStatus.set(
            pass ? "both are 404 with the identical body: existence cannot be probed" : "statuses " + a.status + " / " + b.status + ", identical: " + same,
            pass ? "ok" : "bad"
          );
          return null;
        });
      });
    }

    body.appendChild(
      el("div", {},
        el("p", { "class": "hint", text: "Act as the demo provider. 'List my appointments' must contain only that provider's own rows. To get another provider's appointment id: as the patient, book the seeder's window slot (second provider), copy that appointment id here, then act as the first provider: it must answer the same 404 as a random unknown id." }),
        ui.row(ui.field("status", statusSelect), ui.field("limit", limitInput), ui.button("List my appointments", listMine)),
        listStatus.el,
        listTable,
        ui.row(ui.field("other_appointment_id", otherInput)),
        ui.row(
          ui.button("GET another provider's appointment (must be 404)", function () { getOther().then(function (r) { if (r) { compareStatus.set("status " + r.status + " " + detailOf(r), r.status === 404 ? "ok" : "bad"); } }); }),
          ui.button("GET unknown random id (must be 404)", function () { getUnknown().then(function (r) { compareStatus.set("status " + r.status + " " + detailOf(r), r.status === 404 ? "ok" : "bad"); }); }),
          ui.button("Compare the two 404s", compare)
        ),
        compareStatus.el
      )
    );
  });

  /* ------------------------------------------------ 7. Schedule change */

  VUI.registerPanel("schedule", function (body) {
    var flaggedSelect = ui.select([["true", "true"], ["false", "false"], ["any", "any"]], { value: "true" });
    var statusInput = ui.input({ type: "text", value: "confirmed", size: 14 });
    var providerFilter = ui.input({ type: "text", size: 38, placeholder: "optional provider_id", spellcheck: "false" });
    var flagStatus = ui.statusLine();
    var flagTable = el("div", {});
    var tplStatus = ui.statusLine();
    var tplTable = el("div", {});

    function listFlagged() {
      var query = { status: statusInput.value.trim(), provider_id: providerFilter.value.trim() };
      if (flaggedSelect.value !== "any") {
        query.flagged = flaggedSelect.value;
      }
      VUI.api({ path: "/api/appointments", query: query }).then(function (r) {
        VUI.clear(flagTable);
        if (r.status === 200 && Array.isArray(r.json)) {
          flagStatus.set(r.json.length + " appointments (reasons expected: template_updated, template_deactivated, provider_deactivated)", "ok");
          flagTable.appendChild(
            ui.table(
              [
                { label: "id", key: "id" }, { label: "status", key: "status" }, { label: "slot_start", key: "slot_start" },
                { label: "schedule_change_flagged", key: "schedule_change_flagged" }, { label: "schedule_change_reason", key: "schedule_change_reason" },
              ],
              r.json,
              [{ label: "use", onClick: function (row) { VUI.ctx.set("appointment_id", row.id); } }]
            )
          );
        } else {
          flagStatus.set("status " + r.status + " " + detailOf(r), "bad");
        }
      });
    }

    function listTemplates() {
      var providerId = VUI.ctx.get("provider_id");
      if (!providerId) {
        tplStatus.set("set provider_id in the context first", "warn");
        return;
      }
      VUI.api({ path: "/api/providers/" + encodeURIComponent(providerId) + "/availability" }).then(function (r) {
        VUI.clear(tplTable);
        if (r.status === 200 && Array.isArray(r.json)) {
          tplStatus.set(r.json.length + " templates", "ok");
          tplTable.appendChild(
            ui.table(
              [
                { label: "id", key: "id" }, { label: "day_of_week", key: "day_of_week" }, { label: "start_time", key: "start_time" },
                { label: "end_time", key: "end_time" }, { label: "slot_duration_minutes", key: "slot_duration_minutes" },
                { label: "effective_from", key: "effective_from" }, { label: "is_active", key: "is_active" },
              ],
              r.json,
              [
                { label: "use", onClick: function (row) { VUI.ctx.set("template_id", row.id); } },
                { label: "Deactivate", onClick: function (row) { deactivate(providerId, row); } },
              ]
            )
          );
        } else {
          tplStatus.set("status " + r.status + " " + detailOf(r) + (r.status === 403 ? " (act as that provider or an admin)" : ""), "bad");
        }
      });
    }

    function deactivate(providerId, row) {
      var ok = g.confirm("Deactivate availability template " + row.id + " (day_of_week " + row.day_of_week + ", " + row.start_time + "-" + row.end_time + ")?");
      if (!ok) {
        return;
      }
      VUI.api({
        method: "POST",
        path: "/api/providers/" + encodeURIComponent(providerId) + "/availability/" + encodeURIComponent(row.id) + "/deactivate",
        expect: 200,
      }).then(function (r) {
        tplStatus.set(r.status === 200 ? "template deactivated; wait up to ~15 s, then list flagged appointments" : "status " + r.status + " " + detailOf(r), r.status === 200 ? "ok" : "bad");
        if (r.status === 200) {
          listTemplates();
        }
      });
    }

    body.appendChild(
      el("div", {},
        el("p", { "class": "hint", text: "Act as front_desk/admin for the list, and as the provider (own) or admin for templates. How-to: deactivate a template that covers a booked, confirmed slot, wait up to ~15 s, then list flagged appointments. The Temporal Schedule scheduling-schedule-change-poller is paused on the local Temporal and must be unpaused for live propagation (Temporal UI at http://localhost:8080 -> Schedules, or the temporal CLI 'schedule toggle' command; confirm the exact flags with its --help). Re-pause it afterwards for integration test runs." }),
        el("div", { "class": "sub-block" },
          el("h3", { text: "Flagged appointments" }),
          ui.row(ui.field("flagged", flaggedSelect), ui.field("status", statusInput), ui.field("provider_id", providerFilter), ui.button("List flagged appointments", listFlagged)),
          flagStatus.el,
          flagTable
        ),
        el("div", { "class": "sub-block" },
          el("h3", { text: "Availability templates" }),
          ui.row(ui.button("List availability templates", listTemplates)),
          tplStatus.el,
          tplTable
        )
      )
    );
  });
})(globalThis);
