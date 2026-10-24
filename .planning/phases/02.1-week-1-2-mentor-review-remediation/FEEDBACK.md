# SmartHealth: Week 1 + Week 2 Feedback

**Overall: 🟡 Good progress, a few gaps to close before Week 3.** Week 1 is complete. The Week 2 booking saga is well designed, but two Week 2 deliverables aren't fully met yet.

## What's working well
- **The database settles every race.** Partial unique indexes, exclusion constraints and conditional updates prevent double-booking, overlapping patient appointments and duplicate visits. This is the right approach.
- **The saga is disciplined.** Compensations are registered before each step and are idempotent. Failure reasons are machine-readable, and changes to workflow code go through `workflow.patched` with replay tests.
- **Idempotency is layered:** the Idempotency-Key with a request hash, deterministic workflow IDs, and idempotent Billing calls.
- **Documentation is strong.** The architecture doc, ERD, decision log and an honest "known limitations" section are all there.

## Must fix (Week 2 deliverables)
1. **The appointment state machine is incomplete.** An appointment can be cancelled or rescheduled after the patient has checked in, or even after the visit is completed. When that happens, the slot is rebooked and a refund is issued. Add `checked_in`, `completed` and `no_show` states in Scheduling, have check-in move the appointment into `checked_in`, and limit check-in to a time window around the slot.
2. **Provider schedule changes don't propagate.** Editing or deactivating a template, or deactivating a provider, leaves future slots bookable. Add a schedule-change workflow that withdraws those slots and flags the affected bookings.

## Should fix (reliability)
3. **Failures after commit.** If the audit write fails after `confirm` commits, the saga compensates and voids billing on a *confirmed* booking. Make compensations check the appointment's current state, and make audit failures non-fatal (or move them to an outbox).
4. **Stuck states.** Some failure paths leave states that nothing ever cleans up:
   - `pending` appointments whose workflow died, which block the slot forever;
   - waitlist entries stuck in `matched`;
   - visit billing that is "skipped" and never retried.

   Add a reconciler, and keep retrying billing instead of skipping it.
5. **Waitlist.** A freed slot isn't offered to the waitlist again if a direct booker grabs it and then fails.
6. **Validation.** Slots in the past can be booked.
7. **Notification order.** The "booking confirmed" notification is requested before `confirm`, which can still fail. The README currently says the opposite, so update it too.

## Carry-over from Week 1
- `/refresh` doesn't check the denylist or rotate the refresh token, so tokens still work after logout.
- bcrypt runs on the event loop and will stall logins under load.
- Email uniqueness is case-sensitive.
- The provider role can't view its own appointments, and patients can't view their own profile.
- Patient, provider and clinic lists aren't paginated.

## Missing deliverables
- **No quantified non-functional requirements in the PRD** (target throughput, latency, availability) and no load test. Please add both. Week 3's observability work is the natural place to measure them.
