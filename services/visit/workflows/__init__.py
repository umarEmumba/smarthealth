"""Visit's Temporal workflows (sandbox-safe: temporalio, stdlib and the pure
`domain.contracts` / `libs.temporal_policies` only): `visit_completed`
(`VisitCompletedWorkflow`, the downstream fan-out, started by the visit-downstream
consumer on the `visit.completed` outbox event), `reconciler`
(`VisitReconcilerWorkflow`) and `sweeper` (`VisitDownstreamSweeperWorkflow`).

The sweeper workflow is a RETIRED tombstone (plan 03-12, D-03): the Schedule that
started it is deleted by the visit worker at every start, and the class (with its
`visit.sweep_unstarted` activity, a no-op) stays registered only so that histories
already recorded, and any run in flight at the cutover, still replay and finish. It is
safe to remove once the `list_workflows` query recorded in `workflows/sweeper.py`
(`WorkflowType='VisitDownstreamSweeperWorkflow'`) returns nothing on every
environment."""
