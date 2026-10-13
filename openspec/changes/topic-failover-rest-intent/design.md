# Design

## Context

This is what `#339` actually ships for REST. Orchestrator and operator-completeness are sibling stacks after this tip.

## Decisions

### D1 — Persist only
Create writes master + op and returns 202. No OpMgr enqueue in this change.

### D2 — Thin status
GET returns the durable op (or equivalent) for the active master; enriched barrier/lag view is `topic-failover-rest-api`.

### D3 — Knob honesty deferred
Only oncall-parity fields required for MVP. `lagDrainTimeoutMs` / `onLagTimeout` persistence is `topic-failover-rest-api`; honor in drain is `internal-topic-failover`.

### D4 — Auth
`TOPIC_UPDATE` mutate; `TOPIC_GET` read. No new ResourceAction.
