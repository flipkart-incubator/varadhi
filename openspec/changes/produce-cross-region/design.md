# Design

## Context

Master already has `ProduceTransitionMsgHandler` and wire acks. This change is the produce-path behavior for FO routing and fence visibility to clients.

## Decisions

### D1 — Config-authoritative routing
Pods route only from TopicCache `produceConfigs`. Transition events pre-warm / await; they do not flip routes.

### D2 — Fence → 503 here; Retry-After later
This stack returns 503. `Retry-After` header is owned by `topic-failover-rest-api`.

### D3 — Peer client pool
Single-region client may remain until orchestrator/PREPARE needs real peer open; track gap in `internal-topic-failover` tasks.
