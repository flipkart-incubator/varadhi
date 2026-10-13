# Design

## Context

See `proposal.md` for motivation. On `feature-topicFailoverControllerSide` today:

- REST trio exists: POST/GET/POST-abort with `TOPIC_UPDATE`/`TOPIC_GET`, create/abort return **202**
- `TopicFailoverRequest` already declares `lagDrainTimeoutMs` / `onLagTimeout`, but `FailoverApiRequest` + `ControllerRemoteClient` + `ControllerHandler` drop them; op does not store them; executor uses `TopicFailoverConfig.drainTimeoutMs()` only
- `ControllerApi.getActiveFailovers()` exists on the bus; no HTTP route
- Live GET returns raw `TopicFailoverOperation` only
- `Fenced` → 503 without `Retry-After`
- Broader VIP work lives in sibling change `internal-topic-failover`

## Goals / Non-Goals

**Goals:**

- Make the public HTTP contract complete and truthful for operators
- Minimal controller/bus/op changes so request knobs and live status are real
- Reuse existing OpStore ops for history; reuse bus list for `/v1/failovers`

**Non-Goals:**

- Peer Pulsar clients or fixing lag SPI to real Pulsar metrics (report **unknown** when stub/unreachable)
- Changing stage machine (PREPARE/FENCE/MIGRATE) or OpMgr wiring beyond honoring knobs / composing status
- New `ResourceAction`; hierarchical ZK paths

## Decisions

### D1 — Extend bus envelope rather than dual paths
**Choice:** Add lag knobs + audited overrides to `FailoverApiRequest` and persist on `TopicFailoverOperation`.  
**Why:** One create path; resume/audit see the same knobs.  
**Alt:** Web-only validation — rejected; executor would still ignore caller budget.

### D2 — Live status as a composed response DTO
**Choice:** New response type (e.g. `TopicFailoverStatusView`) joining master + op + `StageAwaiter` missing hosts + optional lag poll. Keep op record as the create/abort body.  
**Why:** Avoid overloading the durable op with ephemeral barrier state.  
**Alt:** Stuff everything onto `TopicFailoverOperation` — rejected; pollutes OpStore.

### D3 — History from OpStore by topicFqn
**Choice:** Add OpStore/list or scan helper filtered by `topicFqn` + limit; no new ZK index in v1 if scan of pending/all ops is acceptable at current scale. Prefer a cheap filter API on `OpStore` if listing all children is already how pending ops work.  
**Why:** Ops already retained after master delete.  
**Alt:** Separate history znodes — deferred.

### D4 — Preflight is web+controller read, no write
**Choice:** New controller RPC (or local composition on web if all data is in caches) that validates target and returns backlog via `StorageTopicService.getReplicationLag` (0/unknown semantics as implemented). Does not create master/op.  
**Why:** Matches VIP dry-run. Prefer controller RPC for lag + membership consistency with executor.

### D5 — Produce-config from topic cache on web
**Choice:** Serve from `ResourceReadCache` topic entry for deployed region + version; no controller hop.  
**Why:** Global SSOT already on topic; lowest latency.  
**Alt:** Always proxy to controller — unnecessary.

### D6 — `/v1/failovers` wraps existing bus list
**Choice:** Thin admin handler → `getActiveFailovers()`. Optional `?state=` filter in-process.  
**Why:** Bus API already exists.

### D7 — Auth unchanged
**Choice:** Topic routes: `TOPIC_UPDATE` / `TOPIC_GET`. List: existing admin auth pattern used by other `/v1/admin`-style or region admin routes in this codebase.  
**Why:** Already decided for VIP; no new action.

### D8 — Retry-After fixed short default
**Choice:** Constant or config knob (e.g. 1–5s) on fenced produce responses.  
**Why:** Open question in parent design; fixed is enough for v1.

### D9 — onLagTimeout defaults when null
**Choice:** Null → grouped ABORT / ungrouped FAILOVER (parent VIP decision); executor reads from op after this change persists the field.  
**Why:** REST change must not leave knobs half-wired.

## Route map

| Method | Path | Auth | Notes |
|---|---|---|---|
| POST | `/v1/projects/:p/topics/:t/failover` | UPDATE | Full body; 202 + op |
| GET | `/v1/projects/:p/topics/:t/failover` | GET | Enriched status view; 404 |
| POST | `/v1/projects/:p/topics/:t/failover/abort` | UPDATE | 202 / 409 |
| GET | `…/failover/preflight?targetRegion=` | GET | Dry run |
| GET | `…/failover/history?limit=` | GET | Past ops |
| GET | `…/produce-config` | GET | Local region entry |
| GET | `/v1/failovers?state=` | admin | Active list |

## Implementation touch list

| Layer | Changes |
|---|---|
| entities | Persist knobs/overrides on `TopicFailoverOperation`; status/preflight/history DTOs; optional request fields `skipValidation` / prepare-force |
| core | Expand `FailoverApiRequest`; `ControllerApi` + remote client methods; stop dropping fields |
| controller | Handler pass-through; create persists knobs; executor uses op budget + onLagTimeout; status/preflight RPCs; OpStore list-by-topic |
| web | Extend `TopicFailoverHandlers`; admin failovers handler; `ProduceHandlers` Retry-After; wire routes |

## Risks / Trade-offs

| Risk | Mitigation |
|---|---|
| Preflight backlog always 0 with stub SPI | Document unknown/zero; do not block REST on Pulsar metrics PR |
| OpStore history scan cost | Cap `limit`; same latent cost as pending-op list today |
| Dual OpenSpec changes overlapping | This change owns HTTP + bus knobs; parent change should defer REST tasks or mark them done here |

## Migration Plan

1. Deploy DTO/bus/op field additions (backward compatible: missing fields → defaults)
2. Deploy enriched GET + new routes
3. Deploy Retry-After
4. Rollback: new routes unused; old clients ignore new response fields

## Open Questions

1. Exact admin auth pattern for `/v1/failovers` (match an existing admin list handler in-tree)
2. Whether prepare-force / skipValidation ship in this REST PR or wait for a follow-up (default: include in body + op audit fields, wire force in executor if already trivial)
