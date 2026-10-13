# Design

## Context

See `proposal.md` for motivation. HEAD already has much of the substrate:

- Global `VaradhiTopic.produceConfigs` (`state`, `produceIdx`, `failOverRegion`) and ingress-correct `TopicResolver`
- Wire contract: `TransitionEvent` / `TransitionAck` (incl. `observedTopicVersion`) / `TransitionStage`
- Ephemeral `TransitionMaster` + durable `TopicFailoverOperation` in OpStore (flat ZK paths)
- REST create/get/abort skeleton (`TOPIC_UPDATE` / `TOPIC_GET`) and pod `ProduceTransitionMsgHandler`
- Producer registry via `ConcurrentHashMap` + `addOnInvalidate` (not Caffeine TTL)

What is missing for VIP-0003: peer-region Pulsar clients, real lag SPI, `TopicFailoverOpExecutor` + OpMgr enqueue/resume/ack barriers, lag knobs on bus/op, richer REST, `Retry-After`, `StorageTopic.regions`.

Code names (not VIP aliases): `TransitionMaster` (not `TopicTransition`), `TopicFailoverOpExecutor` (orchestrator).

## Goals / Non-Goals

**Goals:**

- Finish the orchestrated path on existing TransitionMaster + OpStore + OpMgr seams
- Document exact controller and REST deltas so implementers do not rebuild substrate
- Keep produce routing config-authoritative; events only pre-warm and await convergence

**Non-Goals:**

- Hierarchical ZK sharding; new `ResourceAction`; auto/bulk/external/subscription failover
- Implementing controller HA in this change (assumed available before production use)
- Fixing entity-event HOL blocking in this change (called out as prerequisite)

## Decisions

### D1 — Keep thin TransitionMaster + fat TopicFailoverOperation
**Choice:** Uniqueness lock stays on `TransitionMaster`; request knobs, stage history, overrides live on `TopicFailoverOperation`.  
**Why:** Matches OpMgr / subscription-op pattern; master deleted at terminal; audit survives.  
**Alt:** Fat single `TopicTransition` entity — rejected; would duplicate OpStore and fight existing code.

### D2 — Config-authoritative produce; events are probes
**Choice:** Pods route only from `TopicCache` `produceConfigs`. Events carry stage + version-to-await (+ PREPARE target).  
**Why:** Restarting pods correct by data; no barrier needed to unfence.  
**Alt:** Event-driven route flip — rejected (ordering/split risk).

### D3 — One commit at MIGRATE
**Choice:** FENCE writes only `state=Fenced`. MIGRATE writes `Producing` + `failOverRegion` (or clear FO on failback) together. Drain sits inside MIGRATE between fence and that write.  
**Why:** Abort is a single-field revert; route and reopen are atomic.

### D4 — Lag timeout defaults
**Choice:** Grouped default `onLagTimeout=ABORT`; ungrouped default `FAILOVER`. Caller may override (audited). Ungrouped skips drain.  
**Why:** Ordering bet for grouped; outage bet for ungrouped.

### D5 — PREPARE force is optional audited override
**Choice:** Default PREPARE ack timeout → abort. Optional force/skip proceeds (cold connect risk).  
**Why:** Incident escape hatch without changing the default safety check.

### D6 — Auth stays TOPIC_UPDATE / TOPIC_GET
**Choice:** No new action.  
**Why:** Same mutation class as topic update; less IAM churn.

### D7 — Flat ZK paths unchanged
```
/test-varadhi/entities/Transition/<topicFqn>
/test-varadhi/entities/TopicFailoverOperation/<opId>
```
(Base path becomes `/varadhi/entities` when OSS/internal Project align.) Hierarchical project sharding deferred.

### D8 — Controller ↔ producer interaction
Two channels only:

1. **Authority:** controller tracked writes to global topic → entity-event pipeline → TopicCache → `ProducerService` resolve
2. **Coordination:** `TransitionEvent` broadcast → `ProduceTransitionMsgHandler` → `TransitionAck` (PREPARE may call `ProducerService` to warm)

No direct controller→ProducerService RPC for routing.

---

## Controller layer — required changes

### C1. `TopicFailoverOpExecutor` (new)
Plug into `OperationMgr` like other ordered ops (`orderingKey = "TopicFailover_" + topicFqn`).

Per stage (state-first: advance `TransitionMaster` + persist op stage snapshot **before** side effects; idempotent re-entry):

| Stage | Controller work |
|---|---|
| PENDING | Validate E1–E4 (regions, FO presence for no-op, uniqueness already via master create); advance to PREPARE |
| PREPARE | Broadcast event with `Target.Region`; ack barrier (all server pods); on timeout → abort unless force; else → FENCE |
| FENCE | Tracked global topic write `state=Fenced` (vN+1); broadcast FENCE + `topicVersionToAwait=vN+1`; barrier (version + in-flight settle); timeout → abort + revert fence; else → MIGRATE |
| MIGRATE | Write master MIGRATE; **(1)** if grouped && waitLag: poll backlog until 0 / timeout / growing→abort (E10); **(2)** tracked write `Producing` + set/clear `failOverRegion` (vN+2); broadcast MIGRATE; barrier **non-fatal** (timeout → COMPLETED + alert); else COMPLETED |
| COMPLETED | Finalize op, delete `TransitionMaster` |
| ABORTED | Revert fence if needed; FO untouched; broadcast ABORTED; finalize op; delete master |

Post-commit MIGRATE: abort refused (already partly in `SubscriptionService.abortTopicFailover`).

### C2. Create path must enqueue
Today `createTopicFailover` only `createTopicFailoverOp` (comment: orchestrator follow-up). Change to:

1. Validate + create `TransitionMaster` (unchanged uniqueness)
2. Build op **including** `lagDrainTimeoutMs`, `onLagTimeout`, prepare-force / skipValidation flags
3. `operationMgr.createAndEnqueueTopicFailover(op)` so executor runs

### C3. Bus envelope must carry knobs
`FailoverApiRequest` today: `topicFqn`, source, target, `waitForReplicationLagToClear`, `requestedBy`.  
**Add:** `lagDrainTimeoutMs`, `onLagTimeout`, `skipValidation`, prepare-force flag.  
`ControllerHandler.createFailover` must stop reconstructing a 3-arg `TopicFailoverRequest` that drops fields. Persist those fields on `TopicFailoverOperation`.

### C4. Ack barriers (`recordFailoverAck`)
Replace log-only stub with stage awaiter:

- Match `(opId, stage)`; count success acks with `observedTopicVersion >= required` (or exact, matching pod handler)
- Track missing hosts for live GET
- PREPARE/FENCE: timeout → abort path; MIGRATE: timeout → complete + metric/alert
- Participation recorded at PREPARE; every server pod still must pass version gate at FENCE (VIP: NOT_INVOLVED skips warm only)

### C5. Restart resume
`ControllerVerticle.requeueInProgressFailovers` / `retryTopicFailover` today warn-only. Wire to re-enqueue pending ops:

- PENDING/PREPARE: re-execute or abort (no config written yet → prefer abort if stale)
- FENCE / pre-commit MIGRATE: idempotent re-execute (topic may already be Fenced)
- Post-commit MIGRATE: if `failOverRegion` already target → collect acks / COMPLETED; else write commit

### C6. Global topic writes + CAS
Use existing topic store update with version CAS; on conflict re-read and retry state fields; abort if still conflicting (E6). Failback commit clears FO when target == deployed region.

### C7. Lag polling
Call `StorageTopicService` backlog API (new). Growing backlog during drain → abort (E10). Unknown (admin down) ≠ 0; apply same timeout policy.

### C8. Abort / cleanup hooks
Keep revert-fence + ABORTED broadcast; ensure executor failures call `cleanupFailedTopicFailover`. Point-of-no-return after MIGRATE commit unchanged.

### C9. Metrics (controller-observed)
Implement VIP signals as needed for the goal: stage duration, outage duration (FENCE write → MIGRATE commit), active gauge, lag gauge, lag-timeout outcomes, unconverged pods (if event-completion data available).

### C10. Topic API guard (E5)
Reject topic update/delete while `transitionStore.exists(topicFqn)` (web or entity-service layer; controller-adjacent).

### Controller files (expected touch list)

| Area | Files / types |
|---|---|
| Executor | new `TopicFailoverOpExecutor` (+ stage awaiter helper) |
| Service | `SubscriptionService` create/enqueue, ack, retry, lag, topic writes |
| OpMgr | ensure enqueue + executor registration |
| Verticle | resume on leader/start; ack handler already routes |
| Handler | `ControllerHandler` pass-through of full request |
| Bus DTO | `FailoverApiRequest`, possibly live-status DTO |
| Entities | enrich `TopicFailoverOperation` / request records |
| Config | `OperationsConfig` ack timeout, defaults already partially present |

---

## REST / web layer — required changes

### R1. Existing routes (keep, fix behavior)

| Method | Path | Auth | Change |
|---|---|---|---|
| POST | `/v1/projects/:p/topics/:t/failover` | `TOPIC_UPDATE` | Body: `targetRegion`, optional `sourceRegion`, `lagDrainTimeoutMs`, `onLagTimeout`, `waitForReplicationLagToClear`, audited `skipValidation` / prepare-force. **202** + op (and transition id). Forward **full** body via `ControllerRemoteClient` → enriched `FailoverApiRequest`. |
| GET | `/v1/projects/:p/topics/:t/failover` | `TOPIC_GET` | Enrich response: stage, entered-at, unacked hosts, current backlog, remaining lag budget (compose master + op + barrier + lag poll). 404 if none. |
| POST | `/v1/projects/:p/topics/:t/failover/abort` | `TOPIC_UPDATE` | Keep; ensure 409 after commit. |

### R2. New routes

| Method | Path | Auth | Purpose |
|---|---|---|---|
| GET | `…/topics/:t/failover/preflight?targetRegion=` | `TOPIC_GET` | Dry run: target ∈ `StorageTopic.regions`, `Region.status`, backlog, participant count hint. No transition created. |
| GET | `…/topics/:t/failover/history?limit=` | `TOPIC_GET` | Past ops from OpStore by topic (master may be gone). |
| GET | `…/topics/:t/produce-config` | `TOPIC_GET` | This region's global `produceConfigs` entry + topic version. |
| GET | `/v1/failovers?state=` | admin-scoped (existing admin pattern) | Active/recent in **this** region; bus `listFailovers` already exists — expose over HTTP. |

### R3. Produce HTTP mapping
`ProduceHandlers`: `Fenced` already → 503; **add `Retry-After`** header (config or short fixed hint).

### R4. Validation at edge (cheap rejects)
Mirror E1–E3 where data is local (project/topic cache, regions); controller remains authority for uniqueness and orchestration. `skipValidation` audited on op.

### R5. Web files (expected touch list)

| Area | Files |
|---|---|
| Handlers | `TopicFailoverHandlers` (+ preflight/history/produce-config); optional small admin list handler |
| Wiring | `WebServerVerticle` route registration |
| Client | `ControllerRemoteClient` / `ControllerApi` method signatures for new reads |
| Produce | `ProduceHandlers` Retry-After |
| DTOs | request/response records in `entities` / `core` |
| Auth | reuse `TOPIC_UPDATE` / `TOPIC_GET`; admin list uses existing admin auth pattern |

---

## Produce / messaging changes (summary)

- `MessagingStackProvider` / `PulsarProducerFactory`: client **per region**; `newProducer(..., produceRegion)` selects client
- `StorageTopic.regions` + backlog query on `StorageTopicService` (unknown ≠ 0)
- Pod handler: settle in-flight on FENCE; keep PREPARE warm; ack observed version (mostly done)

## Risks / Trade-offs

| Risk | Mitigation |
|---|---|
| Controller crash while fenced (E13) | Controller HA before production; no fence TTL |
| Entity-event HOL blocks version gates | Prerequisite: independent per-pod event progress (§8 VIP); document as production gate |
| PREPARE force → cold fail after commit | Audited override; default abort |
| Grouped `onLagTimeout=FAILOVER` reorders | Default ABORT for grouped; audit overrides |
| Peer credentials / endpoints config | Static pod config first; Region-derived later (open) |

## Migration Plan

1. Deploy messaging multi-client + SPI lag (safe no-op until FO set)
2. Deploy controller executor + enriched bus/REST
3. Enable operator use per topic; failback clears FO
4. Roll back: abort in-flight if pre-commit; post-commit use failback. Feature is metadata-driven — removing executor leaves FO sticky until manual/failback path available

## Open Questions

1. Exact `Retry-After` value (fixed vs config) — default short fixed OK for v1
2. Peer endpoint config source (static vs `Region` entity) — static for v1
3. Numeric defaults for `ackTimeout` / default `lagDrainTimeoutMs` — measure in soak
4. OpStore retention/purge — shared with subscription ops; not failover-specific
