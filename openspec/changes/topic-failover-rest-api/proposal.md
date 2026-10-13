# Proposal

## Why

Topic failover already has create/get/abort HTTP routes and a working controller executor, but the operator API is incomplete and partly dishonest: lag-budget fields on the request never reach the controller, live status omits barrier/backlog detail, and VIP-required operator endpoints (preflight, history, produce-config, region list) and fenced-produce `Retry-After` are missing. Operators cannot size or observe a failover the way the design requires.

## What Changes

- Plumb **full failover request knobs** (`lagDrainTimeoutMs`, `onLagTimeout`, and audited overrides) through web → bus → op so POST body fields are honored.
- Enrich **GET live failover** with stage, timing, unacked hosts, backlog (or unknown), and remaining lag budget.
- Add **GET preflight**, **GET history**, **GET produce-config**, and **GET `/v1/failovers`**.
- Add **`Retry-After`** on fenced produce (503).
- Keep auth as **`TOPIC_UPDATE` / `TOPIC_GET`**; admin scope for region-wide list.
- **Out of scope:** peer Pulsar clients, implementing real Pulsar lag metrics (preflight/live may report unknown until that lands), orchestrator stage-machine redesign, hierarchical ZK paths.

Sibling change `internal-topic-failover` owns the broader VIP; this change scopes the **REST / operator HTTP** slice (plus the minimum bus/op fields REST needs to be truthful).

## Capabilities

### New Capabilities

- `topic-failover-api`: Region-local HTTP APIs for requesting, observing, aborting, prefighting, and auditing topic produce failover, plus client-visible fenced-produce retry hints.

### Modified Capabilities

- (none — no main specs archived yet)

## Impact

- **web:** `TopicFailoverHandlers` (+ new routes), optional admin list handler, `ProduceHandlers`, `WebServerVerticle` wiring
- **core:** `FailoverApiRequest`, `ControllerRemoteClient` / `ControllerApi` method signatures and response DTOs
- **controller:** pass-through of knobs; persist on `TopicFailoverOperation`; compose live status (barrier + lag poll); honor request lag budget in executor drain
- **entities:** request/response records; op fields for knobs/overrides
