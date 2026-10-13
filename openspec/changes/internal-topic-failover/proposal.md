# Proposal

## Why

A degraded regional messaging stack today means a prolonged produce outage for every topic in that region; recovery is operator action outside Varadhi. Geo-replicated storage topics already exist in peer clusters — only the broker connection stops us from using them. We need metadata-driven internal failover so `varadhi-server` in the affected region can produce to a peer region's Pulsar without moving ingress or redeploying producers, trading a short bounded produce outage for recovery from a multi-minute outage.

## What Changes

- Complete **controller-owned orchestration** for topic produce failover (`PENDING → PREPARE → FENCE → MIGRATE → COMPLETED | ABORTED`), plugged into `operation-manager`, with ack barriers, restart resume, and durable per-attempt audit.
- Enable **cross-region produce**: peer `PulsarClient` pool so `ProduceKey.produceRegion` selects a real cluster; PREPARE pre-warm becomes a connectivity pre-check.
- Wire **replication backlog drain** (grouped topics) inside `MIGRATE` between fence and route commit; caller-chosen `lagDrainTimeoutMs` / `onLagTimeout`.
- Expand **REST** beyond create/get/abort: preflight, history, produce-config view, region-local active list; keep auth as `TOPIC_UPDATE` / `TOPIC_GET`.
- Persist lag knobs on the op and through the web→controller bus (today dropped).
- Map `Fenced` produce to **503 + `Retry-After`**; failback clears `failOverRegion` when `targetRegion` is the local region.
- Add `StorageTopic.regions` (or equivalent) as SSOT for target validation.
- **Out of scope:** external/DNS failover, subscription/RQ/DLQ failover, automatic failover, region-wide bulk, hierarchical ZK path sharding, new `ResourceAction`.

## Capabilities

### New Capabilities

- `topic-failover`: Operator-triggered, controller-orchestrated produce-path failover and failback for a topic in one region — state machine, ack barriers, lag drain, audit, and REST/control-plane APIs.
- `produce-cross-region`: Produce-path ability to open and use a producer against a peer region's messaging cluster selected by `failOverRegion`, including fencing semantics visible to clients.

### Modified Capabilities

- (none — no main specs exist yet)

## Impact

- **Controller:** new `TopicFailoverOpExecutor`, enqueue/resume, ack barriers, global `produceConfigs` writes, lag polling, abort/commit rules.
- **REST / web:** extend `TopicFailoverHandlers` (+ related DTOs); fix bus envelope; HTTP mapping for fenced produce.
- **Producer / Pulsar SPI:** multi-region client pool; backlog query; registry already mostly in place.
- **Entities / metastore:** enrich `TopicFailoverOperation` (+ bus request); optional `StorageTopic.regions`; keep flat ZK paths (`Transition/{fqn}`, `TopicFailoverOperation/{opId}`).
- **Ops:** controller HA is a production gate (no fence TTL); entity-event HOL blocking remains a known prerequisite called out in design.
