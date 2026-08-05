# Varadhi Topic Model: Current State & Global Topics Proposal

## Overview

This document describes the **current** topic model and **proposed** follow-on work for full Global Topics with multi-zone replication and failover. Shipped infrastructure (`RegionName`, region admin APIs, `MemberInfo.region`) is documented separately from topic-entity fields that are not yet wired.

---

## Current Topic Model

### Class hierarchy

```
AbstractTopic (interface)
    │
    ├── StorageTopic (abstract) — id, name; PulsarStorageTopic adds partitionCount
    │
    └── VaradhiTopic extends LifecycleEntity
            ├── SegmentedStorageTopic segmentedStorageTopic   // shared across regions
            ├── Map<RegionName, ProduceConfig> produceConfigs // per-region produce policy (SSOT)
            ├── boolean autoFailover
            └── capacity, grouped, topicCategory, rateLimiterMode, …
```

### VaradhiTopic

**Inherited:** `name`, `version`, `status` (from `LifecycleEntity`).

**Core fields:**
- `segmentedStorageTopic` — single shared storage segment for the topic (not a per-region map).
- `produceConfigs` — `Map<RegionName, ProduceConfig>`; wire JSON key `produceConfigs`. Region membership here gates produce access.
- `autoFailover` — controller may auto-failover on region degradation.
- `grouped`, `capacity`, `nfrFilterName`, `topicCategory`, `perRegionQuotaWeights`, `messageSizeProfile`, `rateLimiterMode`.

**Immutable updates:** overloaded `with(...)` copies (storage, per-region config, `autoFailover`).

**Removed from entity surface:** per-region `internalTopics` map, `getSegmentedStorage(region)` (tests use `VaradhiTopicTestUtils`).

**Topic creation today:** `VaradhiTopicFactory` → `TopicResource.toVaradhiTopic(...)` seeds a single entry in `produceConfigs` for the deployment region (`ProduceConfig.producing()`). Multi-region topic creation is follow-up work.

### ProduceConfig (per region)

| Field | Role |
|-------|------|
| `state` (`TopicState`) | `Producing`, `Blocked`, `Fenced` — whether this region accepts produce |
| `produceIdx` | Storage segment id for this region's produce path (partition growth / migration) |
| `failOverRegion` | Optional; when set, produce from this region routes to that region's config |

Factories: `ProduceConfig.producing()`, `ProduceConfig.blocked()`.

Failover is updated via immutable copies — e.g. `topic.with(region, new ProduceConfig(..., failOverRegion))` — not a mutable map on `VaradhiTopic`.

### TopicState and ProduceStatus

| `TopicState` | `isProduceAllowed()` | Client `ProduceStatus` |
|--------------|----------------------|------------------------|
| `Producing` | yes | `Success` (from broker path) |
| `Fenced` | no | `Fenced` — retry after transition |
| `Blocked` | no | `NotAllowed` |

Multiple regions may be `Producing` at once on a global topic. `ProducerService` uses gated resolve and maps non-producing states via `ProduceResult`.

### Produce routing

- **`ProduceKey`** — `(topicFqn, produceRegion, storageTopicId)`; producer cache key after resolve.
- **`TopicResolver`** — resolves `ProduceKey` from `VaradhiTopic` + deployed region.
- **Ungated** `resolve(topic, region)` — cache warm / PREPARE while source is `Fenced`. Does not check `TopicState.isProduceAllowed()`.
- **Gated** `resolve(topic, region, true)` — HTTP produce; empty when source `Blocked`/`Fenced`, region missing from `produceConfigs`, or failover target config missing. Gating applies to the **source** region's `ProduceConfig` only (not the failover target's state).
- **PREPARE / storage migration** — warm an explicit segment id from `TransitionEvent.Target.StorageTopic.storageTopicId`; ungated resolve alone is not sufficient.
- **`ProducerService`** — uses gated resolve; `ResourceNotFoundException` when deployed region not in `produceConfigs`.

### SegmentedStorageTopic

- `storageTopics[]` — backing segments.
- `getTopic(int id)` — public lookup by `StorageTopic.id` (used after resolve). External callers must not use array-index lookup; `produceIdx` is a segment id, not necessarily an array slot.
- Active slot per region comes from `ProduceConfig.produceIdx` on the resolved produce region.

### Wire serialization

- JSON key is `produceConfigs` (not `regionConfigs` or legacy `internalTopics`).
- Round-trip covered by `VaradhiTopicSerializationTest`.
- Legacy metastore JSON migration (old `internalTopics` shape → `produceConfigs`) is **TBD** if not yet implemented in deserializer.

### Transition wire types (entities only)

Under `entities/.../cluster/failover/`: `TransitionEvent`, `TransitionAck`, `TransitionStage`, `TransitionType`, participation enums. Pod/controller wiring is follow-up work

---

## Implemented Infrastructure (shipped; not all wired to topics)

These types exist in the codebase and support multi-region operations. They are **not** the same as full Global Topic entity support below.

### RegionName

Record value object (`entities/.../RegionName.java`):
- `RegionName.of(String value)` — validated, non-blank
- `value()` / `@JsonValue` — string form
- `BOOTSTRAP_REGION` — accepted during cluster bootstrap before metastore has regions

Used in `produceConfigs`, `ProduceKey`, `MemberInfo`, and region APIs.

### Region and RegionStatus

**`Region`** extends `MetaStoreEntity` (name, version, entity type). Status is immutable on an instance; use `withStatus(RegionStatus)` for updates (e.g. `PATCH /v1/regions/:region`).

- `Region.of(RegionName, RegionStatus)` — factory
- `getRegionName()` — type-safe `RegionName` view
- `isProduceAvailable()`, `isConsumeAvailable()`, `isMessageStackAvailable()`, `isAvailable()` — delegate to `RegionStatus`

**`RegionStatus`:** `AVAILABLE`, `UNAVAILABLE`, `PRODUCE_UNAVAILABLE`, `CONSUME_UNAVAILABLE`, `MSP_UNAVAILABLE` (messaging stack down). Same availability helpers as on `Region`.

### TopicTag

Enum: `PROD`, `NON_PROD`, `HIGH_PRIORITY`. **Not yet** a field on `VaradhiTopic`.

### MemberInfo

`core/.../cluster/MemberInfo` record includes `RegionName region` for multi-region topology, routing, and failover decisions.

### Region admin APIs

`web/.../RegionHandlers` (`RegionService`):

| API | Description |
|-----|-------------|
| `GET /v1/regions` | List all regions |
| `GET /v1/regions/:region` | Get one region (400 invalid name, 404 unknown) |
| `POST /v1/regions` | Create — body `RegionCreateRequest`: `{ "name", "status" }` |
| `PATCH /v1/regions/:region` | Update status — body `RegionStatusUpdateRequest`: `{ "status" }` |
| `DELETE /v1/regions/:region` | Delete region |

Write bodies: `RegionCreateRequest` / `RegionStatusUpdateRequest`. Persisted entity: `Region`. Administrative use only — not produce/consume traffic.

---

## Proposed Global Topics Support (topic entity — not yet fully implemented)

### New fields on VaradhiTopic

```java
private final Set<TopicTag> tags;   // PROD, NON_PROD, HIGH_PRIORITY — enum exists; field TBD
```

### Project.replicationRegions

**Proposed:** mandatory `Set<RegionName>` on `Project`:
- Default replication regions for all topics under the project.
- Topics created without explicit regions inherit from the project.
- At least one region; validated in constructor.
- `TopicResource.toVaradhiTopic(Project)` would use `project.getReplicationRegions()`.

**Not implemented** — `Project` has no `replicationRegions` today; factory uses deployment region only.

### Explicit replication vs produce regions

**Proposed** first-class sets on topic or create API:
- `replicationRegions` — where the topic is replicated.
- `produceRegions` — subset allowed to accept produce; defaults to all replication regions.

**Current model:** region membership and produce policy live in `produceConfigs` only. A region not in the map cannot produce. No separate replication set on the entity.

### Convenience query methods (proposed)

```java
boolean canProduceInRegion(RegionName regionName)
boolean isReplicatedInRegion(RegionName regionName)
boolean isGlobalTopic()              // multiple regions in produceConfigs / replication set
boolean isLocalTopic()
boolean supportsFailover()           // multi-region + autoFailover
boolean supportsMessageFailureFailover()
boolean supportsTopicFailureFailover()
String getOrderingSemantics()        // "Mostly Ordered" if grouped, else "Unordered"
```

None of these exist on `VaradhiTopic` today. Equivalent checks use `produceConfigs` + `TopicResolver` + `grouped`.

### Proposed factory signatures (future)

Convenience factories taking `Set<RegionName> replicationRegions` / `produceRegions` may wrap building `produceConfigs` and `segmentedStorageTopic`. Current factory:

```java
VaradhiTopic.of(project, name, grouped, capacity, actionCode, nfrStrategy,
                topicCategory, perRegionQuotaWeights, messageSizeProfile, rateLimiterMode,
                segmentedStorageTopic, autoFailover, produceConfigs)
```

---

## Design Decisions

### 1. Type-safe region names (shipped)
`RegionName` in `produceConfigs` and routing. Prevents invalid region strings at API and entity boundaries.

### 2. produceConfigs as SSOT (shipped)
Per-region `TopicState`, `produceIdx`, and `failOverRegion` in one map. Replaces per-region `internalTopics` and implicit region strings.

### 3. Explicit replication sets (proposed)
Separate `replicationRegions` / `produceRegions` for clarity at create time; must stay consistent with `produceConfigs` when implemented.

### 4. Failover (partially shipped)
- **Shipped:** `autoFailover` on topic; per-region `failOverRegion` in `ProduceConfig`; `TopicResolver` follows failover to target region's `produceIdx`.
- **Proposed:** controller-driven updates during region/topic failure; integration with `RegionStatus` / transition protocol.

### 5. Topic classification (proposed)
`TopicTag` on `VaradhiTopic` for capacity planning and operational policy.

### 6. Ordering
`grouped` only — no separate `ordered` field. Grouped ⇒ mostly ordered semantics; ungrouped ⇒ unordered.

---

## Migration Path

### Backward compatibility

1. **Wire shape** — new topics serialize `produceConfigs`. Legacy `internalTopics` JSON in metastore requires a one-time migration or deserializer backfill (**TBD**).
2. **Single-region default** — `VaradhiTopicFactory` continues to create one `produceConfigs` entry until multi-region creation and `Project.replicationRegions` land.
3. **`grouped`** — unchanged; ordering semantics unchanged.

### Migration steps (planned)

1. **Existing topics** — backfill `produceConfigs` from legacy storage layout; set replication regions from project defaults when `Project.replicationRegions` exists.
2. **New topics** — accept region sets on create API; build `produceConfigs` from replication/produce region policy.
3. **Controller** — use `Region` / `RegionStatus` and transition types to update `ProduceConfig` state and `failOverRegion` during failover.

---

## Example Usage

### Current: multi-region topic with failover (tests / manual construction)

```java
Map<RegionName, ProduceConfig> configs = Map.of(
    RegionName.of("CH"), ProduceConfig.producing(),
    RegionName.of("HYD"), new ProduceConfig(TopicState.Blocked, 0, RegionName.of("CH")),
    RegionName.of("SIN"), ProduceConfig.producing()
);

VaradhiTopic topic = VaradhiTopic.of(
    "project1", "topic1",
    false,
    new TopicCapacityPolicy(100, 1000, 2),
    LifecycleStatus.ActionCode.USER_ACTION,
    null,
    VaradhiTopic.TopicCategory.TOPIC,
    null, null, null,
    new SegmentedStorageTopic(new StorageTopic[] { /* ... */ }),
    true,   // autoFailover
    configs
);

// Produce routing (HTTP path)
Optional<ProduceKey> key = TopicResolver.resolve(topic, RegionName.of("HYD"), true);
```

### Current: topic creation via factory (single deployment region)

```java
// VaradhiTopicFactory.planDeployment — one region today
Map.of(RegionName.of(deploymentRegion), ProduceConfig.producing())
```

### Proposed: global topic create API (future)

```java
Set<RegionName> replicationRegions = Set.of(RegionName.of("HYD"), RegionName.of("CH2"));
Set<RegionName> produceRegions = Set.of(RegionName.of("HYD"));
Set<TopicTag> tags = Set.of(TopicTag.PROD, TopicTag.HIGH_PRIORITY);
// Factory TBD — would build produceConfigs + storage from project + region sets
```

---

## Summary

| Aspect | Current | Proposed |
|--------|---------|----------|
| **Region IDs** | `RegionName` in `produceConfigs` | Same + project/topic region sets |
| **Region admin** | `Region`, `/v1/regions`, `MemberInfo.region` | Wired into failover controller |
| **Replication** | Implicit via `produceConfigs` keys | Explicit `replicationRegions` on project/topic |
| **Produce policy** | `produceConfigs` + `TopicState` | Optional `produceRegions` subset at create |
| **Failover** | `failOverRegion` + `autoFailover` + `TopicResolver` | Controller updates + transition protocol |
| **Classification** | None on topic | `Set<TopicTag>` on `VaradhiTopic` |
| **Ordering** | `grouped` only | Same |
| **Topic helpers** | `TopicResolver`, `getProduceConfig` | `isGlobalTopic()`, `canProduceInRegion()`, etc. |

---

## Next Steps

1. `Project.replicationRegions` and multi-region topic creation API.
2. `TopicTag` (and optional query helpers) on `VaradhiTopic`.
3. Controller + pod wiring for transition protocol and automatic failover.
4. Tests for end-to-end failover across regions.
