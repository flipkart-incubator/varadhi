# TOPIC FAILOVER (PRODUCE PATH) — MASTER DOCUMENT

## HOW TO PASTE INTO GOOGLE DOCS

1. Select all in this file, copy.
2. Paste into Google Docs. Lines starting with `#`, `##`, or `###` are **Markdown headings** — some tools convert them automatically; in Docs you can use **Extensions** (e.g. “Markdown”) or manually apply **Normal text → Heading 1 / 2 / 3** to those lines.
3. Rule of thumb: `#` = document title, `##` = major section, `###` = subsection.
4. Tables: if layout breaks, insert a table in Docs and paste cells, or paste the table block alone.
5. There are **no Mermaid diagrams** here (poor paste support). Flows use numbered lists.
6. Full design with diagrams: `varadhi/docs/topic-failover.md`.

---

## DOCUMENT CONTROL

| Field | Value |
|-------|--------|
| **STATUS** | Draft — design + grooming + LLD in one file |
| **SCOPE** | OSS Varadhi — topic failover, **produce path only**. **Approach 1 (manual) only for current delivery** — **automated / controller-driven failover (Approach 2) is deferred.** Phase 3 consumer follow-the-producer is **out of scope**. |
| **BUILDS ON** | PR #307 — Adding region support (`flipkart-incubator/varadhi`). Names follow that PR. |
| **AUDIENCE** | Engineers, TPMs, operators |

---

## A — PROBLEM AND GOALS

### A.1 Problem

A topic can span regions logically, but OSS today assumes one deployment region per topic at create time. There is no first-class way to:

1. Ask which region is authoritative for producing to topic **X**.
2. Move that authority to another region (planned or outage).
3. Have producers pick up the change without redeploy.

### A.2 Goals

- Persist authoritative produce regions on the topic (`produceRegions`; standby `failoverRegion`).
- Admin APIs to change them safely (**manual path — current delivery**).
- **Deferred:** automation when `Region.status` drives flips without an operator (Approach 2 — **not** in current backlog).
- Producer JVMs must refresh routing and broker clients after metadata changes (see **Section K — LLD**).

---

## B — ONCALL VS OSS (PRODUCE SLICE)

| Concern | Oncall (internal) | OSS target |
|---------|-------------------|------------|
| Topic / authority | `activeProduceZone` | `produceRegions` (+ `failoverRegion`) |
| Metadata | `GlobalTopic` (ZK) | `VaradhiTopic` in metastore |
| Manual flip API | `PUT …/activeProduceZone` | `PUT …/produceRegions`, `POST …/failover`, etc. |
| Auto vs manual | `FailoverType` enum | `autoFailover` boolean |
| Producer cache | Zone-aware `ProducerKey` | Today: `(topicFQN, storageTopicId)` only — **add region + invalidation** |

---

## C — CURRENT OSS (SHORT)

- **VaradhiTopic:** `internalTopics` map per region; `getProduceTopicForRegion(region)` exists.
- **VaradhiTopicFactory:** one internal topic for `deploymentRegion` only (TODO for full regional policy).
- **ProducerService:** `produceRegion` fixed per JVM; `produceToValidTopic` uses `topic.getProduceTopicForRegion(produceRegion)`.
- **ProducerCacheKey:** `(varadhiTopicFQN, storageTopicId)` — region implicit in JVM.
- **TopicHandlers:** GET / CREATE / DELETE / LIST / RESTORE — **no** failover routes yet.
- **Controller:** `ResourceEventProcessor` fans out events; `DefaultMetaStoreChangeListener` maps TOPIC → UPSERT/INVALIDATE. PR #307 may add a REGION hook — useful later for **deferred** Approach 2; **not required** to ship manual failover.

### C.1 Key OSS paths (implementers)

- `producer/.../ProducerService.java`
- `core/.../ResourceReadCache.java`
- `controller/.../DefaultMetaStoreChangeListener.java`
- `controller/.../events/ResourceEventProcessor.java`
- `server/.../VaradhiApplication.java` (dispatcher binds caches)

---

## D — PR #307 CONTRACT (SUMMARY)

- **Region entity:** `name` + `RegionStatus` (`AVAILABLE`, `UNAVAILABLE`, `PRODUCE_UNAVAILABLE`, `CONSUME_UNAVAILABLE`, `MSP_UNAVAILABLE`). Health is **data** in metastore, not an in-Varadhi probe.
- **VaradhiTopic (proposed):** `replicationRegions`, `produceRegions`, `autoFailover`, `failoverRegion`, `tags`.
- **Project (proposed):** `replicationRegions` bounds what topics may use.

**Semantics:**

- `produceRegions` is a **Set** → single-active and multi-active; multi-active “failover” = remove bad region from set.
- `failoverRegion` = standby when produce set would become empty.
- `autoFailover` exists on the model for forward compatibility; **defaults false** and **no controller job runs** until Approach 2 is implemented (deferred).

---

## E — CAPABILITY MATRIX

| Capability | OSS today | After PR #307 | Still needed for failover |
|------------|-----------|-----------------|---------------------------|
| `internalTopics` multi-region map | Yes | Same | Populate all replicas |
| Region entity + status API | No | Yes | Validate / trigger |
| Topic `produceRegions` / `failoverRegion` | No | Proposed | Runtime authority |
| Producer per-topic region + cache key | No | No | **Yes** |
| Failover REST | No | No | **Yes** |
| Controller auto job | No | Hook (optional) | **Deferred** (Approach 2) |
| Consumer follow-producer | No | No | Phase 3 — out |

---

## F — APPROACH 1: MANUAL PRODUCER FAILOVER

### F.1 Goal

Operator (or script) calls REST; Varadhi validates, persists `produceRegions`, propagates so **all** producer nodes refresh. No in-Varadhi automation. Consumers unchanged.

### F.2 Service (conceptual)

**`setProduceRegions(topicFqn, newSet, skipValidation)`**

- Validate non-empty; subset of `replicationRegions`; regions known and produce-capable (`RegionService`); optional replication pre-flight unless `skipValidation`.
- `topicStore.update`; publish topic change (existing event path).

**`setFailoverRegion` / `clearFailoverRegion`:** same style validations for standby.

**Idempotent:** same `produceRegions` → 200 no-op.

### F.3 REST (planned)

- `PUT  /v1/projects/:project/topics/:topic/produceRegions` — body: `produceRegions`, `skipValidation`
- `PUT  /v1/projects/:project/topics/:topic/failoverRegion/:region`
- `DELETE /v1/projects/:project/topics/:topic/failoverRegion`
- `POST /v1/projects/:project/topics/:topic/failover` — body: `to`, `skipValidation` (convenience, single-active)

**Auth:** `TOPIC_UPDATE` (admin-gated like restore).

### F.4 `pickProduceRegion` (default policy)

- If JVM local region **∈** `produceRegions` → use local.
- Else if `crossRegionProduce` enabled → pick from `produceRegions` (prefer `failoverRegion`, else stable hash by FQN).
- Else → `ProducerNotAvailableException` (client/LB should use another Varadhi region). **Default: local-only.**

### F.5 Failure modes (Approach 1)

| Condition | Result |
|-----------|--------|
| Region unknown in ZK | 400 |
| Region not produce-capable | 400 unless `skipValidation` |
| Region not in topic `replicationRegions` | 400 |
| Same `produceRegions` | 200 no-op |
| Metastore write fails | 5xx; no event |
| Event not delivered to a node | Stale until TTL or next UPSERT — see Section K |
| Local-only + local ∉ `produceRegions` | 5xx; no silent wrong broker |

### F.6 Work breakdown (Approach 1)

1. Entities: `VaradhiTopic` fields + migration (empty `produceRegions` → `deploymentRegion`).
2. `TopicResource` / handlers: create-update accepts new fields.
3. `VaradhiTopicFactory`: one storage topic per replication region; seed `produceRegions`.
4. `VaradhiTopicService`: `setProduceRegions`, `setFailoverRegion`, clear, convenience failover.
5. SPI optional: `getReplicationLag`; Pulsar impl when ready.
6. Web: routes above.
7. Producer: per-topic resolution; `ProducerCacheKey` includes region; invalidate on TOPIC event.
8. `ProducerOptions`: `crossRegionProduce.enabled` default **false**.
9. Tests + operator runbook.

**Effort (indicative):** ~1.5–2 dev-weeks after PR #307; +~1 week if #307 not landed.

---

## G — APPROACH 2: CONTROLLER AUTOMATIC FAILOVER (**DEFERRED**)

**Not in current delivery.** Implement **Approach 1** first. Keep this section as the agreed future shape when automation is prioritized.

### G.1 Goal

For topics with `autoFailover=true`: on `Region` upsert where region is not produce-capable, **leader** controller evicts that region from each topic’s `produceRegions`; if empty, add `failoverRegion`. Same `setProduceRegions` as manual. Metrics + audit + cooldowns + rate limit + ZK freeze kill-switch.

### G.2 Eviction (conceptual)

Index topics by “currently producing into region R” (`TopicByRegionIndex`). Per candidate: cooldowns, `maxTopicsPerMinute`, `HIGH_PRIORITY` first. **All-dark** guard if no valid failover.

### G.3 Controller config (YAML shape)

```yaml
controller:
  failover:
    enabled: true
    perRegionCooldownSec: 300
    perTopicCooldownSec: 600
    maxTopicsPerMinute: 50
    failback:
      enabled: false
      perRegionFailbackCooldownSec: 1800
    priority:
      tagOrder: [HIGH_PRIORITY]
```

### G.4 Work breakdown (Approach 2)

`AutomaticFailoverJob` on REGION listener; `TopicByRegionIndex`; freeze node; `ControllerConfiguration`; Region status update API if missing; metrics; audit; tests + e2e.

**Effort (indicative):** ~2 dev-weeks on top of Approach 1.

### G.5 Approach 2 failure modes (summary)

- Wrong status flaps → cooldowns + rate limit.
- True outage but status still `AVAILABLE` → manual API + monitoring.
- All regions bad → all-dark; no empty set.
- Leader loss → idempotent per-topic writes; resume on next event.

---

## H — COMPARISON AND ROLLOUT

### H.1 Approach 1 vs 2

| Dimension | Approach 1 | Approach 2 |
|-----------|--------------|------------|
| Decision maker | Human / script | Leader controller + `Region` |
| Time to failover | Minutes | Seconds |
| Risk of bad flip | Low (no auto) | Mitigated by guards |
| Maps to oncall | MANUAL half | AUTOMATIC half |
| Ship first? | **Yes** | After 1 |

### H.2 Phased rollout

1. **Phase 0:** PR #307 lands (region vocabulary; manual failover can still ship if 307 is split — align with program plan).
2. **Phase 1 (current delivery):** Approach 1 only — REST + topic model + producer/cache; `autoFailover` defaults **false**; **no** `AutomaticFailoverJob` / `TopicFailoverService` in this release.
3. **Phase 2 (deferred):** Approach 2 — controller automation when explicitly scheduled.
4. **Phase 3:** Consumer follow-producer — separate program of work.

---

## I — GROOMING / BACKLOG CHECKLIST

### I.1 Prerequisites

- [ ] PR #307 entities + stores + REST + IAM merged or aligned (as needed for **manual** failover + `RegionService` validation on flip)
- [ ] *(When Phase 2 is scheduled)* `Region.status` updatable via API for Approach 2 automation

### I.2 Phase 1 stories

- [ ] `VaradhiTopic` + `TopicResource` fields
- [ ] `VaradhiTopicFactory` multi-region
- [ ] `VaradhiTopicService` failover methods
- [ ] `TopicHandlers` routes
- [ ] Producer resolution + cache key + invalidation (Section K)
- [ ] `crossRegionProduce` flag
- [ ] Tests + runbook

### I.3 Phase 2 / automated failover (**deferred — not current backlog**)

- [ ] **`TopicFailoverService`** (or `FailoverOrchestrationService`) in **controller** — see **Section N**
- [ ] `AutomaticFailoverJob` (thin REGION listener → delegates to `TopicFailoverService`) + leader gate + freeze
- [ ] `TopicByRegionIndex` + cold load
- [ ] Metrics + audit store decision
- [ ] E2E: region status → topics move

### I.4 Optional / decisions

- [ ] Replication lag SPI scope
- [ ] Default single-active vs multi-active for new topics
- [ ] Audit: ZK vs DB

---

## J — ALTERNATIVE: SIDECAR ON PRODUCER POD

- **Idea:** Sidecar next to app proxies produce traffic; picks regional Varadhi (or broker) from cached metadata (poll admin API / watch ZK).
- **When:** Clients that cannot use Varadhi’s Java producer; local egress without multi-region LB.
- **Caveats:** Do not fight Varadhi routing logic; same source of truth (metastore); mTLS/tokens; staleness similar to producer TTL.
- **Decision:** MVP vs defer until Approach 1 is proven.

---

## K — LOW-LEVEL DESIGN (LLD): CACHES, EVENTS, INVALIDATION, STAGES

### K.1 Two caches on each producer node

1. **`topicCache`** — `ResourceReadCache` of `VaradhiTopic` by FQN.  
   - **UPSERT:** replace if `event.version >` cached version.  
   - **INVALIDATE:** remove key.

2. **`producerCache`** — Caffeine `LoadingCache`.  
   - **Key TODAY:** `(topicFQN, storageTopicId)` only; loader uses JVM `produceRegion` — **wrong** for per-topic failover.  
   - **Key TARGET:** `(topicFQN, storageTopicId, chosenRegionName)`.

**TTL:** `ProducerOptions.producerCacheTtlSeconds` (default 3600), `expireAfterAccess` — safety net if events are missed.

### K.2 Event chain (topic update → all nodes)

1. `VaradhiTopicService` → `topicStore.update(topic)`.
2. Metastore emits `MetaStoreChangeEvent` (TOPIC).
3. `DefaultMetaStoreChangeListener` loads fresh topic → `ResourceEvent(TOPIC, UPSERT, entity, version, committer)`.
4. `ResourceEventProcessor` enqueues to every cluster member; senders deliver `ClusterMessage`.
5. On each member: `ResourceEventDispatcher` invokes listeners.
6. `topicCache.onChange(UPSERT)` merges new topic by version.
7. **Today:** `producerCache` is **not** tied to this event → **stale producers** possible until TTL.  
8. **Required (new):** On TOPIC UPSERT/INVALIDATE, **invalidate** all `producerCache` keys for that topic **FQN** (do not flush entire cache).

### K.3 Why invalidate producer cache even if the key includes region

`topicCache` update and producer invalidation may run in **different order**. Evict-by-FQN avoids returning a `Producer` built from a briefly stale topic. Loader always re-reads `topicCache` and recomputes `chosenRegion`.

### K.4 Version discipline

Every `topicStore.update` that changes routing must **bump** entity/version so `ResourceReadCache` does not ignore a legitimate UPSERT.

### K.5 Where “failover stages” are tracked

| Layer | What | Purpose |
|-------|------|---------|
| **Ground truth** | Metastore: `produceRegions`, `failoverRegion`, `autoFailover` | Who may produce where |
| **Operator** | HTTP response of PUT/POST failover | Confirmation; idempotent retries |
| **Propagation** | Optional: metrics/logs when event committed on all nodes | Uses `ResourceEvent` committer |
| **Approach 2 (deferred)** | Logs + metrics + optional ZK `/varadhi/failover/history/...` | Audit when automation ships |
| **Controller job (deferred)** | In-memory steps only | Not persisted as topic “stage” |
| **Controller failover service (deferred)** | `TopicFailoverService` — **Section N** | When Approach 2 is built |

**MVP:** No mandatory `FAILOVER_IN_PROGRESS` on topic. For future “validating replication” UX, use a **separate** job/task record.

### K.6 Manual failover sequence (numbered)

1. Operator calls `PUT produceRegions` or `POST failover` on Web API.
2. `TopicHandlers` authorizes `TOPIC_UPDATE`.
3. `VaradhiTopicService` validates → `topicStore.update`.
4. Metastore notifies `DefaultMetaStoreChangeListener`.
5. Listener builds TOPIC UPSERT `ResourceEvent`.
6. `ResourceEventProcessor` fans out to all members.
7. Each node: `topicCache` applies UPSERT.
8. Each node: `ProducerService` invalidates `producerCache` keys for that topic FQN.
9. Next produce: cache miss → loader reads fresh topic → `pickProduceRegion` → new `Producer` for correct regional `SegmentedStorageTopic`.

### K.7 Automatic failover sequence (**deferred** — same metastore/event tail as K.6)

When Approach 2 exists:

1. Operator/automation sets `Region` to `PRODUCE_UNAVAILABLE` (or similar).
2. Metastore persists; REGION upsert to listener.
3. Leader-only **`AutomaticFailoverJob`** receives the event → delegates to **`TopicFailoverService`** (controller — oncall parity; see **Section N**).
4. **`TopicFailoverService`:** freeze + cooldown + rate-limit checks; **`TopicByRegionIndex`** lists topics producing into bad region; tag ordering (`HIGH_PRIORITY` first).
5. Per eligible topic: compute next `produceRegions` → **`VaradhiTopicService.setProduceRegions`** (same persistence path as manual REST).
6. Repeat **K.6 steps 4–9** per topic (metastore → fan-out → caches).

### K.8 Configuration knobs

- `producerCacheTtlSeconds` — lower in DR-sensitive sites.
- `producer.crossRegionProduce.enabled` — default **false** (local-only).
- `controller.failover.*` — **deferred**; only when Approach 2 ships.

### K.9 Test matrix (short)

- UPSERT with new `produceRegions` → `topicCache` updated; produce goes to new region’s broker.
- Out-of-order lower-version UPSERT → topic cache unchanged (version rule).
- INVALIDATE topic → topic gone; producer keys for FQN evicted.
- Concurrent produce during flip → no indefinite wrong-region `Producer` after invalidation.
- No cluster members → event warning; TTL back-stop.

### K.10 Manual failover: `TopicFailoverInformation`? Blocking produce across N pods?

**Is `TopicFailoverInformation` (oncall-style) required for manual (Approach 1)?**

- **Not required for correctness.** Manual failover is: REST → `VaradhiTopicService` → metastore → **TOPIC UPSERT** fan-out (K.6 steps 4–9). The “source of truth” is the updated `VaradhiTopic` in the metastore; every producer pod learns via the **same event path** already used for topic updates.
- **Optional:** You can still build a small **audit DTO** (same *shape* as oncall’s `TopicFailoverInformation` — who, when, before/after `produceRegions`, correlation id) **on the API node** when handling `PUT …/produceRegions` / `POST …/failover`, and write it to logs or an audit store. That is for **compliance / ops visibility**, not for cross-pod orchestration.
- **Controller-only** `TopicFailoverInformation` is most valuable when the **leader controller** drives many automatic evictions (Approach 2 — deferred).

**Do we need to orchestrate “blocking produce” on every producer pod?**

- **No separate controller RPC to “block” each pod** in the documented OSS design. Orchestration is **metadata + cluster events**:
  - `ResourceEventProcessor` already delivers the TOPIC UPSERT to **all** Varadhi cluster members (every node that runs `ProducerService` + `ResourceEventDispatcher`).
  - After invalidation, each pod **re-reads** `produceRegions` from `topicCache` and applies **`pickProduceRegion`**.
- **Default (local-only):** If a pod’s JVM region is **not** in `produceRegions`, produce for that topic **fails fast** (`ProducerNotAvailableException` / 5xx). **Load balancers / clients** are expected to send traffic to Varadhi **pods in a region that is still in** `produceRegions`. That is how you “steer” traffic without a global barrier.
- **Optional `crossRegionProduce`:** Pods in the old region **can** keep accepting HTTP produce and open brokers in the **new** active region — still **no** per-pod block list; routing follows metadata.

**If product needs “nobody may produce until all pods ack”:** that is a **stronger** protocol (e.g. distributed barrier, `PRODUCING_BLOCKED` flag on topic, two-phase flip) — **not** part of the current manual design; add only if you explicitly want global ordering before cutover.

---

## N — CONTROLLER FAILOVER SERVICE (**DEFERRED** — ONCALL PARITY FOR APPROACH 2)

**Not in current delivery.** When automated failover is prioritized, oncall-style **controller-resident** orchestration should be introduced as below.

Oncall runs a **dedicated failover service in the controller** (long-lived orchestration: guards, batching, audit). OSS should add the **same class of component** when Approach 2 is built so automatic failover is not “only a job class” scattered across listeners without a single service boundary.

### N.1 Role in OSS

| Piece | Responsibility |
|-------|----------------|
| **`TopicFailoverService`** (suggested name; e.g. `FailoverOrchestrationService`) | **Leader-only.** Region-eviction policy: who to flip, in what order, cooldowns, rate limits, freeze ZK, **all-dark** handling, structured audit + metrics. |
| **`AutomaticFailoverJob`** (or `RegionFailoverListener`) | **Thin adapter:** REGION metastore / listener event → call `TopicFailoverService.onRegionNotProduceCapable(region, status, …)`. |
| **`VaradhiTopicService`** | **Persistence + validation** for `setProduceRegions` / `setFailoverRegion` — **one** implementation used by both REST and controller (no duplicated topic writes). |
| **`TopicByRegionIndex`** | Feeds the service with O(topics in bad region) candidates. |

### N.2 Manual path vs controller service (Approach 1 vs 2)

- **Approach 1 (manual):** `TopicHandlers` → **`VaradhiTopicService`** → `topicStore.update` → **K.6 steps 4–9**. No **requirement** that the HTTP request hits the controller process; any API node may handle admin REST. That is still correct as long as metastore + event fan-out run.
- **Approach 2 (automatic):** **Must** go through **`TopicFailoverService` on the leader** so behaviour matches oncall (single orchestrator, observability, kill-switch).

**Optional later (architectural decision):** route **manual** failover RPC to the leader `TopicFailoverService` for identical code paths and optional global locks — not required for MVP if `VaradhiTopicService` stays authoritative for writes.

### N.3 Lifecycle and wiring

- Construct **`TopicFailoverService`** when the controller verticle starts (or when **leader is elected**); inject `VaradhiTopicService`, `RegionService`, `TopicByRegionIndex`, failover config, metrics, audit sink, freeze-node reader.
- Register **`AutomaticFailoverJob`** with `DefaultMetaStoreChangeListener` (REGION upsert) **only on leader**; job forwards to `TopicFailoverService`.
- **`TopicFailoverService` must not run eviction on followers** — same leadership gate as other controller work.

### N.4 What moves *into* the service vs stays in core

| In `TopicFailoverService` (controller) | Stays in `VaradhiTopicService` (core) |
|----------------------------------------|--------------------------------------|
| Per-region / per-topic cooldown, rate limit, freeze | `setProduceRegions` validation vs `Region` + `replicationRegions` |
| Iteration over `TopicByRegionIndex`, tag priority | `topicStore.update`, version bump, idempotency |
| Audit lines, `varadhi_failover_*` metrics | Optional `validateReplicationReady` / SPI when present |
| “All-dark” skip + alert | |

### N.5 Updated mental model (both approaches)

- **Manual:** Operator → Web → **core topic service** → metastore → **existing** listener + **ResourceEventProcessor** → data plane caches (K.6).
- **Automatic:** Region change → **leader** `AutomaticFailoverJob` → **`TopicFailoverService`** → **same** `VaradhiTopicService.setProduceRegions` → then same **K.6 steps 4–9** per topic.

---

## L — OPEN QUESTIONS (DECISION LOG)

1. **`Region.status` updates:** For **deferred** Approach 2 — confirm PUT/PATCH ships with PR #307 or follow-up. **Approach 1** can ship with REST-only topic flips and `RegionService` used for validation on the target region.
2. **New topic default:** single-active `produceRegions` vs multi-active opt-in.
3. **Replication-lag SPI:** GA requirement vs optional + `skipValidation` runbooks.
4. **`crossRegionProduce`:** default false — maintainer sign-off.
5. **Audit storage:** ZK history vs relational table.
6. **In-flight at flip:** metadata-only flip OK? (no bounded drain today.)
7. **Tags:** ordering beyond `HIGH_PRIORITY`?
8. **Sidecar:** in MVP or defer?
9. **Manual REST → leader only?** Optional RPC to `TopicFailoverService` for manual flips vs keep direct `VaradhiTopicService` on API node (Section N.2).

---

## P — INTERNAL SEQUENCE DIAGRAM ↔ OSS (MANUAL `produceRegions` / FAILOVER)

The internal sequence (operator → VaradhiServer Web API → topic service → metastore → `DefaultMetaStoreChangeListener` → `ResourceEvent` **TOPIC UPSERT** → `ResourceEventProcessor` fan-out → each node **topicCache** + **ProducerService** `invalidate(topicFQN)` → next produce **cache miss** → `pickProduceRegion` → new producer for regional **`SegmentedStorageTopic`**) is the **same logical model** as **§K.6** and `topic-failover.md` §6.3.5. OSS implements that model with the names and paths below.

### P.1 Swimlane mapping (diagram → OSS)

| Diagram / internal | OSS |
|--------------------|-----|
| Operator | Same |
| Web API (VaradhiServer) | Vert.x admin **`TopicHandlers`** — add routes per design (`PUT …/produceRegions`, `POST …/failover`, etc.); today only CRUD/restore |
| `TopicHandlers.updateTopicRegions()` (conceptual) | New handlers → **`VaradhiTopicService.setProduceRegions`** / convenience failover |
| **`VaradhiTopicService`** | `core/.../VaradhiTopicService` — validate, **`topicStore.update`** |
| **Metastore** | `spi/.../MetaStore` + `TopicStore`; ZK impl under `metastore-zk/` |
| **`DefaultMetaStoreChangeListener`** | `controller/.../DefaultMetaStoreChangeListener` — TOPIC branch loads entity, emits **`ResourceEvent(TOPIC, UPSERT, …)`** |
| **`ResourceEventProcessor`** fan-out | `controller/.../events/ResourceEventProcessor` — queues to **all** cluster members |
| Per-node **`topicCache`** | **`ResourceReadCache<VaradhiTopic>`** — `ResourceEventDispatcher` → `onChange(UPSERT)` |
| **`ProducerService`** + **`invalidate(topicFQN)`** | **Implement:** register listener for TOPIC UPSERT/INVALIDATE and evict **`producerCache`** keys for that FQN (diagram step; not wired today — see **K.2**) |
| Cache miss → **`pickProduceRegion`** → **`SegmentedStorageTopic`** | **Implement:** read `produceRegions` from cached topic; choose region; **`ProducerCacheKey`** includes region; loader uses **`getProduceTopicForRegion(chosen)`** |

### P.2 Internal oncall model (names only — for diffing)

From `topic-failover.md` §11: **`GlobalTopic`** (ZK), **`activeProduceZone`**, **`FailoverType`**, **`UmbrellaTopicService` / `PulsarTopicService.updateActiveProduceZone`**, **`TopicResourceV2`** `PUT …/activeProduceZone/{zone}`**, **`ProducerKey`** (zone in key). OSS uses **`VaradhiTopic`** + **`produceRegions`** / **`failoverRegion`**, same metastore → **TOPIC UPSERT** → fan-out idea, and **region in producer cache key** (parity with **`ProducerKey`**).

### P.3 OSS gap list (to match the diagram end-to-end)

1. **REST** — failover routes on **`TopicHandlers`** + auth **`TOPIC_UPDATE`**.  
2. **Core** — **`setProduceRegions`** / validation / **`topicStore.update`** (and PR #307 topic fields).  
3. **Producer** — **`pickProduceRegion`**, cache key includes **region**, **invalidate on TOPIC event** (the diagram’s explicit producer-cache step).  
4. **Factory / entity** — multi-region **`internalTopics`** + persisted **`produceRegions`**.  
5. **No extra controller “block all pods” RPC** — fan-out is the existing **`ResourceEventProcessor`** path to every Varadhi member running the dispatcher + caches.

---

## M — REFERENCES

| What | Where |
|------|--------|
| Canonical design (Mermaid + code) | `varadhi/docs/topic-failover.md` |
| PR #307 | https://github.com/flipkart-incubator/varadhi/pull/307 |
| Topic model | `docs/TOPIC_MODEL_STRUCTURE.md` |
| This master (Markdown headings) | `varadhi/docs/TOPIC_FAILOVER_MASTER_FOR_GOOGLE_DOCS.md` |
| LLD with diagrams (points here) | `varadhi/docs/topic-failover-produce-lld.md` |

**Internal oncall (diff behaviour):** `GlobalTopic`, `FailoverType`, `UmbrellaTopicService.updateActiveProduceZone`, `PulsarTopicService`, `TopicResourceV2` activeProduceZone route, `ProducerKey`.

---

# END OF DOCUMENT
