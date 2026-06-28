# CDC Split Entity Types — OSS Impact (Approach C)

> Brief grooming note: what OSS must build if CDC uses **split entity types** instead of merge-at-apply.
> Parent: [`cdc-v2-runtime-fields-grooming.md`](cdc-v2-runtime-fields-grooming.md) §2.1 (approach **C**).

| Field | Value |
|-------|-------|
| Status | Grooming — decision aid |
| Chosen alternative | **Not chosen** (primary path remains merge-at-apply **A**) |
| Scope | OSS (`varadhi/`) only; oncall CDC writes config entity only |

---

## Summary

Split entity types replaces one persisted `VaradhiTopic` ZNode with two L1 metastore entities:

```text
/varadhi/entities/TopicConfig/{fqn}   ← CDC-owned structural fields
/varadhi/entities/TopicRuntime/{fqn}  ← OSS-only runtime fields
```

CDC never addresses the runtime path, so there is no clobber risk and no `CdcMergePolicy` in OSS. The cost is a cross-cutting refactor: every topic reader must join config + runtime, and writers must target the correct store.

---

## Entity model (`entities`)

Split `VaradhiTopic` into `VaradhiTopicConfig` and `VaradhiTopicRuntime`, both extending `MetaStoreEntity`.

**Config (CDC):** name, lifecycle status, grouped, capacity, nfrFilterName, topicCategory, `internalTopics` / storage layout.

**Runtime (OSS):** `topicState`, persisted `produceIndex`, future region routing (`regionConfigs`, failover maps), OSS-only rate-limit and telemetry fields.

Provide an in-memory join type (`ResolvedVaradhiTopic` or facade `VaradhiTopic`) for callers. Remove `topicState` from `SegmentedStorageTopic`. Apply the same split to subscriptions if IQ/RQ/DLQ `produceIndex` / `consumeIndex` must survive CDC tails.

---

## Metastore (`metastore-zk`, `spi`)

Add `MetaStoreEntityType` values and `ZNode` kinds for `TopicConfig` and `TopicRuntime`. Implement `TopicConfigStore` and `TopicRuntimeStore` in `VaradhiMetaStore`. Topic create/delete uses a ZK multi-op on both nodes. Versions are independent: failover SWITCH bumps runtime version without touching config CAS.

On CDC create of config, OSS must materialize a default runtime node (`Producing`, indices `0`) via lazy bootstrap or a config-create watcher. On CDC delete, remove both nodes.

---

## Cache and read path (`core`, `producer`, `web`)

Replace the single `ResourceReadCache<VaradhiTopic>` with a **join cache** that loads config + runtime and builds the logical view. `DefaultMetaStoreChangeListener` must UPSERT/INVALIDATE on both entity types. `ProducerService`, admin handlers, rate limiter, and authz — every current `VaradhiTopic` consumer — reads the joined view.

Benefit: runtime updates do not invalidate config cache entries. Cost: handle partial state (config without runtime) during bootstrap.

---

## Writers

| Operation | Store |
|-----------|-------|
| CDC tail | Config only |
| Topic CRUD / structural admin | Config |
| Failover SWITCH / block / throttle | Runtime |
| Failover MIGRATED routing commit | Config and/or runtime (possibly dual CAS or one ZK transaction) |
| Storage migration | Config layout from CDC; `produceIndex` in runtime |

`TopicFailoverOpExecutor` and admin APIs must be retargeted. Transition coordination (`TransitionObject`, bus acks) is unchanged.

---

## What you skip vs merge-at-apply

No `CdcMergePolicy`, no `@V2Governed`, no merge integration tests. Instead: join-cache tests, dual-create/delete atomicity, runtime-only CAS during live CDC, and orphan-runtime cleanup.

---

## Verdict

Use split entity types only if v2-governed fields and write frequency outgrow merge policy, or if split is the desired **steady-state** model post-migration. For the live CDC window alone, merge-at-apply (§5.4 of parent doc) is the lower-cost OSS path.
