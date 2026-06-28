# Topic Failover — Grooming: Three Refinement Decisions

> Decision record refining the implemented topic-failover design (see `topic-failover-lld.md`).
> Captures three follow-up questions raised during grooming and the agreed direction for each.
> **Status: groomed, not yet implemented.** This doc supersedes the relevant parts of the LLD
> (multi-txn commit path, the `FailoverTransitionObject` entity, and the locking model).

| Field | Value |
|-------|-------|
| Status | Groomed — pending implementation |
| Supersedes | `topic-failover-lld.md` §commit-path, §FTO, §locking |
| Components touched | `entities`, `spi`, `metastore-zk`, `controller`, `web` |
| Decisions | D1: no multi-txn (eventual consistency) · D2: lock-free + CRUD guards · D3: generic operation pointer |

---

## 0. The single fact that drives all three decisions

In the implemented flow, pods are told to switch produce regions by **waiting for `TopicCache` to reach
the new Topic version** (`vTo = N+1`), carried in the `SWITCH` stage event — **not** by reading the
failover Op:

```text
runSwitch():
  commit Topic (source=Blocked, target=Producing, version -> N+1)
  broadcast FailoverStageEvent.forSwitch(opId, fqn, fenceVersion, newTopicVersion = N+1)
  pod waits until TopicCache.version(fqn) >= N+1, then acks
```

Therefore:

- The **`VaradhiTopic` entity is the functional source of truth** for produce routing.
- The **`TopicFailoverOperation` is orchestration bookkeeping** — recoverable, not on the data path.
- A pod **never makes a routing decision from Op state**, only from the Topic version it observes.

This is why we can remove the cross-entity transaction (D1), avoid a held lock (D2), and demote the
pointer entity to a generic, derived index (D3) — none of them sit on the correctness-critical path.

---

## D1 — Drop the `multi()` transaction; Op/state is the source of truth (eventual consistency)

### Decision
Remove all uses of the cross-entity ZK `multi()` transaction from the failover commit path. Replace
each with **ordered single-znode writes**, made safe by **idempotent, resumable stages** and a
**controller-startup reconciler**. The `TopicFailoverOperation` (plus the Topic entity it points at)
becomes the source of truth; consistency between Op, pointer, and Topic is restored by convergence,
not by atomicity.

### Today (to be removed)
`TopicFailoverTransactionsImpl` uses `multi()` in three places:

| Site | Current writes (atomic) | Verdict |
|------|-------------------------|---------|
| `createFailoverWithOp` | create Op + create FTO | atomicity not needed — Op is authoritative, pointer is derived |
| `commitSwitch` | setData(Op, untracked) + setData(Topic, **tracked**) | atomicity not needed — but **write ordering matters** |
| `commitSuccess` / `commitFailure` | setData(Op) + delete(FTO) [+ optional Topic source=Replicating] | atomicity not needed — already cosmetic at this point |

### Target write ordering (per stage)

1. **Create (Phase 0)**
   - Write **Op** first (durable, authoritative) → enqueue in `OperationMgr` → write **pointer** (index).
   - Crash between Op and pointer ⇒ pointer missing; rebuilt by the reconciler from active ops.

2. **SWITCH** — *Topic first, Op second* (non-negotiable ordering)
   - Write **Topic** (tracked, fans out via L1, drives the pod barrier) **before** flipping the **Op** stage, **before** any pod is pinged.
   - Crash after Topic, before Op ⇒ Topic already correct (pods can switch); resumed executor re-enters `SWITCH`, sees Topic already at target, idempotently re-applies and flips Op. **No produce disruption.**
   - Crash after Op, before pods notified ⇒ harmless: pods only switch on the Topic version they see, so a stale "Op says SWITCH" never moves traffic on its own.

3. **COMPLETED (success cleanup)**
   - Update **Op** state, delete **pointer**, optionally set source `TopicState=Replicating` (cosmetic, untracked) — independent single writes, any order. A terminal Op with a lingering pointer is GC-able.

4. **ABORTED rollback (failure after a partial SWITCH)** — *Topic first, Op second*
   - Per the LLD (§5 commit table) the rollback writes a **tracked** Topic restore so pods revert. Same ordering rule as SWITCH: write the **Topic rollback** (tracked) first so pods converge back, then mark **Op** `ERRORED`, then delete the **pointer**. Pre-SWITCH aborts have no Topic write — just mark Op + delete pointer.

> **Module-boundary note (per `.windsurf/rules/core-instructions.md`):** the controller must reach ZK only through SPIs (`OpStore`, `TopicStore`), never `ZKMetaStore`/`ZNode` directly. Dropping the `TopicFailoverTransactions` SPI must therefore route these ordered single-writes through `OpStore.updateTopicFailoverOp(...)` / `TopicStore.update(...)`, **not** by reintroducing a controller → `metastore-zk` dependency.

### What we must add to keep this safe

| Mechanism | Responsibility |
|-----------|----------------|
| **Idempotent / resumable stages** | Re-entering a stage re-derives the desired Topic snapshot and re-applies it; completing a stage twice is a no-op. Each stage checks "am I already past this?" before acting. |
| **Startup reconciler** (controller) | Formalizes the **"leader rehydration"** the LLD already anticipates (`getAllActiveTopicFailoverOps()` + FTO as "leader-rehydration anchor", LLD §4.2/§ store). On leadership-acquired: load all non-terminal ops from `OpStore`, re-enqueue them into the executor, and rebuild any missing pointer for each. Drop pointers whose op is terminal/absent. |
| **Optimistic concurrency** | Topic write keeps `setData ... withVersion(N)`; a `BadVersion` (someone edited the topic) fails the stage, which re-reads and retries. (Unchanged from today.) |

### Trade-offs accepted
- **Pro:** no cross-entity transaction; simpler ZK primitives; aligns with "Op/state is truth".
- **Con:** we now owe a reconciler + idempotency guarantees on each stage (previously avoided by atomic commit). There is a brief crash window where Op, pointer, and Topic disagree — always healed by convergence, never visible as wrong routing because the pod barrier keys off Topic version.

### Implementation impact (for the impl pass, not done here)
- Delete `TopicFailoverTransactions` SPI + `TopicFailoverTransactionsImpl` (or reduce them to plain ordered single-writes via `OpStore` / `TopicStore`).
- Remove the `multi()` / `CuratorOp`-builder dependency from the failover path (the generic `ZKMetaStore.multi()` can stay for other callers).
- `TopicFailoverOpExecutor`: make `runPrepare` / `runSwitch` / `runDrain` / `cleanup` re-entrant; Topic-first ordering in `runSwitch`.
- Add `FailoverReconciler` wired into controller leadership-acquired startup.

---

## D2 — No CRUD lock held across the failover; lock-free coordination + explicit guards

### Decision
Do **not** acquire a distributed lock for the duration of a failover (which can run minutes during
`DRAIN`). Coordinate lock-free using mechanisms already present, and **add two explicit guards** on
topic CRUD:

1. **`DELETE topic`** → reject while a failover is active.
2. **`UPDATE topic` (config)** → reject while a failover is active.

### Why no held lock
A multi-minute distributed lock (`InterProcessMutex`) is fragile: ZK session loss / GC pause drops the
lock mid-process, and it blocks unrelated CRUD for the whole window. We don't need it because three
lock-free mechanisms already provide the guarantees:

| Concern | Mechanism (already present) |
|---------|-----------------------------|
| Two concurrent failovers on the same topic | Pointer is a ZK **create on path = topic FQN** → second create fails `NodeExists` (natural mutual exclusion). |
| Serialized execution per topic | `OperationMgr` orders by `orderingKey = "TopicFailover_" + topicFqn` → at most one executing per topic. |
| Failover vs. concurrent topic edit | SWITCH Topic write uses **optimistic version** (`withVersion`) → `BadVersion` on race, stage re-reads/retries. |
| Duplicate create requests | Existing **`requestId` idempotency window** (LLD §10.2 `checkIdempotency`, `requestIdLookbackWindowMs`) dedupes retried create calls — complementary to the pointer's atomic-create uniqueness. |

### What we add (the guards)
- **Delete guard:** in the topic delete path, check pointer existence (O(1) read). If a failover is active, reject with a clear "abort the active failover first" error.
- **Update guard:** in the topic update path, same existence check; reject config mutation while a failover is active. (This is stricter than relying on optimistic-version to lose the race, and gives operators a clear message instead of a surprise `BadVersion`.)

### Trade-offs accepted
- **Pro:** no long-lived lock, no lock-loss failure mode; guards are cheap existence reads; clear operator errors.
- **Con:** guards are advisory at the API layer — they rely on the pointer being present (D3). With D1's eventual consistency, there is a sub-second window at create time before the pointer exists; the `OperationMgr` ordering key still serializes execution, so the worst case is a redundant op that the create-time pointer check rejects.

### Implementation impact
- `VaradhiTopicService` / topic delete handler: add active-failover existence check before delete.
- Topic update handler: add active-failover existence check before update.
- Surface a typed error (e.g. `ResourceFailoverInProgressException` / 409 Conflict) for both.

---

## D3 — Replace `FailoverTransitionObject` with a generic operation pointer that lives under the topic

### Decision
Drop the bespoke `FailoverTransitionObject` (FTO) L2 entity (`TOPIC_FAILOVER` / `SUB_FAILOVER` entity
types). Replace it with a **generic, topic-scoped "active operation pointer"** modelled as a **child
znode under the topic's path**, e.g.:

```text
/varadhi/entities/Topic/{topicFqn}/operations/{opId}
        (or a single-active variant: /varadhi/entities/Topic/{topicFqn}/activeOp)
```

The pointer is a tiny record `{ opId, opKind, startTime }`. It is **derived** from the authoritative
Op and exists only as an index + guard.

### What the pointer must still do (same three jobs as FTO)
1. **Discovery** — `GET /failover` resolves the active op for a topic in O(1) (read one child), no scan of all op znodes.
2. **Delete / update guard** — D2's checks are an O(1) existence read of the child.
3. **Atomic uniqueness** — ZK `create` on the child path enforces one active failover per topic.

### Why this shape (and why not the alternatives)

| Option | Verdict |
|--------|---------|
| **(A) Drop pointer, scan active ops** | Works (active failovers are rare) but loses the atomic-create uniqueness guard and adds a scan per GET/guard. Only acceptable if we fully embrace D1 scan-based discovery — we are **not** choosing this. |
| **(B) Field `activeFailoverOpId` on `VaradhiTopic`** | ❌ Rejected. Mutating the Topic is a **tracked** write → L1 fan-out to every pod on every failover start/stop, and pollutes the Topic version history with control-plane churn. |
| **(C) Generic operation pointer child znode under the topic** | ✅ Chosen. O(1) discovery + guard, atomic-create uniqueness, **no Topic mutation / no fan-out / no version coupling**, and generalizes to any long-running topic operation (not just failover). This is what FTO already *was*, relocated under the topic and made generic. |

> **Precision vs. the status quo:** the *current* `FailoverTransitionObject` is a **separate sibling znode** (`/varadhi/entities/TopicFailover/{fqn}`) bundled into **no** pod cache, so it **already avoids** Topic fan-out — the fan-out problem is unique to option (B). D3's win over the implemented FTO is therefore **genericity + topic-locality + reuse**, *not* a fan-out fix. The fan-out argument is the reason to reject (B), and matches `topic-failover-modeling-decision.md`'s rejection of the L2 "sub-field bundled in the Topic entity" model.

### Naming / typing
- Replace entity type `TOPIC_FAILOVER` / `SUB_FAILOVER` with a generic operation-pointer concept
  (e.g. `OPERATION_POINTER`, or a child-znode kind under `TOPIC` rather than a top-level entity type).
- The pointer payload is operation-kind-agnostic: `{ opId, opKind, startTime }` so future long-running
  topic operations can reuse it.

### Trade-offs accepted
- **Pro:** generic + reusable; no Topic fan-out; keeps atomic uniqueness; satisfies "resides inside the topic".
- **Con:** introduces a child znode under the topic path (the entity layer currently treats topic znodes as leaves) — list/delete of a topic must account for children. The reconciler (D1) rebuilds these pointers from active ops on startup, so a lost pointer is self-healing.

### Implementation impact
- Remove `FailoverTransitionObject`, `ZNode.ofTopicFailover/ofSubFailover`, and the `TopicStore`
  failover methods (`createFailover` / `getFailover` / `hasFailover` / `deleteFailover` / `getAllActiveFailovers`).
- Add a generic operation-pointer ZNode kind/path under the topic and corresponding store methods
  (`createOperationPointer`, `getOperationPointer`, `hasOperationPointer`, `deleteOperationPointer`,
  `listOperationPointers` — names TBD at impl).
- Update `ControllerApiMgr` discovery/`listActive`, the executor create/cleanup, the reconciler, and
  the D2 guards to use the pointer.
- Update `InMemoryMetaStore` test fixture accordingly.

---

## Summary of how the three interlock

```text
D3 generic pointer  --provides-->  O(1) discovery + atomic uniqueness
                                     |
D2 lock-free        --uses-------->  pointer existence as the CRUD guard (delete + update)
                                     |
D1 eventual         --requires--->  reconciler rebuilds pointer + re-enqueues ops on restart
   consistency      --guarantees->  Topic-first ordering at SWITCH; pod barrier keys off Topic version,
                                     so Op/pointer/Topic divergence is always healed, never visible as
                                     wrong produce routing.
```

- **Correctness anchor:** Topic entity + version-gated pod barrier (unchanged).
- **Removed:** cross-entity `multi()` on the failover path; `FailoverTransitionObject` entity; any held lock.
- **Added:** ordered single writes + idempotent stages + startup reconciler (D1); two CRUD guards (D2); generic operation pointer under the topic (D3).

---

## Verification against the docs folder

Cross-checked these decisions against the existing `topic-failover-*` docs to confirm they refine, rather than contradict, the agreed design.

| Source doc | What it says | Consistency with D1–D3 |
|------------|--------------|------------------------|
| `topic-failover-lld.md` §intro, §4.3 | "The Op is the **single source of truth** for failover stage state"; FTO carries no stage data | ✅ Anchors D1 (Op is truth) and D3 (pointer is a pure index). |
| `topic-failover-lld.md` §4.x event | Pod waits for `TopicCache` to reach `topicVersionToAwait` before acking SWITCH | ✅ This is §0 — the reason D1/D2/D3 are off the correctness path. |
| `topic-failover-lld.md` §4.2 | FTO purposes: discovery, **topic-delete guard**, leader-rehydration anchor | ✅ D2 delete guard = finishing what the LLD already specified; D2 adds the **update** guard as new. |
| `topic-failover-lld.md` store, §store | `getAllActiveTopicFailoverOps()` "for leader rehydration" | ✅ D1's reconciler formalizes this existing notion. |
| `topic-failover-lld.md` §5 commit table | SWITCH + ABORTED rollback are tracked Topic writes; create/PREPARE/DRAIN/COMPLETED untracked | ✅ D1 keeps tracked Topic writes (SWITCH, ABORT rollback) Topic-first; only the *cross-entity atomicity* is dropped. |
| `topic-failover-lld.md` §10.2 | `requestId` idempotency window on create | ✅ Complements D2's create-time uniqueness. |
| `topic-failover-modeling-decision.md` §1 | Failover state = **Operation**, not L1, not L2 sub-field-in-Topic | ✅ D3 changes only the *pointer*, not the state model; rejection of D3-option-(B) matches this doc's rejection of the bundled-in-Topic L2 model. |
| `.windsurf/rules/core-instructions.md` §module boundaries | "Only use APIs/interfaces from other modules, not internal classes" | ✅ D1 ordered writes go via `OpStore`/`TopicStore` SPIs — no controller → `metastore-zk` coupling. |

**No contradictions found.** The one nuance worth flagging: D3's benefit over the *implemented* FTO is genericity/locality (the current FTO already avoids fan-out); the fan-out argument applies only to the rejected option (B).

The older `topic-failover-grooming-final.md` describes a superseded **L1-entity** approach (`TopicFailover` as a first-class L1 with its own `ResourceReadCache`). That is already superseded by the LLD's Op model; these decisions build on the LLD, not on grooming-final.

---

## Open items to confirm at implementation time
1. Single-active pointer (`activeOp`) vs. multi (`operations/{opId}`) — single-active is simplest for
   "one failover per topic"; multi generalizes to concurrent op kinds later.
2. Exact error type/HTTP status for D2 guards (proposed: 409 Conflict).
3. Reconciler trigger point — confirm the controller leadership-acquired hook to attach it to.
4. Whether to keep `ZKMetaStore.multi()` for non-failover callers (yes — only the failover path drops it).
