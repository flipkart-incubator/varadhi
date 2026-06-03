# varadhi-controller — Components

> Code-facing view of `varadhi-controller`'s internal architecture: the building blocks inside the container, what each owns, and the behaviours that matter for reasoning about change impact. For the container's purpose, tech, and external relationships see [containers.md](../containers.md); shared modules it depends on are in [shared-components.md](../shared-components.md). This doc describes what components **are**, not how requests flow through them.

## Overview

`varadhi-controller` is the cluster's coordination brain — a Vert.x application that runs with `member.roles:[Controller]`, carries **no data-path traffic**, and has no HTTP API. It coordinates over the clustered event bus and the metastore only.

Its inbound surface, [`controller-api`](#varadhi-controllercontroller-api--controller-api), takes subscription lifecycle commands from `varadhi-server` and shard-operation updates from `varadhi-consumer`, plus consumer-node membership events; it also assumes leadership and restores in-flight state on start. Those land on [`subscription-coordinator`](#varadhi-controllersubscription-coordinator--subscription-coordinator), the hub that validates state and turns a request into a persisted, retryable operation.

Operations run through two cooperating pieces: [`operation-manager`](#varadhi-controlleroperation-manager--operation-manager) is the generic engine (per-entity serialized queues, parallelism across entities, retry, persistence), while [`operation-executors`](#varadhi-controlleroperation-executors--operation-executors) hold the per-operation logic that assigns shards and dispatches start/stop/unsideline to the owning consumers. Shard placement is owned by [`assignment-manager`](#varadhi-controllerassignment-manager--assignment-manager). Independently, [`event-distributor`](#varadhi-controllerevent-distributor--entity-event-distributor) watches the metastore and fans entity changes out to every node to keep their caches coherent.

Cluster RPC, the persistence/assignment/operation stores, and the process bootstrap are **shared** — see [shared-components.md](../shared-components.md) (`shared.cluster-rpc`, `shared.backend-spi`, `shared.app-bootstrap`).

## Component Summary

| Component | Archetype | Responsibility |
|---|---|---|
| `varadhi-controller.controller-api` | Inbound Gateway | Inbound cluster-RPC surface + leadership/bootstrap; dispatches to the coordinator |
| `varadhi-controller.subscription-coordinator` | Application Service / Coordinator | Validates state, creates/enqueues lifecycle ops, handles consumer-node membership |
| `varadhi-controller.operation-executors` | Application Service / Domain | Per-op logic: assign shards + dispatch start/stop/unsideline to consumers |
| `varadhi-controller.operation-manager` | State Manager | Operation engine: serialized per-entity queues, parallelism, retry, persistence |
| `varadhi-controller.assignment-manager` | Domain Logic Engine | Shard-to-consumer assignment via a capacity-aware strategy |
| `varadhi-controller.event-distributor` | Cross-Cutting Provider | Watches metastore, fans entity changes to all nodes for cache coherence |

## Components

### varadhi-controller.controller-api — Controller API

**Archetype**: Inbound Gateway
**Packages**: `controller` (`ControllerVerticle`, `ControllerApiHandler`)
**Public Interface**: `ControllerVerticle` (verticle lifecycle, deployed for `roles:[Controller]`).

#### Responsibility

The controller's inbound control surface and composition root. It hosts the verticle, assumes leadership on start, initializes consumer nodes from current cluster membership, restores in-flight state (re-queues pending operations, drops assignments of departed consumers), and registers the `ROUTE_CONTROLLER` cluster-RPC handlers plus the consumer-membership listener. `ControllerApiHandler` adapts cluster messages to the coordinator.

#### Collaborators

| Communicates With | Direction | Protocol | Purpose |
|---|---|---|---|
| `shared.app-bootstrap` | called-by | method call | Verticle deployed for the Controller role |
| `shared.cluster-rpc` (← server/consumer) | called-by | event bus | Receive `ROUTE_CONTROLLER` commands + shard-op updates |
| `shared.cluster-rpc` | calls | method call | Membership (`getAllMembers`, `addMembershipListener`) |
| `varadhi-controller.subscription-coordinator` | calls | method call | Dispatch decoded operations |

#### Side Effects

- None external directly — wires components, registers handlers/listeners, and dispatches. Leadership lifecycle only.

#### Runtime Characteristics

- **Failure mode (leadership)**: leadership is **assumed**, not elected — `ControllerVerticle#onLeaderElected` runs unconditionally on start, and bootstrap failure throws `#abortLeadership`. There is no election or handover, so running more than one controller risks split-brain coordination. (See memory.)
- **Startup state restore**: on becoming leader it re-queues pending operations and drops assignments of absent consumers (`ControllerVerticle#restoreControllerState`); recovery of already-failed operations is a noted TODO.

#### Notes for Coding Agents

- Treat single-active-controller as a current invariant — there is no election. Don't add logic that assumes safe concurrent controllers until election/handover exists.
- New cluster-RPC operations are registered in `ControllerVerticle#setupApiHandlers` against `ROUTE_CONTROLLER`; keep request vs send semantics aligned with the handler's return type.
- Leadership/state-restore lives here; per-subscription logic belongs in `subscription-coordinator`, not in the verticle.

---

### varadhi-controller.subscription-coordinator — Subscription Coordinator

**Archetype**: Application Service / Use-Case Coordinator
**Packages**: `controller` (`ControllerApiMgr`)
**Public Interface**: `ControllerApiMgr` implementing `ControllerApi` / `ControllerConsumerApi` (`startSubscription`, `stopSubscription`, `getSubscriptionState`, `unsideline`, `getShardAssignments`, `update`, `consumerNodeJoined`, `consumerNodeLeft`).

#### Responsibility

The coordination hub. It validates subscription state (rejecting e.g. starting an already-assigned subscription), creates lifecycle operations and enqueues them with the appropriate executor, assembles per-shard consumer state into an overall subscription state, and reacts to consumer-node membership — on a node leaving, it re-assigns that node's orphaned shards. It implements both the server-facing `ControllerApi` and the consumer-facing `ControllerConsumerApi` (`update`).

#### Collaborators

| Communicates With | Direction | Protocol | Purpose |
|---|---|---|---|
| `varadhi-controller.controller-api` | called-by | method call | Decoded commands + membership events |
| `varadhi-controller.operation-manager` | calls | method call | Create/enqueue/retry operations |
| `varadhi-controller.operation-executors` | calls | method call | Instantiate the per-op executor |
| `varadhi-controller.assignment-manager` | calls | method call | Read assignments; reassign on node leave |
| `shared.cluster-rpc` → `varadhi-consumer` | calls | event bus | Query consumer state/info (`ConsumerClientFactory`) |
| `shared.backend-spi` | calls | ZK | Read subscriptions (`SubscriptionStore`) |

#### Side Effects

- Consumer state/info queries over the event bus. Operation and assignment side effects are delegated to those components.

#### Runtime Characteristics

- **Consistency (state read)**: `getSubscriptionState` fans out to every shard's assigned consumer and merges the results (`ControllerApiMgr#getSubscriptionShardsState`); an unreachable consumer yields an empty per-shard state rather than failing the whole read.
- **Contention**: `update` (consumer shard-op callback) runs **inline on the dispatcher thread** (`ControllerApiMgr#update`) — keep it cheap; heavy work there stalls cluster-message dispatch.

#### Notes for Coding Agents

- This is the place that decides *whether* an operation is valid and *which* executor runs; the executor itself belongs in `operation-executors`.
- Consumer-node leave triggers reassignment here (`consumerNodeLeft` → `ReAssignShardData` ops); preserve that path when changing membership handling.

---

### varadhi-controller.operation-executors — Operation Executors

**Archetype**: Application Service / Domain Logic
**Packages**: `controller.impl.opexecutors` (`SubscriptionOpExecutor` base; `StartOpExecutor`, `StopOpExecutor`, `ReAssignOpExecutor`, `UnsidelinepOpExecutor`, `SubscriptionStartShardExecutor`)
**Public Interface**: the `SubscriptionOpExecutor` / `OpExecutor` family run by `operation-manager`.

#### Responsibility

The concrete subscription operations. Each executor performs one use case across a subscription's shards: assign or reassign shards via `assignment-manager`, dispatch the corresponding shard start/stop/unsideline to the owning consumers over the event bus, and track per-shard completion so the parent operation's state advances. Failures of individual shard operations are reported back through `operation-manager`.

#### Collaborators

| Communicates With | Direction | Protocol | Purpose |
|---|---|---|---|
| `varadhi-controller.subscription-coordinator` | called-by | method call | Instantiated per operation |
| `varadhi-controller.operation-manager` | called-by / calls | method call | Executed by the engine; report op-state |
| `varadhi-controller.assignment-manager` | calls | method call | Assign/reassign shards for the op |
| `shared.cluster-rpc` → `varadhi-consumer` | calls | event bus | Dispatch shard start/stop/unsideline (`ConsumerApi`) |

#### Side Effects

- Shard-operation dispatch to consumers (event bus); operation-state updates via `operation-manager`.

#### Runtime Characteristics

- **Failure mode**: a shard operation that fails is marked failed (`SubscriptionOpExecutor#failShardOperation`) and propagates to the parent subscription operation, which `operation-manager` may retry; partial-shard outcomes are tracked rather than rolled back.

#### Notes for Coding Agents

- A new subscription operation = a new `SubscriptionOpExecutor` subclass + wiring in `subscription-coordinator` (and `ControllerApiMgr#getOpExecutor` for retry reconstruction). Keep the executor idempotent — it can be re-run on retry.
- Executors translate coordination into consumer commands; they should go through `assignment-manager` for placement rather than choosing nodes directly.

---

### varadhi-controller.operation-manager — Operation Manager

**Archetype**: State Manager
**Packages**: `controller` (`OperationMgr`, `OpExecutor`, `RetryPolicy`)
**Public Interface**: `OperationMgr` (`createAndEnqueue`, `enqueue`, `updateSubOp`, `updateShardOp`, `getPendingSubOps`); `OpExecutor`; `RetryPolicy`.

#### Responsibility

The operation-execution engine and state machine. It keeps a per-entity (ordering-key) task queue where only the head runs and the rest wait, executes different entities' operations in parallel via a fixed thread pool, retries failed operations with backoff (preempting stale retries when a newer operation for the same entity arrives), and persists subscription/shard operations. It also turns consumer `update` callbacks into operation-state transitions.

#### Collaborators

| Communicates With | Direction | Protocol | Purpose |
|---|---|---|---|
| `varadhi-controller.subscription-coordinator` | called-by | method call | Enqueue/retry operations |
| `varadhi-controller.operation-executors` | calls / called-by | method call | Run executors; receive op-state updates |
| `shared.backend-spi` | calls | ZK | Persist operations (`OpStore`) |

#### Side Effects

- `OpStore` read/write → zookeeper.

#### Runtime Characteristics

- **Concurrency model**: parallelism across entities is bounded by a fixed pool sized `maxConcurrentOps` (`OperationMgr` constructor); within an ordering key, operations are strictly serialized (`enqueueOpTask` — only the head executes). Retries are scheduled on a single-thread delayed scheduler.
- **Failure mode (head-of-line block)**: if persisting an operation's failure throws, the operation is **not** removed from its queue and subsequent operations for the same ordering key wait indefinitely (`OperationMgr.OpTask#saveFailure`). A metastore write failure here stalls that entity's pipeline.
- **Retry preemption**: a newer operation for an ordering key cancels/aborts a pending retry of an older one (`enqueueOpTask`, `RetryOpTask#cancel`).

#### Notes for Coding Agents

- The ordering key serializes operations per entity — changing how it's derived changes the concurrency/safety model. Don't run two operations for the same subscription concurrently.
- `RetryPolicy` is internal to this component; retry/backoff changes live here.
- Persisting via `OpStore` is on the critical path of completing an operation — a store failure has the head-of-line consequence above; preserve the "update DB inside the handler" pattern to avoid version conflicts.

---

### varadhi-controller.assignment-manager — Assignment Manager

**Archetype**: Domain Logic Engine
**Packages**: `controller` (`AssignmentManager`, `AssignmentStrategy`), `controller.impl` (`LeastAssignedStrategy`)
**Public Interface**: `AssignmentManager` (`assignShards`, `unAssignShards`, `reAssignShard`, `getSubAssignments`, `getConsumerNodeAssignments`, `getAllAssignments`, `consumerNodeJoined`, `consumerNodeLeft`); `AssignmentStrategy`.

#### Responsibility

Owns shard-to-consumer assignment. It computes placements via a pluggable strategy (`LeastAssignedStrategy`) honoring each consumer node's capacity, persists assignments, and supports idempotent assign/unassign/reassign. It tracks consumer-node membership and per-node capacity in memory, freeing capacity when assignments are removed or fail.

#### Collaborators

| Communicates With | Direction | Protocol | Purpose |
|---|---|---|---|
| `varadhi-controller.subscription-coordinator` | called-by | method call | Read assignments; node join/leave |
| `varadhi-controller.operation-executors` | called-by | method call | Assign/reassign shards for an op |
| `shared.backend-spi` | calls | ZK | Persist assignments (`AssignmentStore`) |
| `core` (`ConsumerNode`) | calls | method call | Track/free node capacity |

#### Side Effects

- `AssignmentStore` read/write → zookeeper.

#### Runtime Characteristics

- **Contention**: all assignment mutations are serialized on a single-thread executor (`AssignmentManager` `assigner-%d`); assignment throughput is bound by that one thread.
- **Consistency**: operations are idempotent against the store — `assignShards` skips already-assigned shards; `unAssignShards` skips already-deleted ones; on partial failure it frees capacity it had reserved.

#### Notes for Coding Agents

- The placement algorithm is `AssignmentStrategy`; swap/extend via a new strategy rather than editing the manager. In-memory node capacity must stay consistent with the store — go through this component, don't write `AssignmentStore` elsewhere.
- The single-thread executor is the serialization guarantee; moving to a pool changes assignment safety.

---

### varadhi-controller.event-distributor — Entity Event Distributor

**Archetype**: Cross-Cutting Provider
**Packages**: `controller.events` (`ResourceEventProcessor`), `controller` (`DefaultMetaStoreChangeListener`)
**Public Interface**: `ResourceEventProcessor` (`create`, `onChange`, `close`) implementing `ResourceEventListener`.

#### Responsibility

Keeps cluster-wide caches coherent. It watches metastore entity changes, translates them into `ResourceEvent`s (UPSERT/INVALIDATE) via `DefaultMetaStoreChangeListener`, and fans each event out to **all** cluster nodes over the event bus. Each node has its own virtual-thread sender with retry, and a committer marks the change processed only once every participating node has acknowledged it.

#### Collaborators

| Communicates With | Direction | Protocol | Purpose |
|---|---|---|---|
| `shared.backend-spi` | called-by | ZK watch | Receive metastore change events (`registerEventListener`) |
| `shared.cluster-rpc` → all nodes | calls | event bus | Fan out entity events (`ENTITY_EVENTS_HANDLER`) |
| `shared.cluster-rpc` | calls | method call | Membership (own listener for sender queues) |

#### Side Effects

- Metastore change **watch** (`registerEventListener`); entity-event fan-out → all nodes (event bus).

#### Runtime Characteristics

- **Completion semantics**: an event is committed (and the source metastore change marked processed) only after **every** participating node acknowledges it (`ResourceEventProcessor.EventCommitter` / `EventWrapper#isCompleteForAllNodes`). A slow or unreachable node delays that event's completion; per-node senders retry with backoff (Failsafe), and a node leaving removes it as a participant to unblock.
- **Concurrency**: one virtual-thread sender per cluster member plus one committer thread; senders are interrupted and queues cleared on `close()`.

#### Notes for Coding Agents

- This is the controller's cache-coherence mechanism — the source side of the `shared.resource-cache` that `varadhi-server`/`varadhi-consumer` read. Changing the all-nodes-ack contract changes the consistency guarantee consumers of that cache rely on.
- The metastore→`ResourceEvent` mapping (UPSERT vs INVALIDATE, supported entity types) lives in `DefaultMetaStoreChangeListener`; new cached entity types are added there.
