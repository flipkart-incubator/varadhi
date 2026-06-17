# Shared Components

Cross-container, in-repo modules that Varadhi's app containers (`varadhi-server`, `varadhi-controller`, `varadhi-consumer`) depend on. They are **not** owned by any single container.

## shared.app-bootstrap

**Location**: [VaradhiApplication](/server/src/main/java/com/flipkart/varadhi/VaradhiApplication.java), [CuratorFrameworkCreator](/server/src/main/java/com/flipkart/varadhi/utils/CuratorFrameworkCreator.java)
**Purpose**: Role-agnostic application bootstrap. Loads/validates configuration, builds `CoreServices`, creates the ZooKeeper-backed cluster manager and clustered Vert.x, initializes the resource read caches + the cluster event dispatcher, and deploys the per-role verticle(s) selected by `member.roles` (`Server`/`Controller`/`Consumer`).
**Public Interface**: [VaradhiApplication](/server/src/main/java/com/flipkart/varadhi/VaradhiApplication.java) is the composition root — the process `main` boots the app through it and gets the role→verticle wiring; [CuratorFrameworkCreator](/server/src/main/java/com/flipkart/varadhi/utils/CuratorFrameworkCreator.java) builds the shared ZooKeeper client. Internal wiring (verticle construction, region validation) is not a caller surface.
**Used By**: all three app containers (the role(s) a process runs are chosen by `member.roles`).
**Runtime Characteristics**: Single composition root. Fails fast at startup — validates the member region against the metastore and exits the process on verticle-deploy failure. [VaradhiApplication](/server/src/main/java/com/flipkart/varadhi/VaradhiApplication.java)

## shared.cluster-rpc

**Location**: [core.cluster package](/core/src/main/java/com/flipkart/varadhi/core/cluster)
**Purpose**: The Vert.x clustered event-bus abstraction and cluster membership used for inter-container RPC, with ZooKeeper as the cluster manager. It is also the transport that distributes `concept.entity-change-event`s to all nodes for cache coherence.
**Public Interface**: Callers send/receive cluster RPC through [MessageExchange](/core/src/main/java/com/flipkart/varadhi/core/cluster/MessageExchange.java) (fire-and-track send / await-response request) and register inbound handlers through [MessageRouter](/core/src/main/java/com/flipkart/varadhi/core/cluster/MessageRouter.java); membership is observed through [VaradhiClusterManager](/core/src/main/java/com/flipkart/varadhi/core/cluster/VaradhiClusterManager.java) (a node-id-keyed member snapshot plus join/leave listeners). On top of the manager, [ClusterMembershipView](/core/src/main/java/com/flipkart/varadhi/core/cluster/ClusterMembershipView.java) keeps a live in-memory member index and [PodCountProvider](/core/src/main/java/com/flipkart/varadhi/core/cluster/PodCountProvider.java) derives a change-notifying live pod count (optionally filtered by role) — the source of the per-pod even split in produce rate limiting. The controller↔consumer RPC contracts and `concept.entity-change-event` distribution ([ResourceEventDispatcher](/core/src/main/java/com/flipkart/varadhi/core/cluster/events/ResourceEventDispatcher.java)) ride on top of these. Concrete client/handler types are implementation detail behind these facades.
**Used By**:
- `varadhi-server` — calls the controller for subscription lifecycle ops and consumer shards for DLQ calls; dispatches `concept.entity-change-event`s for cache freshness; observes live membership/pod count (`PodCountProvider`) for produce rate limiting.
- `varadhi-consumer` — registers inbound cluster-RPC handlers (keyed by consumerId) and reports shard-op completion/failure to the controller.
- `varadhi-controller` — implements the controller RPC contracts, calls consumers, tracks membership, and fans `concept.entity-change-event`s to **all** nodes.
**Runtime Characteristics**: `request` awaits a response bounded by the configured send timeout (a tunable knob); transport rides the clustered event bus, so availability is tied to the ZooKeeper cluster manager. Despite the `*RestClient` naming, transport is the event bus, not HTTP. [MessageExchange](/core/src/main/java/com/flipkart/varadhi/core/cluster/MessageExchange.java)

## shared.entity-services

**Location**: [core package](/core/src/main/java/com/flipkart/varadhi/core) (`*Service`, `*Factory`, [ShardProvisioner](/core/src/main/java/com/flipkart/varadhi/core/subscription/ShardProvisioner.java))
**Purpose**: Business logic for managing the resource hierarchy (`concept.org`, `concept.team`, `concept.project`) and messaging resources (`concept.topic`, `concept.subscription`, `concept.queue`) over the metastore and messaging-stack SPIs. `varadhi-server`'s control-plane HTTP handlers are thin shells over these services.
**Public Interface**: The `*Service` facades — [OrgService](/core/src/main/java/com/flipkart/varadhi/core/OrgService.java), [VaradhiTopicService](/core/src/main/java/com/flipkart/varadhi/core/VaradhiTopicService.java), [VaradhiSubscriptionService](/core/src/main/java/com/flipkart/varadhi/core/VaradhiSubscriptionService.java), and peers for team/project/region/queue: callers perform CRUD + lifecycle on the resource-hierarchy and messaging `concept.*` entities through them. Entity construction ([ShardProvisioner](/core/src/main/java/com/flipkart/varadhi/core/subscription/ShardProvisioner.java) and the `*Factory` types) sits behind these services. Topic creation ([VaradhiTopicFactory](/core/src/main/java/com/flipkart/varadhi/core/topic/VaradhiTopicFactory.java)) defaults the per-topic rate-limiter mode, validates that configured capacity can sustain the declared message-size profile, and resolves per-region produce-quota weights at the create boundary.
**Used By**: `varadhi-server` (`http-ingress` control-plane handlers; topic/subscription lookups); also `varadhi-controller` (subscription lifecycle).
**Runtime Characteristics**: Side effects target the metastore and messaging stack. `[TODO: per-service failure/consistency semantics — core not scanned in depth]`

## shared.resource-cache

**Location**: [ResourceReadCache](/core/src/main/java/com/flipkart/varadhi/core/ResourceReadCache.java), [ResourceReadCacheRegistry](/core/src/main/java/com/flipkart/varadhi/core/ResourceReadCacheRegistry.java)
**Purpose**: In-process caches of `concept.org` / `concept.project` / `concept.topic` resources, preloaded from the metastore at startup and kept fresh via `concept.entity-change-event`s. Serves hot-path reads (notably produce) without hitting the metastore per request.
**Public Interface**: [ResourceReadCache](/core/src/main/java/com/flipkart/varadhi/core/ResourceReadCache.java) is the read boundary (get / create / preload a cache); [ResourceReadCacheRegistry](/core/src/main/java/com/flipkart/varadhi/core/ResourceReadCacheRegistry.java) registers and hands out the per-resource caches. Consumers may also register an invalidation listener ([ResourceReadCache#addOnInvalidate](/core/src/main/java/com/flipkart/varadhi/core/ResourceReadCache.java)) to drop derived per-entity state when a resource is evicted — the cache is the authoritative source for such cleanup.
**Used By**: `varadhi-server` (`produce-service` topic/project/org lookups + NFR filter; control-plane handlers' project lookups; `produce-rate-limiter` topic-eviction cleanup).
**Runtime Characteristics**: **Eventually consistent** — updates arrive asynchronously via `concept.entity-change-event` distribution, so reads can be stale within the event-propagation window. [ResourceReadCache](/core/src/main/java/com/flipkart/varadhi/core/ResourceReadCache.java) Preloaded at startup. [VaradhiApplication](/server/src/main/java/com/flipkart/varadhi/VaradhiApplication.java)

## shared.metadata-spi

**Location**: [spi.db package](/spi/src/main/java/com/flipkart/varadhi/spi/db)
**Purpose**: Pluggable interface abstracting the metadata store (metastore SPI) — the seam behind which ZooKeeper is the default implementation. Persists the resource hierarchy (`concept.org`, `concept.team`, `concept.project`, `concept.topic`, `concept.subscription`), `concept.assignment`s, and operation state.
**Public Interface**: [MetaStore](/spi/src/main/java/com/flipkart/varadhi/spi/db/MetaStore.java) / [MetaStoreProvider](/spi/src/main/java/com/flipkart/varadhi/spi/db/MetaStoreProvider.java) are the boundary: callers read/write the resource-hierarchy entities, the `concept.assignment` and operation stores, and the IAM policy store, and register a change-watch over metastore mutations (the source of `concept.entity-change-event`s). Concrete `*Store` types are detail behind the provider.
**Used By**:
- `varadhi-server` — metastore access; IAM policy store.
- `varadhi-controller` — `concept.assignment` store, operation persistence, subscription reads, and the metastore change **watch**.
- `varadhi-consumer` — no direct `MetaStore` use found.
Implemented by `metastore-zk` (→ zookeeper).
**Runtime Characteristics**: Contracts only; runtime behavior is the implementation's (see the `zookeeper` infrastructure container in [containers.md](./containers.md)).

## shared.messaging-spi

**Location**: [spi.services package](/spi/src/main/java/com/flipkart/varadhi/spi/services)
**Purpose**: Pluggable interface abstracting the messaging stack (messaging SPI) — the seam behind which Pulsar is the default implementation. Produces and consumes `concept.message`s against `concept.storage-topic`s.
**Public Interface**: [MessagingStackProvider](/spi/src/main/java/com/flipkart/varadhi/spi/services/MessagingStackProvider.java) is the boundary: it vends producer/consumer factories ([Producer](/spi/src/main/java/com/flipkart/varadhi/spi/services/Producer.java)) and the storage-topic/subscription services through which `concept.message`s are produced and consumed against `concept.storage-topic`s. Concrete factory types are detail behind the provider.
**Used By**:
- `varadhi-server` — producer factory (produce); storage-topic/subscription provisioning.
- `varadhi-consumer` — consumer/producer factories (consume main + retry topics; produce to retry/DLQ).
Implemented by `pulsar` (→ pulsar).
**Runtime Characteristics**: Contracts only; runtime behavior is the implementation's (see the `pulsar` infrastructure container in [containers.md](./containers.md)).

## shared.commons

**Location**: [common](/common), [entities](/entities)
**Purpose**: `common` = shared utilities (constants, result/exception types, host/reflection helpers); `entities` = the code types behind the domain model — including the classes that back `concept.message`, `concept.topic`, `concept.org` / `concept.team` / `concept.project`, auth types, and message headers (`StdHeaders`).
**Public Interface**: Foundational data types + helpers imported across all modules; no behavioral facade.
**Used By**: all modules.
**Runtime Characteristics**: None of architectural note — foundational libraries.
