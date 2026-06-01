# Shared Components

Cross-container, in-repo modules that Varadhi's app containers (`varadhi-server`, `varadhi-controller`, `varadhi-consumer`) depend on. They are **not** owned by any single container.

> Names follow `naming-conventions.md` (`shared.<role>`); the **Location** line in each entry maps the role name to its source module. Entries are evidence-based from the component fact-gathering passes; `Used By` and per-module depth expand as more containers are analyzed. Unverified depth is marked `[TODO]`.

## shared.app-bootstrap

**Location**: `server/src/main/java/com/flipkart/varadhi/VaradhiApplication.java`, `server/.../utils/CuratorFrameworkCreator.java`, `server/.../LoggingSpanExporter.java`
**Purpose**: Role-agnostic application bootstrap. Loads/validates configuration, builds `CoreServices`, creates the ZooKeeper-backed cluster manager and clustered Vert.x, initializes resource read caches + the cluster event dispatcher, and deploys the per-role verticle(s): `Server`→`WebServerVerticle`, `Controller`→`ControllerVerticle`, `Consumer`→`ConsumerVerticle`.
**Public API**: `VaradhiApplication#main`, `VaradhiApplication#getComponentVerticles` (role→verticle mapping), `CuratorFrameworkCreator#create`.
**Used By**: all three app containers (the role(s) a process runs are chosen by `member.roles`).
**Runtime Characteristics**: Single composition root. Fails fast at startup — validates the member region against the metastore (`VaradhiApplication#validateMemberRegion`) and exits the process on verticle-deploy failure (`VaradhiApplication#main` `onFailure` → `System.exit(-1)`).

## shared.cluster-rpc

**Location**: `core/src/main/java/com/flipkart/varadhi/core/cluster/*`
**Purpose**: Vert.x clustered event-bus abstraction and cluster membership for inter-container RPC, with ZooKeeper as the cluster manager.
**Public API**: `MessageExchange` (`#send` fire-and-track, `#request` await-response); `MessageRouter` — both the **sending** side and the **receiving** side (`#sendHandler` / `#requestHandler` register handlers keyed by node id); `VaradhiClusterManager` / `VaradhiZkClusterManager`; `ControllerApi` / `ControllerRestClient` (caller→controller), `ControllerConsumerClient` (consumer→controller op-state `#update`); `ConsumerApi` / `ConsumerClientFactoryImpl` / `ConsumerClient` (caller→consumer); `ResourceEventDispatcher`.
**Used By**:
- `varadhi-server` — `ControllerRestClient` (subscription lifecycle ops), `ConsumerClientFactoryImpl` (DLQ shard calls), `ResourceEventDispatcher` (cache freshness).
- `varadhi-consumer` — `MessageRouter#sendHandler`/`#requestHandler` (registers its inbound cluster-RPC handlers by consumerId), `ControllerConsumerClient#update` (reports shard-op completion/failure).
- also `varadhi-controller`.
**Runtime Characteristics**: `#request` awaits a response bounded by the configured send timeout (`DeliveryOptions`); transport rides the clustered event bus, so availability is tied to the ZooKeeper cluster manager. Despite the `*RestClient` name, transport is the event bus, not HTTP.

## shared.entity-services

**Location**: `core/src/main/java/com/flipkart/varadhi/core/` (`OrgService`, `TeamService`, `ProjectService`, `RegionService`, `VaradhiTopicService`, `VaradhiSubscriptionService`, `VaradhiQueueService`, `VaradhiTopicFactory`, `VaradhiSubscriptionFactory`, `ShardProvisioner`)
**Purpose**: Business logic for managing the resource hierarchy and messaging resources over the metastore and messaging-stack SPIs. `varadhi-server`'s control-plane HTTP handlers are thin shells over these services.
**Public API**: the `*Service` classes (CRUD + lifecycle of org/team/project/region/topic/subscription/queue), the `*Factory` classes, `ShardProvisioner`.
**Used By**: `varadhi-server` (`http-ingress` control-plane handlers; topic/subscription lookups); also controller (subscription lifecycle). 
**Runtime Characteristics**: Side effects target the metastore (zookeeper) and messaging stack (pulsar). `[TODO: per-service failure/consistency semantics — core not scanned in depth]`

## shared.resource-cache

**Location**: `core/.../ResourceReadCache`, `core/.../ResourceReadCacheRegistry`, `core/.../OrgReadCache`
**Purpose**: In-process caches of ORG/PROJECT/TOPIC resources, preloaded from the metastore at startup and kept fresh via cluster entity events. Serves hot-path reads (notably produce) without hitting the metastore per request.
**Public API**: `ResourceReadCache#get` / `#create` / `#preload`, `ResourceReadCacheRegistry#register` / `#getCache`, `OrgReadCache`.
**Used By**: `varadhi-server` (`produce-service` topic/project/org lookups + NFR filter; control-plane handlers' project lookups).
**Runtime Characteristics**: **Eventually consistent** — updates arrive asynchronously via `ResourceEventDispatcher` cluster events; reads can be stale within the event-propagation window. Preloaded at startup (`VaradhiApplication#initializeEventManager`). `[TODO: staleness window magnitude]`

## shared.backend-spi

**Location**: `spi/src/main/java/com/flipkart/varadhi/spi/` (`db`: `MetaStore`, `MetaStoreProvider`, `IamPolicyStore`, `*Store`; `services`: `Producer`, `ProducerFactory`, `MessagingStackProvider`, `StorageTopicService`, `StorageSubscriptionService`)
**Purpose**: Pluggable interfaces abstracting the metadata store (ZooKeeper default, via `metastore-zk`) and the messaging stack (Pulsar default, via `pulsar`).
**Public API**: `MetaStore` (`orgs()`/`teams()`/`projects()`/`regions()`/`topics()`/`subscriptions()`, `IamPolicyStore.Provider#iamPolicies()`), `MetaStoreProvider`; **messaging** — `Producer#produceAsync` + `ProducerFactory#newProducer`, `Consumer` (+ `Offset`) + `ConsumerFactory#newConsumer`, `MessagingStackProvider` (`getProducerFactory`/`getConsumerFactory`/`getStorageTopicService`/`getStorageSubscriptionService`/`getSubscriptionFactory`/`getStorageTopicFactory`).
**Used By**:
- `varadhi-server` — metastore access, IAM policy store, `Producer`/`ProducerFactory` (produce), provisioning services.
- `varadhi-consumer` — `Consumer`/`ConsumerFactory` (consume main + retry topics), `Producer`/`ProducerFactory` (produce to retry/DLQ). No direct `MetaStore` use found.
- also `varadhi-controller`.
Implemented by `metastore-zk` (→ zookeeper) and `pulsar` (→ pulsar).
**Runtime Characteristics**: Contracts only; runtime behavior is the implementation's (see the `zookeeper` and `pulsar` infrastructure containers in `docs/containers.md`).

## shared.commons

**Location**: `common/`, `entities/`
**Purpose**: `common` = shared utilities (`Constants`, `Result`, exceptions, host/reflection helpers); `entities` = the domain model (`Message`, `VaradhiTopic`, `Org`/`Team`/`Project`, auth types, `StdHeaders`, etc.).
**Public API**: data types + helpers used across all modules.
**Used By**: all modules.
**Runtime Characteristics**: None of architectural note — foundational libraries.
