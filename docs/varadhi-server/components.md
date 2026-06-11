# varadhi-server — Components

## Overview

`varadhi-server` is a Vert.x application that runs with `member.roles:[Server]` and exposes Varadhi's HTTP surface: the **control-plane** API (`concept.org`, `concept.team`, `concept.project`, regions, `concept.topic`, `concept.subscription`, `concept.queue`, IAM) and the **produce** API. Its internal architecture is a **route + behaviour-chain** design rather than a layered framework: a single verticle ([`http-ingress`](#varadhi-serverhttp-ingress--http-ingress)) builds a route table from `RouteProvider`s and attaches an ordered chain of cross-cutting behaviours to each route — telemetry, authentication, body handling, hierarchy resolution, and authorization.

The components split along a **hybrid-by-ownership** line. Control-plane handlers are thin and delegate to shared `core` entity services, so they live inside `http-ingress`. Where the backing logic is the server's own code, the handler and its service form one vertical component: [`produce-service`](#varadhi-serverproduce-service--produce-service) (the only event-loop / non-blocking path, publishing `concept.message`s to the messaging stack), [`dlq-service`](#varadhi-serverdlq-service--dlq-service) (dead-letter browse/unsideline that fans out to the controller and consumer shards), and [`iam-policy-management`](#varadhi-serveriam-policy-management--iam-policy-management) (IAM policy CRUD, present only with the built-in authz).

Three components implement the cross-cutting chain as distinct mechanisms: [`authentication`](#varadhi-serverauthentication--authentication) and [`authorization`](#varadhi-serverauthorization--authorization-rbac) (both pluggable Policy/Guards; the built-in authz is an RBAC engine over a resource hierarchy) and [`request-telemetry`](#varadhi-serverrequest-telemetry--request-telemetry). The actual domain services, cluster RPC clients, read caches, and persistence/messaging contracts are **shared** (see [shared-components.md](../shared-components.md)), as is the process bootstrap.

## Component Summary

| Component | Archetype | Responsibility |
|---|---|---|
| `varadhi-server.http-ingress` | Inbound Gateway | Hosts the HTTP server, builds routes + the behaviour chain, serves control-plane requests via shared `core` services |
| `varadhi-server.authentication` | Policy / Guard | Pluggable request authentication; establishes user context or 401s |
| `varadhi-server.authorization` | Policy / Guard | Pluggable RBAC over the resource hierarchy; built-in provider evaluates role bindings |
| `varadhi-server.produce-service` | Application Service + Outbound Gateway | The produce use case: validate headers, filter, resolve/cache producer, publish to the messaging stack |
| `varadhi-server.iam-policy-management` | Application Service (conditional) | IAM policy CRUD persisted in the metastore; only wired with the built-in authz |
| `varadhi-server.dlq-service` | Application Service | Dead-letter browse/unsideline, fanning out to controller + consumer shards over the event bus |
| `varadhi-server.request-telemetry` | Cross-Cutting Provider | Per-request span/metrics/error-classification as a behaviour-chain stage |

## Components

### varadhi-server.http-ingress — HTTP Ingress

**Archetype**: Inbound Gateway
**Packages**: `web` (root), `web.routes`, `web.v1.admin`, `web.v1` (health)
**Public Interface**: [WebServerVerticle](/web/src/main/java/com/flipkart/varadhi/web/WebServerVerticle.java) is the boundary — it is the Server-role verticle and the container's composition root. Handlers plug into it through the [web.routes routing DSL](/web/src/main/java/com/flipkart/varadhi/web/routes) (`RouteProvider` / `RouteDefinition` / `RouteBehaviour`): callers register route definitions and per-route behaviours; the verticle owns building and serving the router. Control-plane handlers traffic in `concept.org` / `concept.team` / `concept.project` / `concept.topic` / `concept.subscription` / `concept.queue`.

#### Responsibility

Hosts the Server-role HTTP server and is the container's composition root: it constructs the entity services, builds the route table by collecting route definitions, and attaches the ordered per-route behaviour chain (telemetry → authn → body-limit → parse → hierarchy → authz). It directly owns the **control-plane** handlers (org/team/project/region/`concept.topic`/`concept.subscription`/`concept.queue` plus health) — these are thin and delegate to shared `core` entity services. It also instantiates and mounts the handlers owned by `produce-service`, `dlq-service`, and `iam-policy-management`. Route groups are gated by `APIUsecases` (ADMIN / PRODUCE / ALL), which is how a process can be a control-plane-only or produce-only server.

#### Collaborators

| Communicates With | Direction | Protocol | Purpose |
|---|---|---|---|
| external admin/control-plane clients | called-by | HTTP/JSON | Receive control-plane + health requests |
| `shared.app-bootstrap` | called-by | method-call | Verticle is deployed by `VaradhiApplication` for the Server role |
| `shared.entity-services` | calls | method-call | Execute control-plane operations (org/team/project/region/topic/subscription/queue) |
| `shared.cluster-rpc` → `varadhi-controller` | calls | event-bus | Subscription start/stop/state |
| `varadhi-server.authentication` / `varadhi-server.authorization` / `varadhi-server.request-telemetry` | calls | method-call | Apply per-route cross-cutting behaviours |

#### Side Effects

- Serves HTTP (Vert.x `HttpServer`). [WebServerVerticle](/web/src/main/java/com/flipkart/varadhi/web/WebServerVerticle.java)
- Control-plane metadata read/write — **indirect**, through shared `core` services. `http-ingress` owns no persistence of its own.

#### Runtime Characteristics

- **Contention/blocking**: control-plane handlers run **blocking on the Vert.x worker pool**, so worker-pool saturation throttles control-plane throughput; this is separate from the event-loop path used by produce. [WebServerVerticle](/web/src/main/java/com/flipkart/varadhi/web/WebServerVerticle.java)
- **Ordering significance**: the behaviour chain is order-sensitive — behaviours are sorted by their declared order and applied in that sequence. Authn precedes hierarchy precedes authz by design. [WebServerVerticle](/web/src/main/java/com/flipkart/varadhi/web/WebServerVerticle.java)

#### Notes for Coding Agents

- `WebServerVerticle` is the single composition root — new handlers/services are wired here. A new `RouteProvider` is mounted by adding it to the relevant route grouping.
- Do **not** reorder the behaviour chain; auth must run before authorization, which depends on the hierarchy stage. Change `RouteBehaviour` order only with intent.
- Keep control-plane handlers thin — business logic belongs in shared `core` services, not here. Adding persistence or external calls directly in a handler changes the component's failure profile.
- Respect `APIUsecases` gating when adding routes (admin vs produce), or you break the control-plane-only / produce-only split.

---

### varadhi-server.authentication — Authentication

**Archetype**: Policy / Guard
**Packages**: `web.authn`, `web.configurators` (`AuthnConfigurator`), `web-spi.web.spi.authn`
**Public Interface**: [AuthnConfigurator](/web/src/main/java/com/flipkart/varadhi/web/configurators/AuthnConfigurator.java) is the boundary — it contributes the `authenticated` route behaviour that `http-ingress` applies. Pluggability is the [AuthenticationHandlerProvider](/web-spi/src/main/java/com/flipkart/varadhi/web/spi/authn/AuthenticationHandlerProvider.java) SPI (provider / options): integrators supply an authentication scheme through it rather than editing the chain.

#### Responsibility

Authenticates inbound requests using a handler **loaded reflectively from configuration**, establishes the request-scoped `USER_CONTEXT`, and rejects with 401 when no user context is produced. Ships several handlers — header-based (default), anonymous, and custom — but the mechanism is provider-agnostic; the real authentication scheme is a deployment choice.

#### Collaborators

| Communicates With | Direction | Protocol | Purpose |
|---|---|---|---|
| `varadhi-server.http-ingress` | called-by | method-call | Applied as the `authenticated` stage on protected routes |
| ext.identity-provider | calls | method-call | Delegate the actual authentication (reflective `AuthenticationHandlerProvider`) |

#### Side Effects

None external — sets request-scoped user context; may fail the request 401.

#### Runtime Characteristics

- **Failure mode**: **fail-closed** — if no `USER_CONTEXT` is established the request is rejected `UNAUTHORIZED`. Misconfiguration of the provider class fails verticle startup. [AuthnConfigurator](/web/src/main/java/com/flipkart/varadhi/web/configurators/AuthnConfigurator.java)

#### Notes for Coding Agents

- The authentication mechanism is pluggable; do not hardcode assumptions about header-based vs token-based auth. Extend by providing an `AuthenticationHandlerProvider`, not by editing the chain.
- The `web-spi` authn interfaces are this component's public contract — changing them affects every custom provider.
- Health check is intentionally `unAuthenticated`; preserve that when touching route behaviours.

---

### varadhi-server.authorization — Authorization (RBAC)

**Archetype**: Policy / Guard
**Packages**: `web.authz` (decision logic), `web.hierarchy`, `web.configurators` (`AuthzConfigurator`), `web-spi.web.spi.authz`
**Public Interface**: [AuthzConfigurator](/web/src/main/java/com/flipkart/varadhi/web/configurators/AuthzConfigurator.java) is the boundary — it contributes the `authorized` route behaviour. The decision engine is pluggable behind the [AuthorizationProvider](/web-spi/src/main/java/com/flipkart/varadhi/web/spi/authz/AuthorizationProvider.java) SPI; the built-in [DefaultAuthorizationProvider](/web/src/main/java/com/flipkart/varadhi/web/authz/DefaultAuthorizationProvider.java) is an RBAC engine over the `concept.org` → `concept.team` → `concept.project` → leaf resource hierarchy.

#### Responsibility

Enforces authorization for control-plane and produce actions. The built-in provider builds the leaf→root resource hierarchy for the request (ORG → TEAM → PROJECT → TOPIC|SUBSCRIPTION|REGION) via `web.hierarchy`, looks up role bindings for the subject, and checks whether any bound role grants the required permission, with a superuser bypass. The `addHierarchy` behaviour stage exists to produce the resource path this engine consumes.

#### Collaborators

| Communicates With | Direction | Protocol | Purpose |
|---|---|---|---|
| `varadhi-server.http-ingress` | called-by | method-call | Applied as the `authorized` stage |
| `varadhi-server.iam-policy-management` | calls | method-call | Read role bindings (built-in provider only) |
| ext.authorization-provider | calls | method-call | Authorization decisions when a custom/external provider is configured |
| `shared.metadata-spi` | calls | ZK / Curator | Role-binding/policy lookups via its **own** `MetaStoreProvider` (built-in provider only) |

#### Side Effects

- Metastore reads for role bindings / IAM policies, via a **separate** `MetaStoreProvider` instance that the built-in provider initializes from its own config — distinct from the connection `http-ingress` uses. [DefaultAuthorizationProvider](/web/src/main/java/com/flipkart/varadhi/web/authz/DefaultAuthorizationProvider.java)

#### Runtime Characteristics

- **Failure mode**: **fail-closed**. A missing policy yields no roles → deny; superusers bypass entirely. If its metastore connection is unavailable, lookups fail and authorized requests cannot be served — authorization init also fails hard. [DefaultAuthorizationProvider](/web/src/main/java/com/flipkart/varadhi/web/authz/DefaultAuthorizationProvider.java)
- **Consistency**: reads role bindings from the metastore through `iam-policy-management`; freshness follows that store. [DefaultAuthorizationProvider](/web/src/main/java/com/flipkart/varadhi/web/authz/DefaultAuthorizationProvider.java)

#### Notes for Coding Agents

- This component opens its **own** metastore connection — operational changes to metastore connectivity affect authz independently of the rest of the server. Keep this in mind when changing metastore config.
- The resource-hierarchy construction (`web.hierarchy`) encodes the org→team→project→leaf authorization model; changing path parsing changes who can access what.
- Authorization is pluggable — a custom `AuthorizationProvider` replaces the built-in RBAC (and then `iam-policy-management` is not wired; see below).

---

### varadhi-server.produce-service — Produce Service

**Archetype**: Application Service + Outbound Gateway
**Packages**: `web.v1.producer`, `producer.produce`, `producer.produce.telemetry`
**Public Interface**: [ProduceHandlers](/web/src/main/java/com/flipkart/varadhi/web/v1/producer/ProduceHandlers.java) (the produce HTTP route) over [ProducerService](/producer/src/main/java/com/flipkart/varadhi/produce/ProducerService.java) is the boundary: callers produce a `concept.message` to a `concept.topic`, and the service resolves the storage producer and publishes. Exact signatures are behind these facades.

#### Responsibility

Owns the message-produce use case end to end: filter/normalize compliant headers and validate header semantics/size, build the `concept.message`, evaluate the org NFR (server-side) `concept.filter`, resolve and cache the storage producer, publish asynchronously to the messaging stack, map produce status to an HTTP code, and record produce metrics. This is the container's hot path and its sole Outbound Gateway to the messaging stack.

#### Collaborators

| Communicates With | Direction | Protocol | Purpose |
|---|---|---|---|
| produce HTTP clients | called-by | HTTP/JSON | Receive produce requests (via `http-ingress` behaviour chain) |
| `shared.messaging-spi` → pulsar | calls | Pulsar client | Publish `concept.message`s to the storage producer |
| `shared.resource-cache` | calls | method-call | Topic/project/org lookups + NFR filter inputs |

#### Side Effects

- **Messaging-stack publish** — the owned external effect. [ProducerService](/producer/src/main/java/com/flipkart/varadhi/produce/ProducerService.java)
- In-process **Caffeine producer cache** (state). [ProducerService](/producer/src/main/java/com/flipkart/varadhi/produce/ProducerService.java)
- Produce metrics → otel-collector.

#### Runtime Characteristics

- **Contention/blocking**: the produce route is the only **`nonBlocking` / event-loop** route. Any blocking work introduced on this path stalls the event loop — keep it async. [ProduceHandlers](/web/src/main/java/com/flipkart/varadhi/web/v1/producer/ProduceHandlers.java)
- **Consistency**: producers are cached in a Caffeine cache with an access-based expiry (a tunable knob); a producer is reused until it expires. Topic/project/org reads come from eventually-consistent shared caches (`shared.resource-cache`), so very recent metadata changes may not be reflected immediately. [ProducerService](/producer/src/main/java/com/flipkart/varadhi/produce/ProducerService.java)
- **Failure mode**: a missing/inactive `concept.topic` throws `ResourceNotFoundException`; produce outcomes map to HTTP codes (blocked/not-allowed, throttled, failed). Capacity-based throttling is expected behaviour, not a fault. [ProducerService](/producer/src/main/java/com/flipkart/varadhi/produce/ProducerService.java)

#### Notes for Coding Agents

- Keep this path non-blocking. Do not add synchronous DB/metastore/HTTP calls in the produce handler or `ProducerService`.
- The producer cache key is `(varadhiTopicFQN, storageTopicId)`; changing the key shape affects producer reuse and in-flight producers.
- Header handling depends on deployment-configured names and non-compliant-header filtering; don't hardcode header names.
- The org NFR filter is evaluated here on the produce path only — server-side filtering semantics live in this component.

---

### varadhi-server.iam-policy-management — IAM Policy Management

**Archetype**: Application Service (conditional)
**Packages**: `web.authz` (`IamPolicyService`), `web.v1.authz` (`IamPolicyHandlers`)
**Public Interface**: [IamPolicyHandlers](/web/src/main/java/com/flipkart/varadhi/web/v1/authz/IamPolicyHandlers.java) (IAM policy HTTP routes) over [IamPolicyService](/web/src/main/java/com/flipkart/varadhi/web/authz/IamPolicyService.java) is the boundary — callers CRUD IAM policy / role-binding records; `IamPolicyService` is also the read path `authorization` consumes.

#### Responsibility

Manages IAM policy records / role bindings: CRUD exposed over HTTP and persisted in the metastore, plus the read path that `authorization` uses to resolve a subject's roles. It is the storage/management side of access control, kept separate from the RBAC decision engine. **Conditional**: it is wired only when the built-in `DefaultAuthorizationProvider` is configured.

#### Collaborators

| Communicates With | Direction | Protocol | Purpose |
|---|---|---|---|
| `varadhi-server.http-ingress` | called-by | method-call | IAM routes mounted into the router |
| `varadhi-server.authorization` | called-by | method-call | Role-binding reads during authorization |
| `shared.metadata-spi` | calls | ZK / Curator | IAM policy read/write via `IamPolicyStore` (spi) |

#### Side Effects

- IAM policy **read/write** to the metastore via `IamPolicyStore`. [IamPolicyService](/web/src/main/java/com/flipkart/varadhi/web/authz/IamPolicyService.java)

#### Runtime Characteristics

- **Conditional wiring**: routes are added only when the configured authz provider is `DefaultAuthorizationProvider`. With a custom/centralized external authz provider these handlers are absent and IAM policy management lives outside Varadhi. [WebServerVerticle](/web/src/main/java/com/flipkart/varadhi/web/WebServerVerticle.java)

#### Notes for Coding Agents

- Do not assume these routes always exist — they are tied to the built-in authz. Logic that depends on Varadhi-managed IAM must account for the external-authz case.
- `IamPolicyService` is shared between the HTTP CRUD surface and `authorization`'s read path; changes to its contract affect both.

---

### varadhi-server.dlq-service — DLQ Service

**Archetype**: Application Service
**Packages**: `web.v1.admin` (`DlqHandlers`), `web.subscription.dlq`
**Public Interface**: [DlqHandlers](/web/src/main/java/com/flipkart/varadhi/web/v1/admin/DlqHandlers.java) (DLQ HTTP routes) over [DlqService](/web/src/main/java/com/flipkart/varadhi/web/subscription/dlq/DlqService.java) is the boundary — callers browse and unsideline a `concept.subscription`'s `concept.dead-letter-queue` messages; the per-shard fan-out is internal.

#### Responsibility

Serves dead-letter browse and unsideline operations for a `concept.subscription`. Because a subscription is sharded across consumer nodes, it fans out — to the controller and **directly to consumer shards** over the Vert.x clustered event bus — and aggregates the per-shard responses. This is the only place `varadhi-server` talks directly to `varadhi-consumer`.

#### Collaborators

| Communicates With | Direction | Protocol | Purpose |
|---|---|---|---|
| `varadhi-server.http-ingress` | called-by | method-call | DLQ routes mounted into the router |
| `shared.cluster-rpc` → `varadhi-controller` | calls | event-bus | Coordinate DLQ operations |
| `shared.cluster-rpc` → `varadhi-consumer` | calls | event-bus | Browse/unsideline against consumer shards |
| `shared.entity-services` | calls | method-call | Subscription lookups |

#### Side Effects

- Event-bus RPC to `varadhi-controller` and (directly) to `varadhi-consumer` shards. No direct datastore writes of its own. [DlqService](/web/src/main/java/com/flipkart/varadhi/web/subscription/dlq/DlqService.java)

#### Runtime Characteristics

- **Failure mode**: a scatter-gather across consumer shards — results depend on shard availability; partial-shard unavailability affects DLQ responses. [ShardDlqMsgResponseCollector](/web/src/main/java/com/flipkart/varadhi/web/subscription/dlq/ShardDlqMsgResponseCollector.java) `[exact partial-failure handling TODO]`

#### Notes for Coding Agents

- This component is the source of the direct `varadhi-server → varadhi-consumer` edge (DLQ browse/unsideline over the event bus), reflected in `docs/containers.md`. If you change DLQ fan-out, keep that L2 edge in sync.
- DLQ fan-out targets shards by `concept.assignment`; correctness depends on the controller/consumer shard model — coordinate changes with those containers.

---

### varadhi-server.request-telemetry — Request Telemetry

**Archetype**: Cross-Cutting Provider
**Packages**: `web.configurators` (`RequestTelemetryConfigurator`, `MsgProduceRequestTelemetryConfigurator`), `web.metrics`
**Public Interface**: [RequestTelemetryConfigurator](/web/src/main/java/com/flipkart/varadhi/web/configurators/RequestTelemetryConfigurator.java) contributes the generic `telemetry` route behaviour; [MsgProduceRequestTelemetryConfigurator](/web/src/main/java/com/flipkart/varadhi/web/configurators/MsgProduceRequestTelemetryConfigurator.java) is the produce-tuned variant. `http-ingress` applies them as a behaviour-chain stage.

#### Responsibility

Provides per-request instrumentation as a behaviour-chain stage: span creation, API metrics, and HTTP error-type classification. It is the generic request-level telemetry mechanism, distinct from `produce-service`'s produce-specific metrics (the produce-tuned variant is applied to produce routes).

#### Collaborators

| Communicates With | Direction | Protocol | Purpose |
|---|---|---|---|
| `varadhi-server.http-ingress` | called-by | method-call | Applied as the `telemetry` stage |
| otel-collector | calls | OTLP (via Micrometer/OpenTelemetry) | Emit per-request metrics/traces |

#### Side Effects

- Per-request metrics/traces exported via Micrometer/OpenTelemetry → otel-collector.

#### Notes for Coding Agents

- This is a behaviour-chain stage, not request logic — keep instrumentation here rather than scattering metric/span emission across handlers.
- Watch metric label cardinality when adding tags (e.g. per-topic/per-route) to avoid metric explosion.
