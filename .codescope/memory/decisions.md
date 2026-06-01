# Codescope Decisions

Durable decisions made during codescope documentation workflows. Useful context for future doc efforts on this repo.

## System Context

Decisions from the `system-context` workflow (`docs/system-context.md`):

- **Project maturity**: Varadhi (OSS, `flipkart-incubator/varadhi`) is WIP — not yet productionized, APIs in *Draft*, SLAs/SLOs not finalized. Document accordingly; mark concrete availability/latency numbers as TODO.
- **Ownership**: Treated as Flipkart-incubated OSS. Maintainer contacts (sahil.chachan@, k.dhruv@) + GitHub Issues. No internal team/oncall published for the OSS distribution.
- **Pulsar & ZooKeeper classification**: **Internal infrastructure** (deployment prerequisites), not external dependencies. Documented under Operational Context + a "Backing Infrastructure" subsection, and **excluded from the outside-in system-context diagram**. They are pluggable SPI backends (Pulsar = messaging stack, ZK = metastore); Kafka is roadmap.
- **System-context diagram scope**: Minimal/outside-in. Actors = publisher/producer apps (produce REST) and administrators/operators (control-plane REST). Only delivery edge drawn = Varadhi → subscriber/consumer HTTP endpoints (push). Queue callbacks, metrics export (OTel/Prometheus), identity provider, and Pulsar/ZK are described in prose but NOT drawn.
- **Delivery model**: Push-based — consumers expose an HTTP endpoint Varadhi delivers to; there is no client pull/poll API. Treated as a key consumer-facing constraint.
- **Implementation-detail exclusion**: Class-level material in `docs/TOPIC_MODEL_STRUCTURE.md` (Global Topics, RegionName/RegionStatus, tags, factory methods) is internal/proposed design and was excluded from the system-context doc. Region/geo-replication is kept only as a stated capability and intended-topology note.
- **Intended deployment topology** (not finalized): global ZK for global metadata; geo-replicated Pulsar across regions; per region a web server (control-plane + produce, optionally split into control-plane-only + produce-only), a region-local controller (possibly with region-local ZK), and a consumer worker fleet. Component roles are configured via `member.roles` = [Server, Controller, Consumer].
- **Authentication ambiguity**: Default config uses a header-based handler (`UserHeaderAuthenticationHandler`); the OpenAPI spec models JWT bearer. Authn is pluggable and the production mechanism is not finalized — documented as such rather than asserting one.
- **Canonical doc sources**: GitHub Wiki (link-first; do not duplicate), in-repo `docs/api.yaml` + hosted Swagger UI (https://flipkart-incubator.github.io/varadhi/), and the public Flipkart blog on failure handling.

## Containers

Decisions from the `container-view` workflow (`docs/containers.md`):

- **Single image, role-based deployment**: Varadhi ships as one application image; `member.roles` ⊆ {Server, Controller, Consumer} selects behavior. Default config runs all three in one process (lean/local); Helm deploys them separately.
- **Modeled as 3 App containers**: `varadhi-server`, `varadhi-controller`, `varadhi-consumer` — distinct containers because they deploy/scale/fail independently in the target topology, despite sharing one codebase/image. Names are identifiers for downstream (component-level) workflows.
- **Pulsar & ZooKeeper are Infrastructure containers at L2** (they appeared as excluded backing-infra in L1's outside-in diagram — intended level escalation, not a discrepancy). Pulsar = messaging-stack SPI default; Kafka is roadmap.
- **ZooKeeper has a dual role**: (1) metastore (persisted entities), (2) Vert.x cluster manager (`VaradhiZkClusterManager`) for membership + event-bus coordination. Larger blast radius than a plain config store.
- **Intra-cluster transport = Vert.x clustered event bus** (`core/.../cluster/MessageExchange`). NOTE: `ControllerRestClient` and `ConsumerClient` are named "client"/"Rest" but both use the event bus (send/request), NOT HTTP REST — the names are misnomers.
- **varadhi-controller carries no data-path traffic** and is deployed with no k8s service + auth disabled; it coordinates via event bus + metastore only.
- **Observability**: otel-collector modeled as an Infrastructure container (in the OTLP export path); Prometheus + Grafana kept to prose/References, out of the diagram.
- **Optional per-container subsections**: only **Gotchas** included (user choice); Development / Key Config / Deployment & Sizing omitted. Gotchas are evidence-based only — none invented.
- **`messaging/` directory is a stale build artifact** (not in `settings.gradle`); excluded from the module/container analysis.
- **Inventory confirmed complete** by user: no frontends, cron jobs, serverless functions, gateways, admin UI, or out-of-repo services.

## Components — varadhi-server

Decisions from the component fact-gathering workflow (`.codescope/components/varadhi-server/facts.md`):

- **Container→code scope**: varadhi-server's own code = modules `web` + `web-spi` + `producer` + the Server-role parts of `server`. `controller`/`consumer` modules belong to the other containers (co-packaged today, separable by build).
- **Bootstrap is a SHARED component, not server's own code**: `VaradhiApplication` (+ `CuratorFrameworkCreator`, `LoggingSpanExporter`) is role-agnostic — it boots server/controller/consumer alike (deploys a verticle per `member.roles`). Treated as a shared-component candidate (user insight).
- **Shared in-repo modules** (`core`, `spi`, `entities`, `common`) noted as dependencies; significant ones (core cluster comms, core entity services/factories, core read caches, spi contracts) flagged as shared-component CANDIDATES. `docs/shared-components.md` deliberately NOT written yet (user: document later).
- **Routing is programmatic, not annotation-based**: handlers implement `RouteProvider#get()` → `RouteDefinition`; `WebServerVerticle` is the single composition root that builds routes + entity services. `APIUsecases` (ADMIN/PRODUCE/ALL) gates route groups — this is the mechanism enabling the L1/L2 "control-plane-only vs produce-only server" split.
- **Web handlers are thin**: real domain logic lives in `core` services (`VaradhiTopicService`, `VaradhiSubscriptionService`, etc.). `producer.ProducerService` (produce hot path, Caffeine producer cache, org NFR filter) and `web.authz`/`web.subscription.dlq` services are server's own logic.
- **AuthZ opens a SECOND metastore connection**: `DefaultAuthorizationProvider` initializes its own `MetaStoreProvider` from `authorizationConfig.yml`, separate from the web layer's `metaStore` — both target ZooKeeper.
- **L2 discrepancy (to fix out of band)**: `docs/containers.md` lacks a direct `varadhi-server → varadhi-consumer` edge, but DLQ browse/unsideline (`DlqService` + `ConsumerClientFactoryImpl`) calls consumers directly over the Vert.x event bus.
- **Health check is a static flag**: `HealthCheckHandler` returns `iam_ok`/200 (or 503 via `bringOOR()`); no real dependency probing yet (`// TODO: add appropriate checks`).

### Components — varadhi-server (Step 2: Identification)

7 components accepted (`.codescope/components/varadhi-server/accepted-components.md`):
`http-ingress` (Inbound Gateway), `produce-service` (App Service + Outbound Gateway), `authentication` (Policy/Guard), `authorization` (Policy/Guard), `iam-policy-management` (App Service, conditional), `dlq-service` (App Service), `request-telemetry` (Cross-Cutting Provider).

Durable boundary decisions:
- **Handler grouping = hybrid by ownership** (user): handlers backed by SHARED `core` services live in `http-ingress`; handlers whose backing service is server's OWN code form a vertical with it (`produce-service`⊃`ProduceHandlers`, `dlq-service`⊃`DlqHandlers`, `iam-policy-management`⊃`IamPolicyHandlers`). `WebServerVerticle` still instantiates/mounts all of them (it's the composition root).
- **iam-policy-management is separate from authorization** (Q2): policy storage/CRUD vs the RBAC decision engine are distinct. It is **conditional/pluggable** (user): wired only with the built-in `DefaultAuthorizationProvider`; with a custom/centralized authz provider the IAM handlers aren't hooked in and policy management is external to Varadhi.
- **authorization opens its own metastore connection** (separate from the gateway's), and owns `web.hierarchy` (which exists to produce the resource path authz consumes).
- **request-telemetry kept standalone** (Q4), parallel to authn/authz as behaviour-chain stages; distinct from produce-service's produce metrics.
- **producer cache stays inside produce-service** — not split into a separate State Manager.
- **`web-spi` auth contracts** folded into authentication/authorization as their pluggability interfaces.
- Bootstrap (`VaradhiApplication`) remains a shared component, excluded from server's component set.
- **Naming (user)**: `http-api-gateway` → **`http-ingress`** (HTTP entry into the container, not a multi-backend API gateway). `dlq-coordinator` → **`dlq-service`** (request-scoped scatter-gather across consumer shards, not standing orchestration; matches the `DlqService` class + App Service archetype).
