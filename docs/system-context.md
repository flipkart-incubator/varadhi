# Varadhi (RESTBus)

> **Status:** Open-source, work-in-progress. APIs are in *Draft*; the system is not yet productionized and SLAs are not finalized. This document describes Varadhi from the **outside-in** — what it is, what it offers, and how to integrate with it.

## Overview

See: [Varadhi Wiki — Home](https://github.com/flipkart-incubator/varadhi/wiki) and [Main Concepts](https://github.com/flipkart-incubator/varadhi/wiki/Main-Concepts).

Varadhi is a multi-tenant **message bus with a REST/HTTP interface** ("RESTBus"). It takes an application's HTTP API stack and turns it into service-bus-driven, queue/pub-sub, message-oriented endpoints — the communication between Varadhi and applications stays over HTTP. Senders produce messages over HTTP; Varadhi durably persists them and **pushes** them to consumer application endpoints, handling ordering and failure recovery centrally so applications don't have to.

It supports both **Publish/Subscribe** (topics with one or more independent subscriptions) and **Point-to-Point** (queues, with optional request/response callbacks). Varadhi is the open-source version of a system that has run inside Flipkart for ~10 years as the backbone of async REST communication between microservices.

## Owners

Flipkart-incubated open-source project: [`flipkart-incubator/varadhi`](https://github.com/flipkart-incubator/varadhi).

- **Maintainer contacts:** sahil.chachan@flipkart.com, k.dhruv@flipkart.com
- **Bugs / feedback / feature requests:** [GitHub Issues](https://github.com/flipkart-incubator/varadhi/issues)
- **Contributing:** [CONTRIBUTING.md](../CONTRIBUTING.md)

There is no published internal oncall/escalation for the OSS distribution; operators who deploy Varadhi own its operation in their environment.

## Users & Actors

| Actor | Interaction |
|---|---|
| **Publisher / producer applications** | Produce messages to a topic or queue over the HTTP produce API. |
| **Subscriber / consumer applications** | Expose an HTTP endpoint that Varadhi **pushes** delivered messages to (push-based delivery, not pull). For queues, may receive request/response callbacks. |
| **Administrators** | Manage the resource hierarchy and resources (orgs, teams, projects, topics, subscriptions, IAM role bindings) via the control-plane REST API. |
| **Platform operators / SREs** | Deploy and operate Varadhi and its backing infrastructure; manage regions, scaling, and observability. |

## Capabilities

What Varadhi provides to its consumers (see [Main Concepts](https://github.com/flipkart-incubator/varadhi/wiki/Main-Concepts) for detail):

- **Pub/Sub messaging** — produce to a topic; one or more independent subscriptions each receive the full message stream (broadcast / choreography).
- **Point-to-Point queues** — each message carries its destination endpoint; optional callback enables async request/response and orchestration patterns.
- **Push delivery with at-least-once guarantee** — Varadhi delivers messages to the subscription's configured HTTP endpoint and tracks success/failure.
- **Failure handling** — retriable (soft) failures go to **Retry Queues** (configurable RetryPolicy); non-retriable (hard) failures go to **Dead Letter Queues** for later, explicit redelivery. See [Effective Failure Handling in Flipkart's Message Bus](https://blog.flipkart.tech/effective-failure-handling-in-flipkarts-message-bus-436c36be76cc).
- **Message ordering / grouping** — *intended* per-**GroupId** ordering (see [Message Ordering](https://github.com/flipkart-incubator/varadhi/wiki/Message-Ordering)); produce can route by GroupId, but **end-to-end grouped delivery on the consumer is not implemented yet**.
- **Server-side filtering** — subscriptions can filter on message headers so consumers receive only messages of interest (topics/subscriptions only, not queues).
- **Multi-tenancy** — hierarchical Org → Team → Project isolation with RBAC/IAM. See [Tenancy Model](https://github.com/flipkart-incubator/varadhi/wiki/Tenancy-Model).
- **Pluggable backends** — messaging stack and metadata store are behind SPIs (Apache Pulsar and ZooKeeper are the default implementations).
- **Observability** — Micrometer metrics exported via OpenTelemetry (OTLP), plus distributed tracing. See [Metrics Documentation](https://github.com/flipkart-incubator/varadhi/wiki/Varadhi-Metrics-Documentation).

## System Boundary

### In Scope
- HTTP message ingestion (produce) with authentication, authorization, and configurable header validation.
- Topic-based pub/sub and point-to-point queue delivery (with optional callbacks).
- Push delivery to consumer HTTP endpoints with at-least-once semantics.
- Retry Queues and Dead Letter Queues for soft/hard delivery failures.
- Ordered (grouped) delivery at GroupId granularity *(intended; consumer delivery path not implemented yet)*.
- Server-side, header-based message filtering.
- Multi-tenant resource hierarchy and RBAC/IAM administration.
- Storage-backend and metastore abstraction (Pulsar / ZooKeeper defaults).
- Multi-region replication and failover orchestration (partly implemented / in progress).

### Out of Scope
- Message payload transformation or enrichment (payload is treated as opaque bytes).
- Schema registry / schema validation *(roadmap)*.
- Exactly-once delivery and message deduplication *(roadmap)*.
- Scheduled / delayed delivery and message replay *(roadmap)*.
- Encryption at rest, masking/data-protection *(roadmap)*.
- Consumer offset management exposed to clients (delivery is push-based and managed by Varadhi).

See the [Roadmap](https://github.com/flipkart-incubator/varadhi/wiki/Roadmap) for planned features.

## External Dependencies

**Classification test** — decides what each dependency *is* and where it lives:
- **External service** — a remote system Varadhi **calls but does not run** (a deployment's token issuer, a centralized authorization service). Minted here as an `ext.<name>` node and referenced by ID from L2–L4.
- **Datastore / broker / coordination** — even when externally hosted, Pulsar and ZooKeeper are run-or-managed **infrastructure** → **containers** (`containers.md`), not `ext.`.
- **Actor** — humans or client apps that call *Varadhi* (producers, administrators) or receive push delivery at a **per-subscription URL** (subscriber endpoints). Actors and per-subscription delivery targets stay free-form labels — they are not a single greppable `ext.` node.

### External Services

Services Varadhi **calls but does not run**. Each is defined **once** here with a node-defining `ext.<name>` heading and referenced by that ID elsewhere.

#### ext.identity-provider — Identity Provider
**Relationship**: depends-on (authentication)
**Purpose**: Issues or validates credentials for API authentication. The default deployment uses a header-based handler; the OpenAPI spec models JWT bearer auth. Authentication is pluggable — the production mechanism is not finalized; integrators supply an `AuthenticationHandlerProvider` implementation.

#### ext.authorization-provider — Authorization Provider
**Relationship**: depends-on (authorization, optional)
**Purpose**: External RBAC / policy decision service used **instead of** the built-in `DefaultAuthorizationProvider`. When configured, IAM policy management inside Varadhi is not wired and role bindings live in this provider.

### Backing Infrastructure (deployment prerequisites)
These are deployed **for** Varadhi and operated as part of it (not external integrations). They are documented here and under [Operational Context](#operational-context), and intentionally **excluded from the outside-in diagram**.

| Resource | Type | Relationship | Purpose |
|---|---|---|---|
| Apache Pulsar | Message broker (messaging-stack SPI default) | persists / delivers | Durable message storage and delivery substrate; geo-replicated across regions in the intended topology. Apache Kafka support is on the roadmap. |
| ZooKeeper | Coordination / metadata store (metastore SPI default) | reads / writes | Stores Varadhi metadata (orgs, teams, projects, topics, subscriptions, role bindings, assignments). |

### Platform / Observability
Listed for operators; **not shown in the diagram** to keep the outside-in view focused.

| System | Purpose |
|---|---|
| OpenTelemetry collector → Prometheus / Grafana | Receives OTLP metrics/traces for monitoring and visualization (`otel-collector` is a **container**; Prometheus/Grafana are downstream). |

## Public Concepts

Canonical reference: [Main Concepts](https://github.com/flipkart-incubator/varadhi/wiki/Main-Concepts) and [Tenancy Model](https://github.com/flipkart-incubator/varadhi/wiki/Tenanacy-Model).

### concept.message — Message
A two-part entity: an opaque **payload** (raw bytes — Varadhi attaches no semantics) and **metadata** carried as HTTP request **headers** that tell Varadhi how to handle it. See [Message Configurability](https://github.com/flipkart-incubator/varadhi/wiki/Message-Configurability).
- *Gotcha:* header names are **configurable per deployment** (e.g. `X_MESSAGE_ID`, `X_GROUP_ID`). Don't hardcode names; confirm the target deployment's convention. A Message ID header is required; Group ID is required only for grouped topics.

### concept.topic — Topic
A named stream of messages, identified globally as `{project}/{topic}`. Supports pub/sub and broadcast. Has a `grouped` flag (ordering) and a capacity policy (throughput/QPS guard rails).

### concept.subscription — Subscription
A named, **push-based** consumer of a topic, identified as `{project}/{subscription}`. Defines the delivery endpoint, RetryPolicy, ConsumptionPolicy, optional filter, and ordered/unordered delivery intent. A topic can have many independent subscriptions. **Per-GroupId ordered delivery is not implemented on the consumer yet** — only unordered delivery is live.

### concept.queue — Queue
A topic + auto-created subscription pair for **point-to-point** delivery; each message carries its destination endpoint. Optional **callback** enables request/response. Users cannot create subscriptions on a queue. Queues do **not** support filtering.

### concept.retry-queue — Retry Queue
Destination for retriable (soft) delivery failures; messages are re-attempted from here per the subscription's RetryPolicy.

### concept.dead-letter-queue — Dead Letter Queue
Destination for non-retriable (hard) delivery failures; messages land here for explicit, operator/consumer-initiated redelivery (managed via the control-plane API).

### concept.filter — Filter
A condition over message headers, evaluated on the **first** delivery attempt only; non-matching messages are treated as delivered for bookkeeping. Topic/subscription only.

### concept.grouping — Grouping / Ordering
*Intended* semantics: ordering per **GroupId** (not per partition) — same GroupId in produce order, even across retries/DLQ; different GroupIds may be delivered concurrently and out of relative order. See [Message Ordering](https://github.com/flipkart-incubator/varadhi/wiki/Message-Ordering). **Not implemented end-to-end yet**: produce may hash by GroupId; the consumer grouped path is unwired (ungrouped delivery only).

### concept.org — Org
Top of the resource hierarchy (Org → Team → Project); the tenancy/isolation root under which teams and projects live. See [Tenancy Model](https://github.com/flipkart-incubator/varadhi/wiki/Tenanacy-Model).

### concept.team — Team
A grouping within an Org that owns one or more Projects. See [Tenancy Model](https://github.com/flipkart-incubator/varadhi/wiki/Tenanacy-Model).

### concept.project — Project
The unit that messaging resources (topics/subscriptions/queues) live under. Project names are globally unique per deployment; a resource's project association is immutable. See [Tenancy Model](https://github.com/flipkart-incubator/varadhi/wiki/Tenanacy-Model).

## Public Contracts

### Control-plane REST API
**Type**: REST (HTTP/JSON, OpenAPI 3.0.0)
**Reference**: [Swagger UI](https://flipkart-incubator.github.io/varadhi/) · spec at [`docs/api.yaml`](./api.yaml)
**Scope**: Manage tenants/orgs, teams, projects, topics, subscriptions (CRUD + state), IAM role bindings, regions, and DLT message management.
**Auth**: Authenticated (JWT in spec / pluggable handler) + RBAC authorization.
**Availability / Consistency / Performance**: [TODO: no published SLA/SLO; APIs are in Draft.]

### Produce REST API
**Type**: REST (HTTP/JSON) — `POST /v1/projects/{project}/topics/{topic}/produce`
**Reference**: [`docs/api.yaml`](./api.yaml); message headers in [Message Configurability](https://github.com/flipkart-incubator/varadhi/wiki/Message-Configurability).
**Consistency**: At-least-once persistence (MVP). Produce may route grouped topics by GroupId; **ordered delivery to subscribers is not implemented yet**.
**Performance**: Per-topic capacity policy enforces throughput / QPS rate limits. Other guard rails such as request-size are platform constants applible to all topics and defined in deployment config. [TODO: no published global SLA.]
**Protocols**: HTTP/1.1 and HTTP/2 (ALPN). gRPC and alternate protocols are on the roadmap.

### Push delivery contract (Varadhi → subscriber endpoint)
**Type**: Outbound HTTP request to the subscription's configured endpoint.
**Behavior**: Varadhi delivers the message payload and propagates configured headers (e.g. message id, produce identity/region/timestamp). Non-2xx responses are treated as delivery failures and routed to Retry Queue / DLQ per policy. For queues, an optional callback delivers a response back to the publisher.
**Consistency**: At-least-once (subscribers should be idempotent). **Per-GroupId ordering is not implemented on the consumer yet** — only unordered push delivery is live.
**Performance**: Governed by the subscription's ConsumptionPolicy (latency/parallelism/failure-recovery preferences). [TODO: no published delivery-latency SLA.]

## Operational Context

> **Not yet productionized.** SLAs/SLOs are **not finalized**. The topology below is the *intended* design and may change.

**Backing infrastructure (per deployment):**
- A **global ZooKeeper** for globally-relevant metadata (orgs, teams, projects, topics, subscriptions).
- A **messaging stack** behind an SPI; the default implementation uses **Apache Pulsar, geo-replicated across regions**.

**Per-region Varadhi deployments** (component roles configured via `member.roles`):
- **Web server** — control-plane APIs + produce-message API. Optionally split into a pure control-plane web server and a separate produce-only web server.
- **Controller** — region-local cluster/assignment management. (Likely needs a region-local ZooKeeper as well — not finalized.)
- **Consumer worker fleet** — delivers messages to subscriber endpoints; handles retries/DLQ.

**Regions & failover:** Varadhi is region-aware (`deployedRegion`); topics can carry replication/produce regions and failover configuration. Multi-region replication and failover orchestration are partly implemented / in progress.

**Deployment artifacts:** Docker images and Helm charts under [`setup/`](../setup) (separate server and controller deployments). Local quick-start: [Try Locally](https://github.com/flipkart-incubator/varadhi/wiki/Try-Locally).

**Observability:** Metrics via Micrometer → OpenTelemetry (OTLP) → Prometheus/Grafana; tracing enabled. See [Metrics Documentation](https://github.com/flipkart-incubator/varadhi/wiki/Varadhi-Metrics-Documentation).

[TODO: deployment regions, availability targets, and SLAs/SLOs to be documented once finalized.]

## Known Limitations

Things to know before integrating:

- **Pre-production / WIP** — APIs are in *Draft* and may change; not yet running in production; no finalized SLAs.
- **At-least-once only** — no exactly-once or deduplication today; consumers must be idempotent.
- **Grouped / ordered delivery is not implemented yet** on the consumer — only the ungrouped path is live. When implemented, ordering is intended per-GroupId (not per-partition); relative order across different GroupIds would not be guaranteed.
- **Push-only delivery** — consumers must expose an HTTP endpoint; there is no client pull/poll API.
- **Configurable header names** — message header names are deployment-specific; integrators must confirm the target deployment's convention.
- **Single backend implementation today** — Apache Pulsar (messaging) and ZooKeeper (metastore) are the only shipped implementations; Kafka is on the roadmap.
- **No schema validation, replay, scheduled/delayed delivery, or payload transformation** *(roadmap)*.
- **Queues don't support filtering**; filters are evaluated only on first delivery attempt.

## System Context Diagram

```mermaid
flowchart TD
    Producer[Publisher / Producer Apps]
    Admin[Administrators / Operators]

    Producer -- "produce message (HTTP/JSON REST)" --> Varadhi
    Admin -- "manage orgs, teams, projects,<br/>topics, subscriptions, IAM (REST)" --> Varadhi

    Varadhi[Varadhi RESTBus]

    Varadhi -- "push delivery (HTTP)" --> Subscriber[Subscriber / Consumer App Endpoints]
```

> Backing infrastructure (Apache Pulsar, ZooKeeper), observability (OpenTelemetry/Prometheus/Grafana), and external services (`ext.identity-provider`, optional `ext.authorization-provider`) are intentionally omitted from this outside-in diagram; see [External Dependencies](#external-dependencies) and [Operational Context](#operational-context).
