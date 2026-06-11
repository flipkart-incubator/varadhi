# Index

## Reading order

Start broad and zoom in: **L1 → L2 → L3 → Flows**. Structure (what exists) first, then behavior (what happens).

| Level | Doc | Scope |
|---|---|---|
| **L1 — System Context** | [system-context.md](./system-context.md) | Outside-in view: what Varadhi is, its actors, boundaries, public contracts, and external dependencies. |
| **L2 — Containers** | [containers.md](./containers.md) | Deployable units (varadhi-server/controller/consumer + Pulsar, ZooKeeper, otel-collector) and how they connect. |
| **L3 — Components** | per-container docs below | Code-facing internals of each container: components, responsibilities, side effects, runtime characteristics. |
| **Flows** | [flows.md](./flows.md) | Behavioral view: how work moves across containers/components per trigger — patterns + key flows. |

## L3 — Component docs

| Doc | Container | Components |
|---|---|---|
| [varadhi-server/components.md](./varadhi-server/components.md) | varadhi-server | http-ingress, produce-service, authentication, authorization, iam-policy-management, dlq-service, request-telemetry |
| [varadhi-controller/components.md](./varadhi-controller/components.md) | varadhi-controller | controller-api, subscription-coordinator, operation-executors, operation-manager, assignment-manager, event-distributor |
| [varadhi-consumer/components.md](./varadhi-consumer/components.md) | varadhi-consumer | consumer-api, message-poller, flow-control, message-consumption, consumers-manager, message-delivery, message-failure-routing, execution-context, telemetry |
| [shared-components.md](./shared-components.md) | (cross-container) | shared.app-bootstrap, shared.cluster-rpc, shared.entity-services, shared.resource-cache, shared.metadata-spi, shared.messaging-spi, shared.commons |

## Conventions & memory

- **[.codescope.memory](./.codescope.memory)** — durable judgment calls about this repo (decision + why) that future doc runs must honor. Current truth only; not an activity log.
- **Naming**: identifiers are greppable cross-references — containers are `<name>`, components `<container>.<component>`, shared components `shared.<name>`, flows `flow.<category>.<name>`, patterns `pattern.<name>`. E.g. `grep -rF "varadhi-consumer.message-delivery" docs/` finds its L3 definition and every flow that uses it.

## How the docs link together

- L2 containers ↔ L1 actors/dependencies; L3 components ↔ their L2 container; flows reference L3 `container.component` participants and cross-reference each other (`→ flow.…`, `pattern.…`).
- Shared modules used by multiple containers live in `shared-components.md` (referenced as `shared.*` from L3 and flows).

## Other docs in this folder

| Doc | What it is |
|---|---|
| [api.yaml](./api.yaml) | OpenAPI 3.0 spec (control-plane + produce APIs); rendered at the [hosted Swagger UI](https://flipkart-incubator.github.io/varadhi/). |
| [TOPIC_MODEL_STRUCTURE.md](./TOPIC_MODEL_STRUCTURE.md) | Design note: current topic model + proposed Global Topics / multi-region (internal/proposed; not all wired). |
