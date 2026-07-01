# Rule Catalog — Architecture (Varadhi-specific)

## Scope
Varadhi-specific architectural rules: the Vert.x `RouteProvider`/`RouteDefinition` routing DSL, the `web → core → spi → entities` Gradle module hierarchy, `common` and `spi` module constraints, and request-telemetry logging conventions.

For the underlying generic Java principles, see the generic [java-code-review](https://github.fkinternal.com/gaurav-ashok/auto-dev/tree/main/skills/java-code-review/SKILL.md) skill checklist. These Varadhi rules take precedence on any conflict.

- Key modules:
  - Handlers: `web/src/main/java/com/flipkart/varadhi/web/v1/admin/`, `web/src/main/java/com/flipkart/varadhi/web/v1/producer/`
  - Services: `core/src/main/java/com/flipkart/varadhi/core/`
  - Store interfaces: `spi/src/main/java/com/flipkart/varadhi/spi/db/`
  - Common utilities: `common/src/main/java/com/flipkart/varadhi/common/`

---

## Rules

### All routes must use `RouteDefinition` with explicit authorization

- Category: security
- Severity: critical
- Description: Every HTTP route in Varadhi must be registered through `RouteDefinition.Builder` (returned from `RouteProvider.get()`). This is the only path that threads through authentication, authorization, body-size limits, telemetry, and the global `ErrorHandler`. Bypassing it — adding a handler directly to the Vert.x `Router` — silently skips all of these cross-cutting concerns. Additionally, every protected route must call `.authorize(ResourceAction.XYZ)` explicitly; omitting it makes the endpoint unauthenticated in production with no compile-time warning.
- Suggested fix:
  - Implement `RouteProvider` and return all routes from its `get()` method.
  - Always chain `.authorize(ResourceAction.XYZ)` for protected routes.
  - State-changing routes (POST, PUT, PATCH, DELETE) must include `.hasBody(true)` and a body parser.

---

### Varadhi module dependency hierarchy: `web → core → spi → entities`

- Category: best practices
- Severity: critical
- Description: Varadhi enforces a strict Gradle module dependency order: `web` may depend on `core`; `core` may depend on `spi` and `entities`; `spi` may depend on `entities`; `entities` has no internal Varadhi dependencies. Cross-cutting modules (`common`) are a leaf dependency available to all. Introducing a `build.gradle` `implementation` dependency that reverses this order creates cycles and leaks transport/framework concerns into the domain. This includes a `core` service importing from `web`.
- Suggested fix: If a `core` service needs a type that currently lives only in `web`, extract it to `entities` or `spi`. Do not add `web` as a dependency of `core`.

---

### `common` / `spi` modules must not import from `core` or `web`

- Category: maintainability
- Severity: critical
- Description: `common` is a zero-internal-dependency leaf module, and `spi` may only depend on `entities`. All other Varadhi modules (`core`, `web`, `consumer`, `controller`) depend on `common`. The moment `common` or `spi` imports from `core` or `web`, a circular dependency is created and the build breaks. This includes transitive imports — do not import types that themselves import from higher modules.
- Suggested fix: If a utility in `common` needs domain logic, move it to `core`. Limit `common` to: exception base classes, Vert.x helpers, JSON utilities, reflection tools, and generic cross-cutting concerns that carry no domain knowledge.

---

### `spi` interfaces must expose only domain types, not implementation types

- Category: best practices
- Severity: suggestion
- Description: Store interfaces in `spi.db` (e.g., `OrgStore`, `TopicStore`) and service interfaces in `spi.services` (e.g., `StorageTopicService`) define the contract between `core` and its pluggable backends. Their method signatures must use only domain entities (`Org`, `VaradhiTopic`, etc.) and standard Java types. Exposing ZooKeeper types, Pulsar types, or Curator objects in an SPI method signature couples all callers to a specific implementation and prevents backend replacement without recompiling `core`.
- Suggested fix: Keep implementation-specific types behind the concrete class (`VaradhiMetaStore`, `PulsarStackProvider`). If an implementation detail must be surfaced, wrap it in a domain value object defined in `spi` or `entities`.

---

### Use `@Slf4j(topic = "RequestLogs")` only for request-telemetry logging

- Category: best practices
- Severity: suggestion
- Description: `@Slf4j(topic = "RequestLogs")` directs log output to a named logger whose appender is configured separately in `log4j2.xml` for request-level access logs. This annotation must only be used on `RequestTelemetryConfigurator` (or any future class that feeds the access-log pipeline). Using it on other classes silently routes their output to the request-log appender rather than the application log, causing confusing log interleaving and missed alerts.
- Suggested fix: For all application classes, use plain `@Slf4j`. Reserve `@Slf4j(topic = "RequestLogs")` exclusively for the request telemetry pipeline.

---

### Register span/trace attributes for every new API route

- Category: best practices
- Severity: suggestion
- Description: New API routes must contribute their span/trace attributes through `RequestTelemetryConfigurator`. A route added without the corresponding telemetry wiring produces traces with no operation name or resource identifiers, making the endpoint invisible in distributed tracing and breaking latency attribution and alerting for that path.
- Suggested fix: When adding a route, add its span/trace attributes (operation name, resource identifiers) in `RequestTelemetryConfigurator` alongside the `RouteDefinition`. Verify the new route appears with attributes in a trace before merging.
