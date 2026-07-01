# Rule Catalog — Service Abstraction (Varadhi-specific)

## Scope
Varadhi-specific service wiring: the `ServiceRegistry` pattern in `WebServerVerticle`, using `InMemoryMetaStore` for tests, and reading via `ResourceReadCache` on hot paths.

- Key classes:
  - Service wiring: `web/src/main/java/com/flipkart/varadhi/web/WebServerVerticle.java` → inner `ServiceRegistry`
  - Test doubles: `spi/src/testFixtures/.../mock/InMemoryMetaStore.java`
  - Caching: `core/src/main/java/com/flipkart/varadhi/core/ResourceReadCache.java`

---

## Rules

### Wire services through `ServiceRegistry` in `WebServerVerticle`; never instantiate inside handlers

- Category: maintainability
- Severity: critical
- Description: All services in Varadhi are singletons assembled once inside `WebServerVerticle.setupHandlers()` via the `ServiceRegistry` (`registerIfAbsent` / `get`). Direct instantiation of a service inside a handler or `RouteProvider` constructor bypasses this lifecycle, may create multiple conflicting instances (each with its own state), and makes test substitution impossible since you cannot swap the dependency without modifying the handler. Handlers must receive service instances through constructor injection, not create them.
- Suggested fix:
  - Register services in `WebServerVerticle` with `serviceRegistry.registerIfAbsent(ServiceClass.class, () -> new ServiceClass(deps))`.
  - Pass the service to the handler via `new FooHandlers(serviceRegistry.get(FooService.class))`.
  - Handler constructors should accept only interfaces or service classes — no `MetaStore`, `Vertx`, or other infrastructure objects unless those are genuinely the handler's direct dependency.
---

### Use `InMemoryMetaStore` in service tests; reserve Mockito for failure-mode tests only

- Category: best practices
- Severity: suggestion
- Description: `InMemoryMetaStore` (in `spi/src/testFixtures`) implements the full `MetaStore` interface with `HashMap`-backed in-memory stores. It produces the same exception types as the real ZK implementation (`ResourceNotFoundException`, `DuplicateResourceException`), tracks entity versions, and requires no ZK dependency. Tests that mock individual stores (`@Mock OrgStore`) typically verify only that the service called `store.create()` — they do not verify that a duplicate insert is correctly rejected, or that a retrieved entity matches what was stored.
- Suggested fix:
  - Default to `InMemoryMetaStore` for all service unit tests — it gives realistic behavior at zero infrastructure cost.
  - Use Mockito mocks only when you need to simulate failure modes that `InMemoryMetaStore` cannot produce (e.g., `MetaStoreException` simulating ZK unavailability, `BadVersionException` for a forced concurrency conflict).
  - Handler-level tests should extend `WebTestBase`, which already wires `InMemoryMetaStore`-backed services.
---

### Use `ResourceReadCache` for hot-path reads on `Resource`-typed entities

- Category: performance
- Severity: suggestion
- Description: `ResourceReadCache` is a `ConcurrentHashMap`-backed in-process cache populated at startup and kept consistent by `ResourceEventDispatcher` (which broadcasts invalidation signals across the Vert.x cluster). Services that bypass the cache for hot-path reads (e.g., per-request topic/project lookups) hit ZooKeeper on every request, adding 5–20 ms of network latency and unnecessary ZK load. Always check whether a `ResourceReadCache` exists for an entity type before adding a direct store call.
- Suggested fix:
  - On hot read paths, use `resourceReadCacheRegistry.getCache(ResourceType.TOPIC).get(topicFqn)` instead of `topicStore.get(topicFqn)`.
  - Write paths must still go through the store directly; ensure they publish an invalidation event so the cache stays consistent.
  - Fall back to direct store reads only for admin-only or cold paths (initial cache population, list endpoints that bypass cache intentionally).
---

### Register a new service in `ServiceRegistry` before instantiating any handler that depends on it

- Category: best practices
- Severity: suggestion
- Description: `ServiceRegistry.get(Class)` throws a `NullPointerException` or returns null if the service was not previously registered, causing a confusing startup failure. Since `setupHandlers()` in `WebServerVerticle` runs in declaration order, any service that another service or handler depends on must be registered with `registerIfAbsent` before it is retrieved with `get`. Declare registrations in dependency order: leaf services first, dependent services last.
- Suggested fix:
  - Register independent services (no service-to-service dependencies) first.
  - Register dependent services after their dependencies are registered.
  - Consider grouping related registrations with a comment to make the ordering intent explicit.
