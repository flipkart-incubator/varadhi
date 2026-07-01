# Rule Catalog — Vert.x & ZK Async Patterns (Varadhi-specific)

## Scope
Varadhi-specific async patterns: offloading ZK operations with `vertx.executeBlocking`, writing handler `Future` chains that forward failures to `ctx.fail()`, testing handlers with `WebTestBase` + `VertxTestContext`, ZK optimistic versioning with `BadVersionException`, and ZK distributed lock discipline.

- Key modules:
  - Vert.x verticles: `web/src/main/java/com/flipkart/varadhi/web/WebServerVerticle.java`
  - Handler extension helpers: `web/src/main/java/com/flipkart/varadhi/web/Extensions.java`
  - Test base: `web/src/test/.../WebTestBase.java`
  - ZK stores: `metastore-zk/src/main/java/com/flipkart/varadhi/db/`

---

## Rules

### Wrap every ZK (blocking) service call in `vertx.executeBlocking()`

- Category: performance
- Severity: critical
- Description: All `*Store` calls in Varadhi use the Curator/ZooKeeper client, which is synchronous. Calling them directly in a Vert.x handler method runs on the event loop and blocks it. A single ZK read taking 5 ms means the entire verticle is unresponsive to other requests for 5 ms — under 200 concurrent requests this compounds to a 1-second backlog. Every handler method that calls a service (which in turn calls a store) must be wrapped with `vertx.executeBlocking(callable)` to offload to the Vert.x worker pool.
- Suggested fix:
  - Wrap service calls in `vertx.executeBlocking(() -> service.someMethod(args))`.
  - Chain the `Future` result with `.onSuccess(result -> ctx.endApiWithResponse(result)).onFailure(ctx::fail)`.
  - Never call `future.result()` outside of an `onSuccess` callback in a handler.
---

### Always terminate handler `Future` chains with `.onFailure(ctx::fail)`

- Category: best practices
- Severity: critical
- Description: In Vert.x, if a `Future` fails and no `.onFailure()` (or `.onComplete()` that checks `ar.failed()`) is registered, the failure is silently dropped. In a handler, this means the HTTP request hangs open with no response until the client times out — there is no exception thrown on any thread and nothing in the logs. The global `ErrorHandler` in `WebServerVerticle` correctly maps `VaradhiException` subclasses to HTTP status codes, but it is only invoked when `ctx.fail(throwable)` is called.
- Suggested fix:
  - Every `executeBlocking` chain in a handler must end with `.onFailure(ctx::fail)`.
  - If you need to handle both branches: `.onComplete(ar -> { if (ar.failed()) ctx.fail(ar.cause()); else ctx.endApiWithResponse(ar.result()); })`.
---

### Use `VertxTestContext` with `WebTestBase` for all handler tests

- Category: best practices
- Severity: critical
- Description: JUnit 5 tests run synchronously on the test thread. When a handler method fires an `executeBlocking()` and registers an async callback, the callback runs on a different thread after the test method has already returned. Without `VertxTestContext`, the assertion in the callback is never evaluated and the test always passes regardless of failure. `WebTestBase` (in `web/src/test`) sets up a live Vert.x `HttpServer`, `WebClient`, and `VertxTestContext` via `@ExtendWith(VertxExtension.class)` and is the required base for all handler-level tests.
- Suggested fix:
  - Extend `WebTestBase` for all handler integration tests.
  - Use `VertxTestContext testContext` as a `@Test` parameter (injected by `VertxExtension`).
  - Always call `testContext.completeNow()` or `testContext.failNow(throwable)` inside the response callback.
  - Always call `testContext.awaitCompletion(timeout, SECONDS)` at the end of the test method to block the test thread until the callback fires.
---

### Protect ZK write paths with optimistic versioning; catch `BadVersionException`

- Category: quality
- Severity: critical
- Description: Every ZooKeeper `setData` call in Varadhi's `*Store` implementations must pass `entity.getVersion()` as the expected version. If a concurrent writer has already updated the node, ZK throws `BadVersionException`. Without this guard, concurrent updates silently overwrite each other — the last writer wins with no indication of data loss. Services that perform read-then-write operations must always read the current entity (acquiring its version), then write with that version, and translate `BadVersionException` into `InvalidOperationForResourceException`.
- Suggested fix:
  - Pass `entity.getVersion()` to `zkClient.setData(path, data, version)`, never `-1`.
  - Wrap the `setData` call in a try/catch for `KeeperException.BadVersionException`; translate to `InvalidOperationForResourceException("Concurrent modification detected, retry.")`.
---

### Keep `InterProcessMutex` lock scope minimal; no external I/O inside the locked region

- Category: performance
- Severity: suggestion
- Description: Varadhi uses Curator's `InterProcessMutex` for distributed locking on operations that must be serialized across cluster nodes (e.g., subscription assignment). Holding a distributed lock across slow external calls — Pulsar topic creation (100–500 ms), HTTP calls, large computations — increases lock-hold time proportionally, blocks other nodes from acquiring the lock, and risks cascading timeouts on operations that need the same lock. Structure operations to minimize the locked region to only the ZK write.
- Suggested fix:
  - Fetch all required data and complete all external side effects (Pulsar calls, HTTP) before acquiring the lock.
  - Re-read the entity inside the lock to get the latest version after the external call completes.
  - Use the lock only for the ZK write. Release immediately after.
