# Rule Catalog — Concurrency & Future Composition (Varadhi-specific)

## Scope
Varadhi-specific concurrency patterns outside the Vert.x request path: the consumer's single-threaded `Context`/`EventExecutor` thread-affinity model, bridging Vert.x `Future` with `java.util.concurrent.CompletableFuture`, and the shared lock helpers.

For the Vert.x handler async rules (`executeBlocking`, `.onFailure(ctx::fail)`) see `vertx-varadhi-rule.md`; for generic async principles (don't block event threads, no `.get()`/`.join()` on in-flight futures) see the generic `java-code-review` skill checklist. These Varadhi rules take precedence on any conflict.

- Key classes:
  - Consumer thread affinity: `consumer/src/main/java/com/flipkart/varadhi/consumer/concurrent/Context.java`, `EventExecutor`, `CustomThread`
  - Future/lock helpers: `common/src/main/java/com/flipkart/varadhi/common/Extensions.java`
  - Cluster RPC futures: `core/src/main/java/com/flipkart/varadhi/core/cluster/MessageExchange.java`

---

## Rules

### Mutate consumer/shard state only on its bound `Context` thread

- Category: best practices
- Severity: critical
- Description: The consumer runtime pins each consumer/shard to a single `EventExecutor` thread and tracks the current thread's `Context` via a `FastThreadLocal`/`CustomThread`. State owned by that context (processing loops, ordering state, in-flight bookkeeping) is written **without synchronization** on the assumption that all access happens on the bound thread. Touching that state from an arbitrary thread (a Pulsar client callback, a `CompletableFuture` continuation that ran elsewhere, a timer) is a data race that corrupts ordering and offsets with no exception.
- Suggested fix:
  - Hop back onto the owning thread with `context.runOnContext(runnable)` (runs inline if already in-context, else dispatches to the executor) or `context.executeOnContext(callable)` when you need a `CompletableFuture` result.
  - Use `context.scheduleOnContext(...)` for delayed work that must run on the bound thread.
  - Gate "am I already on the right thread?" with `context.isInContext()` rather than assuming; never share context-owned mutable fields across threads via a plain field.

---

### Bridge Vert.x `Future` and `CompletableFuture` with `Extensions.FutureExtensions`; never block to convert

- Category: best practices
- Severity: critical
- Description: Varadhi mixes two future types — Vert.x `Future` on the event-bus/cluster-RPC side and `java.util.concurrent.CompletableFuture` on the producer/consumer side. The sanctioned bridge is `Extensions.FutureExtensions.handleCompletion(CompletionStage, CompletableFuture)`, which forwards completion/failure without blocking. Converting by calling `.get()`/`.join()`/`future.result()` on an in-flight future blocks the calling thread (often an event-loop or context thread) and can deadlock the single-threaded executors.
- Suggested fix:
  - Use `FutureExtensions.handleCompletion(stage, promise)` to adapt a `CompletionStage` into a `CompletableFuture`.
  - Compose with `thenCompose`/`thenApply`/`whenComplete` (CompletableFuture) or `compose`/`onComplete` (Vert.x) — never block between steps.
  - End every async chain with an explicit failure terminator so a failed future is never silently dropped.

---

### Use `Extensions.LockExtensions` for lock/unlock; never hand-roll lock/finally

- Category: best practices
- Severity: suggestion
- Description: `Extensions.LockExtensions` provides `lockAndCall` / `lockAndSupply` / `lockAndRun`, which guarantee the `Lock` is released in a `finally` even when the body throws. Hand-written `lock.lock(); …; lock.unlock();` blocks repeatedly reintroduce the bug where an exception between `lock()` and `unlock()` leaks the lock and stalls every other waiter. (This is the in-process counterpart to the `InterProcessMutex` discipline in `vertx-varadhi-rule.md`.)
- Suggested fix:
  - Wrap critical sections with `LockExtensions.lockAndRun(lock, () -> …)` (or `lockAndSupply`/`lockAndCall` when a value/checked exception is involved).
  - Keep the locked region minimal — no blocking I/O or future-blocking inside the lock.
