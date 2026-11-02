# Design

## Context

`ProducerService.produceToValidTopic` calls `TopicResolver.resolve(topic, region, true)`. For `Fenced` (not produce-allowed) it is empty and the code returns `ProduceResult.ofNonProducingTopic(..)` with `ProduceStatus.Fenced`. `ProduceHandlers` maps that to 503.

## Decisions

### D1 — Retry on resolution state, not on an exception
OSS models fencing as a `ProduceStatus`, not `ProduceUnavailableException`. Retry predicate: result's `produceStatus == Fenced`. No new exception type.

### D2 — Wrap at `produceToTopic` level
Each attempt re-runs: topic cache read → active check → resolve → filter/ratelimit → produce. Re-reading the cache is required so the retry sees the post-SWITCH route. Org filter and rate limiter run only on the attempt that is not fenced (fenced attempts return before them), so no double counting.
Metrics: `received` recorded once; `accepted` recorded once on the final result.

### D3 — Failsafe, async
Add `RetryUtils.newBackoffExecutor(scheduler, maxAttempts, initialDelayMs, maxDelayMs, factor)` returning a `FailsafeExecutor` (same style as `newPollingExecutor`). Use `getStageAsync`. Exponential factor 1.2 (configurable), bounded by `maxDelayMs`. No jitter beyond Failsafe's optional `withJitter` (omit initially).
`ProduceResult` is not a throwable: use `handleResultIf(r -> r.getProduceStatus() == Fenced)` and a fallback returning the last result on exhaustion (not an exception), keeping the 503 path identical.

### D4 — Defaults
Mirrors internal `RetryUtils.newBackoffPolicy(5, 1)`: Failsafe `withMaxAttempts(5)` = 1 initial attempt + 4 retries, `withBackoff(1000 ms, maxDelay, factor 1.2)`. `ProducerOptions`: `fencedProduceMaxAttempts=5`, `fencedProduceInitialDelayMs=1000`, `fencedProduceBackoffFactor=1.2`, `fencedProduceMaxDelayMs=10000` (safety cap, never reached with defaults).

| Retry | Delay before it | Added vs previous | Cumulative wait |
|---|---|---|---|
| 1 | 1.000 s | — | 1.000 s |
| 2 | 1.200 s | +0.200 s (x1.2) | 2.200 s |
| 3 | 1.440 s | +0.240 s | 3.640 s |
| 4 | 1.728 s | +0.288 s | **5.368 s** |

Worst-case added latency ~5.37 s plus attempt time. Tunable via config.

### D5 — Scheduler
Dedicated single-purpose `ScheduledExecutorService` owned by `ProducerService`; needs a `close()` (ProducerService is not `AutoCloseable` today — add it and call from server shutdown) or reuse an existing shared scheduler if wiring has one.

### D6 — Ordering caveat
Retried messages from one client may reorder relative to later non-fenced messages; acceptable, produce is not ordered across requests.

## Risks

- Held requests during fence increase in-flight count / client timeouts if client timeout < ~5.37 s.
- Retries add load right when the cache converges; bounded by 4 retries.
