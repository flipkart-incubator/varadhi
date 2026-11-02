# Proposal

## Why

During a topic failover the topic sits in `TopicState.Fenced` between PREPARE and SWITCH. Today `ProducerService.produceToValidTopic` returns `ProduceStatus.Fenced` (HTTP 503) on the first attempt, so every produce during the fence window is surfaced to the client. The window is short; a small server-side buffer lets failover complete with no client-visible downtime.

Parity: the internal `varadhi` repo blocks produce during the SWITCH state (`TopicProduceTransitionService.getProducerForTopic` → `shouldBlockProduce` → `ProduceUnavailableException`) and `ProducerService` retries that exception with backoff (`produceInlineRetriesExecutor`). OSS has no equivalent buffer.

## What Changes

- When a produce resolves to a `Fenced` topic, retry with **exponential backoff (1 s initial, x1.2 per retry)**, **5 attempts total (1 initial + 4 retries, ~5.37 s of waiting)**, same as internal `newBackoffPolicy(5, 1)`, re-reading `TopicCache` on every attempt (state may flip to `Producing` or `Blocked`).
- Retry is async (Failsafe on a scheduler); no event-loop blocking, no `sleep`.
- Exhaustion returns the existing `ProduceResult` with `ProduceStatus.Fenced` (503) — wire contract unchanged.
- Backoff knobs added to `ProducerOptions` with defaults.
- Only `Fenced` is retried. `Blocked`/`NotAllowed`, `Throttled`, `Filtered`, and storage `Failed` are not.

## Capabilities

### New Capabilities

- `produce-fenced-retry`: Bounded exponential-backoff retry of produce while the topic is fenced.

## Impact

- **producer:** `ProducerService` (retry wrapper around `produceToValidTopic`), `ProducerServiceTests`
- **common:** `RetryUtils` (backoff executor factory)
- **core:** `ProducerOptions` (4 fields)
- **server:** ProducerService wiring passes the scheduler/options (verify; options already flow in)
- **web:** none (503 mapping and `ProduceHandlers` unchanged)

Depends on: `produce-cross-region` (introduces `Fenced` resolution), failover stages that set `Fenced`.
