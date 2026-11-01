# Proposal

## Why

Once failover metadata can set `failOverRegion` and fence produce, the produce path must honor that config: reject fenced produce, and (eventually) open producers against a peer cluster. `#339` peels the produce-path slice without owning controller orchestration or full REST completeness.

## What Changes

- Honor ingress `produceConfigs` / `failOverRegion` in `TopicResolver` + `ProducerService`.
- Map fenced produce to **HTTP 503** (Retry-After deferred to `topic-failover-rest-api`).
- Region-aware Pulsar producer factory / naming as landed in the peel.
- **Out of scope:** peer client pool completion (may remain stub until `internal-topic-failover`), REST handlers, orchestrator, lag drain.

Depends on: `failover-substrate` (and `#318` wire/handler already on master).

## Capabilities

### New Capabilities

- `produce-cross-region`: Produce-path ability to route by `failOverRegion` and fence produce with client-retryable errors.

## Impact

- **producer / pulsar / entities:** resolve + registry + factory
- **web ProduceHandlers:** 503 on fenced
