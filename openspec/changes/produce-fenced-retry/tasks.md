# Tasks

## 1. Config and util

- [ ] 1.1 `ProducerOptions`: add `fencedProduceMaxAttempts` (5, includes the initial attempt), `fencedProduceInitialDelayMs` (1000), `fencedProduceBackoffFactor` (1.2), `fencedProduceMaxDelayMs` (10000)
- [ ] 1.2 `RetryUtils.newBackoffExecutor(...)` (exponential, result-based predicate, fallback returns last result on exhaustion) + test in `RetryUtilsTest`

## 2. Produce path

- [ ] 2.1 `ProducerService`: wrap per-attempt produce (cache read → resolve → produce) in the executor; retry only when `ProduceStatus.Fenced`
- [ ] 2.2 Own scheduler; make `ProducerService` closable; hook into server shutdown
- [ ] 2.3 Metrics: `received` once, `accepted` once on final result; optional counter for fenced retries

## 3. Tests (extend `ProducerServiceTests`, fakes over mocks, no sleeps — controllable scheduler)

- [ ] 3.1 Fenced → Producing on 2nd attempt: success, producer called once
- [ ] 3.2 Fenced throughout: exactly 5 attempts (4 retries) with delays 1000/1200/1440/1728 ms (cumulative ~5.37 s), result `Fenced`
- [ ] 3.3 Fenced → Blocked: stops retrying, returns `NotAllowed`
- [ ] 3.4 Non-fenced results (Throttled / Failed) are not retried
- [ ] 3.5 Update existing `produceToFencedTopic` expectations
- [ ] 3.6 `ProduceHandlersTest`: unchanged 503 mapping still passes

## 4. Verify

- [ ] 4.1 `./gradlew :producer:test :common:test :web:test spotlessApply`
- [ ] 4.2 Confirm backoff defaults against real fence→switch duration
