# Design

## Context

On `origin/master` today (`#318` / `#332`):

- Global `VaradhiTopic.produceConfigs` and ingress `TopicResolver`
- Wire: `TransitionEvent` / `TransitionAck` / `TransitionStage` + pod `ProduceTransitionMsgHandler`
- Controller transition metrics helpers

Still needed for later REST intent + orchestrator (this change):

- Ephemeral `TransitionMaster` + `TransitionStore`
- Durable `TopicFailoverOperation` in OpStore
- Flat ZK paths unchanged from VIP grooming

## Goals / Non-Goals

**Goals:** uniqueness lock + durable op record + store SPI/impl so create/abort and executor have somewhere to write.

**Non-Goals:** HTTP, enqueue/executor, produce-path FO routing changes, Retry-After, preflight/history.

## Decisions

### D1 — Thin master, fat op
Uniqueness on `TransitionMaster`; knobs/stage history/audit on `TopicFailoverOperation`.

### D2 — Flat ZK
```
…/entities/Transition/<topicFqn>
…/entities/TopicFailoverOperation/<opId>
```

### D3 — Archive when merged
Once this lands on master, archive the change; later OpenSpecs depend on it as given.

## Risks

| Risk | Mitigation |
|---|---|
| Overlap with `#318` | Do not reintroduce wire types already on master; only additive master/op/store |
| Temptation to add REST here | Deferred to `topic-failover-rest-intent` |
