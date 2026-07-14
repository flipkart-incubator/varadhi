# Topic-transition pod wire protocol

Pod-side contract for topic failover and storage-topic migration. Controller orchestration details are out of scope here.

## Bus addresses

| Leg | Route / API | Method | Payload |
|---|---|---|---|
| Controller → pods | `topic.transition` / `event.publish` | publish | `TransitionEvent` |
| Pod → controller | `controller` / `topic.transition.event.ack` | send | `TransitionAck` |

Constants: `TransitionBusAddress`, `ControllerApi.ROUTE_CONTROLLER`.

## `TransitionEvent`

| Field | Meaning |
|---|---|
| `opId` | Controller-assigned operation id (UUID). Barrier key with `stage`. |
| `topicFqn` | Topic under transition (`VaradhiTopicName`). |
| `transitionType` | `TOPIC_FAILOVER` or `STORAGE_MIGRATION`. |
| `stage` | `PREPARE`, `SWITCH`, `PENDING`, `COMPLETED`, `ABORTED`. |
| `awaitVersion` | Controller-driven: when `true`, pod waits for `topicVersionToAwait` in TopicCache before acking. |
| `topicVersionToAwait` | Exact topic version to observe when `awaitVersion` is true (ZK version 0 is valid). |
| `target` | Opaque string; type-specific. PREPARE only for warm. Failover → region name; storage migration → storage-topic id. |

## `TransitionAck`

| Field | Meaning |
|---|---|
| `opId`, `stage` | Barrier match key. |
| `topicFqn`, `transitionType` | Echoed for logs / ops (no op-store lookup). |
| `participation` | `INVOLVED` / `NOT_INVOLVED` on PREPARE; `null` on other stages. |
| `hostname` | Acking pod. |
| `errorMsg` | `null`/blank = success; non-blank = failure (`isSuccess()` derived). |

## Participation rules (producer pods)

Decided in `ProduceTransitionMsgHandler` (not in wiring):

1. **INVOLVED** — pod already has a cached producer for the topic (`ProducerService.isProducingTopic`). PREPARE pre-warms the typed target (`PrepareTarget`), then acks.
2. **NOT_INVOLVED** — no cached producer. PREPARE skips warm (local fencing may plug in later) and still acks success so the barrier can complete.

Version wait uses a **fixed** poll interval up to `transitionVersionWaitMs` (not exponential backoff): this is cache convergence within a deadline.

## Observability

- Counters (low cardinality): stage received/acked, participation, ack send failed — tags `type`, `stage`, `success`, `participation` as applicable. **No topic tag.**
- Gauge: `topic.transition.version_waits.in_flight`.
- Topic identity: logs and `TransitionAck` only.

## Delivery failure

Ack send is best-effort. On failure the pod increments `topic.transition.ack.send.failed` and logs; the controller is expected to time out the stage barrier and re-push (orchestrator not fully wired yet).
