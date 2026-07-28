# Topic-transition pod wire protocol

Pod-side contract for topic failover and storage-topic migration. Controller orchestration details are out of scope here.

## Bus addresses

| Leg | Route / API | Method | Payload |
|---|---|---|---|
| Controller → pods | `topic.transition` / `event.publish` | publish | `TransitionEvent` |
| Pod → controller | `controller` / `topic.transition.event.ack` | send | `TransitionAck` |

Constants: `TransitionBusAddress`, `ControllerApi.ROUTE_CONTROLLER`.

Pod client: `ControllerRemoteClient` implements `ControllerApi` / `TransitionApi.ack` over the controller route.
`TransitionApi.sendEvent` is controller-local (`TransitionService`); the remote client throws `UnsupportedOperationException`.
Controller handler: `ControllerHandler.ack` → `TransitionService.ack`.

## `TransitionEvent`

| Field | Role |
|---|---|
| `opId` | Controller-assigned operation id (UUID). Barrier key with `stage`. |
| `topicFqn` | Topic under transition (`VaradhiTopicName`). |
| `transitionType` | `TOPIC_FAILOVER` or `STORAGE_MIGRATION`. |
| `stage` | `PREPARE`, `SWITCH`, `PENDING`, `DRAIN`, `COMPLETED`, `ABORTED`. `DRAIN` is controller-only (not broadcast to pods). |
| `awaitVersion` | Controller-driven: when `true`, pod waits for `topicVersionToAwait` in TopicCache before acking. |
| `topicVersionToAwait` | Exact topic version to observe when `awaitVersion` is true (ZK version 0 is valid). |
| `target` | `TransitionEvent.Target` (`region` or `storageTopic`). PREPARE only for warm. Wire shape: `{"@targetType":"region","region":"..."}` or `{"@targetType":"storageTopic","storageTopicId":N}`. |

## `TransitionAck`

| Field | Role |
|---|---|
| `opId`, `stage` | Barrier match key — which stage barrier this ack belongs to. |
| `hostname` | Hostname of pod acknowledging transition. |
| `topicFqn`, `transitionType` | Echoed for logs / ops (no op-store lookup). |
| `participation` | `INVOLVED` / `NOT_INVOLVED` on **every** ack; decided at PREPARE, sticky for the op. |
| `errorMsg` | `null`/blank = success; non-blank = failure (`isSuccess()` derived). |

## Wiring

`TopicTransitionPodWiring.wire(...)` registers `ProduceTransitionMsgHandler` on the cluster broadcast bus. Returns an `AutoCloseable` wiring handle that owns the version-wait scheduler; the verticle closes it on shutdown.

## Participation rules (producer pods)

Decided in `ProduceTransitionMsgHandler` (not in wiring lambdas):

1. **INVOLVED** — pod already has a producer for the topic's active produce region (`ProducerService.hasProducer`). PREPARE pre-warms the typed `target`, then acks.
2. **NOT_INVOLVED** — no cached producer. PREPARE skips warm (local fencing may plug in later) and still acks success so the barrier can complete.

## Participation persistence (controller)

Participation is decided at **PREPARE** (`hasProducer` → INVOLVED / NOT_INVOLVED) and stored per `opId` on the pod. **Every** subsequent ack echoes that sticky value so the controller knows involvement on every stage without an op-store lookup.

Late joiners (first event not PREPARE) derive participation once from `hasProducer` and cache it for the op. State is cleared on `COMPLETED` / `ABORTED`.

Barrier completion: `(opId, stage)` identifies the barrier; `hostname` dedupes per-pod acks within it. Outcome comes from `errorMsg`; `participation` tells the controller gate set membership on every ack.

## Version wait

Version-gated stages (PREPARE, SWITCH) poll TopicCache on a dedicated scheduler via a reusable Failsafe executor:

- **Fixed** poll interval up to `podVersionWaitMs` (not exponential backoff): cache convergence within a deadline.
- Config source: `ProducerOptions.transitionVersionWaitMs` / `transitionPollIntervalMs` → `PodTransitionConfig.podVersionWaitMs` / `podPollIntervalMs`.
- Attempts: `ceil(podVersionWaitMs / podPollIntervalMs)`; actual wait ≈ `(attempts - 1) * podPollIntervalMs`.
- Retries only while the probe returns empty (cache behind); probe exceptions abort immediately (`abortOn(Exception.class)`), so they surface as poll errors, not version timeouts.

## Observability

**Pod** (`TransitionMetrics`):

- Counters (tagged `type`, `stage`, plus `success` / `participation` where applicable):
  - `topic.transition.stage.received`
  - `topic.transition.stage.acked`
  - `topic.transition.ack.send.failed`
- Settable gauge: `topic.transition.participation` — tags `type`, `participation`; set at PREPARE, cleared on COMPLETED/ABORTED.
- Gauge: `topic.transition.version_waits.in_flight` — global in-flight count on this pod.

**Controller** (`TopicTransitionMetrics`):

- Counters (tags `type`, `stage`):
  - `topic.transition.ack.received`
  - `topic.transition.ack.processed`
  - `topic.transition.ack.delivery.failed`

Topic identity stays in logs; pod metrics avoid per-topic tags for cardinality.

## Delivery failure

Ack send is best-effort. On failure the pod bumps `topic.transition.ack.send.failed`, logs the full `TransitionAck`, and the controller is expected to time out the stage barrier and re-push (orchestrator not fully wired yet).

On controller-side processing failure, `topic.transition.ack.delivery.failed` is bumped and the full ack is logged.
