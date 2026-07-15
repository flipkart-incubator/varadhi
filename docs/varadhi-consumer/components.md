---
type: Components
title: varadhi-consumer — Components
description: Consumer-role delivery fleet — per-shard consume/deliver/route engine, flow control, and event-bus control surface.
level: L3
okf_version: "0.1"
format_version: "0.1"
generated_by: component-doc-generation@0.2.0
timestamp: 2026-06-21T05:37:39Z
---

# varadhi-consumer — Components

## Overview

`varadhi-consumer` is the delivery worker fleet — a Vert.x application that runs with `member.roles:[Consumer]` and has **no HTTP API**. It receives shard `concept.assignment`s and control commands from the controller over the clustered event bus, then runs a per-shard engine that consumes `concept.message`s from the messaging stack and pushes them to subscriber HTTP endpoints, handling retries and dead-lettering.

The container is organized around two inbound surfaces and a per-shard processing engine. [`consumer-api`](#varadhi-consumerconsumer-api--consumer-api) is the control surface: it takes start/stop/status/etc. commands off the event bus and reports operation outcomes back to the controller. [`consumers-manager`](#varadhi-consumerconsumers-manager--consumers-manager) owns the node-level registry of running shard consumers and the shared node resources. For each assigned shard, [`message-consumption`](#varadhi-consumermessage-consumption--message-consumption) is the engine that orchestrates the work, delegating the three boundary side effects to dedicated components: [`message-poller`](#varadhi-consumermessage-poller--message-poller) (consume from the messaging stack), [`message-delivery`](#varadhi-consumermessage-delivery--message-delivery) (HTTP push), and [`message-failure-routing`](#varadhi-consumermessage-failure-routing--message-failure-routing) (retry/DLQ produce).

Two policy/infra mechanisms support the engine: [`flow-control`](#varadhi-consumerflow-control--flow-control) bounds delivery parallelism and throttles under high error rates, and [`execution-context`](#varadhi-consumerexecution-context--execution-context) is the single-threaded task executor everything runs on. [`telemetry`](#varadhi-consumertelemetry--telemetry) emits consumption metrics. Cluster RPC, the messaging contracts, and the process bootstrap are **shared** — see [shared-components.md](../shared-components.md) (`shared.cluster-rpc`, `shared.messaging-spi`, `shared.app-bootstrap`).

> Two capabilities are present in code but **not wired**: grouped/ordered consumption (`concept.grouping`) is excluded — the connect path throws for `grouped` ([VaradhiConsumerImpl](/consumer/src/main/java/com/flipkart/varadhi/consumer/impl/VaradhiConsumerImpl.java)) — and the consumer-side DLQ message-browsing is stubbed (see `consumer-api`).

## Component Summary

| Component | Archetype | Responsibility |
|---|---|---|
| `varadhi-consumer.consumer-api` | Inbound Gateway | Receives shard control commands over the event bus; reports op outcomes to the controller |
| `varadhi-consumer.message-poller` | Inbound Gateway | Polls + tracks messages from main/retry storage subscriptions |
| `varadhi-consumer.flow-control` | Policy / Guard | Bounds delivery parallelism; dynamic error-rate throttle |
| `varadhi-consumer.message-consumption` | Application Service / Use-Case Coordinator | Per-shard engine: orchestrates consume→deliver→route with backpressure |
| `varadhi-consumer.consumers-manager` | State Manager | Node-level registry of shard consumers + shared node resources |
| `varadhi-consumer.message-delivery` | Outbound Gateway | Async HTTP push to subscriber endpoints |
| `varadhi-consumer.message-failure-routing` | Outbound Gateway + Domain Logic | Retry/DLQ escalation + produce of failed messages |
| `varadhi-consumer.execution-context` | Cross-Cutting Provider | Single-threaded actor-style task executor |
| `varadhi-consumer.telemetry` | Cross-Cutting Provider | Consumer-side metrics emission |

## Components

### varadhi-consumer.consumer-api — Consumer API

**Archetype**: Inbound Gateway
**Packages**: `consumer` (`ConsumerVerticle`, `ConsumerApiHandler`, `ConsumerApiMgr`)
**Public Interface**: [ConsumerVerticle](/consumer/src/main/java/com/flipkart/varadhi/consumer/ConsumerVerticle.java) is the Consumer-role verticle; [ConsumerApiMgr](/consumer/src/main/java/com/flipkart/varadhi/consumer/ConsumerApiMgr.java) is the boundary that implements the `shared.cluster-rpc` `ConsumerApi` — callers (controller/server) drive shard start/stop/unsideline/status over the event bus through it.

#### Responsibility

The consumer's inbound control surface — there is no HTTP. It registers event bus handlers keyed by consumerId for start, stop, unsideline, status, info, and the two DLQ get-messages operations, dispatches them to `consumers-manager`, and reports each operation's completion or failure back to the controller. `unsideline` and the DLQ get-messages operations are currently stubs (no-op / empty response).

#### Collaborators

| Communicates With | Direction | Protocol | Purpose |
|---|---|---|---|
| `shared.app-bootstrap` | called-by | method-call | Verticle deployed for the Consumer role |
| `varadhi-controller` | called-by | event-bus | Receive shard control commands (start/stop/unsideline/status/info) |
| `varadhi-server` | called-by | event-bus | DLQ get-messages requests |
| `varadhi-consumer.consumers-manager` | calls | method-call | Apply start/stop; read state/info |
| `varadhi-controller` | calls | event-bus | Report op completion/failure |

#### Side Effects

- Operation-state RPC → `varadhi-controller`. [ConsumerApiHandler](/consumer/src/main/java/com/flipkart/varadhi/consumer/ConsumerApiHandler.java)

#### Notes for Coding Agents

- `unsideline` and DLQ get (`getMessagesByTimestamp` / `getMessagesByOffset`) are **stubs** returning no-op/empty ([ConsumerApiMgr](/consumer/src/main/java/com/flipkart/varadhi/consumer/ConsumerApiMgr.java)). This is why `varadhi-server.dlq-service` browse returns no data — implementing DLQ browse requires filling these in.
- Handlers are registered by `consumerId` (the node hostname); changing the id scheme breaks controller→consumer addressing.

---

### varadhi-consumer.message-poller — Message Poller

**Archetype**: Inbound Gateway (broker consume)
**Packages**: `consumer` (`MessageSrc`, `MessageSrcSelector`, `UnGroupedMessageSrc`, `MessageBatch`, `MessageTracker`, `PolledMessageTracker`, `DelayedConsumer`)
**Public Interface**: [MessageSrcSelector](/consumer/src/main/java/com/flipkart/varadhi/consumer/MessageSrcSelector.java) is the boundary — the engine pulls the next batch of `concept.message`s through it; [MessageTracker](/consumer/src/main/java/com/flipkart/varadhi/consumer/MessageTracker.java) carries per-message ack/offset state back through the consume lifecycle.

#### Responsibility

Pulls message batches from a shard's storage subscriptions (the main `concept.storage-topic` plus each retry topic), selecting across those internal queues by priority, and tracks each message for acknowledgement/offset bookkeeping. Retry-topic consumption is deferred by a delay so retries are not re-read immediately.

#### Collaborators

| Communicates With | Direction | Protocol | Purpose |
|---|---|---|---|
| `varadhi-consumer.message-consumption` | called-by | method-call | Supply next batch; carry ack callbacks |
| `shared.messaging-spi` → pulsar | calls | Pulsar consumer | Consume + acknowledge |

#### Side Effects

- Consume + acknowledge from the messaging stack (main + retry topics), via `shared.messaging-spi`. [MessageSrcSelector](/consumer/src/main/java/com/flipkart/varadhi/consumer/MessageSrcSelector.java)

#### Runtime Characteristics

- **Consistency/timing**: retry topics are consumed through a `DelayedConsumer` with a **fixed (hardcoded) delay**, so the retry latency floor is fixed, not policy-driven. [VaradhiConsumerImpl](/consumer/src/main/java/com/flipkart/varadhi/consumer/impl/VaradhiConsumerImpl.java)

#### Notes for Coding Agents

- `MessageTracker` carries the offset/ack state for a polled message; acknowledgement happens when `message-consumption` marks completion. Don't ack outside that path or you risk message loss/duplication.
- Internal-queue selection priority is set by the engine (reversed so retries/DLQ precede main); the poller consumes in that order.

---

### varadhi-consumer.flow-control — Flow Control

**Archetype**: Policy / Guard
**Packages**: `consumer.impl` (`ConcurrencyControlImpl`, `SlidingWindowThrottler`, `SlidingWindowThresholdProvider`); `consumer` (`ConcurrencyControl`, `Throttler`, `ThresholdProvider`)
**Public Interface**: [ConcurrencyControl](/consumer/src/main/java/com/flipkart/varadhi/consumer/ConcurrencyControl.java) is the boundary — the engine enqueues delivery tasks through it and marks outcomes; the throttle and dynamic error-rate threshold ([SlidingWindowThrottler](/consumer/src/main/java/com/flipkart/varadhi/consumer/impl/SlidingWindowThrottler.java), [SlidingWindowThresholdProvider](/consumer/src/main/java/com/flipkart/varadhi/consumer/impl/SlidingWindowThresholdProvider.java)) sit behind it.

#### Responsibility

Gates the rate at which the engine pushes messages: bounds in-flight delivery parallelism per internal-queue priority, and applies a dynamic error-rate threshold that tightens the throttle as delivery failures rise (and relaxes as they recover).

#### Collaborators

| Communicates With | Direction | Protocol | Purpose |
|---|---|---|---|
| `varadhi-consumer.message-consumption` | called-by | method-call | Enqueue delivery tasks; mark delivery outcomes |
| `varadhi-consumer.execution-context` | calls | method-call | Run gated tasks on the shared executor |

#### Side Effects

None external — in-process rate/parallelism control.

#### Runtime Characteristics

- **Contention (intended)**: under a rising delivery error rate, the throttle quota shrinks, deliberately slowing delivery; failed deliveries acquire throttle quota before completing. This is a designed backpressure/brownout mechanism, not a fault. [SlidingWindowThrottler](/consumer/src/main/java/com/flipkart/varadhi/consumer/impl/SlidingWindowThrottler.java)

#### Notes for Coding Agents

- Parallelism and the error threshold are driven by the subscription's ConsumptionPolicy (`maxParallelism`, `maxErrorThreshold`). The throttler and threshold provider are wired together externally in the engine's connect path (a listener bridges threshold changes to the throttler) — preserve that wiring if refactoring.

---

### varadhi-consumer.message-consumption — Message Consumption

**Archetype**: Application Service / Use-Case Coordinator
**Packages**: `consumer.impl` (`VaradhiConsumerImpl`), `consumer.processing` (`ProcessingLoop`, `UngroupedProcessingLoop`); `consumer` (`VaradhiConsumer`, `MessageConsumptionStatus`)
**Public Interface**: [VaradhiConsumer](/consumer/src/main/java/com/flipkart/varadhi/consumer/VaradhiConsumer.java) is the boundary (connect / start / close / state for a shard); the consume→deliver→route iteration is owned by [ProcessingLoop](/consumer/src/main/java/com/flipkart/varadhi/consumer/processing/ProcessingLoop.java) behind it.

#### Responsibility

The per-shard engine and the "how" of consumption. For an assigned shard it wires the main + retry consumers, the delivery client, the retry/DLQ producers, flow-control, and the execution context, then runs the consume→deliver→route iteration with in-flight backpressure. It owns the per-`concept.message` outcome decision: on successful delivery it marks the message consumed (ack); on failure it hands the message to `message-failure-routing`. It also owns the shard consumer's lifecycle (connect/start/close) and reported state.

#### Collaborators

| Communicates With | Direction | Protocol | Purpose |
|---|---|---|---|
| `varadhi-consumer.consumers-manager` | called-by | method-call | Created + started/stopped per shard |
| `varadhi-consumer.message-poller` | calls | method-call | Fetch next batch |
| `varadhi-consumer.message-delivery` | calls | method-call | Push messages to the endpoint |
| `varadhi-consumer.message-failure-routing` | calls | method-call | Route failed deliveries to retry/DLQ |
| `varadhi-consumer.flow-control` | calls | method-call | Bound parallelism + throttle |
| `varadhi-consumer.execution-context` | calls | method-call | Run the loop on the shared executor |

#### Side Effects

- No direct external effect of its own — orchestrates the poller/delivery/failure-routing effects, and marks acknowledgement on success. [ProcessingLoop](/consumer/src/main/java/com/flipkart/varadhi/consumer/processing/ProcessingLoop.java)

#### Runtime Characteristics

- **Backpressure**: the loop self-limits via a max-in-flight bound (a ConsumptionPolicy knob) — it skips enqueuing the next iteration when in-flight exceeds the threshold and resumes under it. [ProcessingLoop](/consumer/src/main/java/com/flipkart/varadhi/consumer/processing/ProcessingLoop.java)
- **Blocking shutdown**: `close()` blocks the calling thread until in-flight messages drain — stop is not instantaneous and is explicitly flagged in-code as a candidate for non-blocking rework. [VaradhiConsumerImpl](/consumer/src/main/java/com/flipkart/varadhi/consumer/impl/VaradhiConsumerImpl.java)
- **Not implemented**: grouped/ordered consumption (`concept.grouping`) throws `UnsupportedOperationException`; only the ungrouped path is live. [VaradhiConsumerImpl](/consumer/src/main/java/com/flipkart/varadhi/consumer/impl/VaradhiConsumerImpl.java)

#### Notes for Coding Agents

- This is the per-shard composition root — adding a new processing stage means wiring it here and into `ProcessingLoop`. Keep work non-blocking: the loop runs on the shared single-threaded `execution-context`, so a blocking call stalls every shard on the node.
- The success/ack decision lives in `ProcessingLoop`; the failure branch delegates to `message-failure-routing`. Don't move ack earlier than confirmed delivery.
- Grouped support is intentionally absent; the `ordering` code exists but is unwired. Implementing it means replacing the `connectUnsafe` throw and providing a grouped processing-loop path.

---

### varadhi-consumer.consumers-manager — Consumers Manager

**Archetype**: State Manager
**Packages**: `consumer.impl` (`ConsumersManagerImpl`); `consumer` (`ConsumersManager`, `ConsumerEnvironment`, `ShardId`)
**Public Interface**: [ConsumersManager](/consumer/src/main/java/com/flipkart/varadhi/consumer/ConsumersManager.java) is the boundary — callers start/stop a shard subscription and read consumer state/info through it.

#### Responsibility

Owns the node-level registry of running shard consumers (`ShardId → consumer`) and the shared node resources: the single `EventExecutor`/scheduler and the `HttpClient`. It creates and tears down per-shard engines on start/stop and exposes consumer state and capacity info. `pauseSubscription` / `resumeSubscription` are stubs.

#### Collaborators

| Communicates With | Direction | Protocol | Purpose |
|---|---|---|---|
| `varadhi-consumer.consumer-api` | called-by | method-call | start/stop/status/info |
| `varadhi-consumer.message-consumption` | calls | method-call | Create/start/stop a shard engine |
| `varadhi-consumer.execution-context` | calls | method-call | Owns the shared `EventExecutor`/scheduler |
| `shared.messaging-spi` | calls | method-call | Producer/consumer factories for engines |

#### Side Effects

- None external directly — manages in-process lifecycle and holds node resources.

#### Runtime Characteristics

- **Node-level contention**: one `EventExecutor` (single thread), one scheduler, and one `HttpClient` are created per node and shared across **all** shard consumers. All shard processing on a node is therefore serialized on a single thread — the dominant scaling/contention property of the consumer (scale out by adding nodes/shards across nodes). [ConsumersManagerImpl](/consumer/src/main/java/com/flipkart/varadhi/consumer/impl/ConsumersManagerImpl.java)
- **Duplicate-start guard**: starting a shard that already exists throws. [ConsumersManagerImpl](/consumer/src/main/java/com/flipkart/varadhi/consumer/impl/ConsumersManagerImpl.java)

#### Notes for Coding Agents

- The shared single-thread executor is created here but documented as `execution-context`. Changing it from one-thread-per-node to a pool changes the consumer's concurrency and ordering guarantees — treat as an architectural change.
- `pause`/`resume` are unimplemented; don't assume they work.

---

### varadhi-consumer.message-delivery — Message Delivery

**Archetype**: Outbound Gateway
**Packages**: `consumer.delivery` (`MessageDelivery` / `HttpMessageDelivery`, `DeliveryResponse`)
**Public Interface**: [MessageDelivery](/consumer/src/main/java/com/flipkart/varadhi/consumer/delivery/MessageDelivery.java) is the boundary — the engine delivers a `concept.message` to the subscription's endpoint through it; the HTTP implementation and protocol selection are internal.

#### Responsibility

Delivers a `concept.message` to the subscription's configured HTTP endpoint asynchronously, propagating the message's headers and applying the endpoint's request timeout. Only HTTP/1.1 and HTTP/2 endpoints are supported.

#### Collaborators

| Communicates With | Direction | Protocol | Purpose |
|---|---|---|---|
| `varadhi-consumer.message-consumption` | called-by | method-call | Deliver a polled message |
| `actor.subscriber` | calls | HTTP/1.1, HTTP/2 | Push the message payload + headers |

#### Side Effects

- Outbound HTTP push to the subscriber endpoint. [MessageDelivery](/consumer/src/main/java/com/flipkart/varadhi/consumer/delivery/MessageDelivery.java)

#### Runtime Characteristics

- **Failure mode**: a non-2xx response is returned as a `DeliveryResponse` (not an exception) and treated by the engine as a delivery failure → retry/DLQ routing. The per-request timeout comes from the endpoint config. [MessageDelivery](/consumer/src/main/java/com/flipkart/varadhi/consumer/delivery/MessageDelivery.java)
- **Shared client**: uses the node's single shared `HttpClient` (from `consumers-manager`); connection-pool limits are shared across all shards on the node. [ConsumersManagerImpl](/consumer/src/main/java/com/flipkart/varadhi/consumer/impl/ConsumersManagerImpl.java)

#### Notes for Coding Agents

- Unsupported endpoint protocols are rejected at construction; add new protocols there.
- Header propagation is verbatim from the message; if you add system headers, do it before delivery, not here.

---

### varadhi-consumer.message-failure-routing — Message Failure Routing

**Archetype**: Outbound Gateway + Domain Logic
**Packages**: `consumer` (`FailedMsgProducer`, `ConsumptionFailurePolicy`); routing logic in `consumer.processing` (`UngroupedProcessingLoop`)
**Public Interface**: [FailedMsgProducer](/consumer/src/main/java/com/flipkart/varadhi/consumer/FailedMsgProducer.java) is the boundary — the engine hands a failed `concept.message` to it and it produces the message to the next internal topic (`concept.retry-queue` or `concept.dead-letter-queue`); the escalation policy is [ConsumptionFailurePolicy](/consumer/src/main/java/com/flipkart/varadhi/consumer/ConsumptionFailurePolicy.java).

#### Responsibility

Decides where a non-successfully-delivered `concept.message` goes next in the retry/DLQ escalation (`Main → Retry(1) → … → Retry(N) → DeadLetter`) and produces it to that internal topic. Encodes the failure/retry policy and the `concept.dead-letter-queue` terminal.

#### Collaborators

| Communicates With | Direction | Protocol | Purpose |
|---|---|---|---|
| `varadhi-consumer.message-consumption` | called-by | method-call | Handle a failed delivery |
| `shared.messaging-spi` → pulsar | calls | Pulsar producer | Produce to retry / dead-letter topics |

#### Side Effects

- Produce failed messages to `concept.retry-queue` / `concept.dead-letter-queue` topics via `shared.messaging-spi`. [FailedMsgProducer](/consumer/src/main/java/com/flipkart/varadhi/consumer/FailedMsgProducer.java)

#### Runtime Characteristics

- **Failure-path bottleneck**: if producing to the retry/DLQ topic is slow or failing, messages buffer and the engine's in-flight count stays high, throttling new consumption. The retry→DLQ escalation count comes from the subscription's RetryPolicy. [ConsumptionFailurePolicy](/consumer/src/main/java/com/flipkart/varadhi/consumer/ConsumptionFailurePolicy.java)

#### Notes for Coding Agents

- The routing decision currently lives **inside** [UngroupedProcessingLoop](/consumer/src/main/java/com/flipkart/varadhi/consumer/processing/UngroupedProcessingLoop.java), while the produce wrapper (`FailedMsgProducer`) and policy live in `consumer`. This is a known seam — if you extract this component physically, move the routing logic here and keep the escalation order intact.
- `FailedMsgProducer` strips/sets the follow-through header; preserve that for grouped-failure semantics when grouped is implemented.

---

### varadhi-consumer.execution-context — Execution Context

**Archetype**: Cross-Cutting Provider
**Packages**: `consumer.concurrent` (`EventExecutor`, `Context`, `CustomThread`, `EventExecutorGroup`)
**Public Interface**: [Context](/consumer/src/main/java/com/flipkart/varadhi/consumer/concurrent/Context.java) (run / runOnContext / isInContext) and [EventExecutor](/consumer/src/main/java/com/flipkart/varadhi/consumer/concurrent/EventExecutor.java) (execute / schedule) are the boundary — components run their tasks on the shared single-threaded executor through these.

#### Responsibility

The single-threaded, actor-style task-execution substrate that all per-shard processing runs on. Tasks are bound to a `Context` and dispatched serially by one `EventExecutor` thread (per node). This establishes the consumer's execution and task-ordering model.

#### Collaborators

| Communicates With | Direction | Protocol | Purpose |
|---|---|---|---|
| `varadhi-consumer.consumers-manager` | called-by | method-call | Instantiates the executor |
| `varadhi-consumer.message-consumption` / `varadhi-consumer.flow-control` | called-by | method-call | Run their tasks on this context |

#### Side Effects

None external — in-process task scheduling.

#### Runtime Characteristics

- **Single-thread serialization**: one `EventExecutor` thread drains a queue of tasks; tasks assert they run in-context. A blocking or slow task blocks **all** shards sharing that executor — the central contention point of the consumer. Tasks bound to a different executor are rejected. [EventExecutor](/consumer/src/main/java/com/flipkart/varadhi/consumer/concurrent/EventExecutor.java)

#### Notes for Coding Agents

- Never perform blocking I/O on a `Context` task — it stalls every shard on the node. Offload blocking work and re-enter via `Context#runOnContext`.
- Code asserts in-context execution; preserve the `Context` binding when scheduling follow-up work, or you risk running on an unintended thread.

---

### varadhi-consumer.telemetry — Telemetry

**Archetype**: Cross-Cutting Provider
**Packages**: `consumer` (`ConsumerMetrics`, `ConsumerMetricsBuilder`)
**Public Interface**: [ConsumerMetricsBuilder](/consumer/src/main/java/com/flipkart/varadhi/consumer/ConsumerMetricsBuilder.java) builds a per-shard [ConsumerMetrics](/consumer/src/main/java/com/flipkart/varadhi/consumer/ConsumerMetrics.java) recorder; the engine records consumption metrics through it.

#### Responsibility

Consumer-side metrics: builds a per-shard metric recorder and emits consumption metrics (e.g. received / accepted / delivery outcomes) through Micrometer.

#### Collaborators

| Communicates With | Direction | Protocol | Purpose |
|---|---|---|---|
| `varadhi-consumer.message-consumption` | called-by | method-call | Record metrics along the loop |
| `varadhi-consumer.consumers-manager` | called-by | method-call | Built per shard with the node `MeterRegistry` |
| otel-collector | calls | OTLP (via Micrometer/OpenTelemetry) | Export metrics |

#### Side Effects

- Metrics export → otel-collector (via `MeterRegistry`).

#### Notes for Coding Agents

- Keep emission inside this component rather than scattering metric calls across the engine.
- Watch metric label cardinality (per-subscription/shard tags) to avoid metric explosion.
- `[TODO: exact metric set not enumerated in facts — confirm against ConsumerMetrics if a precise list is needed]`
