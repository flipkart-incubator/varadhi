# Design

## Context

See proposal.md for why. varadhiV1 has three layers: a pod handler (`GET /producers/topics/{topic}`), an admin proxy by producer IP, and a fleet fan-out (HTTP to producers, JMX to consumers) used by `ProducerStateValidator`. OSS failover advances on push acks. Cluster members are `MemberInfo` (hostname, address, port, roles). There is no producer-app registry.

```
  caller
    |
    | GET .../topics/:topic/producer-info
    v
  produce pod ---- topic cache + TopicResolver
    |
    +--> this pod only
         (no HTTP to peers, no JMX)
```

The varadhiV1 admin path first selects a producer IP and then proxies to that pod. Its fleet path repeats that operation for registered producer applications and queries consumer applications through JMX. Those are aggregation layers above the pod-local payload, not part of the OSS endpoint.

## Goals / Non-Goals

**Goals:**

- One read on the produce role, served from the in-memory topic cache.
- Same resolution as produce (`TopicResolver`, ungated), including `failOverRegion`.
- Report cache presence without calling `getProducer`.
- Return the cached producer's five-window success and failure counts.
- Return this pod's in-flight transition type, stage, and node status.

**Non-Goals:**

- varadhiV1 admin `GET /producers/{ip}/topics/{topic}`.
- varadhiV1 fleet fan-out across producer and consumer applications. Not required for the OSS pod-local read or failover.
- Wire-compatible names (`activeProduceZone`, `activeStorageTopicId`, `produceTransitionType`).

## Decisions

### D1 — Pod-local, cache-backed

The handler runs on the produce role and reads `ProducerService`, which reads the topic cache. Alternative: ZooKeeper read on the request. Rejected because produce already trusts the cache, and a metadata read on this route would block the event loop.

### D2 — Ungated resolve

Use the same ungated resolve as PREPARE, not the produce-allowed gate. A fenced topic still has a path; `produceAllowed` carries the gate. Alternative: return 503 when fenced. Rejected because this is a diagnostic read, not a produce.

### D3 — Empty local config is 200

A cached topic with no produce config for the deployed region returns 200 with produce not allowed and no region or storage. A missing topic is 404. Alternative: 404 for both. Rejected so "not configured here" is distinct from "no such topic".

### D4 — OSS field names

`deployedRegion`, `produceRegion`, `storageTopicId`, `storageTopicName`, `state`, `produceAllowed`, `producerCached`, `produceStats`, `transitionType`, `transitionStage`, `nodeStatus`. Null routing, stats, and transition fields are omitted (`NON_NULL`). Alternative: clone the internal DTO. Rejected because zone, storage-id, and stage names do not match this model.

### D5 — `TOPIC_GET`

Same authorization as other topic reads. The route is non-blocking and ends the response itself, like produce.

### D6 — Five-window produce stats

VaradhiV1 returns `successCounts` and `failureCounts`, each a map of window-start epoch millis to a count, five windows of 60 seconds, from the live producer. OSS produce telemetry is Micrometer lifetime counters and latency timers. Those meters are not this snapshot.

Keep a five-bucket counter on the cached producer. A successful publish increments the current window's success count. A failed publish increments the failure count. Rate-limit, fence, and filter outcomes stay out of these maps. The producer-info read copies the buckets when `producerCached` is true and omits `produceStats` otherwise. It does not call `getProducer`.

### D7 — This pod's transition snapshot

VaradhiV1's `produceTransitionType`, `produceTransitionState`, and `producerTransitionNodeStatus` are the transition context on that node. OSS `ProduceFailoverTransition` stays on the controller and is not in the pod cache, so this route does not call `getTopicFailover`.

The produce handler already receives `TransitionType` (`TOPIC_FAILOVER`, `STORAGE_MIGRATION`) and `TransitionStage` (`PENDING`, `PREPARE`, `FENCE`, `MIGRATE`, `COMPLETED`, `ABORTED`) and remembers participation per operation. It does not keep a status the info route can read, and it drops stage tracking when it acks `COMPLETED` or `ABORTED`. Keep a per-topic snapshot for the info route:

| Response field | Source |
|---|---|
| `transitionType` | event type. VaradhiV1 `FAILOVER` / `MIGRATION` are `TOPIC_FAILOVER` / `STORAGE_MIGRATION`. |
| `transitionStage` | stage this pod is handling. VaradhiV1 `SWITCH` / `MIGRATED` are `FENCE` / `MIGRATE`. |
| `nodeStatus` | `NOT_INVOLVED` when participation is `NOT_INVOLVED`. `IN_PROGRESS` while this pod is involved and the current stage has not failed here. `FAILED` when this pod acks the stage as failed. `ERRORED` when applying the stage throws outside that failed ack. |

`NOT_INVOLVED` stays for the rest of the operation. `FAILED` and `ERRORED` stay until a later event for that topic updates the snapshot. A successful ack of `COMPLETED` or `ABORTED` clears it, and the three fields are then absent. A pod that has not received the event yet also omits them.

## Risks / Trade-offs

- [Cache lags a metadata write] → The view can be stale until the cache updates. Same window as produce.
- [Only this pod] → A caller cannot see another producer's cache, stats, or node status. Fleet view needs a later change that calls members from `MemberInfo`.
- [Resolve throws if the storage segment id is missing] → That surfaces as a server error. Topics in the cache are expected to have a valid segment.
- [Stats live in the producer process] → A restart zeroes the five windows. The maps are a recent sample, not a durable count.
- [Snapshot is what this pod has applied] → It can lag the controller's stage until the next `TransitionEvent` arrives.

## Migration Plan

Ship on the produce role with the existing authz. No data migration. Rollback is removing the route; nothing else writes this payload.

## Open Questions

None. Fleet fan-out stays a separate change if an operator tool needs every pod at once.
