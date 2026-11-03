## 2026-11-03 Happy path (`default` → `region2`, FENCE/MIGRATE stages)

- **Date:** 2026-11-03
- **Branch:** `feature-topicFailoverControllerSide`
- **Server:** `http://localhost:18488`
- **Topic:** `public.topic7`
- **Operation:** `e2888116-4f76-443d-b653-cddc39942af2`
- **Request:** `waitForReplicationLagToClear: false`
- **Prep:** prior `region2` → `default` reset so `default` was Producing; delete stale ZK `TopicFailoverOperation` nodes and `/test-varadhi/entities/ChangeEvent/change_event_listener` before restart so `ResourceEventProcessor` registers.

Stage model: `PENDING` → `PREPARE` → `FENCE` → `MIGRATE` → `COMPLETED` (transition znode removed on success → GET **404**).

### POST `/v1/projects/public/topics/topic7/failover` → 202

```json
{
  "name": "e2888116-4f76-443d-b653-cddc39942af2",
  "version": 0,
  "topicFqn": {
    "projectName": "public",
    "topicName": "topic7"
  },
  "requestedBy": "thanos",
  "createdAt": 1793700492661,
  "updatedAt": 1793700492661,
  "retryAttempt": 0,
  "state": "IN_PROGRESS",
  "entityType": "TOPIC_FAILOVER_OPERATION"
}
```

### Live `GET /v1/projects/public/topics/topic7/failover` payloads (distinct snapshots)

#### 1. `currentStage`: `PENDING` (transition `version`: 0)

```json
{
  "topicFqn": "public.topic7",
  "version": 0,
  "transitionKind": "FAILOVER",
  "operationId": "e2888116-4f76-443d-b653-cddc39942af2",
  "sourceRegion": "default",
  "targetRegion": "region2",
  "waitForReplicationLagToClear": false,
  "onLagTimeout": "ABORT",
  "currentStage": "PENDING",
  "topicVersionToAwait": 0,
  "stageStartedAt": 1793700492671,
  "abortRequested": false,
  "stageHistory": [],
  "name": "public.topic7",
  "entityType": "TRANSITION_OBJECT"
}
```

#### 2. `currentStage`: `FENCE` (transition `version`: 2)

```json
{
  "topicFqn": "public.topic7",
  "version": 2,
  "transitionKind": "FAILOVER",
  "operationId": "e2888116-4f76-443d-b653-cddc39942af2",
  "sourceRegion": "default",
  "targetRegion": "region2",
  "waitForReplicationLagToClear": false,
  "onLagTimeout": "ABORT",
  "currentStage": "FENCE",
  "topicVersionToAwait": 36,
  "stageStartedAt": 1793700492748,
  "abortRequested": false,
  "stageHistory": [
    {
      "stage": "PENDING",
      "startedAt": 1793700492671,
      "hostAcks": {},
      "endedAt": 1793700492702,
      "outcome": "OK"
    },
    {
      "stage": "PREPARE",
      "startedAt": 1793700492702,
      "hostAcks": {
        "BLRETV-YWCP2P6V.local": "ACKED"
      },
      "endedAt": 1793700492748,
      "outcome": "OK"
    }
  ],
  "name": "public.topic7",
  "entityType": "TRANSITION_OBJECT"
}
```

#### 3. `currentStage`: `MIGRATE` (transition `version`: 3)

```json
{
  "topicFqn": "public.topic7",
  "version": 3,
  "transitionKind": "FAILOVER",
  "operationId": "e2888116-4f76-443d-b653-cddc39942af2",
  "sourceRegion": "default",
  "targetRegion": "region2",
  "waitForReplicationLagToClear": false,
  "onLagTimeout": "ABORT",
  "currentStage": "MIGRATE",
  "topicVersionToAwait": 37,
  "stageStartedAt": 1793700492772,
  "abortRequested": false,
  "stageHistory": [
    {
      "stage": "PENDING",
      "startedAt": 1793700492671,
      "hostAcks": {},
      "endedAt": 1793700492702,
      "outcome": "OK"
    },
    {
      "stage": "PREPARE",
      "startedAt": 1793700492702,
      "hostAcks": {
        "BLRETV-YWCP2P6V.local": "ACKED"
      },
      "endedAt": 1793700492748,
      "outcome": "OK"
    },
    {
      "stage": "FENCE",
      "startedAt": 1793700492748,
      "hostAcks": {
        "BLRETV-YWCP2P6V.local": "ACKED"
      },
      "endedAt": 1793700492772,
      "outcome": "OK"
    }
  ],
  "name": "public.topic7",
  "entityType": "TRANSITION_OBJECT"
}
```

#### 4. Transition removed → **404**

(No body; executor deleted the transition after `COMPLETED`.)

### ZK produce route after success

```json
{
  "default": "Blocked",
  "region2": "Producing",
  "topicVersion": 36
}
```

### Verification summary

- **Stage progression (logs):** `PENDING` → `PREPARE` → `FENCE` → `MIGRATE` → `COMPLETED`
- **Host ACKs:** `BLRETV-YWCP2P6V.local` ACKED at `PREPARE`, `FENCE`, and `MIGRATE` (see `stageHistory` in snapshots above).
- **Note:** `PREPARE` and `COMPLETED` often complete within one poll interval; this run captured distinct GET bodies at `PENDING`, `FENCE`, and `MIGRATE` before cleanup.
