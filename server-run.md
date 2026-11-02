# Local Server Run & Topic Failover Full Verification

- **Date:** 2026-11-02
- **Branch:** `feature-topicFailoverControllerSide`
- **Server Endpoint:** `http://localhost:18488`
- **Dependencies:** Docker Compose `zookeeper` and `pulsar` (Profile: `dev`)

---

## Live In-Flight Stage Snapshots Captured During Failover

Below are the complete JSON payloads returned by `GET /v1/projects/public/topics/topic7/failover` as the failover operation progressed through each stage (`PENDING` → `PREPARE` → `DRAIN` → `SWITCH` → `COMPLETED`):

### 1. Initial State: `PENDING`
```json
{
  "topicFqn": "public.topic7",
  "version": 0,
  "transitionKind": "FAILOVER",
  "operationId": "029936c8-9405-4faa-a0da-35fdbc3bdd78",
  "sourceRegion": "default",
  "targetRegion": "region2",
  "waitForReplicationLagToClear": true,
  "onLagTimeout": "ABORT",
  "currentStage": "PENDING",
  "topicVersionToAwait": 0,
  "stageStartedAt": 1793624949021,
  "abortRequested": false,
  "stageHistory": [],
  "name": "public.topic7",
  "entityType": "TRANSITION_OBJECT"
}
```

### 2. In-Flight Stage: `PREPARE`
```json
{
  "topicFqn": "public.topic7",
  "version": 1,
  "transitionKind": "FAILOVER",
  "operationId": "029936c8-9405-4faa-a0da-35fdbc3bdd78",
  "sourceRegion": "default",
  "targetRegion": "region2",
  "waitForReplicationLagToClear": true,
  "onLagTimeout": "ABORT",
  "currentStage": "PREPARE",
  "topicVersionToAwait": 2,
  "stageStartedAt": 1793624949040,
  "abortRequested": false,
  "stageHistory": [
    {
      "stage": "PENDING",
      "startedAt": 1793624949021,
      "ackedHosts": [],
      "endedAt": 1793624949039,
      "outcome": "OK",
      "hostAcks": {}
    }
  ],
  "name": "public.topic7",
  "entityType": "TRANSITION_OBJECT"
}
```

### 3. In-Flight Stage: `DRAIN`
```json
{
  "topicFqn": "public.topic7",
  "version": 2,
  "transitionKind": "FAILOVER",
  "operationId": "029936c8-9405-4faa-a0da-35fdbc3bdd78",
  "sourceRegion": "default",
  "targetRegion": "region2",
  "waitForReplicationLagToClear": true,
  "onLagTimeout": "ABORT",
  "currentStage": "DRAIN",
  "topicVersionToAwait": 3,
  "stageStartedAt": 1793624949107,
  "abortRequested": false,
  "stageHistory": [
    {
      "stage": "PENDING",
      "startedAt": 1793624949021,
      "ackedHosts": [],
      "endedAt": 1793624949039,
      "outcome": "OK",
      "hostAcks": {}
    },
    {
      "stage": "PREPARE",
      "startedAt": 1793624949040,
      "ackedHosts": [
        "BLRETV-YWCP2P6V.local"
      ],
      "endedAt": 1793624949106,
      "outcome": "OK",
      "hostAcks": {
        "BLRETV-YWCP2P6V.local": "ACKED"
      }
    }
  ],
  "name": "public.topic7",
  "entityType": "TRANSITION_OBJECT"
}
```

### 4. In-Flight Stage: `SWITCH`
```json
{
  "topicFqn": "public.topic7",
  "version": 3,
  "transitionKind": "FAILOVER",
  "operationId": "029936c8-9405-4faa-a0da-35fdbc3bdd78",
  "sourceRegion": "default",
  "targetRegion": "region2",
  "waitForReplicationLagToClear": true,
  "onLagTimeout": "ABORT",
  "currentStage": "SWITCH",
  "topicVersionToAwait": 4,
  "stageStartedAt": 1793624949138,
  "abortRequested": false,
  "stageHistory": [
    {
      "stage": "PENDING",
      "startedAt": 1793624949021,
      "ackedHosts": [],
      "endedAt": 1793624949039,
      "outcome": "OK",
      "hostAcks": {}
    },
    {
      "stage": "PREPARE",
      "startedAt": 1793624949040,
      "ackedHosts": [
        "BLRETV-YWCP2P6V.local"
      ],
      "endedAt": 1793624949106,
      "outcome": "OK",
      "hostAcks": {
        "BLRETV-YWCP2P6V.local": "ACKED"
      }
    },
    {
      "stage": "DRAIN",
      "startedAt": 1793624949107,
      "ackedHosts": [
        "BLRETV-YWCP2P6V.local"
      ],
      "endedAt": 1793624949138,
      "outcome": "OK",
      "hostAcks": {
        "BLRETV-YWCP2P6V.local": "ACKED"
      }
    }
  ],
  "name": "public.topic7",
  "entityType": "TRANSITION_OBJECT"
}
```

### 5. Completed Transition: `COMPLETED` (Before ZK Node Deletion)
```json
{
  "topicFqn": "public.topic7",
  "version": 4,
  "transitionKind": "FAILOVER",
  "operationId": "029936c8-9405-4faa-a0da-35fdbc3bdd78",
  "sourceRegion": "default",
  "targetRegion": "region2",
  "waitForReplicationLagToClear": true,
  "onLagTimeout": "ABORT",
  "currentStage": "COMPLETED",
  "topicVersionToAwait": 0,
  "stageStartedAt": 1793624949177,
  "abortRequested": false,
  "stageHistory": [
    {
      "stage": "PENDING",
      "startedAt": 1793624949021,
      "ackedHosts": [],
      "endedAt": 1793624949039,
      "outcome": "OK",
      "hostAcks": {}
    },
    {
      "stage": "PREPARE",
      "startedAt": 1793624949040,
      "ackedHosts": [
        "BLRETV-YWCP2P6V.local"
      ],
      "endedAt": 1793624949106,
      "outcome": "OK",
      "hostAcks": {
        "BLRETV-YWCP2P6V.local": "ACKED"
      }
    },
    {
      "stage": "DRAIN",
      "startedAt": 1793624949107,
      "ackedHosts": [
        "BLRETV-YWCP2P6V.local"
      ],
      "endedAt": 1793624949138,
      "outcome": "OK",
      "hostAcks": {
        "BLRETV-YWCP2P6V.local": "ACKED"
      }
    },
    {
      "stage": "SWITCH",
      "startedAt": 1793624949138,
      "ackedHosts": [
        "BLRETV-YWCP2P6V.local"
      ],
      "endedAt": 1793624949177,
      "outcome": "OK",
      "hostAcks": {
        "BLRETV-YWCP2P6V.local": "ACKED"
      }
    }
  ],
  "name": "public.topic7",
  "entityType": "TRANSITION_OBJECT"
}
```

---

## Verification Summary
- **Stage Progression:** `PENDING` → `PREPARE` → `DRAIN` → `SWITCH` → `COMPLETED`
- **Host ACKs:** `ackedHosts` and `hostAcks` now capture all participating cluster hosts (e.g. `BLRETV-YWCP2P6V.local: ACKED`).
- **Timestamps:** Every historical stage entry in `stageHistory` records accurate `startedAt` and `endedAt` timestamps in milliseconds.
