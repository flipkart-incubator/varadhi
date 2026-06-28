# Topic Failover — Base Case Timeline

**Date:** 2026-07-20 16:53:25 
**Topic:** `default_project.failover_grouped` (grouped, `X_GROUP_ID=g1`)
**Direction:** region-a → region-b
**Raw log:** `failover_validation_report_20260720_165313.log`

## Outcome

| Check | Result |
|-------|--------|
| Functional pass | **Pass** |
| ZK producing region | `region-b` |
| ZK topicState | `Producing` |
| Produce HTTP 200 | 6 |
| Produce HTTP 422 (Fenced) | 49 |
| Fenced window | 16:53:19.353 → 16:53:24.459 |
| First 200 after fence | 16:53:24.652 |

## Timeline (main scenario)

| Time | Event | API | HTTP | Response / payload | Notes |
|------|-------|-----|------|-------------------|-------|
| 16:53:13.296 | Health check | GET /v1/health-check | **200** | `"iam_ok"` |  |
| 16:53:19.294 | POST failover | POST /failover | **200** | `{"operationId":"b74bdc25-742d-4feb-8209-aed28680c5e8","version":0,"topicFqn":"default_project.failover_grouped","requestedBy":"thanos","s...` | region-a → region-b, waitLag=false |
| 16:53:19.316 | GET failover → PREPARE | GET /failover | **200** | `{"topicFqn":"default_project.failover_grouped","version":1,"transitionKind":"FAILOVER","operationId":"b74bdc25-742d-4feb-8209-aed28680c5e8","sourceRegion":"region-a","targetRegi...` | topicVersionToAwait=38 |
| 16:53:19.353 | produce #6 | POST /produce | **422** | `{"reason":"Topic/Queue is fenced during failover. Retry after failover completes."}` | First Fenced (422) |
| 16:53:19.414 | GET failover → SWITCH | GET /failover | **200** | `{"topicFqn":"default_project.failover_grouped","version":2,"transitionKind":"FAILOVER","operationId":"b74bdc25-742d-4feb-8209-aed28680c5e8","sourceRegion":"region-a","targetRegi...` | topicVersionToAwait=39 |
| 16:53:24.443 | GET failover → DRAIN | GET /failover | **200** | `{"topicFqn":"default_project.failover_grouped","version":3,"transitionKind":"FAILOVER","operationId":"b74bdc25-742d-4feb-8209-aed28680c5e8","sourceRegion":"region-a","targetRegi...` | topicVersionToAwait=0 |
|  | Produce ×49 | POST /produce | **422** | `{"reason":"Topic/Queue is fenced during failover..."}` | 49 fenced responses |
| 16:53:24.604 | produce #55 | POST /produce | **200** | `"load-failover_grouped-54-1784546604539"` |  |
| 16:53:24.727 | GET failover (idle) | GET /failover | **500** | `{"reason":"java.lang.Exception: com.flipkart.varadhi.common.exceptions.ResourceNotFoundException: No active failover for topic default_pr...` | Transition deleted |
| 16:53:24.773 | produce #56 | POST /produce | **200** | `"load-failover_grouped-55-1784546604733"` |  |

### PREPARE GET response (16:53:19.316)

```json
{
  "topicFqn": "default_project.failover_grouped",
  "version": 1,
  "transitionKind": "FAILOVER",
  "operationId": "b74bdc25-742d-4feb-8209-aed28680c5e8",
  "sourceRegion": "region-a",
  "targetRegion": "region-b",
  "createdAt": 1784546599287,
  "currentStage": "PREPARE",
  "topicVersionToAwait": 38,
  "updatedAt": 1784546599301,
  "stageHistory": [
    {
      "stage": "PREPARE",
      "startedAt": 1784546599301,
      "ackedHosts": [],
      "endedAt": 0,
      "outcome": "IN_PROGRESS"
    }
  ],
  "name": "default_project.failover_grouped",
  "entityType": "TRANSITION_OBJECT"
}
```

### SWITCH GET response (16:53:19.414)

```json
{
  "topicFqn": "default_project.failover_grouped",
  "version": 2,
  "transitionKind": "FAILOVER",
  "operationId": "b74bdc25-742d-4feb-8209-aed28680c5e8",
  "sourceRegion": "region-a",
  "targetRegion": "region-b",
  "createdAt": 1784546599287,
  "currentStage": "SWITCH",
  "topicVersionToAwait": 39,
  "updatedAt": 1784546599323,
  "stageHistory": [
    {
      "stage": "PREPARE",
      "startedAt": 1784546599301,
      "ackedHosts": [],
      "endedAt": 0,
      "outcome": "IN_PROGRESS"
    },
    {
      "stage": "SWITCH",
      "startedAt": 1784546599323,
      "ackedHosts": [],
      "endedAt": 0,
      "outcome": "IN_PROGRESS"
    }
  ],
  "name": "default_project.failover_grouped",
  "entityType": "TRANSITION_OBJECT"
}
```

### First Fenced produce 422 (16:53:19.353)

```json
{
  "reason": "Topic/Queue is fenced during failover. Retry after failover completes."
}
```

## ZK topic snapshots during failover

| Time | Phase | version | topicState | producing | Snapshot |
|------|-------|---------|------------|-----------|----------|
| 16:53:18.832 | before failover | 37 | `Producing` | `region-a` | see below |
| 16:53:19.374 | SWITCH (Fenced) | 38 | `Fenced` | `region-b` | see below |
| 16:53:24.477 | DRAIN | 39 | `Producing` | `region-b` | see below |

### ZK topic @ before failover (16:53:18.832)

```json
{
  "topicState": "Producing",
  "version": 37,
  "grouped": true,
  "autoFailover": false,
  "regionConfigs": {
    "region-a": {
      "produceAllowed": true
    },
    "region-b": {
      "produceAllowed": false
    }
  },
  "_zkVersion": 38
}
```

### ZK topic @ SWITCH (Fenced) (16:53:19.374)

```json
{
  "topicState": "Fenced",
  "version": 38,
  "grouped": true,
  "autoFailover": false,
  "regionConfigs": {
    "region-a": {
      "produceAllowed": false
    },
    "region-b": {
      "produceAllowed": true
    }
  },
  "_zkVersion": 39
}
```

### ZK topic @ DRAIN (16:53:24.477)

```json
{
  "topicState": "Producing",
  "version": 39,
  "grouped": true,
  "autoFailover": false,
  "regionConfigs": {
    "region-a": {
      "produceAllowed": false
    },
    "region-b": {
      "produceAllowed": true
    }
  },
  "_zkVersion": 40
}
```

## ZK topic after failover

```json
{
  "topicState": "Producing",
  "version": 39,
  "grouped": true,
  "autoFailover": false,
  "regionConfigs": {
    "region-a": {
      "produceAllowed": false
    },
    "region-b": {
      "produceAllowed": true
    }
  },
  "_zkVersion": 40
}
```

## Produce HTTP summary

| HTTP | Count | Phase |
|------|-------|-------|
| 200 | 6 | PREPARE + post-COMPLETED |
| 422 | 49 | SWITCH fenced window |
