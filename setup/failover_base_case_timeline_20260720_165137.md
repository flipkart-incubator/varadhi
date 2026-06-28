# Topic Failover — Base Case Timeline

**Date:** 2026-07-20 16:51:43 
**Topic:** `default_project.failover_grouped` (grouped, `X_GROUP_ID=g1`)
**Direction:** region-a → region-b
**Raw log:** `failover_validation_report_20260720_165137.log`

## Outcome

| Check | Result |
|-------|--------|
| Functional pass | **Pass** |
| ZK producing region | `region-b` |
| ZK topicState | `Producing` |
| Produce HTTP 200 | 4 |
| Produce HTTP 422 (Fenced) | 53 |
| Fenced window | 16:51:38.593 → 16:51:43.558 |
| First 200 after fence | 16:51:43.705 |

## Timeline (main scenario)

| Time | Event | API | HTTP | Response / payload | Notes |
|------|-------|-----|------|-------------------|-------|
| 16:51:37.268 | Health check | GET /v1/health-check | **200** | `"iam_ok"` |  |
| 16:51:38.488 | POST failover | POST /failover | **200** | `{"operationId":"efb92d14-f46a-4618-be1d-d5a2e274c0af","version":0,"topicFqn":"default_project.failover_grouped","requestedBy":"thanos","s...` | region-a → region-b, waitLag=false |
| 16:51:38.573 | produce #5 | POST /produce | **422** | `{"reason":"Topic/Queue is fenced during failover. Retry after failover completes."}` | First Fenced (422) |
| 16:51:38.591 | GET failover → SWITCH | GET /failover | **200** | `{"topicFqn":"default_project.failover_grouped","version":2,"transitionKind":"FAILOVER","operationId":"efb92d14-f46a-4618-be1d-d5a2e274c0af","sourceRegion":"region-a","targetRegi...` | topicVersionToAwait=35 |
| 16:51:43.626 | GET failover (idle) | GET /failover | **500** | `{"reason":"java.lang.Exception: com.flipkart.varadhi.common.exceptions.ResourceNotFoundException: No active failover for topic default_pr...` | Transition deleted |
|  | Produce ×53 | POST /produce | **422** | `{"reason":"Topic/Queue is fenced during failover..."}` | 53 fenced responses |
| 16:51:43.701 | produce #58 | POST /produce | **200** | `"load-failover_grouped-57-1784546503639"` |  |

### SWITCH GET response (16:51:38.591)

```json
{
  "topicFqn": "default_project.failover_grouped",
  "version": 2,
  "transitionKind": "FAILOVER",
  "operationId": "efb92d14-f46a-4618-be1d-d5a2e274c0af",
  "sourceRegion": "region-a",
  "targetRegion": "region-b",
  "createdAt": 1784546498474,
  "currentStage": "SWITCH",
  "topicVersionToAwait": 35,
  "updatedAt": 1784546498559,
  "stageHistory": [
    {
      "stage": "PREPARE",
      "startedAt": 1784546498501,
      "ackedHosts": [],
      "endedAt": 0,
      "outcome": "IN_PROGRESS"
    },
    {
      "stage": "SWITCH",
      "startedAt": 1784546498559,
      "ackedHosts": [],
      "endedAt": 0,
      "outcome": "IN_PROGRESS"
    }
  ],
  "name": "default_project.failover_grouped",
  "entityType": "TRANSITION_OBJECT"
}
```

### First Fenced produce 422 (16:51:38.573)

```json
{
  "reason": "Topic/Queue is fenced during failover. Retry after failover completes."
}
```

## ZK topic snapshots during failover

| Time | Phase | version | topicState | producing | Snapshot |
|------|-------|---------|------------|-----------|----------|
| 16:51:37.515 | before failover | 32 | `Producing` | `region-a` | see below |
| 16:51:38.590 | SWITCH (Fenced) | 34 | `Fenced` | `region-b` | see below |
| 16:51:43.609 | SWITCH | 35 | `Producing` | `region-b` | see below |

### ZK topic @ before failover (16:51:37.515)

```json
{
  "topicState": "Producing",
  "version": 32,
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
  "_zkVersion": 34
}
```

### ZK topic @ SWITCH (Fenced) (16:51:38.590)

```json
{
  "topicState": "Fenced",
  "version": 34,
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
  "_zkVersion": 35
}
```

### ZK topic @ SWITCH (16:51:43.609)

```json
{
  "topicState": "Producing",
  "version": 35,
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
  "_zkVersion": 36
}
```

## ZK topic after failover

```json
{
  "topicState": "Producing",
  "version": 35,
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
  "_zkVersion": 36
}
```

## Produce HTTP summary

| HTTP | Count | Phase |
|------|-------|-------|
| 200 | 4 | PREPARE + post-COMPLETED |
| 422 | 53 | SWITCH fenced window |
