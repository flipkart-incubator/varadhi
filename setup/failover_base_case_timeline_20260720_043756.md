# Topic Failover — Base Case Timeline

**Date:** 2026-07-20 04:38:08 
**Topic:** `default_project.failover_grouped` (grouped, `X_GROUP_ID=g1`)
**Direction:** region-a → region-b
**Raw log:** `failover_validation_report_20260720_043756.log`

## Outcome

| Check | Result |
|-------|--------|
| Functional pass | **Pass** |
| ZK producing region | `region-b` |
| ZK topicState | `Producing` |
| Produce HTTP 200 | 5 |
| Produce HTTP 422 (Fenced) | 53 |
| Fenced window | 04:38:02.687 → 04:38:07.626 |
| First 200 after fence | 04:38:07.765 |

## Timeline (main scenario)

| Time | Event | API | HTTP | Response / payload | Notes |
|------|-------|-----|------|-------------------|-------|
| 04:37:56.126 | Health check | GET /v1/health-check | **200** | `"iam_ok"` |  |
| 04:38:02.551 | POST failover | POST /failover | **200** | `{"operationId":"10894df1-d561-469e-8555-deb1d54a810e","version":0,"topicFqn":"default_project.failover_grouped","requestedBy":"thanos","s...` | region-a → region-b, waitLag=false |
| 04:38:02.563 | GET failover → PREPARE | GET /failover | **200** | `{"topicFqn":"default_project.failover_grouped","version":1,"transitionKind":"FAILOVER","operationId":"10894df1-d561-469e-8555-deb1d54a810e","sourceRegion":"region-a","targetRegi...` | topicVersionToAwait=27 |
| 04:38:02.571 | produce #5 | POST /produce | **200** | `"load-failover_grouped-4-1784502482540"` |  |
| 04:38:02.683 | produce #6 | POST /produce | **422** | `{"reason":"Topic/Queue is fenced during failover. Retry after failover completes."}` | First Fenced (422) |
| 04:38:02.714 | GET failover → SWITCH | GET /failover | **200** | `{"topicFqn":"default_project.failover_grouped","version":2,"transitionKind":"FAILOVER","operationId":"10894df1-d561-469e-8555-deb1d54a810e","sourceRegion":"region-a","targetRegi...` | topicVersionToAwait=28 |
| 04:38:07.699 | GET failover (idle) | GET /failover | **500** | `{"reason":"java.lang.Exception: com.flipkart.varadhi.common.exceptions.ResourceNotFoundException: No active failover for topic default_pr...` | Transition deleted |
|  | Produce ×53 | POST /produce | **422** | `{"reason":"Topic/Queue is fenced during failover..."}` | 53 fenced responses |
| 04:38:07.756 | produce #59 | POST /produce | **200** | `"load-failover_grouped-58-1784502487709"` |  |

### PREPARE GET response (04:38:02.563)

```json
{
  "topicFqn": "default_project.failover_grouped",
  "version": 1,
  "transitionKind": "FAILOVER",
  "operationId": "10894df1-d561-469e-8555-deb1d54a810e",
  "sourceRegion": "region-a",
  "targetRegion": "region-b",
  "createdAt": 1784502482543,
  "currentStage": "PREPARE",
  "topicVersionToAwait": 27,
  "updatedAt": 1784502482558,
  "stageHistory": [
    {
      "stage": "PREPARE",
      "startedAt": 1784502482558,
      "ackedHosts": [],
      "endedAt": 0,
      "outcome": "IN_PROGRESS"
    }
  ],
  "name": "default_project.failover_grouped",
  "entityType": "TRANSITION_OBJECT"
}
```

### SWITCH GET response (04:38:02.714)

```json
{
  "topicFqn": "default_project.failover_grouped",
  "version": 2,
  "transitionKind": "FAILOVER",
  "operationId": "10894df1-d561-469e-8555-deb1d54a810e",
  "sourceRegion": "region-a",
  "targetRegion": "region-b",
  "createdAt": 1784502482543,
  "currentStage": "SWITCH",
  "topicVersionToAwait": 28,
  "updatedAt": 1784502482605,
  "stageHistory": [
    {
      "stage": "PREPARE",
      "startedAt": 1784502482558,
      "ackedHosts": [],
      "endedAt": 0,
      "outcome": "IN_PROGRESS"
    },
    {
      "stage": "SWITCH",
      "startedAt": 1784502482605,
      "ackedHosts": [],
      "endedAt": 0,
      "outcome": "IN_PROGRESS"
    }
  ],
  "name": "default_project.failover_grouped",
  "entityType": "TRANSITION_OBJECT"
}
```

### First Fenced produce 422 (04:38:02.683)

```json
{
  "reason": "Topic/Queue is fenced during failover. Retry after failover completes."
}
```

## ZK topic snapshots during failover

| Time | Phase | version | topicState | producing | Snapshot |
|------|-------|---------|------------|-----------|----------|
| 04:38:01.786 | before failover | 26 | `Producing` | `region-a` | see below |
| 04:38:02.629 | SWITCH (Fenced) | 27 | `Fenced` | `region-b` | see below |
| 04:38:07.716 | after failover (COMPLETED) | 28 | `Producing` | `region-b` | see below |

### ZK topic @ before failover (04:38:01.786)

```json
{
  "topicState": "Producing",
  "version": 26,
  "grouped": true,
  "autoFailover": false,
  "regionConfigs": {
    "region-a": {
      "produceAllowed": true,
      "replicated": true
    },
    "region-b": {
      "produceAllowed": false,
      "replicated": true
    }
  },
  "_zkVersion": 27
}
```

### ZK topic @ SWITCH (Fenced) (04:38:02.629)

```json
{
  "topicState": "Fenced",
  "version": 27,
  "grouped": true,
  "autoFailover": false,
  "regionConfigs": {
    "region-a": {
      "produceAllowed": false,
      "replicated": true
    },
    "region-b": {
      "produceAllowed": true,
      "replicated": true
    }
  },
  "_zkVersion": 28
}
```

### ZK topic @ after failover (COMPLETED) (04:38:07.716)

```json
{
  "topicState": "Producing",
  "version": 28,
  "grouped": true,
  "autoFailover": false,
  "regionConfigs": {
    "region-a": {
      "produceAllowed": false,
      "replicated": true
    },
    "region-b": {
      "produceAllowed": true,
      "replicated": true
    }
  },
  "_zkVersion": 29
}
```

## ZK topic after failover

```json
{
  "topicState": "Producing",
  "version": 28,
  "grouped": true,
  "autoFailover": false,
  "regionConfigs": {
    "region-a": {
      "produceAllowed": false,
      "replicated": true
    },
    "region-b": {
      "produceAllowed": true,
      "replicated": true
    }
  },
  "_zkVersion": 29
}
```

## Produce HTTP summary

| HTTP | Count | Phase |
|------|-------|-------|
| 200 | 5 | PREPARE + post-COMPLETED |
| 422 | 53 | SWITCH fenced window |
