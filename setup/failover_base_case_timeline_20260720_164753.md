# Topic Failover — Base Case Timeline

**Date:** 2026-07-20 16:48:01 
**Topic:** `default_project.failover_grouped` (grouped, `X_GROUP_ID=g1`)
**Direction:** region-a → region-b
**Raw log:** `failover_validation_report_20260720_164753.log`

## Outcome

| Check | Result |
|-------|--------|
| Functional pass | **Fail** |
| ZK producing region | `region-a` |
| ZK topicState | `Producing` |
| Produce HTTP 200 | 6 |
| Produce HTTP 422 (Fenced) | 0 |
| Fenced window | — |
| First 200 after fence | — |

## Timeline (main scenario)

| Time | Event | API | HTTP | Response / payload | Notes |
|------|-------|-----|------|-------------------|-------|
| 16:47:53.881 | Health check | GET /v1/health-check | **200** | `"iam_ok"` |  |
| 16:48:00.600 | POST failover | POST /failover | **200** | `{"operationId":"f5028cf2-5693-41fe-8a1f-bd5f2231f6ab","version":0,"topicFqn":"default_project.failover_grouped","requestedBy":"thanos","s...` | region-a → region-b, waitLag=false |
| 16:48:00.626 | GET failover → PREPARE | GET /failover | **200** | `{"topicFqn":"default_project.failover_grouped","version":1,"transitionKind":"FAILOVER","operationId":"f5028cf2-5693-41fe-8a1f-bd5f2231f6ab","sourceRegion":"region-a","targetRegi...` | topicVersionToAwait=32 |
| 16:48:00.749 | produce #5 | POST /produce | **200** | `"load-failover_grouped-4-1784546280688"` |  |
| 16:48:00.779 | GET failover (idle) | GET /failover | **500** | `{"reason":"java.lang.Exception: com.flipkart.varadhi.common.exceptions.ResourceNotFoundException: No active failover for topic default_pr...` | Transition deleted |
| 16:48:00.891 | produce #6 | POST /produce | **200** | `"load-failover_grouped-5-1784546280859"` |  |
| 16:48:01.065 | GET failover (idle) | GET /failover | **500** | `{"reason":"java.lang.Exception: com.flipkart.varadhi.common.exceptions.ResourceNotFoundException: No active failover for topic default_pr...` | Transition deleted |
| 16:48:01.069 | produce #7 | POST /produce | **200** | `"load-failover_grouped-6-1784546280994"` |  |

### PREPARE GET response (16:48:00.626)

```json
{
  "topicFqn": "default_project.failover_grouped",
  "version": 1,
  "transitionKind": "FAILOVER",
  "operationId": "f5028cf2-5693-41fe-8a1f-bd5f2231f6ab",
  "sourceRegion": "region-a",
  "targetRegion": "region-b",
  "createdAt": 1784546280592,
  "currentStage": "PREPARE",
  "topicVersionToAwait": 32,
  "updatedAt": 1784546280610,
  "stageHistory": [
    {
      "stage": "PREPARE",
      "startedAt": 1784546280610,
      "ackedHosts": [],
      "endedAt": 0,
      "outcome": "IN_PROGRESS"
    }
  ],
  "name": "default_project.failover_grouped",
  "entityType": "TRANSITION_OBJECT"
}
```

## ZK topic snapshots during failover

| Time | Phase | version | topicState | producing | Snapshot |
|------|-------|---------|------------|-----------|----------|
| 16:47:59.539 | before failover | 31 | `Producing` | `region-a` | see below |

### ZK topic @ before failover (16:47:59.539)

```json
{
  "topicState": "Producing",
  "version": 31,
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
  "_zkVersion": 32
}
```

## ZK topic after failover

```json
{
  "topicState": "Producing",
  "version": 31,
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
  "_zkVersion": 32
}
```

## Produce HTTP summary

| HTTP | Count | Phase |
|------|-------|-------|
| 200 | 6 | PREPARE + post-COMPLETED |
| 422 | 0 | SWITCH fenced window |
