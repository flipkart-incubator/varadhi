# Topic Failover — Base Case Timeline

**Date:** 2026-07-20 04:02:18 
**Topic:** `default_project.failover_grouped` (grouped, `X_GROUP_ID=g1`)
**Direction:** region-a → region-b
**Raw log:** `failover_validation_report_20260720_040206.log`

## Outcome

| Check | Result |
|-------|--------|
| Functional pass | **Pass** |
| ZK activeRegion | `region-b` |
| ZK topicState | `Producing` |
| Produce HTTP 200 | 8 |
| Produce HTTP 422 (Fenced) | 53 |
| Fenced window | 04:02:12.932 → 04:02:17.811 |
| First 200 after fence | 04:02:17.948 |

## Timeline (main scenario)

| Time | Event | API | HTTP | Response / payload | Notes |
|------|-------|-----|------|-------------------|-------|
| 04:02:06.444 | Health check | GET /v1/health-check | **200** | `"iam_ok"` |  |
| 04:02:12.791 | POST failover | POST /failover | **200** | `{"operationId":"2b171b9f-7e92-4b4f-b5ce-2d692d7524d4","version":0,"topicFqn":"default_project.failover_grouped","requestedBy":"thanos","s...` | region-a → region-b, waitLag=false |
| 04:02:12.808 | GET failover → PENDING | GET /failover | **200** | `{"topicFqn":"default_project.failover_grouped","version":0,"transitionKind":"FAILOVER","operationId":"2b171b9f-7e92-4b4f-b5ce-2d692d7524d4","sourceRegion":"region-a","targetRegi...` | topicVersionToAwait=0 |
| 04:02:12.833 | produce #5 | POST /produce | **200** | `"load-failover_grouped-4-1784500332802"` |  |
| 04:02:12.926 | produce #6 | POST /produce | **422** | `{"reason":"Topic/Queue is fenced during failover. Retry after failover completes."}` | First Fenced (422) |
| 04:02:12.956 | GET failover → SWITCH | GET /failover | **200** | `{"topicFqn":"default_project.failover_grouped","version":2,"transitionKind":"FAILOVER","operationId":"2b171b9f-7e92-4b4f-b5ce-2d692d7524d4","sourceRegion":"region-a","targetRegi...` | topicVersionToAwait=17 |
|  | Produce ×53 | POST /produce | **422** | `{"reason":"Topic/Queue is fenced during failover..."}` | 53 fenced responses |
| 04:02:17.945 | produce #59 | POST /produce | **200** | `"load-failover_grouped-58-1784500337896"` |  |
| 04:02:18.064 | produce #60 | POST /produce | **200** | `"load-failover_grouped-59-1784500338029"` |  |
| 04:02:18.139 | GET failover (idle) | GET /failover | **500** | `{"reason":"java.lang.Exception: com.flipkart.varadhi.common.exceptions.ResourceNotFoundException: No active failover for topic default_pr...` | Transition deleted |
| 04:02:18.185 | produce #61 | POST /produce | **200** | `"load-failover_grouped-60-1784500338148"` |  |
| 04:02:18.250 | GET failover (idle) | GET /failover | **500** | `{"reason":"java.lang.Exception: com.flipkart.varadhi.common.exceptions.ResourceNotFoundException: No active failover for topic default_pr...` | Transition deleted |
| 04:02:18.305 | produce #62 | POST /produce | **200** | `"load-failover_grouped-61-1784500338272"` |  |

### SWITCH GET response (04:02:12.956)

```json
{
  "topicFqn": "default_project.failover_grouped",
  "version": 2,
  "transitionKind": "FAILOVER",
  "operationId": "2b171b9f-7e92-4b4f-b5ce-2d692d7524d4",
  "sourceRegion": "region-a",
  "targetRegion": "region-b",
  "createdAt": 1784500332781,
  "currentStage": "SWITCH",
  "topicVersionToAwait": 17,
  "updatedAt": 1784500332839,
  "stageHistory": [
    {
      "stage": "PREPARE",
      "startedAt": 1784500332803,
      "ackedHosts": [],
      "endedAt": 0,
      "outcome": "IN_PROGRESS"
    },
    {
      "stage": "SWITCH",
      "startedAt": 1784500332839,
      "ackedHosts": [],
      "endedAt": 0,
      "outcome": "IN_PROGRESS"
    }
  ],
  "name": "default_project.failover_grouped",
  "entityType": "TRANSITION_OBJECT"
}
```

### First Fenced produce 422 (04:02:12.926)

```json
{
  "reason": "Topic/Queue is fenced during failover. Retry after failover completes."
}
```

## ZK topic after failover

```json
{
  "activeRegion": "region-b",
  "topicState": "Producing",
  "version": 17,
  "grouped": true
}
```

## Produce HTTP summary

| HTTP | Count | Phase |
|------|-------|-------|
| 200 | 8 | PREPARE + post-COMPLETED |
| 422 | 53 | SWITCH fenced window |
