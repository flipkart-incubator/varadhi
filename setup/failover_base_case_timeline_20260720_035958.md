# Topic Failover — Base Case Timeline

**Date:** 2026-07-20 03:59:59 
**Topic:** `default_project.failover_grouped` (grouped)
**Direction:** region-a → region-b
**Raw log:** `failover_validation_report_20260720_035958.log`

## Outcome

| Check | Result |
|-------|--------|
| Functional pass | **Pass** |
| ZK activeRegion | `region-b` |
| ZK topicState | `Producing` |
| Produce 200 count | 8 |
| Produce 422 (Fenced) count | 0 |
| First 422 | — |
| Last 422 | — |
| First 200 after fence | — |

## Timeline

| Time | Event | API | HTTP | Response / payload | Notes |
|------|-------|-----|------|-------------------|-------|
| 03:59:58.103 | Health check | GET health-check | **200** | `"iam_ok"` |  |
| 03:59:58.193 | POST failover | POST failover | **200** | `{"operationId":"bac7fa5f-2852-4fdc-a472-3213365583df","version":0,"topicFqn":"default_project.failover_grouped","requestedBy":"thanos","sourceRegion":"region-b","targetRegion":"region-a","waitForRe...` | region-a → region-b |
| 03:59:58.203 | GET failover → PENDING | GET failover | **200** | `{"topicFqn":"default_project.failover_grouped","version":0,"transitionKind":"FAILOVER","operationId":"bac7fa5f-2852-4fdc-a472-3213365583df","sourceRegion":"region-b","targetRegion":"region-a","crea...` | topicVersionToAwait=0 |
| 03:59:58.626 | GET failover (idle) | GET failover | **500** | `{"reason":"java.lang.Exception: com.flipkart.varadhi.common.exceptions.ResourceNotFoundException: No active failover for topic default_project.failover_grouped."}` | Transition deleted |
| 03:59:58.698 | produce #1 | POST produce | **200** | `"pre-failover_grouped"` |  |
| 03:59:58.719 | GET failover (idle) | GET failover | **500** | `{"reason":"java.lang.Exception: com.flipkart.varadhi.common.exceptions.ResourceNotFoundException: No active failover for topic default_project.failover_grouped."}` | Transition deleted |
| 03:59:58.740 | produce #2 | POST produce | **200** | `"load-failover_grouped-1-1784500198707"` |  |
| 03:59:58.880 | produce #3 | POST produce | **200** | `"load-failover_grouped-2-1784500198824"` |  |
| 03:59:59.002 | produce #4 | POST produce | **200** | `"load-failover_grouped-3-1784500198968"` |  |
| 03:59:59.119 | produce #5 | POST produce | **200** | `"load-failover_grouped-4-1784500199086"` |  |
| 03:59:59.131 | POST failover | POST failover | **200** | `{"operationId":"902c2686-2f28-44ff-b946-48bd0b5a1bf6","version":0,"topicFqn":"default_project.failover_grouped","requestedBy":"thanos","sourceRegion":"region-a","targetRegion":"region-b","waitForRe...` | region-a → region-b |
| 03:59:59.155 | GET failover → PREPARE | GET failover | **200** | `{"topicFqn":"default_project.failover_grouped","version":1,"transitionKind":"FAILOVER","operationId":"902c2686-2f28-44ff-b946-48bd0b5a1bf6","sourceRegion":"region-a","targetRegion":"region-b","crea...` | topicVersionToAwait=12 |
| 03:59:59.237 | GET failover (idle) | GET failover | **500** | `{"reason":"java.lang.Exception: com.flipkart.varadhi.common.exceptions.ResourceNotFoundException: No active failover for topic default_project.failover_grouped."}` | Transition deleted |
| 03:59:59.237 | produce #6 | POST produce | **200** | `"load-failover_grouped-5-1784500199205"` |  |
| 03:59:59.376 | produce #7 | POST produce | **200** | `"load-failover_grouped-6-1784500199340"` |  |
| 03:59:59.498 | produce #8 | POST produce | **200** | `"load-failover_grouped-7-1784500199462"` |  |
| 03:59:59.574 | GET failover (idle) | GET failover | **500** | `{"reason":"java.lang.Exception: com.flipkart.varadhi.common.exceptions.ResourceNotFoundException: No active failover for topic default_project.failover_grouped."}` | Transition deleted |
| 03:59:59.619 | produce #9 | POST produce | **200** | `"load-failover_grouped-8-1784500199583"` |  |

### PREPARE GET response (03:59:59.155)

```json
{
  "topicFqn": "default_project.failover_grouped",
  "version": 1,
  "transitionKind": "FAILOVER",
  "operationId": "902c2686-2f28-44ff-b946-48bd0b5a1bf6",
  "sourceRegion": "region-a",
  "targetRegion": "region-b",
  "createdAt": 1784500199124,
  "currentStage": "PREPARE",
  "topicVersionToAwait": 12,
  "updatedAt": 1784500199138,
  "stageHistory": [
    {
      "stage": "PREPARE",
      "startedAt": 1784500199138,
      "ackedHosts": [],
      "endedAt": 0,
      "outcome": "IN_PROGRESS"
    }
  ],
  "name": "default_project.failover_grouped",
  "entityType": "TRANSITION_OBJECT"
}
```

## ZK topic after failover

```json
{
  "activeRegion": "region-b",
  "topicState": "Producing",
  "version": 13,
  "grouped": true
}
```
