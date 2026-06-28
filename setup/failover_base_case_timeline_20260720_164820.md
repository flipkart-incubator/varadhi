# Topic Failover — Base Case Timeline

**Date:** 2026-07-20 16:48:21 
**Topic:** `default_project.failover_grouped` (grouped, `X_GROUP_ID=g1`)
**Direction:** region-a → region-b
**Raw log:** `failover_validation_report_20260720_164820.log`

## Outcome

| Check | Result |
|-------|--------|
| Functional pass | **Fail** |
| ZK producing region | `region-a` |
| ZK topicState | `Producing` |
| Produce HTTP 200 | 7 |
| Produce HTTP 422 (Fenced) | 0 |
| Fenced window | — |
| First 200 after fence | — |

## Timeline (main scenario)

| Time | Event | API | HTTP | Response / payload | Notes |
|------|-------|-----|------|-------------------|-------|
| 16:48:20.379 | Health check | GET /v1/health-check | **200** | `"iam_ok"` |  |
| 16:48:20.941 | POST failover | POST /failover | **200** | `{"operationId":"c1b2d91c-3454-4fa4-818f-9fbc6a76a387","version":0,"topicFqn":"default_project.failover_grouped","requestedBy":"thanos","s...` | region-a → region-b, waitLag=false |
| 16:48:20.950 | GET failover → PENDING | GET /failover | **200** | `{"topicFqn":"default_project.failover_grouped","version":0,"transitionKind":"FAILOVER","operationId":"c1b2d91c-3454-4fa4-818f-9fbc6a76a387","sourceRegion":"region-a","targetRegi...` | topicVersionToAwait=0 |
| 16:48:21.037 | produce #6 | POST /produce | **200** | `"load-failover_grouped-5-1784546300988"` |  |
| 16:48:21.051 | GET failover (idle) | GET /failover | **500** | `{"reason":"java.lang.Exception: com.flipkart.varadhi.common.exceptions.ResourceNotFoundException: No active failover for topic default_pr...` | Transition deleted |
| 16:48:21.156 | produce #7 | POST /produce | **200** | `"load-failover_grouped-6-1784546301120"` |  |
| 16:48:21.308 | produce #8 | POST /produce | **200** | `"load-failover_grouped-7-1784546301242"` |  |
| 16:48:21.380 | GET failover (idle) | GET /failover | **500** | `{"reason":"java.lang.Exception: com.flipkart.varadhi.common.exceptions.ResourceNotFoundException: No active failover for topic default_pr...` | Transition deleted |

## ZK topic snapshots during failover

| Time | Phase | version | topicState | producing | Snapshot |
|------|-------|---------|------------|-----------|----------|
| 16:48:20.436 | before failover | 31 | `Producing` | `region-a` | see below |

### ZK topic @ before failover (16:48:20.436)

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
| 200 | 7 | PREPARE + post-COMPLETED |
| 422 | 0 | SWITCH fenced window |
