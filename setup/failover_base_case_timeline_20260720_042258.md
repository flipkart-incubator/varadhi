# Topic Failover — Base Case Timeline

**Date:** 2026-07-20 04:23:38 
**Topic:** `default_project.failover_grouped` (grouped, `X_GROUP_ID=g1`)
**Direction:** region-a → region-b
**Raw log:** `failover_validation_report_20260720_042258.log`

## Outcome

| Check | Result |
|-------|--------|
| Functional pass | **Pass** |
| ZK producing region | `region-b` |
| ZK topicState | `Producing` |
| Produce HTTP 200 | 7 |
| Produce HTTP 422 (Fenced) | 0 |
| Fenced window | — |
| First 200 after fence | — |

## Timeline (main scenario)

| Time | Event | API | HTTP | Response / payload | Notes |
|------|-------|-----|------|-------------------|-------|
| 04:22:58.624 | Health check | GET /v1/health-check | **200** | `"iam_ok"` |  |
| 04:23:37.437 | POST failover | POST /failover | **200** | `{"operationId":"465c9234-73d2-4912-944f-5ccef0d667f8","version":0,"topicFqn":"default_project.failover_grouped","requestedBy":"thanos","s...` | region-a → region-b, waitLag=false |
| 04:23:37.450 | GET failover → PENDING | GET /failover | **200** | `{"topicFqn":"default_project.failover_grouped","version":0,"transitionKind":"FAILOVER","operationId":"465c9234-73d2-4912-944f-5ccef0d667f8","sourceRegion":"region-a","targetRegi...` | topicVersionToAwait=0 |
| 04:23:37.489 | produce #5 | POST /produce | **200** | `"load-failover_grouped-4-1784501617452"` |  |
| 04:23:37.603 | GET failover (idle) | GET /failover | **500** | `{"reason":"java.lang.Exception: com.flipkart.varadhi.common.exceptions.ResourceNotFoundException: No active failover for topic default_pr...` | Transition deleted |
| 04:23:37.607 | produce #6 | POST /produce | **200** | `"load-failover_grouped-5-1784501617573"` |  |
| 04:23:37.735 | produce #7 | POST /produce | **200** | `"load-failover_grouped-6-1784501617703"` |  |
| 04:23:37.857 | produce #8 | POST /produce | **200** | `"load-failover_grouped-7-1784501617825"` |  |
| 04:23:37.876 | GET failover (idle) | GET /failover | **500** | `{"reason":"java.lang.Exception: com.flipkart.varadhi.common.exceptions.ResourceNotFoundException: No active failover for topic default_pr...` | Transition deleted |

## ZK topic snapshots during failover

| Time | Phase | version | topicState | producing | Snapshot |
|------|-------|---------|------------|-----------|----------|
| 04:23:36.674 | before failover | 22 | `Fenced` | `region-a` | see below |
| 04:23:37.515 | PENDING | 24 | `Producing` | `region-b` | see below |

### ZK topic @ before failover (04:23:36.674)

```json
{
  "topicState": "Fenced",
  "version": 22,
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
  "_zkVersion": 23
}
```

### ZK topic @ PENDING (04:23:37.515)

```json
{
  "topicState": "Producing",
  "version": 24,
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
  "_zkVersion": 25
}
```

## ZK topic after failover

```json
{
  "topicState": "Producing",
  "version": 24,
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
  "_zkVersion": 25
}
```

## Produce HTTP summary

| HTTP | Count | Phase |
|------|-------|-------|
| 200 | 7 | PREPARE + post-COMPLETED |
| 422 | 0 | SWITCH fenced window |
