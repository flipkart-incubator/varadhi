# Topic Failover — Base Case Timeline

**Date:** 2026-07-20 04:06:15 
**Topic:** `default_project.failover_grouped` (grouped, `X_GROUP_ID=g1`)
**Direction:** region-a → region-b
**Raw log:** `failover_validation_report_20260720_040603.log`

## Outcome

| Check | Result |
|-------|--------|
| Functional pass | **Pass** |
| ZK activeRegion | `region-b` |
| ZK topicState | `Producing` |
| Produce HTTP 200 | 4 |
| Produce HTTP 422 (Fenced) | 54 |
| Fenced window | 04:06:09.804 → 04:06:14.798 |
| First 200 after fence | — |

## Timeline (main scenario)

| Time | Event | API | HTTP | Response / payload | Notes |
|------|-------|-----|------|-------------------|-------|
| 04:06:03.704 | Health check | GET /v1/health-check | **200** | `"iam_ok"` |  |
| 04:06:09.729 | POST failover | POST /failover | **200** | `{"operationId":"615ca744-2b4a-4879-b653-1a07fa94f64c","version":0,"topicFqn":"default_project.failover_grouped","requestedBy":"thanos","s...` | region-a → region-b, waitLag=false |
| 04:06:09.767 | GET failover → SWITCH | GET /failover | **200** | `{"topicFqn":"default_project.failover_grouped","version":2,"transitionKind":"FAILOVER","operationId":"615ca744-2b4a-4879-b653-1a07fa94f64c","sourceRegion":"region-a","targetRegi...` | topicVersionToAwait=21 |
| 04:06:09.803 | produce #6 | POST /produce | **422** | `{"reason":"Topic/Queue is fenced during failover. Retry after failover completes."}` | First Fenced (422) |
| 04:06:14.831 | GET failover (idle) | GET /failover | **500** | `{"reason":"java.lang.Exception: com.flipkart.varadhi.common.exceptions.ResourceNotFoundException: No active failover for topic default_pr...` | Transition deleted |
|  | Produce ×54 | POST /produce | **422** | `{"reason":"Topic/Queue is fenced during failover..."}` | 54 fenced responses |

### SWITCH GET response (04:06:09.767)

```json
{
  "topicFqn": "default_project.failover_grouped",
  "version": 2,
  "transitionKind": "FAILOVER",
  "operationId": "615ca744-2b4a-4879-b653-1a07fa94f64c",
  "sourceRegion": "region-a",
  "targetRegion": "region-b",
  "createdAt": 1784500569722,
  "currentStage": "SWITCH",
  "topicVersionToAwait": 21,
  "updatedAt": 1784500569756,
  "stageHistory": [
    {
      "stage": "PREPARE",
      "startedAt": 1784500569737,
      "ackedHosts": [],
      "endedAt": 0,
      "outcome": "IN_PROGRESS"
    },
    {
      "stage": "SWITCH",
      "startedAt": 1784500569756,
      "ackedHosts": [],
      "endedAt": 0,
      "outcome": "IN_PROGRESS"
    }
  ],
  "name": "default_project.failover_grouped",
  "entityType": "TRANSITION_OBJECT"
}
```

### First Fenced produce 422 (04:06:09.803)

```json
{
  "reason": "Topic/Queue is fenced during failover. Retry after failover completes."
}
```

## ZK topic snapshots during failover

| Time | Phase | version | topicState | activeRegion | Snapshot |
|------|-------|---------|------------|--------------|----------|
| 04:06:09.254 | before failover | 19 | `Producing` | `region-a` | see below |
| 04:06:09.806 | SWITCH (Fenced) | 20 | `Fenced` | `region-b` | see below |
| 04:06:14.854 | after failover (COMPLETED) | 21 | `Producing` | `region-b` | see below |

### ZK topic @ before failover (04:06:09.254)

```json
{
  "activeRegion": "region-a",
  "topicState": "Producing",
  "version": 19,
  "grouped": true,
  "autoFailover": false,
  "regionConfigs": {},
  "_zkVersion": 20
}
```

### ZK topic @ SWITCH (Fenced) (04:06:09.806)

```json
{
  "activeRegion": "region-b",
  "topicState": "Fenced",
  "version": 20,
  "grouped": true,
  "autoFailover": false,
  "regionConfigs": {},
  "_zkVersion": 21
}
```

### ZK topic @ after failover (COMPLETED) (04:06:14.854)

```json
{
  "activeRegion": "region-b",
  "topicState": "Producing",
  "version": 21,
  "grouped": true,
  "autoFailover": false,
  "regionConfigs": {},
  "_zkVersion": 22
}
```

## ZK topic after failover

```json
{
  "activeRegion": "region-b",
  "topicState": "Producing",
  "version": 21,
  "grouped": true,
  "autoFailover": false,
  "regionConfigs": {},
  "_zkVersion": 22
}
```

## Produce HTTP summary

| HTTP | Count | Phase |
|------|-------|-------|
| 200 | 4 | PREPARE + post-COMPLETED |
| 422 | 54 | SWITCH fenced window |
