# Topic Failover — Base Case Run Report

**Date:** 2026-07-17  
**Environment:** Local OSS Varadhi (`localhost:18488`), single pod (Server + Controller + Consumer)  
**Raw log:** `setup/failover_validation_report_20260717_052153.log`

---

## Scenario

| Field | Value |
|-------|-------|
| Topic | `default_project.failover_grouped` (grouped topic) |
| Direction | `region-a` → `region-b` |
| Replication lag wait | **Skipped** (`waitForReplicationLagToClear: false`) |
| Active produce | **Yes** — continuous loop (~80 ms interval) with `X_GROUP_ID: g1` |
| Test delay | `transitionAckReportDelayMs: 5000` (extends SWITCH blocked window for observability) |

**Goal:** Confirm produce stays allowed during PREPARE, is blocked during SWITCH, resumes after COMPLETED, and `activeRegion` flips to `region-b`.

---

## Outcome (functional)

| Check | Result |
|-------|--------|
| Failover created | **Pass** — op `2fb92562-d4b3-42f2-b03b-135e911211e5` |
| PREPARE — produce allowed | **Pass** — HTTP 200 on produce #1–5 |
| SWITCH — produce blocked | **Pass** — 46× HTTP 422 over ~5 s |
| COMPLETED — produce resumed | **Pass** — HTTP 200 from `05:22:00.007` onward |
| ZK `activeRegion` after forward | **Pass** — `region-b` |
| ZK `topicState` after forward | **Pass** — `Producing` |
| GET failover when idle | **Expected 404** — returns **500** (known cluster-bus exception wrapping bug) |

Harness marked this scenario **FAIL** only because the poll loop waited for HTTP **404** on GET failover after completion; the server returned **500** with `ResourceNotFoundException` embedded in the body. Functionally the transition was deleted and failover succeeded.

---

## Timeline (ordered)

### Phase 0 — Setup

| Time | Event | HTTP | Notes |
|------|-------|------|-------|
| 05:21:53.801 | Health check | 200 | Server ready |
| 05:21:53.978 | Pre-check | — | `activeRegion=region-a` (expected) |
| 05:21:54.065 | Pre-produce | **200** | Sanity produce before failover |
| 05:21:54.151 | GET failover | 500* | No active transition (*should be 404) |

### Phase 1 — Trigger failover (PREPARE)

| Time | Event | HTTP | Response excerpt |
|------|-------|------|------------------|
| 05:21:54.174 | Produce #1 | **200** | Load loop started |
| 05:21:54.365 | Produce #2 | **200** | |
| 05:21:54.471 | **POST failover** | — | `region-a → region-b`, skip lag |
| 05:21:54.515 | Produce #3 | **200** | Still allowed in PREPARE |
| 05:21:54.553 | POST failover response | **200** | `"state": "IN_PROGRESS"`, opId `2fb92562-…` |
| 05:21:54.609 | GET failover | **200** | `"currentStage": "PREPARE"`, `topicVersionToAwait: 4` |
| 05:21:54.660 | Produce #4 | **200** | |
| 05:21:54.819 | Produce #5 | **200** | Last 200 before block |

**PREPARE GET response (formatted):**

```json
{
  "topicFqn": "default_project.failover_grouped",
  "operationId": "2fb92562-d4b3-42f2-b03b-135e911211e5",
  "sourceRegion": "region-a",
  "targetRegion": "region-b",
  "currentStage": "PREPARE",
  "topicVersionToAwait": 4,
  "stageHistory": [
    { "stage": "PREPARE", "outcome": "IN_PROGRESS", "ackedHosts": [] }
  ]
}
```

### Phase 2 — SWITCH (produce blocked)

| Time | Event | HTTP | Notes |
|------|-------|------|-------|
| 05:21:54.912 | Produce #6 | **422** | First blocked — SWITCH window opens |
| 05:21:55.056 | GET failover | **200** | `"currentStage": "SWITCH"`, `topicVersionToAwait: 5` |
| 05:21:55.007 – 05:21:59.864 | Produce #7–51 | **422** | 46 blocked responses total |
| 05:21:59.864 | Produce #51 | **422** | Last blocked |

**422 body (every blocked produce):**

```json
{ "reason": "Topic/Queue is blocked. Unblock the Topic/Queue before produce." }
```

**SWITCH GET response (formatted):**

```json
{
  "currentStage": "SWITCH",
  "topicVersionToAwait": 5,
  "stageHistory": [
    { "stage": "PREPARE", "outcome": "IN_PROGRESS", "ackedHosts": [] },
    { "stage": "SWITCH",  "outcome": "IN_PROGRESS", "ackedHosts": [] }
  ]
}
```

> **Note:** `ackedHosts` is empty in GET responses because acks are tracked in-memory on the controller (`StageAwaiter`) and not yet persisted to the ZK transition object.

**Blocked window:** ~5.0 s (`05:21:54.912` → `05:22:00.007`), matching `transitionAckReportDelayMs: 5000`.

### Phase 3 — COMPLETED (produce unblocked)

| Time | Event | HTTP | Notes |
|------|-------|------|-------|
| 05:22:00.007 | Produce #52 | **200** | Produce resumed |
| 05:22:00.017 | GET failover | 500* | Transition deleted — no active failover |
| 05:22:00.132 | Produce #53 | **200** | |
| 05:22:00.268 | Produce #54 | **200** | Steady 200s continue |

**Post-failover state (ZK):** `activeRegion=region-b`, `topicState=Producing`

---

## Produce HTTP summary (forward leg)

| HTTP code | Count | Phase |
|-----------|-------|-------|
| **200** | 863 | PREPARE + post-COMPLETED |
| **422** | 46 | SWITCH blocked window (~5 s) |

First blocked: `05:21:54.913`  
Last blocked:  `05:21:59.864`  
First success after block: `05:22:00.007`

---

## Equivalent curl commands

### Terminal 1 — continuous produce (grouped)

```bash
while true; do
  code=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST "http://localhost:18488/v1/projects/default_project/topics/failover_grouped/produce" \
    -H "x_user_id: thanos" \
    -H "X_MESSAGE_ID: loop-$(date +%s%N)" \
    -H "X_GROUP_ID: g1" \
    -H "Content-Type: application/octet-stream" \
    --data-binary '{"t":1}')
  echo "$(date '+%H:%M:%S.%3N') HTTP $code"
  sleep 0.08
done
```

### Terminal 2 — poll failover status

```bash
while true; do
  echo "--- $(date '+%H:%M:%S') ---"
  curl -s -w "\nHTTP:%{http_code}\n" -H "x_user_id: thanos" \
    "http://localhost:18488/v1/projects/default_project/topics/failover_grouped/failover"
  sleep 0.5
done
```

### Terminal 3 — trigger failover

```bash
curl -X POST "http://localhost:18488/v1/projects/default_project/topics/failover_grouped/failover" \
  -H "x_user_id: thanos" \
  -H "Content-Type: application/json" \
  -d '{"sourceRegion":"region-a","targetRegion":"region-b","waitForReplicationLagToClear":false}'
```

---

## Stage flow (observed)

```
POST failover (200, IN_PROGRESS)
        │
        ▼
   PREPARE  ── produce: 200 (5 messages)
        │
        ▼
   SWITCH   ── produce: 422 (46 messages, ~5 s)
        │
        ▼
  COMPLETED ── transition deleted from ZK
        │
        ▼
  produce: 200 (863+ messages)
  activeRegion: region-b
```

---

## Known gaps (for presentation footnotes)

1. **GET failover when idle** returns HTTP **500** instead of **404** — exception is wrapped crossing the cluster bus; `FailureHandler` fix is in code but requires server rebuild/restart.
2. **`ackedHosts` always empty** in GET transition JSON — acks live in `StageAwaiter` memory only; not written back to `TransitionObject` in ZK yet.
3. **Harness timeout false-negative** — poll loop treats only HTTP 404 as “done”; completed failovers that return 500 were logged as FAIL despite functional success.

---

## Full raw trace

Complete timestamped HTTP log for this scenario: lines **8–1427** in  
`setup/failover_validation_report_20260717_052153.log`
