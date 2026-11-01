# Topic failover runbook

The REST failover endpoints are:

```text
POST /v1/projects/{project}/topics/{topic}/failover
GET  /v1/projects/{project}/topics/{topic}/failover
POST /v1/projects/{project}/topics/{topic}/failover/abort
GET  /v1/projects/{project}/failovers
```

Create payload:

```json
{
  "sourceRegion": "r1",
  "targetRegion": "r2",
  "waitForReplicationLagToClear": true,
  "lagDrainTimeoutMs": 30000,
  "onLagTimeout": "ABORT"
}
```

`sourceRegion` may be omitted; the deployed region is used. The create and abort
requests return `202 Accepted`.

Example:

```bash
curl -X POST "$VARADHI_URL/v1/projects/demo/topics/orders/failover" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $TOKEN" \
  --data '{"sourceRegion":"r1","targetRegion":"r2","waitForReplicationLagToClear":true,"lagDrainTimeoutMs":30000,"onLagTimeout":"ABORT"}'

curl "$VARADHI_URL/v1/projects/demo/topics/orders/failover" \
  -H "Authorization: Bearer $TOKEN"

curl -X POST "$VARADHI_URL/v1/projects/demo/topics/orders/failover/abort" \
  -H "Authorization: Bearer $TOKEN"
```

Validation performed on this branch:

- `./gradlew compileJava` — passed.
- Entity failover transition tests — 3 passed.
- Controller client failover tests — 12 passed.
- Controller failover tests — 10 passed.

Live run on `http://localhost:18488`:

- Started Pulsar 3.3.2, ZooKeeper 3.9.2, and Varadhi.
- Registered `default` and `region2`.
- Provisioned `failover_project.success_topic` with `default=Producing` and
  `region2=Blocked`.
- Sent the create payload above with `x_user_id: thanos`.

Create response:

```text
HTTP/1.1 202 Accepted
{"name":"e328115e-8cf5-4c21-9aa1-63b97a84506a","version":0,
 "topicFqn":"failover_project.success_topic","requestedBy":"thanos",
 "state":"IN_PROGRESS","entityType":"TOPIC_FAILOVER_OPERATION"}
```

The operation completed and was removed from the active-operation store, so the
subsequent status request correctly returned:

```text
HTTP/1.1 404 No active failover for topic failover_project.success_topic.
{"reason":"No active failover for topic failover_project.success_topic."}
```

Final topic state from ZooKeeper:

```json
{
  "default": {"state": "Blocked", "produceIdx": 0, "failOverRegion": null},
  "region2": {"state": "Producing", "produceIdx": 0, "failOverRegion": null}
}
```

Relevant Varadhi logs:

```text
Created topic failover op e328115e-8cf5-4c21-9aa1-63b97a84506a
replication lag cleared for failover_project.success_topic (default -> region2)
fenced produce for failover_project.success_topic (default -> region2), topic now v6
Completed Task(... state=COMPLETED ...)
Topic failover op e328115e-8cf5-4c21-9aa1-63b97a84506a for failover_project.success_topic completed.
```

This is a successful live failover: the operation reached `COMPLETED`, the
controller logged completion, and the final persisted routing state has only
`region2` producing.
