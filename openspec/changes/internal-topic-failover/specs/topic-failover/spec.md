# Spec Delta

## Purpose

Operator-triggered, controller-orchestrated failover of a topic's produce path in one region to a peer messaging cluster, with bounded outage, ordering protection for grouped topics, failback, and a durable audit trail.

## ADDED Requirements

### Requirement: Failover request is recorded and accepted asynchronously
The system MUST accept a region-local failover request for a topic, durably record the attempt, and return success of acceptance without waiting for completion.

#### Scenario: Create returns accepted with identifiers
- **WHEN** an authorized caller requests failover of a topic to a valid target region
- **THEN** the system returns HTTP 202 with durable operation identifiers and the attempt is queryable as in-flight

#### Scenario: Concurrent failover rejected
- **WHEN** a failover is already active for the same topic
- **THEN** a second create request is rejected with HTTP 409

### Requirement: Produce route flips only after fence and optional lag drain
The system MUST fence produce on the ingress region, drain source-to-target replication backlog for grouped topics (unless skipped by policy), then commit `failOverRegion` together with restoring produce-allowed state in one topic update.

#### Scenario: Grouped topic waits for backlog clear
- **WHEN** failover runs for a grouped topic with lag wait enabled
- **THEN** the route commit MUST NOT occur until source-to-target backlog is zero or the lag timeout policy decides

#### Scenario: Ungrouped topic skips lag wait
- **WHEN** failover runs for an ungrouped topic
- **THEN** the system skips the replication backlog wait inside the migrate phase

### Requirement: Produce outage is bounded
The system MUST bound the fenced window using a configurable ack timeout and a caller-chosen lag drain timeout; on barrier or lag expiry the attempt MUST advance or abort according to stage policy, never remain stuck indefinitely while the controller is running.

#### Scenario: Prepare or fence ack timeout aborts
- **WHEN** a prepare or fence ack barrier exceeds the ack timeout
- **THEN** the system aborts the failover and restores produce-allowed state without committing a new route

#### Scenario: Lag timeout default by topic type
- **WHEN** lag drain exceeds `lagDrainTimeoutMs` on a grouped topic and the caller did not override
- **THEN** the system aborts by default; for an ungrouped topic the default is to proceed with failover

#### Scenario: Audited prepare force proceeds
- **WHEN** the caller sets the audited prepare-force override and prepare would otherwise abort on timeout
- **THEN** the system MAY proceed past prepare, and the override is recorded on the operation audit

### Requirement: Failback uses the same mechanism
The system MUST support failback by accepting the local region as `targetRegion`, clearing `failOverRegion` on commit so produce returns to the local cluster.

#### Scenario: Failback clears failover region
- **WHEN** a successful failover has set `failOverRegion` to a peer and a later request targets the local region
- **THEN** after commit the ingress produce config has no `failOverRegion` and produce uses the local cluster

### Requirement: Abort is allowed only before route commit
The system MUST allow abort through fence and pre-commit migrate, refuse abort after the route is committed, and leave a durable aborted audit record.

#### Scenario: Abort while fenced
- **WHEN** an authorized caller aborts during fence before migrate commit
- **THEN** produce is restored to produce-allowed without changing `failOverRegion` and the attempt ends aborted

#### Scenario: Abort after commit rejected
- **WHEN** the migrate commit has already written the new route
- **THEN** abort is rejected with HTTP 409

### Requirement: Controller restart resumes durable in-flight work
The system MUST resume or safely abort in-flight failover operations from durable state after controller restart, without requiring the original request to be resent.

#### Scenario: Resume after fence
- **WHEN** the controller restarts while a topic is fenced and the route is not yet committed
- **THEN** the reconciler re-enters the current stage idempotently or aborts, and does not leave the topic fenced without an owning in-flight operation once reconciliation completes

#### Scenario: Resume after migrate commit
- **WHEN** the controller restarts after `failOverRegion` is already committed to the target
- **THEN** the reconciler finishes post-commit convergence bookkeeping and completes the operation

### Requirement: Durable audit trail per attempt
The system MUST retain a per-attempt operation record with requester, regions, stage history, timeouts/overrides, and terminal outcome after the ephemeral transition lock is deleted.

#### Scenario: History after completion
- **WHEN** a failover completes or aborts
- **THEN** a history query for the topic returns the attempt including stage history without requiring an active transition

### Requirement: Preflight exposes sizing inputs
The system MUST provide a dry-run preflight that reports target validity, current backlog, and participant-related inputs so callers can choose `lagDrainTimeoutMs` before opening an outage.

#### Scenario: Preflight for valid target
- **WHEN** an authorized caller requests preflight with a target region
- **THEN** the response includes whether the target is allowed, current source-to-target backlog (or unknown), and does not start a failover

### Requirement: Authorization for mutating failover APIs
Mutating failover APIs MUST require `TOPIC_UPDATE` on the topic; read APIs for a topic's failover status and history MUST require `TOPIC_GET`.

#### Scenario: Update required to create
- **WHEN** a caller without `TOPIC_UPDATE` posts a failover
- **THEN** the request is rejected as unauthorized

### Requirement: Topic mutate blocked during active transition
The system MUST reject topic update or delete while an active transition exists for that topic FQN.

#### Scenario: Delete during failover
- **WHEN** a failover transition is active for a topic
- **THEN** topic delete or update for that topic is rejected
