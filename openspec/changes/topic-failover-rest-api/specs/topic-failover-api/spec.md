# Spec Delta

## Purpose

Defines the region-local HTTP operator surface for topic produce failover: request knobs that are honored, live and historical observation, preflight sizing, produce-config visibility, and retryable fenced produce responses.

## ADDED Requirements

### Requirement: Create failover accepts and honors request knobs
The system MUST accept a POST failover body with target region, optional source region, wait-for-lag flag, optional lag drain timeout, optional on-lag-timeout policy, and optional audited overrides, return HTTP 202 with durable identifiers, and apply those knobs to the in-flight attempt.

#### Scenario: Accepted create returns 202
- **WHEN** an authorized caller POSTs a valid failover request
- **THEN** the response is HTTP 202 with operation identifiers and the attempt is queryable as in-flight

#### Scenario: Lag timeout from request is used
- **WHEN** the caller supplies `lagDrainTimeoutMs` and lag wait is enabled
- **THEN** the migrate lag wait uses that budget rather than silently ignoring the field

### Requirement: Live failover status is enriched
While a failover is active, GET failover MUST return current stage, entered-at timing, hosts that have not acked the current barrier when applicable, current backlog or unknown, and remaining lag budget when lag wait is active. GET MUST return 404 when no active transition exists.

#### Scenario: Active failover details
- **WHEN** an authorized caller GETs failover for a topic with an active transition
- **THEN** the response includes stage and enough progress fields to see barrier/lag progress

#### Scenario: No active failover
- **WHEN** an authorized caller GETs failover and no transition exists
- **THEN** the response is HTTP 404

### Requirement: Abort after route commit is conflict
POST abort MUST succeed with HTTP 202 while abortable, and MUST fail with HTTP 409 once the route commit has landed.

#### Scenario: Abort after commit
- **WHEN** the caller aborts after migrate has committed the new route
- **THEN** the response is HTTP 409 and the committed route is unchanged

### Requirement: Preflight does not start a failover
GET preflight MUST report whether the target is allowed, region produce availability when known, current backlog or unknown, and a participant-count hint, without creating a transition or operation.

#### Scenario: Preflight dry run
- **WHEN** an authorized caller GETs preflight with a target region
- **THEN** the response includes validation and backlog inputs and no active failover is created

### Requirement: History lists past attempts
GET history MUST return prior failover operation records for the topic from durable storage, including completed and aborted attempts, without requiring an active transition.

#### Scenario: History after completion
- **WHEN** a failover has completed or aborted and the caller GETs history
- **THEN** the attempt appears with stage history and terminal outcome

### Requirement: Produce-config shows this region's routing
GET produce-config MUST return this region's produce config entry (state, produce index, fail-over region) and the topic version from the global topic.

#### Scenario: View current produce route
- **WHEN** an authorized caller GETs produce-config for a topic
- **THEN** the response reflects the ingress region's `produceConfigs` entry and topic version

### Requirement: Region-wide failover list
GET `/v1/failovers` MUST list active (and optionally recent) failovers in this region for callers with admin scope.

#### Scenario: List active failovers
- **WHEN** an admin-scoped caller GETs `/v1/failovers`
- **THEN** the response includes active transitions for the region

### Requirement: Mutating and read authorization
Mutating failover endpoints MUST require `TOPIC_UPDATE` on the topic. Topic-scoped read endpoints (status, preflight, history, produce-config) MUST require `TOPIC_GET`. The region-wide list MUST use admin-scoped authorization.

#### Scenario: Unauthorized create
- **WHEN** a caller without `TOPIC_UPDATE` POSTs failover
- **THEN** the request is rejected as unauthorized

### Requirement: Fenced produce includes Retry-After
When produce is rejected because the topic is fenced, the HTTP response MUST be 503 and MUST include a `Retry-After` header.

#### Scenario: Client retries fenced produce
- **WHEN** a client produces while the ingress region is fenced
- **THEN** the response is HTTP 503 with `Retry-After` and the message is not written to a broker
