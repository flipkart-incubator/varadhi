# Spec Delta

## Purpose

Region-local HTTP APIs that accept and observe durable topic failover intent without requiring the orchestrator to advance stages.

## ADDED Requirements

### Requirement: Create records intent asynchronously
The system MUST accept a region-local failover create, durably record the attempt, and return HTTP 202 without waiting for stage completion.

#### Scenario: Create returns 202
- **WHEN** an authorized caller POSTs a valid failover request
- **THEN** the response is HTTP 202 with durable identifiers and a transition master exists for the topic

#### Scenario: Concurrent create rejected
- **WHEN** a failover is already active for the same topic
- **THEN** a second create is rejected with HTTP 409

### Requirement: Get and abort against active intent
GET MUST return the active attempt when a master exists (404 otherwise). Abort MUST be accepted with HTTP 202 while abortable under intent-only rules.

#### Scenario: Get active
- **WHEN** an authorized caller GETs failover and a master exists
- **THEN** the response identifies the in-flight operation

#### Scenario: Abort accepted
- **WHEN** an authorized caller aborts an active intent before route commit
- **THEN** the response is HTTP 202 and the attempt ends aborted

### Requirement: Authorization
Mutating endpoints MUST require `TOPIC_UPDATE`; GET MUST require `TOPIC_GET`.

#### Scenario: Unauthorized create
- **WHEN** a caller without `TOPIC_UPDATE` POSTs failover
- **THEN** the request is rejected as unauthorized
