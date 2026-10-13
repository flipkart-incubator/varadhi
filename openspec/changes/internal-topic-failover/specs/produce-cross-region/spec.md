# Spec Delta

## Purpose

Enables varadhi-server produce traffic in one region to write to a peer region's messaging cluster when topic metadata sets `failOverRegion`, while fencing produce during cutover with client-retryable errors.

## ADDED Requirements

### Requirement: Ingress config selects cluster and segment
Produce resolution MUST use only the ingress region's `produceConfigs` entry for gating, `produceIdx`, and optional `failOverRegion`; the cluster used for the broker connection MUST be `failOverRegion` when set, otherwise the local region.

#### Scenario: Steady-state local produce
- **WHEN** a produce request arrives and the ingress config has no `failOverRegion` and state is produce-allowed
- **THEN** the message is written to the local region's cluster for the ingress `produceIdx` segment

#### Scenario: Failed-over produce
- **WHEN** a produce request arrives and the ingress config is produce-allowed with `failOverRegion` set to a peer region
- **THEN** the message is written to that peer region's cluster for the same storage topic name and ingress `produceIdx`

### Requirement: Cross-region producer can be created
The messaging stack integration MUST be able to create a producer against a configured peer region's cluster endpoints, selected by the produce region's name on the produce key.

#### Scenario: Peer producer open
- **WHEN** a server pod needs a producer for a produce key whose region is a configured peer
- **THEN** the system opens a producer on that peer's client without requiring a process restart

### Requirement: Fenced produce is retryable to clients
When the ingress region's produce state is fenced, the system MUST reject produce without delivering to any broker and MUST respond with HTTP 503 including a `Retry-After` hint.

#### Scenario: Client sees fenced as unavailable
- **WHEN** a client produces while the ingress region is fenced
- **THEN** the response is HTTP 503 with `Retry-After` and the message is not written to a broker

### Requirement: Target region membership is validated
A failover target MUST be rejected unless it is in the storage topic's known replicated regions (unless an audited skip-validation override is used) and the target region's status allows produce (unless that override applies).

#### Scenario: Unknown replication target rejected
- **WHEN** create or preflight names a target not in the storage topic's replicated regions and skip-validation is not set
- **THEN** the request is rejected with HTTP 400

### Requirement: Replication backlog query
The messaging SPI MUST expose source-to-target replication backlog for a storage topic; when the source admin API is unreachable the backlog MUST be treated as unknown, not zero.

#### Scenario: Unreachable source reports unknown
- **WHEN** backlog is queried and the source cluster admin API is unreachable
- **THEN** the result indicates unknown backlog rather than zero
