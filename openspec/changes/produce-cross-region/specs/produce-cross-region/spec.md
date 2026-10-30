# Spec Delta

## Purpose

Enables produce traffic to honor failover metadata: fence rejection and route selection via `failOverRegion`.

## ADDED Requirements

### Requirement: Ingress config selects cluster and segment
Produce resolution MUST use only the ingress region's `produceConfigs` entry for gating, `produceIdx`, and optional `failOverRegion`; the cluster used for the broker connection MUST be `failOverRegion` when set, otherwise the local region.

#### Scenario: Steady-state local produce
- **WHEN** a produce request arrives and the ingress config has no `failOverRegion` and state is produce-allowed
- **THEN** the message is written to the local region's cluster for the ingress `produceIdx` segment

#### Scenario: Failed-over produce
- **WHEN** a produce request arrives and the ingress config is produce-allowed with `failOverRegion` set to a peer region
- **THEN** the message is written to that peer region's cluster for the same storage topic name and ingress `produceIdx`

### Requirement: Fenced produce is unavailable
When the ingress region's produce state is fenced, the system MUST reject produce without delivering to any broker and MUST respond with HTTP 503.

#### Scenario: Client sees fenced as unavailable
- **WHEN** a client produces while the ingress region is fenced
- **THEN** the response is HTTP 503 and the message is not written to a broker
