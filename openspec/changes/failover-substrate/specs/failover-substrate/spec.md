# Spec Delta

## Purpose

Defines the durable and ephemeral metastore substrate for topic produce failover uniqueness and audit, independent of HTTP and orchestration.

## ADDED Requirements

### Requirement: One active transition lock per topic
The system MUST allow at most one active transition master per topic FQN.

#### Scenario: Second lock rejected
- **WHEN** a transition master already exists for a topic FQN
- **THEN** a second create of that master is rejected as a conflict

### Requirement: Durable operation record per attempt
The system MUST persist a per-attempt failover operation record keyed by operation id that survives deletion of the ephemeral transition master.

#### Scenario: Op remains after master delete
- **WHEN** the transition master for a completed or aborted attempt is deleted
- **THEN** the TopicFailoverOperation record remains readable by operation id

### Requirement: Flat ZK layout
Transition masters and failover operations MUST use flat entity paths under the configured entities root (no hierarchical project sharding in this change).

#### Scenario: Paths are flat
- **WHEN** a transition master and op are created
- **THEN** they are stored under `Transition/<topicFqn>` and `TopicFailoverOperation/<opId>` respectively
