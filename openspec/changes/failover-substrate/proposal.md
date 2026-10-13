# Proposal

## Why

Topic produce failover needs a durable control-plane substrate before REST or orchestration can land: uniqueness lock, wire events, and OpStore audit. `#318` / `#332` already shipped the produce wire contract and topic `produceConfigs` shape on `master`. This change captures the remaining substrate from the `#339` peel — `TransitionMaster`, `TransitionStore`, and `TopicFailoverOperation` OpStore paths — so later stacks do not re-litigate entity/ZK design.

## What Changes

- Document and land **TransitionMaster** + **TransitionStore** (flat ZK) as the ephemeral uniqueness lock.
- Land durable **TopicFailoverOperation** in OpStore (flat ZK) for per-attempt audit.
- Keep wire types (`TransitionEvent` / `TransitionAck` / stages) aligned with `#318`.
- **Out of scope:** HTTP handlers, ProducerService FO behavior, `TopicFailoverOpExecutor`, peer Pulsar clients, lag drain.

## Capabilities

### New Capabilities

- `failover-substrate`: Entities, ZK paths, and bus wire types required for topic produce failover uniqueness and audit, without operator HTTP or stage execution.

### Modified Capabilities

- (none)

## Impact

- **entities / spi / metastore-zk:** TransitionMaster, TransitionStore, TopicFailoverOperation, MetaStore wiring
- **core:** Transition bus address / message helpers as needed by the substrate
- Downstream stacks: `produce-cross-region`, `topic-failover-rest-intent`, then parallel `topic-failover-rest-api` and `internal-topic-failover`
