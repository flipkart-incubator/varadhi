# Proposal

## Why

Operators need a region-local HTTP surface to start, observe, and abort a topic produce failover. `#339` delivers that MVP (oncall-parity trio) without a working orchestrator: create persists intent and returns 202; stages do not advance until `internal-topic-failover`.

## What Changes

- POST/GET/POST-abort topic failover routes with `TOPIC_UPDATE` / `TOPIC_GET`, **202** on create/abort.
- Web → controller bus RPCs: create / get / abort / list active.
- `createTopicFailoverOp` persists TransitionMaster + TopicFailoverOperation only (**no executor enqueue**).
- Request body may carry `waitForReplicationLagToClear` (oncall parity); lag knobs honesty deferred to `topic-failover-rest-api`.
- **Out of scope:** preflight, history, produce-config, `/v1/failovers` HTTP, Retry-After, knob persistence/honor, `TopicFailoverOpExecutor`.

Depends on: `failover-substrate`, `produce-cross-region`.

## Capabilities

### New Capabilities

- `topic-failover-rest-intent`: Region-local HTTP create/get/abort that records durable failover intent without driving stages.

## Impact

- **web:** `TopicFailoverHandlers`, route wiring
- **core/controller:** ControllerApi + remote client + TransitionService create/get/abort/list
