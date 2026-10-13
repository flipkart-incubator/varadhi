# Tasks

## 1. Request knobs through bus and op

- [ ] 1.1 Add `lagDrainTimeoutMs`, `onLagTimeout`, and audited override fields to `FailoverApiRequest` and `TopicFailoverOperation` (persist + serdes); verify unit round-trip
- [ ] 1.2 Update `ControllerRemoteClient` and `ControllerHandler` to forward the full create body without dropping fields; verify a unit/contract test that lag knobs survive web→controller
- [ ] 1.3 Honor op lag budget and `onLagTimeout` in `TopicFailoverOpExecutor.awaitLagCleared` (null defaults: grouped ABORT / ungrouped FAILOVER); verify executor unit tests for timeout policies

## 2. Live status and new controller reads

- [ ] 2.1 Add `TopicFailoverStatusView` (or equivalent) composed from master + op + StageAwaiter missing hosts + lag/remaining budget; verify controller unit test for active and 404 paths
- [ ] 2.2 Add controller RPC for preflight (validate target, backlog or unknown, participant hint; no writes); verify unit test creates no transition
- [ ] 2.3 Add OpStore (or service) list of failover ops by `topicFqn` with `limit` for history; verify returns completed ops after master delete

## 3. REST handlers and wiring

- [ ] 3.1 Point GET `…/failover` at enriched status view; verify 200 shape and 404 when inactive
- [ ] 3.2 Add GET `…/failover/preflight` and GET `…/failover/history` on `TopicFailoverHandlers` with `TOPIC_GET`; verify handler tests
- [ ] 3.3 Add GET `…/produce-config` from topic cache (local region entry + version) with `TOPIC_GET`; verify handler test
- [ ] 3.4 Add GET `/v1/failovers` using existing `getActiveFailovers` bus API and the repo’s existing admin auth pattern; verify list + auth test
- [ ] 3.5 Register all new routes in `WebServerVerticle`; verify routes resolve in a web test or smoke assertion

## 4. Produce Retry-After

- [ ] 4.1 Add `Retry-After` on fenced produce 503 in `ProduceHandlers` (fixed or config default); verify `ProduceHandlersTest` asserts header and 503

## 5. Integration checks

- [ ] 5.1 End-to-end: POST with `lagDrainTimeoutMs` appears on stored op and affects drain deadline in a controller/integration test
- [ ] 5.2 End-to-end: create → GET live → abort/complete → GET history returns the attempt; verify against test fixtures

## Workflow follow-up

- Mark overlapping REST tasks in `internal-topic-failover` as deferred/done-elsewhere after this change lands.
- Archive when implementation review is complete (`/openspec-archive-change`).
