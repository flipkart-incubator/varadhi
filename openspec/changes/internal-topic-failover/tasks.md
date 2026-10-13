# Tasks

## 1. Entities and bus contracts

- [ ] 1.1 Persist `lagDrainTimeoutMs`, `onLagTimeout`, `skipValidation`, and prepare-force on `TopicFailoverOperation` (and factory/`of` helpers); verify unit serialization round-trip
- [ ] 1.2 Extend `FailoverApiRequest` and `TopicFailoverRequest` with the same knobs; verify `ControllerHandler` / `ControllerRemoteClient` forward them without dropping fields
- [ ] 1.3 Add `StorageTopic.regions` (or equivalent SSOT) and topic-create seeding; verify target validation unit tests for missing/valid regions

## 2. Messaging SPI — cross-region produce and lag

- [ ] 2.1 Provision/pool `PulsarClient` (or stack client) per configured peer region; verify `newProducer(..., produceRegion)` uses the matching client in unit/integration tests
- [ ] 2.2 Implement `StorageTopicService` replication backlog query (source→target); verify unreachable source yields unknown (not zero) and happy-path returns summed lag
- [ ] 2.3 Confirm PREPARE warm via `ProduceTransitionMsgHandler` opens a peer producer; verify existing handler tests still pass and add a peer-region warm case

## 3. Controller — orchestrator and OpMgr

- [ ] 3.1 Implement `TopicFailoverOpExecutor` for PENDING→PREPARE→FENCE→MIGRATE→COMPLETED/ABORTED with state-first durable advances; verify stage-order unit tests with fakes for topic store, transition store, bus, lag
- [ ] 3.2 Register executor with `OperationMgr` and change `createTopicFailover` to `createAndEnqueueTopicFailover`; verify create leaves an enqueued in-progress op that the executor advances past PENDING
- [ ] 3.3 Implement ack barrier in `recordFailoverAck` (version-aware, missing hosts); verify PREPARE/FENCE timeout aborts and MIGRATE timeout completes with alert metric
- [ ] 3.4 Implement FENCE and MIGRATE tracked topic writes (failback clears FO when target is local); verify CAS retry/abort (E6) unit coverage
- [ ] 3.5 Implement grouped lag drain inside MIGRATE (skip ungrouped; E10 growing abort; default onLagTimeout ABORT grouped / FAILOVER ungrouped); verify with fake lag sequence tests
- [ ] 3.6 Wire `retryTopicFailover` / `requeueInProgressFailovers` resume rules (pre-commit re-exec/abort; post-commit detect FO); verify restart simulation tests
- [ ] 3.7 Ensure abort path + `cleanupFailedTopicFailover` remain correct with executor; verify abort-after-commit returns conflict
- [ ] 3.8 Add controller metrics (stage/outage duration, active, lag, lag-timeout outcomes); verify meters register in a unit test

## 4. REST / web layer

- [ ] 4.1 Update POST failover handler + client to send full body (lag knobs, overrides); verify handler/web test or contract test asserts bus payload fields
- [ ] 4.2 Enrich GET live failover status (stage, unacked hosts, backlog, remaining budget); verify 404 when no master and 200 shape when active
- [ ] 4.3 Add GET `…/failover/preflight`; verify no transition created and validation/backlog fields present
- [ ] 4.4 Add GET `…/failover/history`; verify completed ops returned after master delete
- [ ] 4.5 Add GET `…/produce-config` for local region entry + topic version; verify against topic cache fixture
- [ ] 4.6 Expose GET `/v1/failovers` over HTTP using existing `listFailovers` bus API; verify admin auth and list filtering
- [ ] 4.7 Add `Retry-After` on fenced produce (503); verify produce handler unit test
- [ ] 4.8 Reject topic update/delete while transition exists (E5); verify API/service test

## 5. Integration verification

- [ ] 5.1 End-to-end happy path (ungrouped): create → fence → migrate → produce to peer; verify with testcluster or existing setup script against a build with executor
- [ ] 5.2 End-to-end grouped with lag clear and with lag-timeout ABORT; verify ordering policy outcomes
- [ ] 5.3 Failback to local region clears FO and restore local produce; verify produce-config and produce path
- [ ] 5.4 Controller restart mid-FENCE and mid-post-commit MIGRATE; verify resume/abort behavior matches design

## Workflow follow-up

- Archive the change after review when implementation is complete (`/openspec-archive-change`).
- Production gates outside this task list: controller HA; independent per-pod entity-event progress.
