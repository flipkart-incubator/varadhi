# Tasks

## 1. Entities and SPI

- [x] 1.1 `TransitionMaster` + stage/snapshot types used by the lock
- [x] 1.2 `TopicFailoverOperation` OpStore entity (minimal fields for create/abort audit)
- [x] 1.3 `TransitionStore` SPI + ZK impl + in-memory test fixtures
- [x] 1.4 OpStore CRUD for topic failover ops; MetaStoreProvider / ZNode wiring

## 2. Verification

- [x] 2.1 Unit tests for TransitionStore / serialization already covered in peel commit
- [ ] 2.2 Confirm branch builds against current `master` after rebase

## Workflow

- Stack base for `feature-failover-produce-path`
- Archive after merge to master
