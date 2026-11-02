# Tasks

## 1. Resolve and produce

- [x] 1.1 TopicResolver / TopicProduceConfigs honor failOverRegion + fence state
- [x] 1.2 ProducerService registry uses produce region from resolve
- [x] 1.3 Pulsar producer factory region / producerName wiring from peel
- [x] 1.4 ProduceHandlers return 503 when fenced

## 2. Follow-ups (other changes)

- [ ] 2.1 Peer PulsarClient pool — `internal-topic-failover`
- [ ] 2.2 Retry-After on 503 — `topic-failover-rest-api`

## Workflow

- Stacks on `feature-failover-substrate`
- Base for `feature-failover-rest-intent`
