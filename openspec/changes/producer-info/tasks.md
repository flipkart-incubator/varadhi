# Tasks

## 1. Pod-local read

- [x] 1.1 Add `TopicProducerInfo` and `ProducerService.getProducerInfo` (cache + ungated resolve, no producer create) and verify `ProducerServiceProducerInfoTest` covers the failover path, a topic with no local produce config, and a missing topic
- [x] 1.2 Add `GET /v1/projects/:project/topics/:topic/producer-info` with `TOPIC_GET` and verify `TopicProducerInfoHandlerTest` covers HTTP 200 and HTTP 404

## 2. Produce stats

- [x] 2.1 Keep five 60-second success and failure windows on a cached producer (successful and failed publishes only) and verify a test covers increment, window roll, and that a producer-info read does not create a producer
- [x] 2.2 Add `produceStats` (`successCounts`, `failureCounts`) to `TopicProducerInfo` when `producerCached` is true, and omit it otherwise. Extend `ProducerServiceProducerInfoTest` and `TopicProducerInfoHandlerTest`

## 3. This pod's transition

- [x] 3.1 Keep a per-topic snapshot on the produce transition handler: type, stage, and node status (`NOT_INVOLVED`, `IN_PROGRESS`, `FAILED`, `ERRORED`). Clear it only after a successful `COMPLETED` or `ABORTED` ack. Verify the handler test covers involved prepare, not involved, failed stage, and clear on terminal ack
- [x] 3.2 Add `transitionType`, `transitionStage`, and `nodeStatus` to `TopicProducerInfo` from that snapshot. Omit all three when this pod has nothing in flight. The read must not call the controller. Extend `ProducerServiceProducerInfoTest` and `TopicProducerInfoHandlerTest`

## 4. varadhiV1 parity boundary

- [x] 4.1 Keep the OSS endpoint pod-local, matching varadhiV1's `/producers/topics/{topic}` handler. Do not add admin proxying or fleet fan-out to this change.
- [x] 4.2 Document a future cluster-wide endpoint separately if operators need aggregation across producer pods and consumer applications.

## Workflow follow-up

- Sections 2 and 3 are not on `feature-producer-info`. Apply them there before opening the pull request.
- `gh` was not authenticated when that branch was pushed. Open the pull request against `master` after these tasks land.
- Archive this change after that pull request merges.
