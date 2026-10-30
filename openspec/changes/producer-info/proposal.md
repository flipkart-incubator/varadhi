# Proposal

## Why

Operators need to see what a produce pod would use for a topic — region, storage segment, fence, cached producer — without sending a message. varadhiV1 exposes that as `TopicProducerInfo`. OSS had no equivalent.

## What Changes

- `GET /v1/projects/:project/topics/:topic/producer-info` on the produce role.
- Response is this pod's resolved path for its deployed region: topic state, produce region (including a failover redirect), storage topic, whether produce is allowed, and whether a producer is already cached.
- When a producer is already cached, the response includes a produce-stats snapshot: success and failure counts for the last five 60-second windows.
- When this pod is handling a transition for the topic, the response includes that transition's type, stage, and this pod's node status. With nothing in flight on this pod, those fields are absent.
- Read-only. Does not create a producer. Missing topic is 404. Requires `TOPIC_GET`.
- **Out of scope:** varadhiV1-style admin read of another pod by IP and varadhiV1-style fleet fan-out. The OSS endpoint is intentionally pod-local; a cluster-wide view can be added separately.

## Capabilities

### New Capabilities

- `producer-info`: Read this pod's resolved produce path, cached produce stats, and in-flight transition status for a topic.

### Modified Capabilities

None. There is no existing spec inventory.

## Impact

- **entities:** `TopicProducerInfo` (`produceStats`, `transitionType`, `transitionStage`, `nodeStatus`)
- **producer:** `ProducerService.getProducerInfo`, a five-window counter on the cached producer, and the transition handler's per-topic snapshot
- **web:** route on `ProduceHandlers`
- Draft implementation is on `feature-producer-info` (pushed; pull request not opened).
