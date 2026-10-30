# Spec Delta

## Purpose

Lets a caller read what this produce pod would use to publish to a topic, its recent produce success and failure counts, and any transition this pod is handling, without sending a message or contacting other pods. This is the pod-local layer corresponding to varadhiV1's `GET /producers/topics/{topic}`; admin proxying and fleet aggregation are separate layers.

## ADDED Requirements

### Requirement: Local produce view
The produce role MUST answer a read of a topic's producer info with this pod's resolved path for its deployed region. The response MUST include the topic name, deployed region, produce state, resolved produce region, storage topic identity, whether produce is allowed, and whether a producer is already cached. The read MUST NOT create a producer.

#### Scenario: Steady-state local path
- **WHEN** an authorized caller reads producer info for a topic that is in this pod's cache and whose deployed-region produce state allows produce, with no failover region
- **THEN** the response is HTTP 200, the produce region is the deployed region, produce is allowed, and no producer is created

#### Scenario: Failover redirect
- **WHEN** an authorized caller reads producer info and the deployed region's produce config points at another region
- **THEN** the response produce region is that other region and the storage topic is the segment produce resolution selects

#### Scenario: Cached producer is reported
- **WHEN** an authorized caller reads producer info and this pod already has a producer cached for the resolved path
- **THEN** the response marks the producer as cached and does not create another one

### Requirement: Missing topic
The produce role MUST reject producer info for a topic that is not in this pod's cache.

#### Scenario: Unknown topic
- **WHEN** an authorized caller reads producer info for a topic name that is not in this pod's cache
- **THEN** the response is HTTP 404

### Requirement: No local produce config
When the topic is cached but has no produce config for the deployed region, the produce role MUST still return HTTP 200 and MUST report produce as not allowed, with no produce region and no storage topic.

#### Scenario: Topic not configured here
- **WHEN** an authorized caller reads producer info for a cached topic that has no produce config for the deployed region
- **THEN** the response is HTTP 200, produce is not allowed, and produce region and storage topic are absent

### Requirement: Produce stats snapshot
When a producer is already cached for the resolved path, the response MUST include `produceStats` with `successCounts` and `failureCounts`. Each MUST be a map of window-start epoch millis to a count for the last five 60-second windows. When no producer is cached, `produceStats` MUST be absent. The read MUST NOT create a producer.

#### Scenario: Cached producer reports recent counts
- **WHEN** an authorized caller reads producer info and this pod already has a producer cached for the resolved path
- **THEN** the response includes `produceStats` with `successCounts` and `failureCounts` for the last five 60-second windows, and no producer is created

#### Scenario: No cached producer
- **WHEN** an authorized caller reads producer info and this pod has no producer cached for the resolved path
- **THEN** `produceStats` is absent and no producer is created

### Requirement: Produce stats count publishes
A successful publish MUST increment the current window's success count. A failed publish MUST increment the current window's failure count. A rate-limited, fenced, or filtered produce MUST NOT change either count.

#### Scenario: Successful publish
- **WHEN** a publish to a cached producer succeeds
- **THEN** the current 60-second window's success count increases by one

#### Scenario: Failed publish
- **WHEN** a publish to a cached producer fails
- **THEN** the current 60-second window's failure count increases by one

#### Scenario: Rejected produce
- **WHEN** a produce is rate-limited, fenced, or filtered
- **THEN** neither success count nor failure count changes

### Requirement: In-flight transition on this pod
When this pod is handling a transition for the topic, the response MUST include `transitionType`, `transitionStage`, and `nodeStatus` from this pod's handler. The read MUST NOT call the controller. When this pod has no in-flight transition for the topic, the three fields MUST be absent.

#### Scenario: Involved pod handling prepare
- **WHEN** an authorized caller reads producer info while this pod is producing the topic and handling `PREPARE` for a `TOPIC_FAILOVER` that has not failed here
- **THEN** `transitionType` is `TOPIC_FAILOVER`, `transitionStage` is `PREPARE`, and `nodeStatus` is `IN_PROGRESS`

#### Scenario: Storage migration stage
- **WHEN** an authorized caller reads producer info while this pod is handling `MIGRATE` for a `STORAGE_MIGRATION`
- **THEN** `transitionType` is `STORAGE_MIGRATION` and `transitionStage` is `MIGRATE`

#### Scenario: No transition on this pod
- **WHEN** an authorized caller reads producer info and this pod has no in-flight transition for the topic
- **THEN** `transitionType`, `transitionStage`, and `nodeStatus` are absent

### Requirement: Transition node status
`nodeStatus` MUST be `NOT_INVOLVED` when this pod is not producing the topic. It MUST be `IN_PROGRESS` when this pod is participating and the current stage has not failed here. It MUST be `FAILED` when this pod has failed the current stage. It MUST be `ERRORED` when applying the stage throws outside that failure.

#### Scenario: Pod is not producing the topic
- **WHEN** an authorized caller reads producer info while this pod is handling a transition for a topic it is not producing
- **THEN** `nodeStatus` is `NOT_INVOLVED`

#### Scenario: Stage failed on this pod
- **WHEN** an authorized caller reads producer info after this pod has failed the stage it is handling
- **THEN** `nodeStatus` is `FAILED` and the type and stage are still present

#### Scenario: Applying the stage throws
- **WHEN** applying the stage throws outside a failed acknowledgement
- **THEN** `nodeStatus` is `ERRORED` and the type and stage are still present

### Requirement: Transition snapshot lifetime
`NOT_INVOLVED`, `FAILED`, and `ERRORED` MUST remain until a later event for that topic updates them. A successful acknowledgement of `COMPLETED` or `ABORTED` MUST clear `transitionType`, `transitionStage`, and `nodeStatus`.

#### Scenario: Status stays after the stage is recorded
- **WHEN** this pod has recorded `NOT_INVOLVED`, `FAILED`, or `ERRORED` and no later event for that topic has arrived
- **THEN** a producer-info read still returns that `nodeStatus`

#### Scenario: Terminal ack clears the snapshot
- **WHEN** this pod successfully acknowledges `COMPLETED` or `ABORTED`
- **THEN** a later producer-info read omits `transitionType`, `transitionStage`, and `nodeStatus`

### Requirement: Topic read authorization
Producer info MUST require the same topic-read authorization as other topic reads. A caller who is not allowed to read the topic MUST be rejected and MUST NOT receive the produce view.

#### Scenario: Caller cannot read the topic
- **WHEN** a caller who lacks topic-read permission requests producer info
- **THEN** the request is rejected and no produce view is returned
