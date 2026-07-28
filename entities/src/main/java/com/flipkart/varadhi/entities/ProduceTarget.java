package com.flipkart.varadhi.entities;

/**
 * Resolved produce target for a request region: producer cache identity
 * ({@link #topicFqn()}, {@link #storageTopicId()}, {@link #produceRegion()}).
 */
public record ProduceTarget(VaradhiTopicName topicFqn, int storageTopicId, RegionName produceRegion) {
}
