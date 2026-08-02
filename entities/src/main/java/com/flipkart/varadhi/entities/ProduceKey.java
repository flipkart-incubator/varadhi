package com.flipkart.varadhi.entities;

/**
 * Resolved produce routing and producer cache identity:
 * logical topic, regional produce context, and storage segment.
 */
public record ProduceKey(VaradhiTopicName topicFqn, RegionName produceRegion, int storageTopicId) {

    public String topicFqnString() {
        return topicFqn.toFqn();
    }
}
