package com.flipkart.varadhi.entities;

/**
 * This pod's resolved produce view for a topic.
 *
 * <p>Scoped to the pod's deployed region. It does not fan out to other producers.
 */
public record TopicProducerInfo(
    String topicName,
    String deployedRegion,
    TopicState state,
    String produceRegion,
    Integer storageTopicId,
    String storageTopicName,
    boolean produceAllowed,
    boolean producerCached
) {
}
