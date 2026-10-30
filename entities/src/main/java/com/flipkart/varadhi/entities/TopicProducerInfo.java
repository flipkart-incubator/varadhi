package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.entities.cluster.transition.TransitionStage;
import com.flipkart.varadhi.entities.cluster.transition.TransitionType;

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
    boolean producerCached,
    TopicProduceStats produceStats,
    TransitionType transitionType,
    TransitionStage transitionStage,
    TransitionNodeStatus nodeStatus
) {
}
