package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.entities.cluster.transition.TransitionStage;
import com.flipkart.varadhi.entities.cluster.transition.TransitionType;

/** The transition currently being handled by this pod for a topic. */
public record TopicTransitionInfo(
    TransitionType transitionType,
    TransitionStage transitionStage,
    TransitionNodeStatus nodeStatus
) {
}
