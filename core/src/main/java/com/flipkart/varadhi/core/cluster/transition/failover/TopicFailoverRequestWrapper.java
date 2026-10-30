package com.flipkart.varadhi.core.cluster.transition.failover;

import com.flipkart.varadhi.entities.VaradhiTopicName;
import com.flipkart.varadhi.entities.cluster.transition.failover.TopicFailoverRequest;

/**
 * Cluster-bus envelope for the controller's topic-failover APIs (web → controller).
 *
 * <p>Create carries the HTTP {@link TopicFailoverRequest}; get, abort, and list leave
 * {@code request} null. Abort also sets {@code requestedBy}.
 */
public record TopicFailoverRequestWrapper(VaradhiTopicName topicFqn, TopicFailoverRequest request, String requestedBy) {
}
