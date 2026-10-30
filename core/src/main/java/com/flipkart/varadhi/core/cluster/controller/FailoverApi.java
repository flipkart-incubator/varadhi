package com.flipkart.varadhi.core.cluster.controller;

import com.flipkart.varadhi.entities.VaradhiTopicName;
import com.flipkart.varadhi.entities.cluster.transition.failover.ProduceFailoverTransition;
import com.flipkart.varadhi.entities.cluster.transition.failover.TopicFailoverOperation;
import com.flipkart.varadhi.entities.cluster.transition.failover.TopicFailoverRequest;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Web → controller RPCs for topic-failover intent. Distinct from {@link TransitionAckApi}
 * (pod → controller acks).
 */
public interface FailoverApi {

    CompletableFuture<TopicFailoverOperation> createTopicFailover(
        VaradhiTopicName topicFqn,
        TopicFailoverRequest request,
        String requestedBy
    );

    /** In-flight transition; 404 if none active. */
    CompletableFuture<ProduceFailoverTransition> getTopicFailover(VaradhiTopicName topicFqn);

    /**
     * Records an abort request on the active transition. The failover executor honors it;
     * this call does not unfence, broadcast, or delete the master.
     */
    CompletableFuture<ProduceFailoverTransition> abortTopicFailover(VaradhiTopicName topicFqn, String requestedBy);

    /**
     * Every in-flight topic failover (one master per topic, all projects). Completes with an
     * empty list when none are stored. Callers that need a single topic should use
     * {@link #getTopicFailover(VaradhiTopicName)}.
     */
    CompletableFuture<List<ProduceFailoverTransition>> getActiveFailovers();
}
