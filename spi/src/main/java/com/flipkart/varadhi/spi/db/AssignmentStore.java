package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.cluster.Assignment;

import java.util.List;

public interface AssignmentStore {
    void createAssignments(List<Assignment> assignments);

    List<Assignment> getSubscriptionAssignments(String subscriptionName);

    List<Assignment> getConsumerNodeAssignments(String consumerNodeId);
}
