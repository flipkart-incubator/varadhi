package com.flipkart.varadhi.spi.db;



import com.flipkart.varadhi.entities.cluster.Assignment;

import java.util.List;

public interface AssignmentStore {
    void createAssignments(List<Assignment> assignments);

    void deleteAssignments(List<Assignment> assignments);

    boolean exists(Assignment assignment);

    List<Assignment> getSubAssignments(String subscriptionName);

    List<Assignment> getConsumerNodeAssignments(String consumerNodeId);

    List<Assignment> getAllAssignments();
}
