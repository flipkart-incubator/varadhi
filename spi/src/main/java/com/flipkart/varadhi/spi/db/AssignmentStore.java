package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.cluster.Assignment;

import java.util.List;

/**
 * Interface for storing and managing assignments.
 * This interface provides methods for creating, deleting, and checking
 * the existence of assignments, as well as retrieving assignments
 * by subscription name and consumer node ID.
 */
public interface AssignmentStore {

    void createAssignments(List<Assignment> assignments);

    List<Assignment> getSubAssignments(String subscriptionName);

    List<Assignment> getConsumerNodeAssignments(String consumerNodeId);

    List<Assignment> getAllAssignments();

    boolean exists(Assignment assignment);

    void deleteAssignments(List<Assignment> assignments);
}
