package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.cluster.Assignment;
import com.flipkart.varadhi.spi.db.AssignmentStore;
import com.flipkart.varadhi.spi.db.MetaStoreException;

import java.util.List;
import java.util.regex.Pattern;

import static com.flipkart.varadhi.db.ZNode.ASSIGNMENT;

/**
 * ZooKeeper-based implementation of the AssignmentStore interface.
 * Manages persistence and retrieval of Assignment resource in ZooKeeper.
 */
public final class AssignmentStoreImpl implements AssignmentStore {
    private static final String SEPARATOR = ":";
    private final ZKMetaStore zkMetaStore;

    /**
     * Constructs a new AssignmentStoreImpl with the given ZooKeeper MetaStore.
     *
     * @throws MetaStoreException if unable to create required ZooKeeper paths
     */
    public AssignmentStoreImpl(ZKMetaStore zkMetaStore) {
        this.zkMetaStore = zkMetaStore;
        ensureEntityTypePathExists();
    }

    /**
     * Ensures that required entity type path exist in ZooKeeper.
     * Creates missing path if necessary.
     *
     * @throws MetaStoreException if path creation fails
     */
    private void ensureEntityTypePathExists() {
        zkMetaStore.createZNode(ZNode.ofEntityType(ASSIGNMENT));
    }

    /**
     * Creates multiple assignments in a single atomic transaction.
     *
     * @param assignments List of assignments to create
     * @throws IllegalStateException if the operation fails
     */
    @Override
    public void createAssignments(List<Assignment> assignments) {
        var nodesToCreate = assignments.stream().map(this::createAssignmentNode).toList();
        zkMetaStore.executeInTransaction(nodesToCreate, List.of());
    }

    /**
     * Retrieves assignments for a specific subscription.
     *
     * @param subscriptionName The name of the subscription
     * @return List of assignments for the specified subscription
     */
    @Override
    public List<Assignment> getSubAssignments(String subscriptionName) {
        return getAssignments(subscriptionName, ".*");
    }

    /**
     * Retrieves assignments for a specific consumer node.
     *
     * @param consumerNodeId The ID of the consumer node
     * @return List of assignments for the specified consumer node
     */
    @Override
    public List<Assignment> getConsumerNodeAssignments(String consumerNodeId) {
        return getAssignments(".*", consumerNodeId);
    }

    /**
     * Retrieves all assignments.
     *
     * @return List of all assignments
     */
    @Override
    public List<Assignment> getAllAssignments() {
        return getAssignments(".*", ".*");
    }

    /**
     * Checks if an assignment exists in ZooKeeper.
     *
     * @param assignment The assignment to check
     * @return true if the assignment exists, false otherwise
     */
    @Override
    public boolean exists(Assignment assignment) {
        ZNode nodeToVerify = ZNode.ofAssignment(getAssignmentMapping(assignment));
        return zkMetaStore.zkPathExist(nodeToVerify);
    }

    /**
     * Deletes multiple assignments in a single atomic transaction.
     *
     * @param assignments List of assignments to delete
     * @throws IllegalStateException if the operation fails
     */
    @Override
    public void deleteAssignments(List<Assignment> assignments) {
        var nodesToDelete = assignments.stream().map(this::createAssignmentNode).toList();
        zkMetaStore.executeInTransaction(List.of(), nodesToDelete);
    }

    /**
     * Creates a ZNode for the given assignment.
     *
     * @param assignment The assignment to create a node for
     * @return ZNode representing the assignment
     */
    private ZNode createAssignmentNode(Assignment assignment) {
        return ZNode.ofAssignment(getAssignmentMapping(assignment));
    }

    /**
     * Retrieves assignments based on subscription name and consumer node ID patterns.
     *
     * @param subscriptionName Subscription name pattern
     * @param consumerNodeId   Consumer node ID pattern
     * @return List of matching assignments
     */
    private List<Assignment> getAssignments(String subscriptionName, String consumerNodeId) {
        var filter = Pattern.compile(
            String.format("^%s%s.*%s%s$", subscriptionName, SEPARATOR, SEPARATOR, consumerNodeId)
        );

        return zkMetaStore.listChildren(ZNode.ofEntityType(ZNode.ASSIGNMENT))
                          .stream()
                          .filter(filter.asPredicate())
                          .map(this::getAssignment)
                          .toList();
    }

    /**
     * Creates the assignment mapping string from an Assignment object.
     *
     * @param assignment The assignment to create mapping for
     * @return String representation of the assignment mapping
     */
    private String getAssignmentMapping(Assignment assignment) {
        return String.join(
            SEPARATOR,
            assignment.getSubscriptionId(),
            String.valueOf(assignment.getShardId()),
            assignment.getConsumerId()
        );
    }

    /**
     * Parses an assignment mapping string into an Assignment object.
     *
     * @param mapping The mapping string to parse
     * @return Assignment object
     */
    private Assignment getAssignment(String mapping) {
        var parts = mapping.split(SEPARATOR);
        return new Assignment(parts[0], Integer.parseInt(parts[1]), parts[2]);
    }
}
