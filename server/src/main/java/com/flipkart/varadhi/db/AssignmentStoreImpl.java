package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.cluster.Assignment;
import com.flipkart.varadhi.spi.db.AssignmentStore;
import org.apache.curator.framework.CuratorFramework;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.flipkart.varadhi.db.ZNode.*;

public class AssignmentStoreImpl implements AssignmentStore {
    private final ZKMetaStore zkMetaStore;
    private final String separator = ":";

    public AssignmentStoreImpl(CuratorFramework zkCurator) {
        this.zkMetaStore = new ZKMetaStore(zkCurator);
        ensureEntityTypePathExists();
    }

    private void ensureEntityTypePathExists() {
        zkMetaStore.createZNode(ZNode.OfEntityType(ASSIGNMENT));
    }

    @Override
    public void createAssignments(List<Assignment> assignments) {
        List<ZNode> nodesToCreate = new ArrayList<>();
        assignments.forEach(a -> nodesToCreate.add(ZNode.OfAssignment(getAssignmentMapping(a))));
        zkMetaStore.executeInTransaction(nodesToCreate, new ArrayList<>());
    }

    @Override
    public void deleteAssignments(List<Assignment> assignments) {
        List<ZNode> nodesToDelete = new ArrayList<>();
        assignments.forEach(a -> nodesToDelete.add(ZNode.OfAssignment(getAssignmentMapping(a))));
        zkMetaStore.executeInTransaction( new ArrayList<>(), nodesToDelete);
    }

    @Override
    public boolean exists(Assignment assignment) {
        ZNode nodeToVerify = ZNode.OfAssignment(getAssignmentMapping(assignment));
        return zkMetaStore.zkPathExist(nodeToVerify);
    }

    @Override
    public List<Assignment> getSubscriptionAssignments(String subscriptionName) {
        return getAssignments(Pattern.compile(String.format("^%s%s.*%s.*$", subscriptionName, separator, separator)));
    }

    @Override
    public List<Assignment> getConsumerNodeAssignments(String consumerNodeId) {
        return getAssignments(Pattern.compile(String.format("^.*%s.*%s%s$", separator, separator, consumerNodeId)));
    }


    private List<Assignment> getAssignments(Pattern pattern) {
        List<String> as = zkMetaStore.listChildren(ZNode.OfEntityType(ZNode.ASSIGNMENT));
        return zkMetaStore.listChildren(ZNode.OfEntityType(ZNode.ASSIGNMENT)).stream()
                .filter(m -> pattern.matcher(m).matches()).map(this::getAssignment).collect(Collectors.toList());
    }

    private String getAssignmentMapping(Assignment assignment) {
        return assignment.getSubscriptionId() + separator + assignment.getShardId() +
                separator + assignment.getConsumerId();
    }

    private Assignment getAssignment(String mapping) {
        String[] parts = mapping.split(separator);
        return new Assignment(parts[0], Integer.parseInt(parts[1]), parts[2]);
    }
}
