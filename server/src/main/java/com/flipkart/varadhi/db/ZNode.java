package com.flipkart.varadhi.db;

import lombok.Getter;

@Getter
public class ZNode {
    public static final ZNodeKind ORG = new ZNodeKind("Org");
    public static final ZNodeKind TEAM = new ZNodeKind("Team");
    public static final ZNodeKind PROJECT = new ZNodeKind("Project");
    public static final ZNodeKind TOPIC = new ZNodeKind("Topic");
    public static final ZNodeKind IAM_POLICY = new ZNodeKind("IamPolicy");
    public static final ZNodeKind SUBSCRIPTION = new ZNodeKind("Subscription");
    public static final ZNodeKind SUB_OP = new ZNodeKind("SubOperation");
    public static final ZNodeKind ASSIGNMENT = new ZNodeKind("Assignment");
    //TODO:: hierarchical or flat ?
    public static final ZNodeKind SHARD_OP = new ZNodeKind("ShardOperation");
    public static final String BASE_PATH = "/varadhi/entities";
    public static final String RESOURCE_NAME_SEPARATOR = ":";
    public static final String ZK_PATH_SEPARATOR = "/";

    private final String path;
    private final String kind;
    private final String name;

    //TODO:: This class can be simplified using Builder pattern.
    private ZNode(ZNodeKind znodeKind) {
        this.name = znodeKind.getKind();
        this.kind = znodeKind.getKind();
        this.path = String.join(ZK_PATH_SEPARATOR, BASE_PATH, znodeKind.getKind());
    }

    private ZNode(ZNodeKind znodeKind, String entityName) {
        this.name = entityName;
        this.kind = znodeKind.getKind();
        this.path = String.join(ZK_PATH_SEPARATOR, BASE_PATH, znodeKind.getKind(), entityName);
    }

    private ZNode(ZNodeKind znodeKind, String parent, String entityName) {
        this.name = entityName;
        this.kind = znodeKind.getKind();
        this.path = String.join(
                ZK_PATH_SEPARATOR, BASE_PATH, znodeKind.getKind(), getResourceFQDN(parent, entityName));
    }

    public static String getResourceFQDN(String parentName, String resourceName) {
        return String.join(RESOURCE_NAME_SEPARATOR, parentName, resourceName);
    }

    public static ZNode OfOrg(String orgName) {
        return new ZNode(ORG, orgName);
    }

    public static ZNode OfTeam(String orgName, String teamName) {
        return new ZNode(TEAM, orgName, teamName);
    }

    public static ZNode OfProject(String projectName) {
        return new ZNode(PROJECT, projectName);
    }

    public static ZNode OfTopic(String topicName) {
        return new ZNode(TOPIC, topicName);
    }

    public static ZNode OfIamPolicy(String authResourceId) {
        return new ZNode(IAM_POLICY, authResourceId);
    }

    public static ZNode ofSubscription(String subscriptionName) {
        return new ZNode(SUBSCRIPTION, subscriptionName);
    }

    public static ZNode OfSubOperation(String operationId) {
        return new ZNode(SUB_OP, operationId);
    }

    public static ZNode OfShardOperation(String operationId) {
        return new ZNode(SHARD_OP, operationId);
    }

    public static ZNode OfAssignment(String assignment) {
        return new ZNode(ASSIGNMENT, assignment);
    }

    public static ZNode OfKind(ZNodeKind zNodeKind, String name) {
        return new ZNode(zNodeKind, name);
    }

    public static ZNode OfEntityType(ZNodeKind kind) {
        return new ZNode(kind);
    }

    @Override
    public String toString() {
        return "ZNode{[%s] %s @ %s}".formatted(kind, name, path);
    }
}
