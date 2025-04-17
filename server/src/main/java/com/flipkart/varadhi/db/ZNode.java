package com.flipkart.varadhi.db;

import lombok.Getter;

/**
 * Represents a ZooKeeper node in the Varadhi system.
 * <p>
 * This class provides a type-safe way to handle ZooKeeper paths and node types.
 * It follows a hierarchical structure for different entity types and their sequences.
 *
 * <p>Supported node kinds:
 * <ul>
 *     <li>ORG - Organization nodes</li>
 *     <li>TEAM - Team nodes (hierarchical under ORG)</li>
 *     <li>PROJECT - Project nodes</li>
 *     <li>TOPIC - Topic nodes</li>
 *     <li>IAM_POLICY - IAM Policy nodes</li>
 *     <li>SUBSCRIPTION - Subscription nodes</li>
 *     <li>SUB_OP - Subscription Operation nodes</li>
 *     <li>SHARD_OP - Shard Operation nodes</li>
 *     <li>ASSIGNMENT - Assignment nodes</li>
 *     <li>EVENT - Event nodes</li>
 * </ul>
 *
 * @see ZNodeKind
 * @see ZKMetaStore
 */
@Getter
public final class ZNode {

    public static final String ENTITIES_BASE_PATH = "/varadhi/entities";
    public static final String RESOURCE_NAME_SEPARATOR = ":";

    public static final ZNodeKind ORG = new ZNodeKind("Org", "%s");
    public static final ZNodeKind ORG_FILTER = new ZNodeKind("Filters", "Org/%s/Filters", false);

    /*
     *   /Team/{org_name}:{team_name}
     */
    public static final ZNodeKind TEAM = new ZNodeKind("Team", "%s" + RESOURCE_NAME_SEPARATOR + "%s");
    public static final ZNodeKind PROJECT = new ZNodeKind("Project", "%s");
    public static final ZNodeKind TOPIC = new ZNodeKind("Topic", "%s");
    public static final ZNodeKind IAM_POLICY = new ZNodeKind("IamPolicy", "%s");
    public static final ZNodeKind SUBSCRIPTION = new ZNodeKind("Subscription", "%s");
    public static final ZNodeKind SUB_OP = new ZNodeKind("SubOperation", "%s");
    public static final ZNodeKind ASSIGNMENT = new ZNodeKind("Assignment", "%s");
    // TODO: Hierarchical or Flat?
    public static final ZNodeKind SHARD_OP = new ZNodeKind("ShardOperation", "%s");
    public static final ZNodeKind EVENT = new ZNodeKind("ChangeEvent", "%s");

    private final String name;
    private final String kind;
    private final String path;

    /**
     * Constructs a ZNode instance with the provided builder.
     */
    private ZNode(String name, String kind, String path) {
        this.name = name;
        this.kind = kind;
        this.path = path;
    }

    public static ZNode ofOrg(String orgName) {
        return new ZNode(orgName, ORG.kind(), ORG.resolvePath(ENTITIES_BASE_PATH, orgName));
    }

    public static ZNode ofOrgNamedFilter(String orgName) {
        return new ZNode(ORG_FILTER.kind(), ORG_FILTER.kind(), ORG_FILTER.resolvePath(ENTITIES_BASE_PATH, orgName));
    }

    public static ZNode ofTeam(String orgName, String teamName) {
        return new ZNode(teamName, TEAM.kind(), TEAM.resolvePath(ENTITIES_BASE_PATH, orgName, teamName));
    }

    public static ZNode ofProject(String projectName) {
        return new ZNode(projectName, PROJECT.kind(), PROJECT.resolvePath(ENTITIES_BASE_PATH, projectName));
    }

    public static ZNode ofTopic(String topicName) {
        return new ZNode(topicName, TOPIC.kind(), TOPIC.resolvePath(ENTITIES_BASE_PATH, topicName));
    }

    public static ZNode ofIamPolicy(String authResourceId) {
        return new ZNode(authResourceId, IAM_POLICY.kind(), IAM_POLICY.resolvePath(ENTITIES_BASE_PATH, authResourceId));
    }

    public static ZNode ofSubscription(String subscriptionName) {
        return new ZNode(
            subscriptionName,
            SUBSCRIPTION.kind(),
            SUBSCRIPTION.resolvePath(ENTITIES_BASE_PATH, subscriptionName)
        );
    }

    public static ZNode ofSubOperation(String operationId) {
        return new ZNode(operationId, SUB_OP.kind(), SUB_OP.resolvePath(ENTITIES_BASE_PATH, operationId));
    }

    public static ZNode ofShardOperation(String operationId) {
        return new ZNode(operationId, SHARD_OP.kind(), SHARD_OP.resolvePath(ENTITIES_BASE_PATH, operationId));
    }

    public static ZNode ofAssignment(String assignment) {
        return new ZNode(assignment, ASSIGNMENT.kind(), ASSIGNMENT.resolvePath(ENTITIES_BASE_PATH, assignment));
    }

    public static ZNode ofEntityChange(String changeNodeName) {
        return new ZNode(changeNodeName, EVENT.kind(), EVENT.resolvePath(ENTITIES_BASE_PATH, changeNodeName));
    }

    public static ZNode ofEntityType(ZNodeKind kind) {
        if (kind.relative()) {
            return new ZNode(kind.kind(), kind.kind(), ENTITIES_BASE_PATH + "/" + kind.kind());
        } else {
            throw new IllegalArgumentException(
                "The base path of zNodeKind: %s with absolute path does not makes sense.".formatted(kind)
            );
        }
    }

    public static ZNode ofKind(ZNodeKind kind, Object... args) {
        return new ZNode(kind.kind(), kind.kind(), kind.resolvePath(ENTITIES_BASE_PATH, args));
    }

    /**
     * Returns a string representation of the ZNode.
     *
     * @return A string in the format "ZNode{[kind] name @ path}"
     */
    @Override
    public String toString() {
        return "ZNode{[%s] %s @ %s}".formatted(kind, name, path);
    }
}
