package com.flipkart.varadhi.db;

import lombok.Getter;

import java.util.Objects;

/**
 * Represents a ZooKeeper node in the Varadhi system.
 * <p>
 * This class provides a type-safe way to handle ZooKeeper paths and node types.
 * It follows a hierarchical structure for different entity types and their sequences.
 * <p>
 * Path structure:
 * <ul>
 *     <li>Entities: /varadhi/entities/[kind]/[name]</li>
 *     <li>Sequences: /varadhi/sequences/[kind]</li>
 * </ul>
 *
 * @see ZNodeKind
 * @see ZKMetaStore
 */
@Getter
public final class ZNode {
    public static final ZNodeKind ORG = new ZNodeKind("Org");
    public static final ZNodeKind TEAM = new ZNodeKind("Team");
    public static final ZNodeKind PROJECT = new ZNodeKind("Project");
    public static final ZNodeKind TOPIC = new ZNodeKind("Topic");
    public static final ZNodeKind IAM_POLICY = new ZNodeKind("IamPolicy");
    public static final ZNodeKind SUBSCRIPTION = new ZNodeKind("Subscription");
    public static final ZNodeKind SUB_OP = new ZNodeKind("SubOperation");
    public static final ZNodeKind ASSIGNMENT = new ZNodeKind("Assignment");
    // TODO: Hierarchical or Flat?
    public static final ZNodeKind SHARD_OP = new ZNodeKind("ShardOperation");
    public static final ZNodeKind EVENT = new ZNodeKind("Event");

    public static final String ENTITIES_BASE_PATH = "/varadhi/entities";
    public static final String SEQUENCES_BASE_PATH = "/varadhi/sequences";
    public static final String RESOURCE_NAME_SEPARATOR = ":";
    public static final String ZK_PATH_SEPARATOR = "/";

    private final String name;
    private final String kind;
    private final String path;

    private ZNode(Builder builder) {
        this.name = builder.name;
        this.kind = builder.kind;
        this.path = builder.path;
    }

    public static class Builder {
        private String name;
        private String kind;
        private String path;
        private String parent;
        private ZNodeKind zNodeKind;
        private boolean isSequence;

        public Builder withZNodeKind(ZNodeKind znodeKind) {
            this.zNodeKind = Objects.requireNonNull(znodeKind, "zNodeKind cannot be null");
            this.kind = znodeKind.kind();
            return this;
        }

        public Builder withName(String name) {
            this.name = Objects.requireNonNull(name, "name cannot be null");
            return this;
        }

        public Builder withParent(String parent) {
            this.parent = Objects.requireNonNull(parent, "parent cannot be null");
            return this;
        }

        public Builder asSequence() {
            this.isSequence = true;
            return this;
        }

        public ZNode build() {
            Objects.requireNonNull(zNodeKind, "zNodeKind must be set");

            if (isSequence) {
                this.name = kind;
                this.path = String.join(ZK_PATH_SEPARATOR, SEQUENCES_BASE_PATH, kind);
            } else if (parent != null) {
                this.path = String.join(ZK_PATH_SEPARATOR, ENTITIES_BASE_PATH, kind, getResourceFQDN(parent, name));
            } else if (name != null) {
                this.path = String.join(ZK_PATH_SEPARATOR, ENTITIES_BASE_PATH, kind, name);
            } else {
                this.name = kind;
                this.path = String.join(ZK_PATH_SEPARATOR, ENTITIES_BASE_PATH, kind);
            }

            return new ZNode(this);
        }
    }

    public static ZNode ofOrg(String orgName) {
        return new Builder().withZNodeKind(ORG).withName(orgName).build();
    }

    public static ZNode ofTeam(String orgName, String teamName) {
        return new Builder().withZNodeKind(TEAM).withName(teamName).withParent(orgName).build();
    }

    public static ZNode ofProject(String projectName) {
        return new Builder().withZNodeKind(PROJECT).withName(projectName).build();
    }

    public static ZNode ofTopic(String topicName) {
        return new Builder().withZNodeKind(TOPIC).withName(topicName).build();
    }

    public static ZNode ofIamPolicy(String authResourceId) {
        return new Builder().withZNodeKind(IAM_POLICY).withName(authResourceId).build();
    }

    public static ZNode ofSubscription(String subscriptionName) {
        return new Builder().withZNodeKind(SUBSCRIPTION).withName(subscriptionName).build();
    }

    public static ZNode ofSubOperation(String operationId) {
        return new Builder().withZNodeKind(SUB_OP).withName(operationId).build();
    }

    public static ZNode ofShardOperation(String operationId) {
        return new Builder().withZNodeKind(SHARD_OP).withName(operationId).build();
    }

    public static ZNode ofAssignment(String assignment) {
        return new Builder().withZNodeKind(ASSIGNMENT).withName(assignment).build();
    }

    public static ZNode ofEvent(String eventName) {
        return new Builder().withZNodeKind(EVENT).withName(eventName).build();
    }

    public static ZNode ofKind(ZNodeKind zNodeKind, String name) {
        return new Builder().withZNodeKind(zNodeKind).withName(name).build();
    }

    public static ZNode ofEntityType(ZNodeKind kind) {
        return new Builder().withZNodeKind(kind).build();
    }

    public static ZNode ofSequence(ZNodeKind kind) {
        return new Builder().withZNodeKind(kind).asSequence().build();
    }

    public static String getResourceFQDN(String parentName, String resourceName) {
        return String.join(RESOURCE_NAME_SEPARATOR, parentName, resourceName);
    }

    @Override
    public String toString() {
        return "ZNode{[%s] %s @ %s}".formatted(kind, name, path);
    }
}
