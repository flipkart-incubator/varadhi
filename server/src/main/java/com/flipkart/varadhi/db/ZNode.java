package com.flipkart.varadhi.db;

import lombok.Getter;

import java.util.Objects;

/**
 * Represents a ZooKeeper node in the Varadhi system.
 * <p>
 * This class provides a type-safe way to handle ZooKeeper paths and node types.
 * It follows a hierarchical structure for different entity types and their sequences.
 *
 * <p>The path structure follows these patterns:
 * <ul>
 *     <li>Entities: /varadhi/entities/[kind]/[name]</li>
 *     <li>Hierarchical Resources: /varadhi/entities/[kind]/[parent]:[name]</li>
 *     <li>Child Resources: /varadhi/entities/[kind]/[parent]/[kind]/[name]</li>
 * </ul>
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
    public static final ZNodeKind ORG = new ZNodeKind("Org");
    public static final ZNodeKind ORG_FILTER = new ZNodeKind("Filters");
    public static final ZNodeKind TEAM = new ZNodeKind("Team");
    public static final ZNodeKind PROJECT = new ZNodeKind("Project");
    public static final ZNodeKind TOPIC = new ZNodeKind("Topic");
    public static final ZNodeKind IAM_POLICY = new ZNodeKind("IamPolicy");
    public static final ZNodeKind SUBSCRIPTION = new ZNodeKind("Subscription");
    public static final ZNodeKind SUB_OP = new ZNodeKind("SubOperation");
    public static final ZNodeKind ASSIGNMENT = new ZNodeKind("Assignment");
    // TODO: Hierarchical or Flat?
    public static final ZNodeKind SHARD_OP = new ZNodeKind("ShardOperation");
    public static final ZNodeKind EVENT = new ZNodeKind("ChangeEvent");

    public static final String ENTITIES_BASE_PATH = "/varadhi/entities";
    public static final String RESOURCE_NAME_SEPARATOR = ":";
    public static final String ZK_PATH_SEPARATOR = "/";

    private final String name;
    private final String kind;
    private final String path;

    /**
     * Constructs a ZNode instance with the provided builder.
     *
     * @param builder The builder containing the node properties
     */
    private ZNode(Builder builder) {
        this.name = builder.name;
        this.kind = builder.kind;
        this.path = builder.path;
    }

    /**
     * Builder class for creating ZNode instances.
     * Provides a fluent interface for setting node properties.
     */
    public static class Builder {
        private String name;
        private String kind;
        private String path;
        private String parent;
        private ZNodeKind zNodeKind;
        private String parentKind;
        private String parentName;

        /**
         * Sets the ZNodeKind for this node.
         *
         * @param znodeKind The kind of node to create
         * @return The builder instance for method chaining
         * @throws NullPointerException if znodeKind is null
         */
        public Builder withZNodeKind(ZNodeKind znodeKind) {
            this.zNodeKind = Objects.requireNonNull(znodeKind, "zNodeKind cannot be null");
            this.kind = znodeKind.kind();
            return this;
        }

        /**
         * Sets the name for this node.
         *
         * @param name The name of the node
         * @return The builder instance for method chaining
         * @throws NullPointerException if name is null
         */
        public Builder withName(String name) {
            this.name = Objects.requireNonNull(name, "name cannot be null");
            return this;
        }

        /**
         * Sets the parent for this node, used in hierarchical resources.
         *
         * @param parent The parent resource name
         * @return The builder instance for method chaining
         * @throws NullPointerException if parent is null
         */
        public Builder withParent(String parent) {
            this.parent = Objects.requireNonNull(parent, "parent cannot be null");
            return this;
        }

        /**
         * Sets the parent for this node, used in child resources.
         *
         * @param parentKind The kind of child resource
         * @param parentName The name of child resource
         * @return The builder instance for method chaining
         * @throws NullPointerException if parent is null
         */
        public Builder withParent(String parentKind, String parentName) {
            this.parentKind = Objects.requireNonNull(parentKind, "childKind cannot be null");
            this.parentName = Objects.requireNonNull(parentName, "childName cannot be null");
            return this;
        }

        /**
         * Builds the ZNode instance with the configured properties.
         *
         * @return A new ZNode instance
         * @throws NullPointerException if required properties are not set
         */
        public ZNode build() {
            Objects.requireNonNull(zNodeKind, "zNodeKind must be set");
            if (parentKind != null && parentName != null) {
                this.path = String.join(ZK_PATH_SEPARATOR, ENTITIES_BASE_PATH, parentKind, parentName, kind);
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

    public static ZNode ofOrgNamedFilter(String orgName) {
        return new Builder().withZNodeKind(ORG_FILTER).withName("Filters").withParent(ORG.kind(), orgName).build();
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

    public static ZNode ofKind(ZNodeKind zNodeKind, String name) {
        return new Builder().withZNodeKind(zNodeKind).withName(name).build();
    }

    public static ZNode ofEntityType(ZNodeKind kind) {
        return new Builder().withZNodeKind(kind).build();
    }

    /**
     * Constructs a fully qualified domain name (FQDN) for a resource.
     *
     * @param parentName   The name of the parent resource
     * @param resourceName The name of the resource
     * @return The FQDN as a string in the format "parentName:resourceName"
     * @throws NullPointerException if either parameter is null
     */
    public static String getResourceFQDN(String parentName, String resourceName) {
        return String.join(RESOURCE_NAME_SEPARATOR, parentName, resourceName);
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
