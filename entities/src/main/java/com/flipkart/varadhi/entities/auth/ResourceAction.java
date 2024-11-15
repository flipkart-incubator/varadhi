package com.flipkart.varadhi.entities.auth;

import lombok.Getter;

@Getter
public enum ResourceAction {

    ORG_CREATE(ResourceType.ORG, "create"),
    ORG_UPDATE(ResourceType.ORG, "update"),
    ORG_DELETE(ResourceType.ORG, "delete"),
    ORG_GET(ResourceType.ORG, "get"),
    ORG_LIST(ResourceType.ROOT, "list"),
    ORG_PROJECT_MIGRATE(ResourceType.ORG, "migrate"),

    TEAM_CREATE(ResourceType.TEAM, "create"),
    TEAM_UPDATE(ResourceType.TEAM, "update"),
    TEAM_DELETE(ResourceType.TEAM, "delete"),
    TEAM_GET(ResourceType.TEAM, "get"),
    TEAM_LIST(ResourceType.ORG, "list"),

    PROJECT_CREATE(ResourceType.PROJECT, "create"),
    PROJECT_UPDATE(ResourceType.PROJECT, "update"),
    PROJECT_DELETE(ResourceType.PROJECT, "delete"),
    PROJECT_GET(ResourceType.PROJECT, "get"),
    PROJECT_LIST(ResourceType.TEAM, "list"),

    TOPIC_CREATE(ResourceType.TOPIC, "create"),
    TOPIC_GET(ResourceType.TOPIC, "get"),
    TOPIC_DELETE(ResourceType.TOPIC, "delete"),
    TOPIC_UPDATE(ResourceType.TOPIC, "update"),
    TOPIC_LIST(ResourceType.PROJECT, "list"),
    TOPIC_CONSUME(ResourceType.TOPIC, "consume"),
    TOPIC_PRODUCE(ResourceType.TOPIC, "produce"),

    SUBSCRIPTION_CREATE(ResourceType.SUBSCRIPTION, "create"),
    SUBSCRIPTION_GET(ResourceType.SUBSCRIPTION, "get"),
    SUBSCRIPTION_LIST(ResourceType.PROJECT, "list"),
    SUBSCRIPTION_DELETE(ResourceType.SUBSCRIPTION, "delete"),
    SUBSCRIPTION_UPDATE(ResourceType.SUBSCRIPTION, "update"),
    SUBSCRIPTION_SEEK(ResourceType.SUBSCRIPTION, "seek"),

    IAM_POLICY_GET(ResourceType.IAM_POLICY, "get"),
    IAM_POLICY_SET(ResourceType.IAM_POLICY, "set"),
    IAM_POLICY_DELETE(ResourceType.IAM_POLICY, "delete");

    private final ResourceType resourceType;
    private final String action;

    ResourceAction(ResourceType type, String action) {
        this.resourceType = type;
        this.action = action;
    }

    @Override
    public String toString() {
        return String.format("%s.%s", resourceType, action);
    }
}
