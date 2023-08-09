package com.flipkart.varadhi.auth;

public enum ResourceAction {


    ORG_CREATE(ResourceType.ORG, "create"),
    ORG_GET(ResourceType.ORG, "get"),
    ORG_DELETE(ResourceType.ORG, "delete"),

    TEAM_CREATE(ResourceType.TEAM, "create"),
    TEAM_GET(ResourceType.TEAM, "get"),
    TEAM_DELETE(ResourceType.TEAM, "delete"),

    PROJECT_CREATE(ResourceType.PROJECT, "create"),
    PROJECT_GET(ResourceType.PROJECT, "get"),
    PROJECT_UPDATE(ResourceType.PROJECT, "update"),
    PROJECT_DELETE(ResourceType.PROJECT, "delete"),

    TOPIC_CREATE(ResourceType.TOPIC, "create"),
    TOPIC_GET(ResourceType.TOPIC, "get"),
    TOPIC_DELETE(ResourceType.TOPIC, "delete"),
    TOPIC_UPDATE(ResourceType.TOPIC, "update"),
    TOPIC_SUBSCRIBE(ResourceType.TOPIC, "subscribe"),
    TOPIC_PRODUCE(ResourceType.TOPIC, "produce"),

    SUBSCRIPTION_CREATE(ResourceType.SUBSCRIPTION, "create"),
    SUBSCRIPTION_DELETE(ResourceType.SUBSCRIPTION, "delete"),
    SUBSCRIPTION_UPDATE(ResourceType.SUBSCRIPTION, "update");

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
