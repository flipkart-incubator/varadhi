package com.flipkart.varadhi.auth;

public enum ResourceAction {
    TENANT_CREATE(ResourceType.TENANT, "create"),
    TENANT_UPDATE(ResourceType.TENANT, "update"),
    TENANT_DELETE(ResourceType.TENANT, "delete"),

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
