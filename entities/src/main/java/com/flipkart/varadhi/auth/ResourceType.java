package com.flipkart.varadhi.auth;

public enum ResourceType {
    TENANT("tenant"),
    TOPIC("topic"),
    SUBSCRIPTION("subscription");
    private final String type;

    ResourceType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return String.format("varadhi.%s", type);
    }
}
