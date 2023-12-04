package com.flipkart.varadhi.entities;

public enum ResourceType {
    ORG("org"),
    TEAM("team"),
    PROJECT("project"),
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
