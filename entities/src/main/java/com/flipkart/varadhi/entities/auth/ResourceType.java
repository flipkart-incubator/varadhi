package com.flipkart.varadhi.entities.auth;

public enum ResourceType {

    ROOT("root"), ORG("org"), TEAM("team"), PROJECT("project"), TOPIC("topic"), SUBSCRIPTION(
        "subscription"
    ), IAM_POLICY("iam_policy");

    private final String type;

    ResourceType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return String.format("varadhi.%s", type);
    }
}
