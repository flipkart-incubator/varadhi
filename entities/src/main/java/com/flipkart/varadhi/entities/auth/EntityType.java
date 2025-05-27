package com.flipkart.varadhi.entities.auth;

public enum EntityType {

    ROOT("root"), ORG("org"), ORG_FILTER("org_filter"), TEAM("team"), PROJECT("project"), TOPIC("topic"), SUBSCRIPTION(
        "subscription"
    ), IAM_POLICY("iam_policy");

    private final String type;

    EntityType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return String.format("varadhi.%s", type);
    }
}
