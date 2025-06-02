package com.flipkart.varadhi.entities;

public enum MetaStoreEntityType {

    ROOT("root"), ORG("org"), ORG_FILTER("org_filter"), TEAM("team"), PROJECT("project"), TOPIC("topic"), SUBSCRIPTION(
        "subscription"
    ), IAM_POLICY("iam_policy"), SUBSCRIPTION_OPERATION("subscription_operation"), SHARD_OPERATION("shard_operation");

    private final String type;

    MetaStoreEntityType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return String.format("%s", type);
    }
}
