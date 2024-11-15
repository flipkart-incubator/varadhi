package com.flipkart.varadhi.entities;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SubscriptionUtils {
    public static Endpoint getHttpEndpoint() {
        return new Endpoint.HttpEndpoint(URI.create("http://localhost:8080"), "GET", "", 500, 500, false);
    }

    public static RetryPolicy getRetryPolicy() {
        return new RetryPolicy(
                new CodeRange[]{new CodeRange(500, 502)},
                RetryPolicy.BackoffType.LINEAR,
                1, 100, 4, 3
        );
    }

    public static Map<String, String> getSubscriptionDefaultProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("unsideline.api.message_count", "100");
        properties.put("unsideline.api.group_count", "20");
        properties.put("getmessages.api.messages_limit", "100");
        return properties;
    }

    public static ConsumptionPolicy getConsumptionPolicy() {
        return new ConsumptionPolicy(1, 1, false, 1, null);
    }

    public static SubscriptionShards getShards(int numShards, TopicCapacityPolicy shardCapacity) {
        Map<Integer, SubscriptionUnitShard> subShards = new HashMap<>();
        for (int shardId = 0; shardId < numShards; shardId++) {
            subShards.put(shardId, getShard(shardId, shardCapacity));
        }
        return new SubscriptionMultiShard(subShards);
    }

    public static SubscriptionUnitShard getShard(int shardId, TopicCapacityPolicy capacity) {
        return new SubscriptionUnitShard(shardId, capacity, null, null, null);
    }

    public static TopicCapacityPolicy getCapacity(int qps, int throughputKbps) {
        return getCapacity(qps, throughputKbps, 2);
    }

    public static TopicCapacityPolicy getCapacity(int qps, int throughputKbps, int readFanOut) {
        return new TopicCapacityPolicy(qps, throughputKbps, readFanOut);
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    public static List<SubscriptionUnitShard> shardsOf(VaradhiSubscription subscription) {
        List<SubscriptionUnitShard> shards = new ArrayList<>();
        for (int shardId = 0; shardId < subscription.getShards().getShardCount(); shardId++) {
            shards.add(subscription.getShards().getShard(shardId));
        }
        return shards;
    }

    public static class Builder {
        private int numShards = 2;
        private TopicCapacityPolicy subCapacity;
        private String description;
        private boolean isGrouped = false;
        private Endpoint endpoint;
        private RetryPolicy retryPolicy;
        private ConsumptionPolicy consumptionPolicy;
        private SubscriptionShards shards;
        private final Map<String, String> properties = getSubscriptionDefaultProperties();

        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder setIsGrouped(boolean isGrouped) {
            this.isGrouped = isGrouped;
            return this;
        }

        public Builder setEndpoint(Endpoint endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder setRetryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = retryPolicy;
            return this;
        }

        public Builder setConsumptionPolicy(ConsumptionPolicy consumptionPolicy) {
            this.consumptionPolicy = consumptionPolicy;
            return this;
        }

        public Builder setShards(SubscriptionShards shards) {
            this.shards = shards;
            return this;
        }

        public Builder setCapacity(TopicCapacityPolicy capacity) {
            this.subCapacity = capacity;
            return this;
        }

        public Builder setNumShards(int numShards) {
            this.numShards = numShards;
            return this;
        }
        public Builder setProperty(String property, String value) {
            this.properties.put(property, value);
            return this;
        }

        public VaradhiSubscription build(String name, String subProject, String subscribedTopic) {
            if (null == subCapacity) {
                subCapacity = getCapacity(1000, 20000, 2);
            }
            if (shards == null) {
                double shardCapacityFactor = (double) 1 / numShards;
                TopicCapacityPolicy shardCapacity = subCapacity.from(shardCapacityFactor, 2);
                shards = getShards(numShards, shardCapacity);
            }

            return VaradhiSubscription.of(
                    name,
                    subProject,
                    subscribedTopic,
                    description == null ? "Test Subscription " + name + "Subscribed to " + subscribedTopic :
                            description,
                    isGrouped,
                    endpoint == null ? getHttpEndpoint() : endpoint,
                    retryPolicy == null ? getRetryPolicy() : retryPolicy,
                    consumptionPolicy == null ? getConsumptionPolicy() : consumptionPolicy,
                    shards,
                    properties
            );
        }
    }
}
