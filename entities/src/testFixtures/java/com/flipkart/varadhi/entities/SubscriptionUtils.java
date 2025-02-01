package com.flipkart.varadhi.entities;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Utility class for creating and managing Varadhi subscriptions.
 */
public class SubscriptionUtils {

    private static final int DEFAULT_NUM_SHARDS = 2;
    private static final int DEFAULT_QPS = 1000;
    private static final int DEFAULT_THROUGHPUT_KBPS = 20000;
    private static final int DEFAULT_READ_FANOUT = 2;

    /**
     * Creates a default HTTP endpoint.
     *
     * @return the default HTTP endpoint
     */
    public static Endpoint getHttpEndpoint() {
        return new Endpoint.HttpEndpoint(
                URI.create("http://localhost:8080"), "GET", "",
                500, 500, false
        );
    }

    /**
     * Creates a default retry policy.
     *
     * @return the default retry policy
     */
    public static RetryPolicy getRetryPolicy() {
        return new RetryPolicy(
                new CodeRange[]{new CodeRange(500, 502)},
                RetryPolicy.BackoffType.LINEAR,
                1, 100, 4, 3
        );
    }

    /**
     * Creates a map of default subscription properties.
     *
     * @return the default subscription properties
     */
    public static Map<String, String> getSubscriptionDefaultProperties() {
        return new HashMap<>(Map.of(
                "unsideline.api.message_count", "100",
                "unsideline.api.group_count", "20",
                "getmessages.api.messages_limit", "100"
        ));
    }

    /**
     * Creates a default consumption policy.
     *
     * @return the default consumption policy
     */
    public static ConsumptionPolicy getConsumptionPolicy() {
        return new ConsumptionPolicy(
                10, 1, 1,
                false, 1, null
        );
    }

    /**
     * Creates subscription shards with the specified number of shards and capacity.
     *
     * @param numShards     the number of shards
     * @param shardCapacity the capacity of each shard
     *
     * @return the subscription shards
     */
    public static SubscriptionShards getShards(int numShards, TopicCapacityPolicy shardCapacity) {
        Map<Integer, SubscriptionUnitShard> subShards = new HashMap<>();
        for (int shardId = 0; shardId < numShards; shardId++) {
            subShards.put(shardId, getShard(shardId, shardCapacity));
        }
        return new SubscriptionMultiShard(subShards);
    }

    /**
     * Creates a subscription unit shard with the specified shard ID and capacity.
     *
     * @param shardId  the shard ID
     * @param capacity the capacity of the shard
     *
     * @return the subscription unit shard
     */
    public static SubscriptionUnitShard getShard(int shardId, TopicCapacityPolicy capacity) {
        return new SubscriptionUnitShard(shardId, capacity, null, null, null);
    }

    /**
     * Creates a topic capacity policy with the specified QPS and throughput.
     *
     * @param qps            the queries per second
     * @param throughputKbps the throughput in kilobits per second
     *
     * @return the topic capacity policy
     */
    public static TopicCapacityPolicy getCapacity(int qps, int throughputKbps) {
        return getCapacity(qps, throughputKbps, DEFAULT_READ_FANOUT);
    }

    /**
     * Creates a topic capacity policy with the specified QPS, throughput, and read fan-out.
     *
     * @param qps            the queries per second
     * @param throughputKbps the throughput in kilobits per second
     * @param readFanOut     the read fan-out
     *
     * @return the topic capacity policy
     */
    public static TopicCapacityPolicy getCapacity(int qps, int throughputKbps, int readFanOut) {
        return new TopicCapacityPolicy(qps, throughputKbps, readFanOut);
    }

    /**
     * Retrieves the list of subscription unit shards from the specified subscription.
     *
     * @param subscription the subscription
     *
     * @return the list of subscription unit shards
     */
    public static List<SubscriptionUnitShard> shardsOf(VaradhiSubscription subscription) {
        List<SubscriptionUnitShard> shards = new ArrayList<>();
        for (int shardId = 0; shardId < subscription.getShards().getShardCount(); shardId++) {
            shards.add(subscription.getShards().getShard(shardId));
        }
        return shards;
    }

    /**
     * Creates a new builder for constructing Varadhi subscriptions.
     *
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder class for constructing Varadhi subscriptions.
     */
    public static class Builder {
        private int numShards = DEFAULT_NUM_SHARDS;
        private TopicCapacityPolicy subCapacity;
        private String description;
        private boolean isGrouped = false;
        private Endpoint endpoint;
        private RetryPolicy retryPolicy;
        private ConsumptionPolicy consumptionPolicy;
        private SubscriptionShards shards;
        private final Map<String, String> properties = getSubscriptionDefaultProperties();

        /**
         * Sets the description for the subscription.
         *
         * @param description the description
         *
         * @return the builder
         */
        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        /**
         * Sets whether the subscription is grouped.
         *
         * @param isGrouped whether the subscription is grouped
         *
         * @return the builder
         */
        public Builder setGrouped(boolean isGrouped) {
            this.isGrouped = isGrouped;
            return this;
        }

        /**
         * Sets the endpoint for the subscription.
         *
         * @param endpoint the endpoint
         *
         * @return the builder
         */
        public Builder setEndpoint(Endpoint endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        /**
         * Sets the retry policy for the subscription.
         *
         * @param retryPolicy the retry policy
         *
         * @return the builder
         */
        public Builder setRetryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = retryPolicy;
            return this;
        }

        /**
         * Sets the consumption policy for the subscription.
         *
         * @param consumptionPolicy the consumption policy
         *
         * @return the builder
         */
        public Builder setConsumptionPolicy(ConsumptionPolicy consumptionPolicy) {
            this.consumptionPolicy = consumptionPolicy;
            return this;
        }

        /**
         * Sets the shards for the subscription.
         *
         * @param shards the shards
         *
         * @return the builder
         */
        public Builder setShards(SubscriptionShards shards) {
            this.shards = shards;
            return this;
        }

        /**
         * Sets the capacity for the subscription.
         *
         * @param capacity the capacity
         *
         * @return the builder
         */
        public Builder setCapacity(TopicCapacityPolicy capacity) {
            this.subCapacity = capacity;
            return this;
        }

        /**
         * Sets the number of shards for the subscription.
         *
         * @param numShards the number of shards
         *
         * @return the builder
         */
        public Builder setNumShards(int numShards) {
            this.numShards = numShards;
            return this;
        }

        /**
         * Sets a property for the subscription.
         *
         * @param property the property name
         * @param value    the property value
         *
         * @return the builder
         */
        public Builder setProperty(String property, String value) {
            this.properties.put(property, value);
            return this;
        }

        /**
         * Builds a Varadhi subscription with the specified parameters.
         *
         * @param name            the subscription name
         * @param subProject      the subscription project
         * @param subscribedTopic the subscribed topic
         *
         * @return the Varadhi subscription
         */
        public VaradhiSubscription build(String name, String subProject, String subscribedTopic) {
            if (subCapacity == null) {
                subCapacity = getCapacity(DEFAULT_QPS, DEFAULT_THROUGHPUT_KBPS, DEFAULT_READ_FANOUT);
            }

            if (shards == null) {
                double shardCapacityFactor = (double) 1 / numShards;
                TopicCapacityPolicy shardCapacity = subCapacity.from(shardCapacityFactor, DEFAULT_READ_FANOUT);
                shards = getShards(numShards, shardCapacity);
            }

            return VaradhiSubscription.of(
                    name,
                    subProject,
                    subscribedTopic,
                    Optional.ofNullable(description)
                            .orElse("Test Subscription " + name + " Subscribed to " + subscribedTopic),
                    isGrouped,
                    Optional.ofNullable(endpoint).orElse(getHttpEndpoint()),
                    Optional.ofNullable(retryPolicy).orElse(getRetryPolicy()),
                    Optional.ofNullable(consumptionPolicy).orElse(getConsumptionPolicy()),
                    shards,
                    properties,
                    LifecycleStatus.ActionCode.SYSTEM_ACTION
            );
        }
    }
}
