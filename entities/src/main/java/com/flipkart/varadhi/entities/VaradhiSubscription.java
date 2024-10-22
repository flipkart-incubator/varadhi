package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Maps;
import lombok.*;

import java.util.Map;

@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
public class VaradhiSubscription extends MetaStoreEntity {
    public static final int DEFAULT_UNSIDELINE_MAX_MSGS = 1000;
    public static final int DEFAULT_UNSIDELINE_MAX_GROUPS = 1000;
    public static final int DEFAULT_GET_MESSAGES_LIMIT = 200;

    public static final String DLQ_UNSIDELINE_MAX_MSGS = "dlq.unsideline.max_messages";
    public static final String DLQ_UNSIDELINE_MAX_GROUPS = "dlq.unsideline.max_groups";
    public static final String DLQ_GET_MESSAGES_LIMIT = "dlq.messages.get_limit";


    private final String project;
    private final String topic;
    private String description;
    private boolean grouped;
    private Endpoint endpoint;
    private RetryPolicy retryPolicy;
    private ConsumptionPolicy consumptionPolicy;
    private SubscriptionShards shards;
    private Status status;
    private Map<String, String> properties;


    private VaradhiSubscription(
            String name,
            int version,
            String project,
            String topic,
            String description,
            boolean grouped,
            Endpoint endpoint,
            RetryPolicy retryPolicy,
            ConsumptionPolicy consumptionPolicy,
            SubscriptionShards shards,
            Status status,
            Map<String, String> properties
    ) {
        super(name, version);
        this.project = project;
        this.topic = topic;
        this.description = description;
        this.grouped = grouped;
        this.endpoint = endpoint;
        this.retryPolicy = retryPolicy;
        this.consumptionPolicy = consumptionPolicy;
        if (shards == null || shards.getShardCount() <= 0) {
            throw new IllegalArgumentException("shards cannot be null or empty");
        }
        this.shards = shards;
        this.status = status;
        this.properties = properties == null ? Maps.newHashMap() : properties;
    }

    public static VaradhiSubscription of(
            String name,
            String project,
            String topic,
            String description,
            boolean grouped,
            Endpoint endpoint,
            RetryPolicy retryPolicy,
            ConsumptionPolicy consumptionPolicy,
            SubscriptionShards shards,
            Map<String, String> properties
    ) {
        return new VaradhiSubscription(
                name, INITIAL_VERSION, project, topic, description, grouped, endpoint, retryPolicy, consumptionPolicy,
                shards, new Status(State.Creating), properties
        );
    }

    @JsonIgnore
    public boolean isWellProvisioned() {
        return status.state == State.Created;
    }

    public void markCreateFailed(String message) {
        status.message = message;
        status.state = State.CreateFailed;
    }

    public void markCreated() {
        status.state = State.Created;
    }

    public void markDeleteFailed(String message) {
        status.message = message;
        status.state = State.DeleteFailed;
    }

    public void markDeleting() {
        status.state = State.Deleting;
    }

    @JsonIgnore
    public int getIntProperty(String property, int defaultValue) {
        if (properties.containsKey(property)) {
            return Integer.parseInt(properties.get(property));
        }
        return defaultValue;
    }

    public enum State {
        Creating,
        CreateFailed,
        Created,
        Deleting,
        DeleteFailed
    }

    @Data
    @AllArgsConstructor(onConstructor = @__(@JsonCreator))
    public static class Status {
        String message;
        State state;

        public Status(State state) {
            this.state = state;
        }
    }
}
