package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.varadhi.entities.filter.FilterPolicy;
import lombok.*;

@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
public class VaradhiSubscription extends MetaStoreEntity {
    private final String project;
    private final String topic;
    private String description;
    private boolean grouped;
    private Endpoint endpoint;
    private RetryPolicy retryPolicy;
    private ConsumptionPolicy consumptionPolicy;
    private SubscriptionShards shards;
    private Status status;
    private FilterPolicy filterPolicy;


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
            FilterPolicy filterPolicy
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

        // TODO(aayush): parse and validate filter policy
        this.filterPolicy = filterPolicy;
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
            FilterPolicy filterPolicy
    ) {
        return new VaradhiSubscription(
                name, INITIAL_VERSION, project, topic, description, grouped, endpoint, retryPolicy, consumptionPolicy,
                shards, new Status(State.Creating), filterPolicy
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
