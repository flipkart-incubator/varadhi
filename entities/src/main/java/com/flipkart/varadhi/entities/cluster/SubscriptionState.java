package com.flipkart.varadhi.entities.cluster;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Optional;

/**
 * Represents the state of a subscription.
 * Can be used to represent the state of the shard or the overall subscription. For a shard, though, the assignment
 * cannot be partially assigned.
 */
@Getter
@EqualsAndHashCode
@RequiredArgsConstructor
public class SubscriptionState {

    /**
     * Subscription state needs to be computed in this priority.
     * ASSIGNED, if all shards are assigned
     * NOT_ASSIGNED, if all shards are unassigned
     * PARTIALLY_ASSIGNED, if some shards are assigned and some are unassigned
     */
    private final AssignmentState assignmentState;

    /**
     * Errored: if any shard is errored. (can be partial)
     * Throttled: if any shard is throttled.
     * Paused: if any shard is paused. (can be partial)
     * Running: if all shards are running.
     *
     * Will be populated only when the assignmentState is ASSIGNED.
     */
    @Nullable
    private final ConsumerState consumerState;

    public static SubscriptionState mergeShardStates(List<SubscriptionState> states) {
        int totalShards = states.size();

        long partial = states.stream()
                             .filter(state -> state.assignmentState == AssignmentState.PARTIALLY_ASSIGNED)
                             .count();
        if (partial > 0) {
            throw new IllegalStateException("Invalid state. A single shard cannot be in partial state");
        }

        long assigned = states.stream().filter(state -> state.assignmentState == AssignmentState.ASSIGNED).count();
        long unassigned = states.stream()
                                .filter(state -> state.assignmentState == AssignmentState.NOT_ASSIGNED)
                                .count();

        if (assigned > 0 && unassigned > 0) {
            return new SubscriptionState(AssignmentState.PARTIALLY_ASSIGNED, null);
        } else if (unassigned == totalShards) {
            return new SubscriptionState(AssignmentState.NOT_ASSIGNED, null);
        } else if (assigned == totalShards) {
            long unknownConsumerState = states.stream().filter(s -> s.consumerState == null).count();
            if (unknownConsumerState > 0) {
                return new SubscriptionState(AssignmentState.ASSIGNED, null);
            }

            Optional<ConsumerState> erroredState = states.stream()
                                                         .map(state -> state.consumerState)
                                                         .filter(state -> state == ConsumerState.ERRORED)
                                                         .findAny();
            if (erroredState.isPresent()) {
                return new SubscriptionState(AssignmentState.ASSIGNED, ConsumerState.ERRORED);
            }

            Optional<ConsumerState> pausedState = states.stream()
                                                        .map(state -> state.consumerState)
                                                        .filter(state -> state == ConsumerState.PAUSED)
                                                        .findAny();
            if (pausedState.isPresent()) {
                return new SubscriptionState(AssignmentState.ASSIGNED, ConsumerState.PAUSED);
            }

            Optional<ConsumerState> throttledState = states.stream()
                                                           .map(state -> state.consumerState)
                                                           .filter(state -> state == ConsumerState.THROTTLED)
                                                           .findAny();
            if (throttledState.isPresent()) {
                return new SubscriptionState(AssignmentState.ASSIGNED, ConsumerState.THROTTLED);
            }

            return new SubscriptionState(AssignmentState.ASSIGNED, ConsumerState.CONSUMING);
        } else {
            throw new IllegalStateException("Invalid state");
        }
    }

    public static SubscriptionState forRunning() {
        return new SubscriptionState(AssignmentState.ASSIGNED, ConsumerState.CONSUMING);
    }

    public static SubscriptionState forStopped() {
        return new SubscriptionState(AssignmentState.NOT_ASSIGNED, null);
    }

    public static SubscriptionState forPartiallyAssigned() {
        return new SubscriptionState(AssignmentState.PARTIALLY_ASSIGNED, null);
    }

    @JsonIgnore
    public boolean isRunningSuccessfully() {
        return assignmentState == AssignmentState.ASSIGNED && consumerState == ConsumerState.CONSUMING;
    }

    @JsonIgnore
    public boolean isStoppedSuccessfully() {
        return assignmentState == AssignmentState.NOT_ASSIGNED;
    }

    @Override
    public String toString() {
        return "SubscriptionState(" + assignmentState + ", " + consumerState + ")";
    }
}
