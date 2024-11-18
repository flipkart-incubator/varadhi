package com.flipkart.varadhi.entities.cluster;

import java.util.List;
import java.util.Optional;

public enum SubscriptionState {
    STARTING,
    RUNNING,
    STOPPING,
    STOPPED,
    ERRORED;

    public static SubscriptionState getFromShardStates(int totalShards, List<Assignment> assignments, List<Optional<ConsumerState>> states) {
        // len(ShardAssignment) == 0, State can be either of Stopped/Errored (Create/Delete failed).

        // len(ShardAssignment) > 0, State cn be Running, Stopping, Starting, Errored.
        //  ShardCount == Running Shard --> Running
        //  Starting shard > 0 --> Starting
        //  Stopping shard > 0 --> Stopping

        // Below would need additional details like last operation executed.
        // For now all the below are being classified as Error'ed.

        //  Starting > 0 & Stopping > 0 --> Starting/Stopping (when sub operation are allowed in parallel)
        //  ShardCount ==  Unknown shard --> Starting/Stopping
        //  ShardCount ==  Unknown Shard + Errored Shard --> Errored (Could be from Start or Stop)
        //  Errored shard > 0 --> Errored (Could be from Start or Stop)

        if (assignments.isEmpty()) {
            //TODO:: error conditions are being ignored for now.
            return STOPPED;
        }

        if(assignments.size() != states.size()) {
            throw new IllegalStateException("Assignment and State size mismatch");
        }

        // TODO: whether this is ok to do? SubscriptionState looks more of assignment pov. We can look at consumptino status as well.
        // There are 2 things. Assignment and actual consumption.
        if (states.stream().anyMatch(s -> s.isPresent() && s.get() == ConsumerState.ERRORED)) {
            return ERRORED;
        }

        // TODO: could be starting as well. need the final "op" on the subscription to distinguish that.
        if (assignments.isEmpty()) {
            return STOPPED;
        }

        // TODO: could be stopping as well. need the final "op" on the subscription to distinguish that.
        if (assignments.size() < totalShards) {
            return STARTING;
        }

        // TODO: could be stopping as well. need the final "op" on the subscription to distinguish that.
        if (assignments.size() == totalShards) {
            return RUNNING;
        }

        throw new IllegalStateException("unreachable code");
    }
}
