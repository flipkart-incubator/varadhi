package com.flipkart.varadhi.entities.cluster;

import com.flipkart.varadhi.entities.VaradhiSubscription;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public enum SubscriptionState {
    STARTING,
    RUNNING,
    STOPPING,
    STOPPED,
    ERRORED;

    public static SubscriptionState getFromShardStates(List<Assignment> assignments, List<ShardStatus> shardStatuses) {
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

        Map<ShardState, Integer> stateCounts = new HashMap<>();
        shardStatuses.forEach(ss -> stateCounts.compute(ss.getState(), (k,v) -> v == null ? 1 : v+1));
        SubscriptionState subState;
        int totalShards = shardStatuses.size();
        int runningShards = stateCounts.getOrDefault(ShardState.STARTED, 0);
        int startingShards = stateCounts.getOrDefault(ShardState.STARTED, 0);
        int stoppingShards = stateCounts.getOrDefault(ShardState.STARTED, 0);
        if (totalShards == runningShards) {
            subState = RUNNING;
        }else if (startingShards > 0 && stoppingShards > 0) {
            subState = ERRORED;
        }else if (startingShards > 0) {
            subState = STARTING;
        }else if (stoppingShards > 0) {
            subState = STOPPING;
        }else{
            //TODO:: Other conditions are ignored for now and being folded into ERRORED.
            subState = ERRORED;
        }
        return subState;
    }
}
