package com.flipkart.varadhi.entities.cluster;


import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class ShardStatus {
    private final ShardAssignmentState state;
    private final String failureReason;

    @JsonIgnore
    public boolean isStarted() {
        return state == ShardAssignmentState.STARTED;
    }

    public boolean canSkipStart() {
        return state == ShardAssignmentState.STARTED ||  state == ShardAssignmentState.STARTING;
    }

    @JsonIgnore
    public boolean canSkipStop() {
        return state == ShardAssignmentState.STOPPED ||
                state == ShardAssignmentState.STOPPING;
    }
}
