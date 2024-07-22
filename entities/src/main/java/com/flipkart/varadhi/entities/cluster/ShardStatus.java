package com.flipkart.varadhi.entities.cluster;


import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ShardStatus {
    private ShardState state;
    private String failureReason;

    @JsonIgnore
    public boolean canStop() {
        return state == ShardState.STARTED || state == ShardState.ERRORED || state == ShardState.STARTING;
    }

    @JsonIgnore
    public boolean canStart() {
        return state == ShardState.STOPPED || state == ShardState.ERRORED || state == ShardState.UNKNOWN ||
                state == ShardState.STOPPING;
    }

    @JsonIgnore
    public boolean isStarted() {
        return state == ShardState.STARTED ||  state == ShardState.STARTING;
    }

    @JsonIgnore
    public boolean isStopped() {
        return state == ShardState.STOPPED ||  state == ShardState.UNKNOWN ||
                state == ShardState.STOPPING;
    }
}
