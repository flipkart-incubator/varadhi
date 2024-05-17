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
    public boolean isAssigned() {
        return state != ShardState.STOPPED && state != ShardState.UNKNOWN;
    }
}
