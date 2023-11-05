package com.flipkart.varadhi.entities;

import lombok.Getter;

@Getter
public enum TopicState {
    Producing(true, ProduceStatus.Success),
    Blocked(false, ProduceStatus.Blocked),
    Throttled(false, ProduceStatus.Throttled),
    Replicating(false, ProduceStatus.NotAllowed);

    private final ProduceStatus produceStatus;
    private final boolean produceAllowed;

    TopicState(boolean produceAllowed, ProduceStatus status) {
        this.produceStatus = status;
        this.produceAllowed = produceAllowed;
    }
}
