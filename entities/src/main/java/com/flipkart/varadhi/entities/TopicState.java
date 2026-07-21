package com.flipkart.varadhi.entities;

import lombok.Getter;

@Getter
public enum TopicState {
    // TODO:: Storage topic should be only Producing & Replicating
    Producing(true, ProduceStatus.Success), Fenced(false, ProduceStatus.Fenced), Replicating(
        false,
        ProduceStatus.NotAllowed
    );

    private final ProduceStatus produceStatus;
    private final boolean produceAllowed;

    TopicState(boolean produceAllowed, ProduceStatus status) {
        this.produceStatus = status;
        this.produceAllowed = produceAllowed;
    }
}
