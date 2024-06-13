package com.flipkart.varadhi.entities;

import lombok.Getter;

@Getter
public enum TopicState {
    // TODO:: Storage topic should be only Producing & Replicating
    // TODO:: Blocked/Throttled are VaradhiTopic state.
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
