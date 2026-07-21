package com.flipkart.varadhi.entities;

import lombok.Getter;

@Getter
public enum ProduceStatus {

    Failed("Produce failure from messaging stack for Topic/Queue."), Throttled(
        "Produce to Topic/Queue is currently rate limited, try again after sometime."
    ), Fenced("Topic/Queue is fenced during failover. Retry after failover completes."), NotAllowed(
        "Produce is not allowed for replicating Topic/Queue."
    ), Success("Produce is allowed to Topic/Queue."), Filtered("Produce is filtered by org filters.");

    private final String message;

    ProduceStatus(String message) {
        this.message = message;
    }
}
