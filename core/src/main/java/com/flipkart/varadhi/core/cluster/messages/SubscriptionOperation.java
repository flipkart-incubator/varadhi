package com.flipkart.varadhi.core.cluster.messages;


import com.flipkart.varadhi.entities.VaradhiSubscription;
import lombok.Data;

import java.util.UUID;

@Data
public class SubscriptionOperation {
    public static enum Kind {
        CREATE, START, STOP, DELETE, UPDATE
    }
    public static enum State {
        SCHEDULED, ERRORED, COMPLETED, IN_PROGRESS
    }

    private String operationId;
    private Kind kind;
    private State state;
    private String subscriptionId;
    private String requestedBy;
    private String errorMessage;
    private long startTime;
    private long endTime;

    public void markInProgress() {
        state = State.IN_PROGRESS;
    }

    public SubscriptionMessage toMessage() {
        return new SubscriptionMessage(this);
    }

    public static SubscriptionOperation getSubscriptionOp(Kind opKind, String subscriptionId, String requestedBy) {
        SubscriptionOperation op = new SubscriptionOperation();
        op.setOperationId(UUID.randomUUID().toString());
        op.setKind(opKind);
        op.setState(State.SCHEDULED);
        op.setSubscriptionId(subscriptionId);
        op.setRequestedBy(requestedBy);
        op.setStartTime(System.currentTimeMillis());
        return op;
    }
}
