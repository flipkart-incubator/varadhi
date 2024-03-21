package com.flipkart.varadhi.cluster.messages;

import com.flipkart.varadhi.core.cluster.SubscriptionOperation;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode(callSuper = true)
public class SubscriptionMessage extends ClusterMessage {
    private final SubscriptionOperation.OpData operation;

    public SubscriptionMessage(SubscriptionOperation.OpData operation) {
        super();
        this.operation = operation;
    }

    public SubscriptionMessage(String id, long timeStamp, SubscriptionOperation.OpData operation) {
        super(id, timeStamp);
        this.operation = operation;
    }
}
