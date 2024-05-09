package com.flipkart.varadhi.cluster.messages;

import com.flipkart.varadhi.core.cluster.ShardOperation;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode(callSuper = true)
public class ShardMessage extends ClusterMessage {
    private final ShardOperation.OpData operation;
    public ShardMessage(ShardOperation.OpData operation) {
        super();
        this.operation = operation;
    }

    public ShardMessage(String id, long timeStamp, ShardOperation.OpData operation) {
        super(id, timeStamp);
        this.operation = operation;
    }
}
