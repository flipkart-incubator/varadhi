package com.flipkart.varadhi.cluster.messages;

import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.ShardRequest;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.utils.JsonMapper;
import lombok.Getter;

@Getter
public class ClusterMessage {
    private final String id;
    private final long timeStamp;
    private final String payload;

    public ClusterMessage(String payload) {
        this.id = java.util.UUID.randomUUID().toString();
        this.timeStamp = System.currentTimeMillis();
        this.payload = payload;
    }

    ClusterMessage(String id, long timeStamp, String payload) {
        this.id = id;
        this.timeStamp = timeStamp;
        this.payload = payload;
    }

    public <T> T getData(Class<T> clazz) {
        return JsonMapper.jsonDeserialize(payload, clazz);
    }

    public <T> T getRequest(Class<T> clazz) {
        //TODO:: there is no enforcement on payload, i.e. request can be deserialized as data.
        return JsonMapper.jsonDeserialize(payload, clazz);
    }

    public static ClusterMessage of(ShardOperation.OpData operation) {
        // This will result in double serialization of the operation object, below and during eventbus call.
        return new ClusterMessage(JsonMapper.jsonSerialize(operation));
    }

    public static ClusterMessage of(SubscriptionOperation.OpData operation) {
        // This will result in double serialization of the operation object, below and during eventbus call.
        return new ClusterMessage(JsonMapper.jsonSerialize(operation));
    }

    public static ClusterMessage of(ShardRequest request) {
        // This will result in double serialization of the operation object, below and during eventbus call.
        return new ClusterMessage(JsonMapper.jsonSerialize(request));
    }

    public ResponseMessage getResponseMessage(Object payload) {
        return ResponseMessage.of(payload, id);
    }
}
