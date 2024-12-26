package com.flipkart.varadhi.cluster.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.core.cluster.entities.ShardStatusRequest;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.utils.JsonMapper;
import lombok.Getter;

@Getter
public class ClusterMessage {
    private final String id;
    private final long timeStamp;
    private final String payload;

    ClusterMessage(String payload) {
        this.id = java.util.UUID.randomUUID().toString();
        this.timeStamp = System.currentTimeMillis();
        this.payload = payload;
    }

    @JsonCreator
    ClusterMessage(String id, long timeStamp, String payload) {
        this.id = id;
        this.timeStamp = timeStamp;
        this.payload = payload;
    }

    public static ClusterMessage of(ShardOperation.OpData operation) {
        // This will result in double serialization of the operation object, below and during eventbus call.
        return new ClusterMessage(JsonMapper.jsonSerialize(operation));
    }

    public static ClusterMessage of(SubscriptionOperation.OpData operation) {
        // This will result in double serialization of the operation object, below and during eventbus call.
        return new ClusterMessage(JsonMapper.jsonSerialize(operation));
    }

    public static ClusterMessage of(ShardStatusRequest request) {
        // This will result in double serialization of the operation object, below and during eventbus call.
        return new ClusterMessage(JsonMapper.jsonSerialize(request));
    }

    public static <T> ClusterMessage of(T payload) {
        return new ClusterMessage(JsonMapper.jsonSerialize(payload));
    }

    public static ClusterMessage of() {
        return new ClusterMessage(null);
    }

    public <T> T getData(Class<T> clazz) {
        return JsonMapper.jsonDeserialize(payload, clazz);
    }

    public <T> T getRequest(Class<T> clazz) {
        //TODO:: there is no enforcement on payload, i.e. request can be deserialized as data.
        return JsonMapper.jsonDeserialize(payload, clazz);
    }

    public ResponseMessage getResponseMessage(Object payload) {
        return ResponseMessage.fromPayload(payload, id);
    }

    public ResponseMessage getResponseMessage(Exception exception) {
        return ResponseMessage.fromException(exception, id);
    }

    @Override
    public String toString() {
        return "ClusterMessage{id=%s}".formatted(id);
    }
}
