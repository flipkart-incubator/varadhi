package com.flipkart.varadhi.cluster.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.flipkart.varadhi.utils.JsonMapper;


public class ResponseMessage extends ClusterMessage {
    private final String requestId;
    ResponseMessage(String payload, String requestId) {
        super(payload);
        this.requestId = requestId;
    }

    @JsonCreator
    ResponseMessage(String id, long timeStamp, String payload, String requestId) {
        super(id, timeStamp, payload);
        this.requestId = requestId;
    }

    public <T> T getResponse(Class<T> clazz) {
        //TODO:: there is no enfrocement on payload, i.e. payload  can be deserialized as request/data/response.
        return JsonMapper.jsonDeserialize(getPayload(), clazz);
    }

    public static ResponseMessage of(Object payload, String requestId) {
        // This will result in double serialization of the operation object, below and during eventbus call.
        return new ResponseMessage(JsonMapper.jsonSerialize(payload), requestId);
    }
}
