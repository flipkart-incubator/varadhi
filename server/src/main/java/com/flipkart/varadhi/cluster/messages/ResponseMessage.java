package com.flipkart.varadhi.cluster.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.flipkart.varadhi.entities.JsonMapper;
import lombok.Getter;


@Getter
public class ResponseMessage extends ClusterMessage {
    private final String requestId;
    private final Exception exception;

    ResponseMessage(String payload, Exception exception, String requestId) {
        super(payload);
        this.requestId = requestId;
        this.exception = exception;
    }

    @JsonCreator
    ResponseMessage(String id, long timeStamp, String payload, String requestId, Exception exception) {
        super(id, timeStamp, payload);
        this.requestId = requestId;
        this.exception = exception;
    }

    public static ResponseMessage fromPayload(Object payload, String requestId) {
        // This will result in double serialization of the operation object, below and during eventbus call.
        return new ResponseMessage(JsonMapper.jsonSerialize(payload), null, requestId);
    }

    public static ResponseMessage fromException(Exception exception, String requestId) {
        // This will result in double serialization of the operation object, below and during eventbus call.
        return new ResponseMessage(null, exception, requestId);
    }

    public <T> T getResponse(Class<T> clazz) {
        //TODO:: there is no enforcement on payload, i.e. payload  can be deserialized as request/data/response.
        return JsonMapper.jsonDeserialize(getPayload(), clazz);
    }
}
