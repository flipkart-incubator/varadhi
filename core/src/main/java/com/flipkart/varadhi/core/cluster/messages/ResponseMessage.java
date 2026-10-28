package com.flipkart.varadhi.core.cluster.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.varadhi.common.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.common.exceptions.ServerNotAvailableException;
import com.flipkart.varadhi.common.exceptions.VaradhiException;
import com.flipkart.varadhi.common.utils.ThrowableUtils;
import com.flipkart.varadhi.entities.JsonMapper;
import lombok.Getter;

@Getter
public class ResponseMessage extends ClusterMessage {
    private final String requestId;
    private final String failureType;
    private final String failureMessage;

    @JsonIgnore
    private final Exception exception;

    ResponseMessage(String payload, Exception exception, String requestId) {
        super(payload);
        this.requestId = requestId;
        Exception root = unwrap(exception);
        this.exception = root;
        this.failureType = root != null ? root.getClass().getName() : null;
        this.failureMessage = root != null ? root.getMessage() : null;
    }

    @JsonCreator
    ResponseMessage(
        String id,
        long timeStamp,
        String payload,
        String requestId,
        String failureType,
        String failureMessage
    ) {
        super(id, timeStamp, payload);
        this.requestId = requestId;
        this.failureType = failureType;
        this.failureMessage = failureMessage;
        this.exception = reconstructException(failureType, failureMessage);
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

    private static Exception unwrap(Exception exception) {
        if (exception == null) {
            return null;
        }
        Throwable current = exception;
        Throwable next;
        while ((next = ThrowableUtils.unwrap(current)) != current) {
            current = next;
        }
        if (current instanceof Exception e) {
            return e;
        }
        return new VaradhiException(current);
    }

    private static Exception reconstructException(String failureType, String failureMessage) {
        if (failureType == null) {
            return null;
        }
        if (DuplicateResourceException.class.getName().equals(failureType)) {
            return new DuplicateResourceException(failureMessage);
        }
        if (ServerNotAvailableException.class.getName().equals(failureType)) {
            return new ServerNotAvailableException(failureMessage);
        }
        if (IllegalArgumentException.class.getName().equals(failureType)) {
            return new IllegalArgumentException(failureMessage);
        }
        if (ResourceNotFoundException.class.getName().equals(failureType)) {
            return new ResourceNotFoundException(failureMessage);
        }
        if (InvalidOperationForResourceException.class.getName().equals(failureType)) {
            return new InvalidOperationForResourceException(failureMessage);
        }
        if (UnsupportedOperationException.class.getName().equals(failureType)) {
            return new UnsupportedOperationException(failureMessage);
        }
        if (VaradhiException.class.getName().equals(failureType)) {
            return new VaradhiException(failureMessage);
        }
        return new VaradhiException(failureMessage != null ? failureMessage : failureType);
    }
}
