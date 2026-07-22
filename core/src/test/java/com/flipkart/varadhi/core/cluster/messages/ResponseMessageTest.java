package com.flipkart.varadhi.core.cluster.messages;

import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.JsonMapper;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

class ResponseMessageTest {

    @Test
    void fromException_roundTripsResourceNotFound() {
        ResponseMessage original = ResponseMessage.fromException(
            new ResourceNotFoundException("No active failover for topic x."),
            "req-1"
        );
        String json = JsonMapper.jsonSerialize(original);
        ResponseMessage decoded = JsonMapper.jsonDeserialize(json, ResponseMessage.class);

        Exception ex = decoded.getException();
        assertInstanceOf(ResourceNotFoundException.class, ex);
        assertEquals("No active failover for topic x.", ex.getMessage());
        assertNull(decoded.getPayload());
    }

    @Test
    void fromException_unwrapsCompletionException() {
        ResponseMessage msg = ResponseMessage.fromException(
            new CompletionException(new ResourceNotFoundException("missing")),
            "req-2"
        );
        assertEquals(ResourceNotFoundException.class.getName(), msg.getFailureType());
        assertInstanceOf(ResourceNotFoundException.class, msg.getException());
    }

    @Test
    void fromPayload_hasNoException() {
        ResponseMessage msg = ResponseMessage.fromPayload("ok", "req-3");
        String json = JsonMapper.jsonSerialize(msg);
        ResponseMessage decoded = JsonMapper.jsonDeserialize(json, ResponseMessage.class);
        assertNull(decoded.getException());
        assertEquals("ok", decoded.getResponse(String.class));
    }
}
