package com.flipkart.varadhi.core.cluster.messages;

import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.common.exceptions.VaradhiException;
import com.flipkart.varadhi.entities.JsonMapper;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

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
    void fromException_roundTripsWrappedResourceNotFound() {
        ResponseMessage original = ResponseMessage.fromException(
            new CompletionException(new ExecutionException(new ResourceNotFoundException("missing"))),
            "req-2"
        );
        ResponseMessage decoded = JsonMapper.jsonDeserialize(JsonMapper.jsonSerialize(original), ResponseMessage.class);

        assertInstanceOf(ResourceNotFoundException.class, decoded.getException());
        assertEquals("missing", decoded.getException().getMessage());
    }

    @Test
    void fromException_roundTripsInvalidOperation() {
        ResponseMessage original = ResponseMessage.fromException(
            new InvalidOperationForResourceException("An active failover already exists."),
            "req-4"
        );
        ResponseMessage decoded = JsonMapper.jsonDeserialize(JsonMapper.jsonSerialize(original), ResponseMessage.class);

        assertInstanceOf(InvalidOperationForResourceException.class, decoded.getException());
        assertEquals("An active failover already exists.", decoded.getException().getMessage());
    }

    @Test
    void fromException_unknownTypeBecomesVaradhiException() {
        ResponseMessage original = ResponseMessage.fromException(new IllegalStateException("nope"), "req-5");
        ResponseMessage decoded = JsonMapper.jsonDeserialize(JsonMapper.jsonSerialize(original), ResponseMessage.class);

        assertEquals(VaradhiException.class, decoded.getException().getClass());
        assertEquals("nope", decoded.getException().getMessage());
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
