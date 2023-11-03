package com.flipkart.varadhi.web.produce;

import com.flipkart.varadhi.Result;
import com.flipkart.varadhi.entities.ProduceResult;
import com.flipkart.varadhi.spi.services.DummyProducer;
import com.flipkart.varadhi.web.ErrorResponse;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static com.flipkart.varadhi.MessageConstants.Headers.FORWARDED_FOR;
import static com.flipkart.varadhi.MessageConstants.Headers.MESSAGE_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

public class BodyHandlerTest extends ProduceTestBase {

    HttpRequest<Buffer> request;

    @BeforeEach()
    public void PreTest() throws InterruptedException {
        super.setUp();
        bodyHandler.setBodyLimit(20);
        route.handler(bodyHandler).handler(produceHandlers::produce);
        setupFailureHandler(route);

        ProduceResult result = ProduceResult.of(messageId, Result.of(new DummyProducer.DummyOffset(10)));
        doReturn(CompletableFuture.completedFuture(result)).when(producerService).produceToTopic(any(), any(), any());
        request = createRequest(HttpMethod.POST, topicPath);
    }

    @AfterEach
    public void PostTest() throws InterruptedException {
        super.tearDown();
    }

    @Test
    public void testProduceWithForBodySize() throws InterruptedException {
        request.putHeader(MESSAGE_ID, messageId);
        request.putHeader(FORWARDED_FOR, "host1, host2");
        payload = "0123456789".getBytes();
        String messageIdObtained = sendRequestWithByteBufferBody(request, payload, String.class);
        Assertions.assertEquals(messageId, messageIdObtained);

        payload = "0123456789012345678".getBytes();
        messageIdObtained = sendRequestWithByteBufferBody(request, payload, String.class);
        Assertions.assertEquals(messageId, messageIdObtained);

        payload = "01234567890123456789".getBytes();
        messageIdObtained = sendRequestWithByteBufferBody(request, payload, String.class);
        Assertions.assertEquals(messageId, messageIdObtained);

        payload = "012345678901234567890".getBytes();
        sendRequestWithByteBufferBody(
                request, payload, 413, "Entity too large.",
                ErrorResponse.class
        );

        payload = "012345678901234567890123456789".getBytes();
        sendRequestWithByteBufferBody(
                request, payload, 413, "Entity too large.",
                ErrorResponse.class
        );
    }
}
