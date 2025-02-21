package com.flipkart.varadhi.web.produce;

import com.flipkart.varadhi.Result;
import com.flipkart.varadhi.entities.MessageHeaderUtils;
import com.flipkart.varadhi.entities.utils.HeaderUtils;
import com.flipkart.varadhi.entities.constants.StandardHeaders;
import com.flipkart.varadhi.produce.ProduceResult;
import com.flipkart.varadhi.spi.services.DummyProducer;
import com.flipkart.varadhi.web.ErrorResponse;
import com.flipkart.varadhi.web.v1.produce.PreProduceHandler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpRequest;
import net.bytebuddy.utility.RandomString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.flipkart.varadhi.Constants.CONTEXT_KEY_RESOURCE_HIERARCHY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

public class PreProduceHandlerTest extends ProduceTestBase {
    PreProduceHandler validationHandler;
    HttpRequest<Buffer> request;

    @BeforeEach
    public void PreTest() throws InterruptedException {
        super.setUp();
        validationHandler = new PreProduceHandler();
        route.handler(bodyHandler).handler(ctx -> {
            ctx.put(CONTEXT_KEY_RESOURCE_HIERARCHY, produceHandlers.getHierarchies(ctx, true));
            ctx.next();
        }).handler(validationHandler::validate).handler(produceHandlers::produce);
        setupFailureHandler(route);
        ProduceResult result = ProduceResult.of(messageId, Result.of(new DummyProducer.DummyOffset(10)));
        doReturn(CompletableFuture.completedFuture(result)).when(producerService).produceToTopic(any(), any(), any());
        request = createRequest(HttpMethod.POST, topicPath);
        HeaderUtils.initialize(MessageHeaderUtils.fetchDummyHeaderConfiguration());
    }

    @AfterEach
    public void PostTest() throws InterruptedException {
        super.tearDown();
    }

    @Test
    public void testProduceWithValidHeaders() throws InterruptedException {

        request.putHeader("X_MESSAGE_ID", messageId);
        request.putHeader("X_ForwardedFor", "host1, host2");
        request.putHeader("x_header1", List.of("h1v1", "h1v2"));
        String messageIdObtained = sendRequestWithPayload(request, payload, String.class);
        Assertions.assertEquals(messageId, messageIdObtained);

        request.putHeader("x_header2", "h2v1");
        messageIdObtained = sendRequestWithPayload(request, payload, String.class);
        Assertions.assertEquals(messageId, messageIdObtained);
    }

    //
    @Test
    public void testProduceWithHighHeaderKeySize() throws InterruptedException {
        String randomString = RandomString.make(101);
        request.putHeader("X_MESSAGE_ID", randomString);
        request.putHeader(HeaderUtils.getHeader(StandardHeaders.HTTP_URI), "host1, host2");
        sendRequestWithPayload(
            request,
            payload,
            400,
            "Message id " + randomString + " exceeds allowed size of 100.",
            ErrorResponse.class
        );
    }

    @Test
    public void testProduceWithHighBodyAndHeaderSize() throws InterruptedException {
        String randomString = RandomString.make(99);
        request.putHeader("X_MESSAGE_ID", randomString);
        request.putHeader(HeaderUtils.getHeader(StandardHeaders.HTTP_URI), "host1, host2");

        // Create a body with a size greater than 5MB. 5MB = 5 * 1024 * 1024 bytes.
        byte[] largeBody = new byte[5 * 1024 * 1024 + 1]; // Byte array of size greater than 5MB
        Arrays.fill(largeBody, (byte)'A'); // Fill it with some data (e.g., 'A')

        sendRequestWithPayload(
            request,
            largeBody,
            400,
            "Request size exceeds allowed limit of 5242880 bytes.",
            ErrorResponse.class
        );
    }

    @Test
    public void testProduceWithMultiValueHeaderIsSingleHeader() throws InterruptedException {
        request.putHeader(HeaderUtils.getHeader(StandardHeaders.MSG_ID), messageId);
        request.putHeader(HeaderUtils.getHeader(StandardHeaders.CALLBACK_CODE), "host1, host2");
        request.putHeader("x_header1", List.of("value1", "value2", "value3", "value4"));
        request.putHeader("x_header3", "value3");
        String messageIdObtained = sendRequestWithPayload(request, payload, String.class);
        Assertions.assertEquals(messageId, messageIdObtained);
    }
}
