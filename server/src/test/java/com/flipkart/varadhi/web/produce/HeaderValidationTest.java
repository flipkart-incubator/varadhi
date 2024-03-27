package com.flipkart.varadhi.web.produce;

import com.flipkart.varadhi.Result;
import com.flipkart.varadhi.config.RestOptions;
import com.flipkart.varadhi.entities.ResourceHierarchy;
import com.flipkart.varadhi.produce.ProduceResult;
import com.flipkart.varadhi.spi.services.DummyProducer;
import com.flipkart.varadhi.web.ErrorResponse;
import com.flipkart.varadhi.web.v1.produce.HeaderValidationHandler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.flipkart.varadhi.Constants.CONTEXT_KEY_RESOURCE_HIERARCHY;
import static com.flipkart.varadhi.entities.StandardHeaders.FORWARDED_FOR;
import static com.flipkart.varadhi.entities.StandardHeaders.MESSAGE_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

public class HeaderValidationTest extends ProduceTestBase {
    HeaderValidationHandler validationHandler;
    HttpRequest<Buffer> request;

    @BeforeEach()
    public void PreTest() throws InterruptedException {
        super.setUp();
        RestOptions options = new RestOptions();
        options.setDeployedRegion("region1");
        options.setHeadersAllowedMax(4);
        options.setPayloadSizeMax(100);
        options.setHeaderNameSizeMax(20);
        options.setHeaderValueSizeMax(20);
        validationHandler = new HeaderValidationHandler(options);
        route.handler(bodyHandler)
                .handler(ctx -> {
                    ResourceHierarchy hierarchy = produceHandlers.getHierarchy(ctx, true);
                    ctx.put(CONTEXT_KEY_RESOURCE_HIERARCHY, hierarchy);
                    ctx.next();
                })
                .handler(validationHandler::validate)
                .handler(produceHandlers::produce);
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
    public void testProduceWithValidHeaders() throws InterruptedException {

        request.putHeader(MESSAGE_ID, messageId);
        request.putHeader(FORWARDED_FOR, "host1, host2");
        request.putHeader("x_header1", List.of("h1v1", "h1v2"));
        String messageIdObtained = sendRequestWithByteBufferBody(request, payload, String.class);
        Assertions.assertEquals(messageId, messageIdObtained);

        request.putHeader("x_header2", "h2v1");
        messageIdObtained = sendRequestWithByteBufferBody(request, payload, String.class);
        Assertions.assertEquals(messageId, messageIdObtained);
    }

    @Test
    public void testProduceWithHighHeaderKeySize() throws InterruptedException {
        request.putHeader(MESSAGE_ID, messageId);
        request.putHeader(FORWARDED_FOR, "host1, host2");
        request.putHeader("x_header1_morethantwentycharsintotal", "value1");
        sendRequestWithByteBufferBody(
                request, payload, 400, "Header name x_header1_morethantwentycharsintotal exceeds allowed size.",
                ErrorResponse.class
        );
    }

    @Test
    public void testProduceWithHighHeaderValueSize() throws InterruptedException {
        request.putHeader(MESSAGE_ID, messageId);
        request.putHeader(FORWARDED_FOR, "host1, host2");
        request.putHeader("x_header1", "morethantwentycharsintotal");
        sendRequestWithByteBufferBody(
                request, payload, 400, "Value of Header x_header1 exceeds allowed size.",
                ErrorResponse.class
        );
    }

    @Test
    public void testProduceWithHighHeaderNumbers() throws InterruptedException {
        request.putHeader(MESSAGE_ID, messageId);
        request.putHeader(FORWARDED_FOR, "host1, host2");
        request.putHeader("x_header1", "value1");
        request.putHeader("x_header2", "value2");
        request.putHeader("x_header3", "value3");
        sendRequestWithByteBufferBody(
                request, payload, 400, "More Varadhi specific headers specified than allowed max(4).",
                ErrorResponse.class
        );
    }

    @Test
    public void testProduceWithMultiValueHeaderIsSingleHeader() throws InterruptedException {
        request.putHeader(MESSAGE_ID, messageId);
        request.putHeader(FORWARDED_FOR, "host1, host2");
        request.putHeader("x_header1", List.of("value1", "value2", "value3", "value4"));
        request.putHeader("x_header3", "value3");
        String messageIdObtained = sendRequestWithByteBufferBody(request, payload, String.class);
        Assertions.assertEquals(messageId, messageIdObtained);
    }
}
