package com.flipkart.varadhi.web.produce;

import com.flipkart.varadhi.Result;
import com.flipkart.varadhi.config.MessageHeaderConfiguration;
import com.flipkart.varadhi.config.RestOptions;
import com.flipkart.varadhi.produce.ProduceResult;
import com.flipkart.varadhi.spi.services.DummyProducer;
import com.flipkart.varadhi.web.ErrorResponse;
import com.flipkart.varadhi.web.v1.produce.HeaderValidationHandler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpRequest;
import net.bytebuddy.utility.RandomString;
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
        MessageHeaderConfiguration messageHeaderConfiguration = new MessageHeaderConfiguration();
        messageHeaderConfiguration.setAllowedPrefix(List.of("X_","x_"));
        messageHeaderConfiguration.setMsgIdHeader("X_MESSAGE_ID");
        messageHeaderConfiguration.setProduceIdentity("X_PRODUCE_IDENTITY");
        messageHeaderConfiguration.setProduceRegion("X_PRODUCE_REGION");
        messageHeaderConfiguration.setProduceTimestamp("X_PRODUCE_TIMESTAMP");
        validationHandler = new HeaderValidationHandler(messageHeaderConfiguration, "region1");
        route.handler(bodyHandler)
                .handler(ctx -> {
                    ctx.put(CONTEXT_KEY_RESOURCE_HIERARCHY, produceHandlers.getHierarchies(ctx, true));
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

        request.putHeader("X_MESSAGE_ID", messageId);
        request.putHeader("X_ForwardedFor", "host1, host2");
        request.putHeader("x_header1", List.of("h1v1", "h1v2"));
        String messageIdObtained = sendRequestWithByteBufferBody(request, payload, String.class);
        Assertions.assertEquals(messageId, messageIdObtained);

        request.putHeader("x_header2", "h2v1");
        messageIdObtained = sendRequestWithByteBufferBody(request, payload, String.class);
        Assertions.assertEquals(messageId, messageIdObtained);
    }

    @Test
    public void testProduceWithHighHeaderKeySize() throws InterruptedException {
        String randomString = RandomString.make(101);
        request.putHeader("X_MESSAGE_ID", randomString);
        request.putHeader(FORWARDED_FOR, "host1, host2");
        sendRequestWithByteBufferBody(
                request, payload, 400, "Message id " + randomString +  " exceeds allowed size of 100.",
                ErrorResponse.class
        );
    }
//
//    @Test
//    public void testProduceWithHighHeaderValueSize() throws InterruptedException {
//        request.putHeader(MESSAGE_ID, messageId);
//        request.putHeader(FORWARDED_FOR, "host1, host2");
//        request.putHeader("x_header1", "morethantwentycharsintotal");
//        sendRequestWithByteBufferBody(
//                request, payload, 400, "Value of Header x_header1 exceeds allowed size.",
//                ErrorResponse.class
//        );
//    }
//
//    @Test
//    public void testProduceWithHighHeaderNumbers() throws InterruptedException {
//        request.putHeader(MESSAGE_ID, messageId);
//        request.putHeader(FORWARDED_FOR, "host1, host2");
//        request.putHeader("x_header1", "value1");
//        request.putHeader("x_header2", "value2");
//        request.putHeader("x_header3", "value3");
//        sendRequestWithByteBufferBody(
//                request, payload, 400, "More Varadhi specific headers specified than allowed max(4).",
//                ErrorResponse.class
//        );
//    }
//
//    @Test
//    public void testProduceWithMultiValueHeaderIsSingleHeader() throws InterruptedException {
//        request.putHeader(MESSAGE_ID, messageId);
//        request.putHeader(FORWARDED_FOR, "host1, host2");
//        request.putHeader("x_header1", List.of("value1", "value2", "value3", "value4"));
//        request.putHeader("x_header3", "value3");
//        String messageIdObtained = sendRequestWithByteBufferBody(request, payload, String.class);
//        Assertions.assertEquals(messageId, messageIdObtained);
//    }
}
