package com.flipkart.varadhi.web.v1.producer;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.flipkart.varadhi.common.Constants.ContextKeys;
import com.flipkart.varadhi.common.Result;
import com.flipkart.varadhi.entities.StdHeaders;
import com.flipkart.varadhi.produce.ProduceResult;
import com.flipkart.varadhi.spi.services.DummyProducer;
import com.flipkart.varadhi.web.WebTestBase;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

public class PreProduceHandlerTest extends ProduceTestBase {
    HttpRequest<Buffer> request;

    @BeforeEach
    public void PreTest() throws InterruptedException {
        super.setUp();
        route.handler(bodyHandler).handler(ctx -> {
            ctx.put(ContextKeys.RESOURCE_HIERARCHY, produceHandlers.getHierarchies(ctx, true));
            ctx.next();
        }).handler(produceHandlers::produce);
        setupFailureHandler(route);
        ProduceResult result = ProduceResult.of(messageId, Result.of(new DummyProducer.DummyOffset(10)));
        doReturn(CompletableFuture.completedFuture(result)).when(producerService).produceToTopic(any(), any());
        request = createRequest(HttpMethod.POST, topicPath);
    }

    @AfterEach
    public void PostTest() throws InterruptedException {
        super.tearDown();
    }

    @Test
    public void testProduceWithValidHeaders() {

        request.putHeader(StdHeaders.get().msgId(), messageId);
        request.putHeader(StdHeaders.get().groupId(), "host1, host2");
        request.putHeader("x_header1", List.of("h1v1", "h1v2"));
        String messageIdObtained = sendRequestWithPayload(request, payload, WebTestBase.c(String.class));
        Assertions.assertEquals(messageId, messageIdObtained);

        request.putHeader("x_header2", "h2v1");
        messageIdObtained = sendRequestWithPayload(request, payload, WebTestBase.c(String.class));
        Assertions.assertEquals(messageId, messageIdObtained);
    }

    @Test
    public void testProduceWithMultiValueHeaderIsSingleHeader() {
        request.putHeader(StdHeaders.get().msgId(), messageId);
        request.putHeader(StdHeaders.get().callbackCodes(), "host1, host2");
        request.putHeader("x_header1", List.of("value1", "value2", "value3", "value4"));
        request.putHeader("x_header3", "value3");
        String messageIdObtained = sendRequestWithPayload(request, payload, WebTestBase.c(String.class));
        Assertions.assertEquals(messageId, messageIdObtained);
    }
}
