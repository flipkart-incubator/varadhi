package com.flipkart.varadhi.web.produce;

import com.flipkart.varadhi.Result;
import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.MessageHeaderUtils;
import com.flipkart.varadhi.entities.TopicState;
import com.flipkart.varadhi.entities.config.MessageHeaderConfiguration;
import com.flipkart.varadhi.entities.utils.HeaderUtils;
import com.flipkart.varadhi.entities.constants.StandardHeaders;
import com.flipkart.varadhi.exceptions.ProduceException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.produce.ProduceResult;
import com.flipkart.varadhi.spi.services.DummyProducer;
import com.flipkart.varadhi.web.ErrorResponse;
import com.flipkart.varadhi.web.routes.TelemetryType;
import io.opentelemetry.api.trace.Span;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.ext.web.client.HttpRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.flipkart.varadhi.Constants.CONTEXT_KEY_RESOURCE_HIERARCHY;
import static com.flipkart.varadhi.entities.TopicState.*;
import static com.flipkart.varadhi.web.RequestTelemetryConfigurator.REQUEST_SPAN_NAME;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class ProduceHandlersTest extends ProduceTestBase {
    Span span;
    @Override
    public void tearDown() throws InterruptedException {
        super.tearDown();
    }

    @BeforeEach
    public void PreTest() throws InterruptedException {
        super.setUp();
        route.handler(bodyHandler).handler(ctx -> {
            ctx.put(CONTEXT_KEY_RESOURCE_HIERARCHY, produceHandlers.getHierarchies(ctx, true));
            ctx.next();
        }).handler(ctx -> {
            requestTelemetryConfigurator.addRequestSpanAndLog(ctx, "Produce", new TelemetryType(true, true, true));
            ctx.next();
        }).handler(produceHandlers::produce);
        setupFailureHandler(route);
        span = mock(Span.class);
        doReturn(span).when(spanProvider).addSpan(REQUEST_SPAN_NAME);
        doReturn(span).when(span).setAttribute(anyString(), anyString());
        HeaderUtils.initialize( MessageHeaderUtils.fetchDummyHeaderConfiguration());
    }

    @AfterEach
    public void PostTest() throws InterruptedException {
        tearDown();
    }

    @Test
    public void testProduceAndDuplicateMessage() throws InterruptedException {
        ProduceResult result = ProduceResult.of(messageId, Result.of(new DummyProducer.DummyOffset(10)));
        doReturn(CompletableFuture.completedFuture(result)).when(producerService)
                                                           .produceToTopic(
                                                               msgCapture.capture(),
                                                               eq(topicFullName),
                                                               any()
                                                           );
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, topicPath);
        request.putHeader(HeaderUtils.getHeader(StandardHeaders.MSG_ID), messageId);
        request.putHeader(HeaderUtils.getHeader(StandardHeaders.CALLBACK_CODE), "host1, host2");
        request.putHeader("RandomHeader", "value1");
        request.putHeader("x_header1", List.of("h1v1", "h1v2"));
        request.putHeader("X_HEADER2", "h2v1");
        String messageIdObtained = sendRequestWithPayload(request, payload, String.class);
        Assertions.assertEquals(messageId, messageIdObtained);
        Message capturedMessage = msgCapture.getValue();
        HeaderUtils.getRequiredHeaders().forEach(s -> Assertions.assertTrue(capturedMessage.hasHeader(s)));
        Assertions.assertArrayEquals(payload, msgCapture.getValue().getPayload());
        verify(spanProvider, times(1)).addSpan(eq(REQUEST_SPAN_NAME));

        Assertions.assertFalse(capturedMessage.hasHeader("RandomHeader"));
        Assertions.assertTrue(capturedMessage.getHeaders("x_header1").contains("h1v1"));
        Assertions.assertTrue(capturedMessage.getHeaders("x_header1").contains("h1v2"));
        Assertions.assertTrue(capturedMessage.getHeaders("x_header2").contains("h2v1"));
        Assertions.assertFalse(capturedMessage.hasHeader("X_HEADER2"));
        messageIdObtained = sendRequestWithPayload(request, payload, String.class);
        Assertions.assertEquals(messageId, messageIdObtained);
        verify(producerService, times(2)).produceToTopic(any(), eq(topicFullName), any());
    }

    @Test
    public void testProduceThrows() throws InterruptedException {
        String exceptionMessage = "Some random message.";
        doThrow(new ResourceNotFoundException(exceptionMessage)).when(producerService)
                                                                .produceToTopic(any(), any(), any());

        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, topicPath);
        request.putHeader(HeaderUtils.getHeader(StandardHeaders.MSG_ID), messageId);
        sendRequestWithPayload(request, payload, 404, exceptionMessage, ErrorResponse.class);
    }

    @Test
    public void testProduceNonProducingTopicState() {
        record testData(int status, String message, TopicState state) {
        }

        List<testData> data = List.of(
            new testData(422, "Topic/Queue is blocked. Unblock the Topic/Queue before produce.", Blocked),
            new testData(429, "Produce to Topic/Queue is currently rate limited, try again after sometime.", Throttled),
            new testData(422, "Produce is not allowed for replicating Topic/Queue.", Replicating)
        );

        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, topicPath);
        request.putHeader(HeaderUtils.getHeader(StandardHeaders.MSG_ID), messageId);

        data.forEach(d -> {
            ProduceResult result = ProduceResult.ofNonProducingTopic(messageId, d.state);
            doReturn(CompletableFuture.completedFuture(result)).when(producerService)
                                                               .produceToTopic(
                                                                   msgCapture.capture(),
                                                                   eq(topicFullName),
                                                                   any()
                                                               );
            try {
                sendRequestWithPayload(request, payload, d.status, d.message, ErrorResponse.class);
            } catch (InterruptedException e) {
                Assertions.fail("Unexpected Interruped Exception.");
            }
        });
    }

    @Test
    public void testProduceFailureResult() throws InterruptedException {

        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, topicPath);
        request.putHeader(HeaderUtils.getHeader(StandardHeaders.MSG_ID), messageId);
        String topicProduceFailureMsg = "Failure from messaging stack in ProduceAsync().";
        ProduceResult result = ProduceResult.of(messageId, Result.of(new ProduceException(topicProduceFailureMsg)));
        doReturn(CompletableFuture.completedFuture(result)).when(producerService)
                                                           .produceToTopic(
                                                               msgCapture.capture(),
                                                               eq(topicFullName),
                                                               any()
                                                           );
        sendRequestWithPayload(
            request,
            payload,
            500,
            String.format("Produce failure from messaging stack for Topic/Queue. %s", topicProduceFailureMsg),
            ErrorResponse.class
        );
    }

    @Test
    public void testProduceUnexpectedFailure() throws InterruptedException {
        String exceptionMessage = "Failure from Producer Service.";
        doReturn(CompletableFuture.failedFuture(new ResourceNotFoundException(exceptionMessage))).when(producerService)
                                                                                                 .produceToTopic(
                                                                                                     msgCapture.capture(),
                                                                                                     eq(topicFullName),
                                                                                                     any()
                                                                                                 );
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, topicPath);
        request.putHeader(HeaderUtils.getHeader(StandardHeaders.MSG_ID), messageId);
        sendRequestWithPayload(request, payload, 404, exceptionMessage, ErrorResponse.class);

        doReturn(CompletableFuture.failedFuture(new RuntimeException(exceptionMessage))).when(producerService)
                                                                                        .produceToTopic(
                                                                                            msgCapture.capture(),
                                                                                            eq(topicFullName),
                                                                                            any()
                                                                                        );
        sendRequestWithPayload(request, payload, 500, exceptionMessage, ErrorResponse.class);
    }

    @Test
    public void testProduceProduceException() throws InterruptedException {
        String exceptionMessage = "Failed to Produce.";
        doThrow(new ProduceException(exceptionMessage)).when(producerService).produceToTopic(any(), any(), any());

        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, topicPath);
        request.putHeader(HeaderUtils.getHeader(StandardHeaders.MSG_ID), messageId);
        sendRequestWithPayload(request, payload, 500, exceptionMessage, ErrorResponse.class);
    }

    @Test
    public void testProduceHeaderOrdering() throws InterruptedException {
        ProduceResult result = ProduceResult.of(messageId, Result.of(new DummyProducer.DummyOffset(10)));
        doReturn(CompletableFuture.completedFuture(result)).when(producerService)
                                                           .produceToTopic(
                                                               msgCapture.capture(),
                                                               eq(topicFullName),
                                                               any()
                                                           );
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, topicPath);
        request.putHeader(HeaderUtils.getHeader(StandardHeaders.MSG_ID), messageId);
        request.putHeader(HeaderUtils.getHeader(StandardHeaders.CALLBACK_CODE), "host1, host2");
        request.putHeader("RandomHeader", "value1");
        MultiMap multimap = new HeadersMultiMap();
        multimap.add("X_HEADER1", "h1v1");
        multimap.add("X_HEADER1", "h1v2");
        multimap.add("X_HEADER1", "h1v3");
        request.putHeaders(multimap);
        request.putHeader("X_HEADER2", "h2v1");
        String messageIdObtained = sendRequestWithPayload(request, payload, String.class);
        Assertions.assertEquals(messageId, messageIdObtained);
        String[] h1Values = msgCapture.getValue().getHeaders("x_header1").toArray(new String[]{});
        Assertions.assertEquals("h1v1", h1Values[0]);
        Assertions.assertEquals("h1v2", h1Values[1]);
        Assertions.assertEquals("h1v3", h1Values[2]);
        Assertions.assertEquals("h1v1", msgCapture.getValue().getHeader("x_header1"));
    }

    @Test
    public void testProduceForNonexistingProject() throws InterruptedException {
        doThrow(new ResourceNotFoundException("Project1 not found.")).when(projectService).getCachedProject("project1");
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, topicPath);
        request.putHeader(HeaderUtils.getHeader(StandardHeaders.MSG_ID), messageId);
        ProduceResult result = ProduceResult.of(messageId, Result.of(new DummyProducer.DummyOffset(10)));
        doReturn(CompletableFuture.completedFuture(result)).when(producerService)
                                                           .produceToTopic(
                                                               msgCapture.capture(),
                                                               eq(topicFullName),
                                                               any()
                                                           );
        sendRequestWithPayload(request, payload, 404, "Project1 not found.", ErrorResponse.class);
    }
}
