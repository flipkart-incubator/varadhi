package com.flipkart.varadhi.web.v1.produce;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.flipkart.varadhi.common.Result;
import com.flipkart.varadhi.common.exceptions.ProduceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.config.MessageHeaderUtils;
import com.flipkart.varadhi.config.RestOptions;
import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.StdHeaders;
import com.flipkart.varadhi.entities.TopicState;
import com.flipkart.varadhi.produce.ProduceResult;
import com.flipkart.varadhi.produce.otel.ProducerMetricHandler;
import com.flipkart.varadhi.produce.otel.ProducerMetricsEmitterNoOpImpl;
import com.flipkart.varadhi.produce.services.ProducerService;
import com.flipkart.varadhi.services.ProjectService;
import com.flipkart.varadhi.spi.services.DummyProducer;
import com.flipkart.varadhi.web.ErrorResponse;
import com.flipkart.varadhi.web.RequestTelemetryConfigurator;
import com.flipkart.varadhi.web.SpanProvider;
import com.flipkart.varadhi.web.routes.TelemetryType;
import com.flipkart.varadhi.web.v1.produce.PreProduceHandler;
import com.flipkart.varadhi.web.v1.produce.ProduceHandlers;
import com.google.common.collect.Multimap;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.trace.Span;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.ext.web.client.HttpRequest;
import net.bytebuddy.utility.RandomString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static com.flipkart.varadhi.common.Constants.CONTEXT_KEY_RESOURCE_HIERARCHY;
import static com.flipkart.varadhi.entities.TopicState.*;
import static com.flipkart.varadhi.web.RequestTelemetryConfigurator.REQUEST_SPAN_NAME;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class ProduceHandlersTest extends ProduceTestBase {
    Span span;
    private static final int MAX_REQUEST_SIZE = 5 * 1024 * 1024;
    private static final int OVERSIZED_HEADER_KEY_LENGTH = 101;

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
        ProduceResult result = ProduceResult.of(messageId, Result.of(new DummyProducer.DummyOffset(10)));
        doReturn(CompletableFuture.completedFuture(result)).when(producerService).produceToTopic(any(), any(), any());
        doReturn(span).when(spanProvider).addSpan(REQUEST_SPAN_NAME);
        doReturn(span).when(span).setAttribute(anyString(), anyString());
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
        request.putHeader(StdHeaders.get().msgId(), messageId);
        request.putHeader(StdHeaders.get().callbackCodes(), "host1, host2");
        request.putHeader("RandomHeader", "value1");
        request.putHeader("x_header1", List.of("h1v1", "h1v2"));
        request.putHeader("X_HEADER2", "h2v1");
        String messageIdObtained = sendRequestWithPayload(request, payload, String.class);
        Assertions.assertEquals(messageId, messageIdObtained);
        Message capturedMessage = msgCapture.getValue();
        Assertions.assertTrue(capturedMessage.hasHeader(StdHeaders.get().msgId()));
        Assertions.assertArrayEquals(payload, msgCapture.getValue().getPayload());
        verify(spanProvider, times(1)).addSpan(eq(REQUEST_SPAN_NAME));

        Assertions.assertFalse(capturedMessage.hasHeader("RandomHeader".toUpperCase()));
        Assertions.assertTrue(capturedMessage.getHeaders("x_header1".toUpperCase()).contains("h1v1"));
        Assertions.assertTrue(capturedMessage.getHeaders("x_header1".toUpperCase()).contains("h1v2"));
        Assertions.assertTrue(capturedMessage.getHeaders("x_header2".toUpperCase()).contains("h2v1"));
        Assertions.assertTrue(capturedMessage.hasHeader("X_HEADER2"));
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
        request.putHeader(StdHeaders.get().msgId(), messageId);
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
        request.putHeader(StdHeaders.get().msgId(), messageId);

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
        request.putHeader(StdHeaders.get().msgId(), messageId);
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
        request.putHeader(StdHeaders.get().msgId(), messageId);
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
        request.putHeader(StdHeaders.get().msgId(), messageId);
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
        request.putHeader(StdHeaders.get().msgId(), messageId);
        request.putHeader(StdHeaders.get().callbackCodes(), "host1, host2");
        request.putHeader("RandomHeader", "value1");
        MultiMap multimap = new HeadersMultiMap();
        multimap.add("X_HEADER1", "h1v1");
        multimap.add("X_HEADER1", "h1v2");
        multimap.add("X_HEADER1", "h1v3");
        request.putHeaders(multimap);
        request.putHeader("X_HEADER2", "h2v1");
        String messageIdObtained = sendRequestWithPayload(request, payload, String.class);
        Assertions.assertEquals(messageId, messageIdObtained);
        String[] h1Values = msgCapture.getValue().getHeaders("X_HEADER1").toArray(new String[] {});
        Assertions.assertEquals("h1v1", h1Values[0]);
        Assertions.assertEquals("h1v2", h1Values[1]);
        Assertions.assertEquals("h1v3", h1Values[2]);
        Assertions.assertEquals("h1v1", msgCapture.getValue().getHeader("X_HEADER1"));
    }

    @Test
    public void testProduceForNonexistingProject() throws InterruptedException {
        doThrow(new ResourceNotFoundException("Project1 not found.")).when(projectCache).getEntity("project1");
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, topicPath);
        request.putHeader(StdHeaders.get().msgId(), messageId);
        ProduceResult result = ProduceResult.of(messageId, Result.of(new DummyProducer.DummyOffset(10)));
        doReturn(CompletableFuture.completedFuture(result)).when(producerService)
                                                           .produceToTopic(
                                                               msgCapture.capture(),
                                                               eq(topicFullName),
                                                               any()
                                                           );
        sendRequestWithPayload(request, payload, 404, "Project1 not found.", ErrorResponse.class);
    }

    @Test
    public void testCopyVaradhiHeaders() {
        MultiMap headers = new HeadersMultiMap();
        headers.add("header1", "h1value1");
        headers.add("Header1", "h1value2");
        headers.add("X_UPPER_CASE", "UPPER_CASE");
        headers.add("x_lower_case", "lower_case");
        headers.add("X_Mixed_Case", "Mixed_Case");
        headers.add("x_multi_value1", "multi_value1_1");
        headers.add("x_multi_value1", "multi_value1_2");
        headers.add("x_Multi_Value2", "multi_value2_1");
        headers.add("x_multi_value2", "multi_Value2_1");
        headers.add("x_multi_value2", "multi_Value2_1");
        headers.add("xy_header2", "value2");
        headers.add("x__header3", "value3");
        headers.add("x-header4", "value4");
        headers.add("x_Restbus_identity", "value5");
        Multimap<String, String> copiedHeaders = produceHandlers.filterCompliantHeaders(headers);
        Assertions.assertEquals(11, copiedHeaders.size());
        Assertions.assertEquals("value5", copiedHeaders.get("x_restbus_identity".toUpperCase()).toArray()[0]);
        Assertions.assertEquals("value3", copiedHeaders.get("x__header3".toUpperCase()).toArray()[0]);
        Assertions.assertEquals("Mixed_Case", copiedHeaders.get("x_mixed_case".toUpperCase()).toArray()[0]);
        Assertions.assertEquals("lower_case", copiedHeaders.get("x_lower_case".toUpperCase()).toArray()[0]);
        Assertions.assertEquals("UPPER_CASE", copiedHeaders.get("x_upper_case".toUpperCase()).toArray()[0]);
        Assertions.assertEquals("value4", copiedHeaders.get("x-header4".toUpperCase()).toArray()[0]);
        Collection<String> multi_value1 = copiedHeaders.get("x_multi_value1".toUpperCase());
        Assertions.assertEquals(2, multi_value1.size());
        Assertions.assertTrue(multi_value1.contains("multi_value1_1"));
        Assertions.assertTrue(multi_value1.contains("multi_value1_2"));
        Collection<String> multi_value2 = copiedHeaders.get("x_multi_value2".toUpperCase());
        Assertions.assertEquals(3, multi_value2.size());
        Assertions.assertTrue(multi_value2.contains("multi_value2_1"));
        Assertions.assertTrue(multi_value2.contains("multi_Value2_1"));
        multi_value2.remove("multi_Value2_1");
        Assertions.assertTrue(multi_value2.contains("multi_Value2_1"));

        Assertions.assertTrue(copiedHeaders.get("xy_header2".toUpperCase()).isEmpty());
        Assertions.assertTrue(copiedHeaders.get("header1".toUpperCase()).isEmpty());
        Assertions.assertTrue(copiedHeaders.get("Header1".toUpperCase()).isEmpty());
    }

    @Test
    public void ensureHeaderOrderIsMaintained() {
        MultiMap headers = new HeadersMultiMap();
        headers.add("x_multi_value1", "multi_value1_2");
        headers.add("x_multi_value1", "multi_value1_1");
        headers.add("x_Multi_Value2", "multi_value2_1");
        headers.add("x_multi_value2", "multi_Value2_1");
        headers.add("x_multi_value1", "multi_value1_3");
        Multimap<String, String> copiedHeaders = produceHandlers.filterCompliantHeaders(headers);
        String[] values = copiedHeaders.get("x_multi_value1".toUpperCase()).toArray(new String[] {});
        Assertions.assertEquals(3, values.length);
        Assertions.assertEquals("multi_value1_2", values[0]);
        Assertions.assertEquals("multi_value1_1", values[1]);
        Assertions.assertEquals("multi_value1_3", values[2]);

        values = copiedHeaders.get("x_multi_value2".toUpperCase()).toArray(new String[] {});
        Assertions.assertEquals(2, values.length);
        Assertions.assertEquals("multi_value2_1", values[0]);
        Assertions.assertEquals("multi_Value2_1", values[1]);
    }

    @ParameterizedTest
    @ValueSource (booleans = {true, false})
    public void ensureHeadersAreProcessedCorrectlyAsInsestiveMultimap(Boolean filterNonCompliantHeaders) {
        MultiMap headers = new HeadersMultiMap();
        headers.add("x_multi_value1", "multi_value1_2");
        headers.add("x_multi_value1", "multi_value1_1");
        headers.add("x_multi_value1", "multi_value1_3");
        headers.add("x_Multi_Value2", "multi_value2_1");
        headers.add("x_multi_value2", "multi_Value2_1");
        headers.add("x_muLti_value2", "multi_Value2_1");
        headers.add("abc_multi_value1", "multi_value1_3");
        headers.add("xyz_multi_value1", "multi_value1_3");
        headers.add("aaa_multi_value1", "multi_value1_3");
        headers.add("bbb_multi_value1", "multi_value1_3");
        projectService = mock(ProjectService.class);
        producerService = mock(ProducerService.class);
        spanProvider = mock(SpanProvider.class);
        RestOptions options = new RestOptions();
        options.setDeployedRegion(deployedRegion);
        requestTelemetryConfigurator = new RequestTelemetryConfigurator(spanProvider, new SimpleMeterRegistry());
        PreProduceHandler preProduceHandler = new PreProduceHandler();
        ProducerMetricHandler metricHandler = mock(ProducerMetricHandler.class);
        doReturn(new ProducerMetricsEmitterNoOpImpl()).when(metricHandler).getEmitter(anyInt(), any());
        produceHandlers = new ProduceHandlers(
            producerService,
            preProduceHandler,
            metricHandler,
            MessageHeaderUtils.getTestConfiguration(filterNonCompliantHeaders),
            deployedRegion,
            projectCache
        );
        Multimap<String, String> copiedHeaders = produceHandlers.filterCompliantHeaders(headers);

        String[] values = copiedHeaders.get("x_multi_value1".toUpperCase()).toArray(new String[] {});
        Assertions.assertEquals(3, values.length);
        Assertions.assertEquals("multi_value1_2", values[0]);
        Assertions.assertEquals("multi_value1_1", values[1]);
        Assertions.assertEquals("multi_value1_3", values[2]);

        // Verify that "x_multi_value2" is always included with its 2 values
        values = copiedHeaders.get("x_multi_value2".toUpperCase()).toArray(new String[] {});
        Assertions.assertEquals(3, values.length);
        Assertions.assertEquals("multi_value2_1", values[0]);
        Assertions.assertEquals("multi_Value2_1", values[1]);
        Assertions.assertEquals("multi_Value2_1", values[2]);

        // Verify that "abc_multi_value1", "xyz_multi_value1", "aaa_multi_value1", and "bbb_multi_value1" are processed based on the filter setting
        List<String> headerKeys = Arrays.asList(
            "abc_multi_value1",
            "xyz_multi_value1",
            "aaa_multi_value1",
            "bbb_multi_value1"
        );

        headerKeys.forEach(headerKey -> {
            int expectedSize = filterNonCompliantHeaders ? 0 : 1;
            Assertions.assertEquals(
                expectedSize,
                copiedHeaders.get(headerKey.toUpperCase()).size(),
                "Header: " + headerKey + " was not processed as expected."
            );
        });
    }

    @Test
    public void testProduceWithHighHeaderKeySize() throws InterruptedException {
        String randomString = RandomString.make(OVERSIZED_HEADER_KEY_LENGTH);
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, topicPath);
        request.putHeader("X_MESSAGE_ID", randomString);
        request.putHeader(StdHeaders.get().httpUri(), "host1, host2");
        sendRequestWithPayload(
            request,
            payload,
            400,
            "X_MESSAGE_ID " + randomString + " exceeds allowed size of 100.",
            ErrorResponse.class
        );
    }

    @Test
    public void testProduceWithHighBodyAndHeaderSize() throws InterruptedException {
        String randomString = RandomString.make(99);
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, topicPath);
        request.putHeader("X_MESSAGE_ID", randomString);
        request.putHeader(StdHeaders.get().httpUri(), "host1, host2");

        // Create a body with a size greater than 5MB. 5MB = 5 * 1024 * 1024 bytes.
        byte[] largeBody = new byte[MAX_REQUEST_SIZE + 1]; // Byte array of size greater than 5MB
        Arrays.fill(largeBody, (byte)'A'); // Fill it with some data (e.g., 'A')

        sendRequestWithPayload(
            request,
            largeBody,
            400,
            "Request size exceeds allowed limit of 5242880 bytes.",
            ErrorResponse.class
        );
    }

}
