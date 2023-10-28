package com.flipkart.varadhi.web.produce;

import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.exceptions.ProduceException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.produce.services.ProducerService;
import com.flipkart.varadhi.services.ProjectService;
import com.flipkart.varadhi.spi.services.DummyProducer;
import com.flipkart.varadhi.web.ErrorResponse;
import com.flipkart.varadhi.web.WebTestBase;
import com.flipkart.varadhi.web.v1.produce.ProduceHandlers;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.client.HttpRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.flipkart.varadhi.MessageConstants.ANONYMOUS_PRODUCE_IDENTITY;
import static com.flipkart.varadhi.MessageConstants.Headers.*;
import static com.flipkart.varadhi.MessageConstants.PRODUCE_CHANNEL_HTTP;
import static com.flipkart.varadhi.entities.InternalTopic.TopicState.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;


public class ProduceHandlersTest extends WebTestBase {
    ProduceHandlers produceHandlers;
    ProducerService producerService;
    ProjectService projectService;
    String deployedRegion = "region1";

    ArgumentCaptor<Message> msgCapture;
    ArgumentCaptor<ProduceContext> ctxCapture;
    String topicPath = "/projects/project1/topics/topic1/produce";
    String topicFullName = "project1.topic1";
    String messageId;
    byte[] payload;


    @BeforeEach
    public void PreTest() throws InterruptedException {
        super.setUp();
        producerService = mock(ProducerService.class);
        projectService = mock(ProjectService.class);
        produceHandlers = new ProduceHandlers(deployedRegion, producerService, projectService);
        Project project = new Project("project1", 0, "description", "team1", "org1");
        doReturn(project).when(projectService).getCachedProject("project1");

        Route route = router.post("/projects/:project/topics/:topic/produce");
        route.handler(bodyHandler).handler(produceHandlers::produce);
        setupFailureHandler(route);
        msgCapture = ArgumentCaptor.forClass(Message.class);
        ctxCapture = ArgumentCaptor.forClass(ProduceContext.class);
        messageId = "messageId1";
        payload = "somerandomdata".getBytes();
    }

    @AfterEach
    public void PostTest() throws InterruptedException {
        super.tearDown();
    }

    @Test
    public void testProduceAndDuplicateMessage() throws InterruptedException {
        ProduceResult result = ProduceResult.onSuccess(messageId, new DummyProducer.DummyOffset(10), 10);
        doReturn(CompletableFuture.completedFuture(result)).when(producerService)
                .produceToTopic(msgCapture.capture(), eq(topicFullName), ctxCapture.capture());
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, topicPath);
        request.putHeader(MESSAGE_ID, messageId);
        request.putHeader(FORWARDED_FOR, "host1, host2");
        request.putHeader("RandomHeader", "value1");
        request.putHeader("x_header1", List.of("h1v1", "h1v2"));
        request.putHeader("X_HEADER2", "h2v1");
        long requestTimeStamp = System.currentTimeMillis();
        String messageIdObtained = sendRequestWithByteBufferBody(request, payload, String.class);
        Assertions.assertEquals(messageId, messageIdObtained);
        Message capturedMessage = msgCapture.getValue();
        REQUIRED_HEADERS.forEach(s -> Assertions.assertTrue(capturedMessage.hasHeader(s)));
        Assertions.assertArrayEquals(payload, msgCapture.getValue().getPayload());

        Assertions.assertFalse(capturedMessage.hasHeader("RandomHeader"));
        Assertions.assertTrue(capturedMessage.getHeaders("x_header1").contains("h1v1"));
        Assertions.assertTrue(capturedMessage.getHeaders("x_header1").contains("h1v2"));
        Assertions.assertTrue(capturedMessage.getHeaders("x_header2").contains("h2v1"));
        Assertions.assertFalse(capturedMessage.hasHeader("X_HEADER2"));

        Assertions.assertEquals("host1", ctxCapture.getValue().getRequestContext().getRemoteHost());
        Assertions.assertEquals(
                ANONYMOUS_PRODUCE_IDENTITY, ctxCapture.getValue().getRequestContext().getProduceIdentity());
        Assertions.assertEquals(PRODUCE_CHANNEL_HTTP, ctxCapture.getValue().getRequestContext().getRequestChannel());
        Assertions.assertTrue(requestTimeStamp <= ctxCapture.getValue().getRequestContext().getRequestTimestamp());
        Assertions.assertEquals(payload.length, ctxCapture.getValue().getRequestContext().getBytesReceived());


        Assertions.assertEquals(deployedRegion, ctxCapture.getValue().getTopicContext().getRegion());
        Assertions.assertEquals("topic1", ctxCapture.getValue().getTopicContext().getTopic());
        Assertions.assertEquals("project1", ctxCapture.getValue().getTopicContext().getProject());

        messageIdObtained = sendRequestWithByteBufferBody(request, payload, String.class);
        Assertions.assertEquals(messageId, messageIdObtained);
        verify(producerService, times(2)).produceToTopic(any(), eq(topicFullName), any());
    }


    @Test
    public void testProduceThrows() throws InterruptedException {
        String exceptionMessage = "Some random message.";
        doThrow(new ResourceNotFoundException(exceptionMessage)).when(producerService)
                .produceToTopic(any(), any(), any());

        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, topicPath);
        request.putHeader(MESSAGE_ID, messageId);
        sendRequestWithByteBufferBody(request, payload, 404, exceptionMessage, ErrorResponse.class);
    }

    @Test
    public void testProduceNonProducingTopicState() {
        record testData(int status, String message, InternalTopic.TopicState state) {
        }

        List<testData> data = List.of(
                new testData(422, "Topic is blocked. Unblock the topic before produce.", Blocked),
                new testData(429, "Produce to Topic is currently rate limited, try again after sometime.", Throttled),
                new testData(422, "Produce is not allowed for replicating topic.", Replicating)
        );

        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, topicPath);
        request.putHeader(MESSAGE_ID, messageId);

        data.forEach(d -> {
                    ProduceResult result = ProduceResult.onNonProducingTopicState(messageId, d.state);
                    doReturn(CompletableFuture.completedFuture(result)).when(producerService)
                            .produceToTopic(msgCapture.capture(), eq(topicFullName), ctxCapture.capture());
                    try {
                        sendRequestWithByteBufferBody(request, payload, d.status, d.message, ErrorResponse.class);
                    } catch (InterruptedException e) {
                        Assertions.fail("Unexpected Interruped Exception.");
                    }
                }
        );
    }

    @Test
    public void testProduceFailureResult() throws InterruptedException {

        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, topicPath);
        request.putHeader(MESSAGE_ID, messageId);
        String topicProduceFailureMsg = "Failure from messaging stack in ProduceAsync().";
        ProduceResult result = ProduceResult.onProducerFailure(messageId, 10, topicProduceFailureMsg);
        doReturn(CompletableFuture.completedFuture(result)).when(producerService)
                .produceToTopic(msgCapture.capture(), eq(topicFullName), ctxCapture.capture());
        sendRequestWithByteBufferBody(request, payload, 500,
                String.format("Produce failed at messaging stack: %s", topicProduceFailureMsg), ErrorResponse.class
        );
    }

    @Test
    public void testProduceUnexpectedFailure() throws InterruptedException {
        String exceptionMessage = "Failure from Producer Service.";
        doReturn(CompletableFuture.failedFuture(new ResourceNotFoundException(exceptionMessage))).when(producerService)
                .produceToTopic(msgCapture.capture(), eq(topicFullName), ctxCapture.capture());
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, topicPath);
        request.putHeader(MESSAGE_ID, messageId);
        sendRequestWithByteBufferBody(request, payload, 404, exceptionMessage, ErrorResponse.class);

        doReturn(CompletableFuture.failedFuture(new RuntimeException(exceptionMessage))).when(producerService)
                .produceToTopic(msgCapture.capture(), eq(topicFullName), ctxCapture.capture());
        sendRequestWithByteBufferBody(request, payload, 500, exceptionMessage, ErrorResponse.class);
    }

    @Test
    public void testProduceProduceException() throws InterruptedException {
        String exceptionMessage = "Failed to Produce.";
        doThrow(new ProduceException(exceptionMessage)).when(producerService).produceToTopic(any(), any(), any());

        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, topicPath);
        request.putHeader(MESSAGE_ID, messageId);
        sendRequestWithByteBufferBody(request, payload, 500, exceptionMessage, ErrorResponse.class);
    }

    @Test
    public void testProduceHeaderOrdering() throws InterruptedException {
        ProduceResult result = ProduceResult.onSuccess(messageId, new DummyProducer.DummyOffset(10), 10);
        doReturn(CompletableFuture.completedFuture(result)).when(producerService)
                .produceToTopic(msgCapture.capture(), eq(topicFullName), ctxCapture.capture());
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, topicPath);
        request.putHeader(MESSAGE_ID, messageId);
        request.putHeader(FORWARDED_FOR, "host1, host2");
        request.putHeader("RandomHeader", "value1");
        MultiMap multimap = new HeadersMultiMap();
        multimap.add("x_header1", "h1v1");
        multimap.add("X_HEADER1", "h1v2");
        multimap.add("x_header1", "h1v3");
        request.putHeaders(multimap);
        request.putHeader("X_HEADER2", "h2v1");
        String messageIdObtained = sendRequestWithByteBufferBody(request, payload, String.class);
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
        request.putHeader(MESSAGE_ID, messageId);
        ProduceResult result = ProduceResult.onSuccess(messageId, new DummyProducer.DummyOffset(10), 10);
        doReturn(CompletableFuture.completedFuture(result)).when(producerService)
                .produceToTopic(msgCapture.capture(), eq(topicFullName), ctxCapture.capture());
        sendRequestWithByteBufferBody(request, payload, 404, "Project1 not found.", ErrorResponse.class
        );
    }
}
