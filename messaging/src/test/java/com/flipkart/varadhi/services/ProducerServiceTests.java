package com.flipkart.varadhi.services;

import com.flipkart.varadhi.core.VaradhiTopicService;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.exceptions.ProduceException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.produce.ProduceResult;
import com.flipkart.varadhi.produce.config.ProducerOptions;
import com.flipkart.varadhi.produce.otel.ProducerMetricsImpl;
import com.flipkart.varadhi.produce.services.ProducerService;
import com.flipkart.varadhi.spi.services.DummyProducer;
import com.flipkart.varadhi.spi.services.Producer;
import com.flipkart.varadhi.spi.services.ProducerFactory;
import com.flipkart.varadhi.utils.JsonMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static com.flipkart.varadhi.MessageConstants.ANONYMOUS_PRODUCE_IDENTITY;
import static com.flipkart.varadhi.MessageConstants.PRODUCE_CHANNEL_HTTP;
import static com.flipkart.varadhi.entities.StandardHeaders.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class ProducerServiceTests {
    ProducerService service;
    ProducerMetricsImpl metricProvider;
    ProducerFactory producerFactory;
    VaradhiTopicService topicService;
    Producer producer;
    Random random;
    String topic = "topic1";
    Project project = new Project("project1", 0, "", "team1", "org1");
    String region = "region1";

    @BeforeEach
    public void preTest() {
        producerFactory = mock(ProducerFactory.class);
        topicService = mock(VaradhiTopicService.class);
        MeterRegistry registry = new OtlpMeterRegistry();
        metricProvider = spy(new ProducerMetricsImpl(registry));
        service = new ProducerService(new ProducerOptions(), producerFactory, metricProvider, topicService, registry);
        random = new Random();
        producer = spy(new DummyProducer(JsonMapper.getMapper()));
    }

    @Test
    public void testProduceMessage() throws InterruptedException {
        ProduceContext ctx = getProduceContext(topic, project, region);
        Message msg1 = getMessage(0, 1, null, 10, ctx);
        VaradhiTopic vt = getTopic(topic, project, region);
        doReturn(vt).when(topicService).get(vt.getName());
        doReturn(producer).when(producerFactory).getProducer(any());
        CompletableFuture<ProduceResult> result =
                service.produceToTopic(msg1, VaradhiTopic.buildTopicName(project.getName(), topic), ctx);
        ResultCapture rc = getResult(result);
        Assertions.assertNotNull(rc.produceResult);
        Assertions.assertNull(rc.throwable);
        verify(producer, times(1)).produceAsync(eq(msg1));

        Message msg2 = getMessage(100, 1, null, 2000, ctx);
        result = service.produceToTopic(msg2, VaradhiTopic.buildTopicName(project.getName(), topic), ctx);
        rc = getResult(result);
        Assertions.assertNotNull(rc.produceResult);
        Assertions.assertNull(rc.throwable);
        verify(producer, times(1)).produceAsync(msg2);
        verify(producerFactory, times(1)).getProducer(any());
        verify(topicService, times(1)).get(vt.getName());
    }

    @Test
    public void testProduceWhenProduceAsyncThrows() {
        ProduceContext ctx = getProduceContext(topic, project, region);
        Message msg1 = getMessage(0, 1, null, 10, ctx);
        VaradhiTopic vt = getTopic(topic, project, region);
        doReturn(vt).when(topicService).get(vt.getName());
        doReturn(producer).when(producerFactory).getProducer(any());
        doThrow(new RuntimeException("Some random error.")).when(producer).produceAsync(msg1);
        // This is testing Producer.ProduceAsync(), throwing an exception which is handled in produce service.
        // This is not expected in general.
        ProduceException pe = Assertions.assertThrows(
                ProduceException.class,
                () -> service.produceToTopic(msg1, VaradhiTopic.buildTopicName(project.getName(), topic), ctx)
        );
        Assertions.assertEquals("Produce failed due to internal error: Some random error.", pe.getMessage());
        verify(metricProvider, never()).onMessageProduced(anyBoolean(), anyLong(), any());
    }

    @Test
    public void testProduceToNonExistingTopic() {
        ProduceContext ctx = getProduceContext(topic, project, region);
        Message msg1 = getMessage(0, 1, null, 0, ctx);
        VaradhiTopic vt = getTopic(topic, project, region);
        doReturn(producer).when(producerFactory).getProducer(any());
        doThrow(new ResourceNotFoundException("Topic doesn't exists.")).when(topicService).get(vt.getName());
        Assertions.assertThrows(
                ResourceNotFoundException.class,
                () -> service.produceToTopic(msg1, VaradhiTopic.buildTopicName(project.getName(), topic), ctx)
        );
        verify(producer, never()).produceAsync(any());
    }

    @Test
    public void testProduceWithUnknownExceptionInGetTopic() {
        ProduceContext ctx = getProduceContext(topic, project, region);
        Message msg1 = getMessage(0, 1, null, 0, ctx);
        VaradhiTopic vt = getTopic(topic, project, region);
        doReturn(producer).when(producerFactory).getProducer(any());
        doThrow(new RuntimeException("Unknown error.")).when(topicService).get(vt.getName());
        ProduceException e = Assertions.assertThrows(
                ProduceException.class,
                () -> service.produceToTopic(msg1, VaradhiTopic.buildTopicName(project.getName(), topic), ctx)
        );
        Assertions.assertEquals(
                "Failed to get produce Topic(project1.topic1). Unknown error.", e.getMessage());
        verify(producer, never()).produceAsync(any());
    }

    @Test
    public void produceToBlockedTopic() throws InterruptedException {
        produceNotAllowedTopicState(
                TopicState.Blocked,
                ProduceStatus.Blocked,
                "Topic/Queue is blocked. Unblock the Topic/Queue before produce."
        );
    }

    @Test
    public void produceToThrottledTopic() throws InterruptedException {
        produceNotAllowedTopicState(
                TopicState.Throttled,
                ProduceStatus.Throttled,
                "Produce to Topic/Queue is currently rate limited, try again after sometime."
        );
    }

    @Test
    public void produceToReplicatingTopic() throws InterruptedException {
        produceNotAllowedTopicState(
                TopicState.Replicating,
                ProduceStatus.NotAllowed,
                "Produce is not allowed for replicating Topic/Queue."
        );
    }

    public void produceNotAllowedTopicState(
            TopicState topicState, ProduceStatus produceStatus, String message
    ) throws InterruptedException {
        ProduceContext ctx = getProduceContext(topic, project, region);
        Message msg1 = getMessage(0, 1, null, 0, ctx);
        VaradhiTopic vt = getTopic(topicState, topic, project, region);
        doReturn(vt).when(topicService).get(vt.getName());
        doReturn(producer).when(producerFactory).getProducer(any());
        CompletableFuture<ProduceResult> result =
                service.produceToTopic(msg1, VaradhiTopic.buildTopicName(project.getName(), topic), ctx);
        ResultCapture rc = getResult(result);
        Assertions.assertNotNull(rc.produceResult);
        Assertions.assertNull(rc.throwable);
        Assertions.assertEquals(produceStatus, rc.produceResult.getProduceStatus());
        Assertions.assertEquals(message, rc.produceResult.getFailureReason());
        verify(producer, never()).produceAsync(any());
    }

    @Test
    public void testProduceWithUnknownExceptionInGetProducer() {
        ProduceContext ctx = getProduceContext(topic, project, region);
        Message msg1 = getMessage(0, 1, null, 0, ctx);
        VaradhiTopic vt = getTopic(topic, project, region);
        doReturn(vt).when(topicService).get(vt.getName());
        doThrow(new RuntimeException("Unknown Error.")).when(producerFactory).getProducer(any());
        ProduceException pe = Assertions.assertThrows(
                ProduceException.class,
                () -> service.produceToTopic(msg1, VaradhiTopic.buildTopicName(project.getName(), topic), ctx)
        );
        Assertions.assertEquals(
                "Failed to create Pulsar producer for Topic(project1.topic1). Unknown Error.", pe.getMessage());
    }

    @Test
    public void testProduceWithknownExceptionInGetProducer() {
        ProduceContext ctx = getProduceContext(topic, project, region);
        Message msg1 = getMessage(0, 1, null, 0, ctx);
        VaradhiTopic vt = getTopic(topic, project, region);
        doReturn(vt).when(topicService).get(vt.getName());
        doThrow(new RuntimeException("Topic doesn't exists.")).when(producerFactory).getProducer(any());
        RuntimeException re = Assertions.assertThrows(
                RuntimeException.class,
                () -> service.produceToTopic(msg1, VaradhiTopic.buildTopicName(project.getName(), topic), ctx)
        );
        verify(producer, never()).produceAsync(any());
        Assertions.assertEquals(
                "Failed to create Pulsar producer for Topic(project1.topic1). Topic doesn't exists.", re.getMessage());
    }

    @Test
    public void testProduceWithProducerFailure() throws InterruptedException {
        ProduceContext ctx = getProduceContext(topic, project, region);
        Message msg1 = getMessage(0, 1, UnsupportedOperationException.class.getName(), 0, ctx);
        VaradhiTopic vt = getTopic(topic, project, region);
        doReturn(vt).when(topicService).get(vt.getName());
        doReturn(producer).when(producerFactory).getProducer(any());

        CompletableFuture<ProduceResult> result =
                service.produceToTopic(msg1, VaradhiTopic.buildTopicName(project.getName(), topic), ctx);

        ResultCapture rc = getResult(result);
        Assertions.assertNotNull(rc.produceResult);
        Assertions.assertNull(rc.throwable);
        Assertions.assertEquals(
                ProduceStatus.Failed, rc.produceResult.getProduceStatus());
        Assertions.assertEquals(
                "Produce failure from messaging stack for Topic/Queue. null", rc.produceResult.getFailureReason()
        );
        verify(producerFactory, times(1)).getProducer(any());
    }


    @Test
    public void testMetricEmitFailureNotIgnored() throws InterruptedException {
        doThrow(new RuntimeException("Failed to send metric.")).when(metricProvider)
                .onMessageProduced(anyBoolean(), anyLong(), any());
        ProduceContext ctx = getProduceContext(topic, project, region);
        Message msg1 = getMessage(0, 1, null, 10, ctx);
        VaradhiTopic vt = getTopic(topic, project, region);
        doReturn(vt).when(topicService).get(vt.getName());
        doReturn(producer).when(producerFactory).getProducer(any());
        CompletableFuture<ProduceResult> result =
                service.produceToTopic(msg1, VaradhiTopic.buildTopicName(project.getName(), topic), ctx);
        ResultCapture rc = getResult(result);
        Assertions.assertNull(rc.produceResult);
        Assertions.assertNotNull(rc.throwable);
        verify(producer, times(1)).produceAsync(eq(msg1));
        verify(metricProvider, times(1)).onMessageProduced(anyBoolean(), anyLong(), any());
        // Exception gets wrapped in CompletionException.
        Assertions.assertEquals("Failed to send metric.", rc.throwable.getCause().getMessage());
    }

    public VaradhiTopic getTopic(String name, Project project, String region) {
        return getTopic(TopicState.Producing, name, project, region);
    }

    public VaradhiTopic getTopic(TopicState state, String name, Project project, String region) {
        VaradhiTopic topic = VaradhiTopic.of(new TopicResource(name, 0, project.getName(), false, null));
        StorageTopic st = new DummyStorageTopic(topic.getName(), 0);
        topic.addInternalTopic(new InternalCompositeTopic(region, state, st));
        return topic;
    }

    public Message getMessage(
            int sleepMs, int offset, String exceptionClass, int payloadSize, ProduceContext ctx
    ) {
        Multimap<String, String> headers = ArrayListMultimap.create();
        byte[] messageId = new byte[30];
        random.nextBytes(messageId);
        headers.put(MESSAGE_ID, new String(messageId));
        headers.put(PRODUCE_IDENTITY, ctx.getRequestContext().getProduceIdentity());
        headers.put(PRODUCE_REGION, ctx.getTopicContext().getRegion());
        headers.put(PRODUCE_TIMESTAMP, Long.toString(ctx.getRequestContext().getRequestTimestamp()));
        byte[] payload = null;
        if (payloadSize > 0) {
            payload = new byte[payloadSize];
            random.nextBytes(payload);
        }
        DummyProducer.DummyMessage message =
                new DummyProducer.DummyMessage(sleepMs, offset, exceptionClass, payload);
        return new Message(JsonMapper.jsonSerialize(message).getBytes(), headers);
    }


    public ProduceContext getProduceContext(String topic, Project project, String region) {
        ProduceContext.RequestContext requestContext = new ProduceContext.RequestContext();
        requestContext.setRequestTimestamp(System.currentTimeMillis());
        requestContext.setBytesReceived(100);
        requestContext.setProduceIdentity(ANONYMOUS_PRODUCE_IDENTITY);
        requestContext.setRemoteHost("remotehost");
        requestContext.setServiceHost("localhost");
        requestContext.setRequestChannel(PRODUCE_CHANNEL_HTTP);
        ProduceContext.TopicContext topicContext = new ProduceContext.TopicContext();
        topicContext.setTopic(topic);
        topicContext.setRegion(region);
        topicContext.setProjectAttributes(project);
        return new ProduceContext(requestContext, topicContext);
    }

    ResultCapture getResult(CompletableFuture<ProduceResult> future) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ResultCapture rc = new ResultCapture();
        future.whenComplete((pr, e) -> {
            rc.produceResult = pr;
            rc.throwable = e;
            latch.countDown();
        });
        latch.await();
        return rc;
    }

    static class ResultCapture {
        ProduceResult produceResult;
        Throwable throwable;
    }

    public static class DummyStorageTopic extends StorageTopic {
        public DummyStorageTopic(String name, int version) {
            super(name, version);
        }
    }
}
