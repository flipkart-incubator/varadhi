package com.flipkart.varadhi.services;

import com.flipkart.varadhi.Constants;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.exceptions.ProduceException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.produce.ProduceResult;
import com.flipkart.varadhi.produce.config.ProducerOptions;
import com.flipkart.varadhi.produce.otel.ProducerMetricsEmitter;
import com.flipkart.varadhi.produce.otel.ProducerMetricsEmitterImpl;
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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static com.flipkart.varadhi.Constants.Tags.*;
import static com.flipkart.varadhi.MessageConstants.ANONYMOUS_IDENTITY;
import static com.flipkart.varadhi.entities.StandardHeaders.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class ProducerServiceTests {
    ProducerService service;
    ProducerFactory<StorageTopic> producerFactory;
    MeterRegistry meterRegistry;
    TopicProvider topicProvider;
    Producer producer;
    Random random;
    String topic = "topic1";
    Project project = Project.of("project1", "", "team1", "org1");
    String region = "region1";

    @BeforeEach
    public void preTest() {
        producerFactory = mock(ProducerFactory.class);
        topicProvider = mock(TopicProvider.class);
        meterRegistry = new OtlpMeterRegistry();

        service = new ProducerService(
            region,
            new ProducerOptions(),
            producerFactory::newProducer,
            topicProvider::get,
            meterRegistry
        );
        random = new Random();
        producer = spy(new DummyProducer(JsonMapper.getMapper()));

    }

    @Test
    public void testProduceMessage() throws InterruptedException {
        ProducerMetricsEmitter emitter = getMetricEmitter(topic, project, region);
        Message msg1 = getMessage(0, 1, null, 10);
        VaradhiTopic vt = getTopic(topic, project, region);
        doReturn(vt).when(topicProvider).get(vt.getName());
        doReturn(producer).when(producerFactory).newProducer(any());
        CompletableFuture<ProduceResult> result = service.produceToTopic(
            msg1,
            VaradhiTopic.buildTopicName(project.getName(), topic),
            emitter
        );
        ResultCapture rc = getResult(result);
        Assertions.assertNotNull(rc.produceResult);
        Assertions.assertNull(rc.throwable);
        verify(producer, times(1)).produceAsync(eq(msg1));

        Message msg2 = getMessage(100, 1, null, 2000);
        result = service.produceToTopic(msg2, VaradhiTopic.buildTopicName(project.getName(), topic), emitter);
        rc = getResult(result);
        Assertions.assertNotNull(rc.produceResult);
        Assertions.assertNull(rc.throwable);
        verify(producer, times(1)).produceAsync(msg2);
        verify(producerFactory, times(1)).newProducer(any());
        verify(topicProvider, times(1)).get(vt.getName());
    }

    @Test
    public void testProduceWhenProduceAsyncThrows() {
        ProducerMetricsEmitter emitter = mock(ProducerMetricsEmitter.class);
        Message msg1 = getMessage(0, 1, null, 10);
        VaradhiTopic vt = getTopic(topic, project, region);
        doReturn(vt).when(topicProvider).get(vt.getName());
        doReturn(producer).when(producerFactory).newProducer(any());
        doThrow(new RuntimeException("Some random error.")).when(producer).produceAsync(msg1);
        // This is testing Producer.ProduceAsync(), throwing an exception which is handled in produce service.
        // This is not expected in general.
        ProduceException pe = Assertions.assertThrows(
            ProduceException.class,
            () -> service.produceToTopic(msg1, VaradhiTopic.buildTopicName(project.getName(), topic), emitter)
        );
        Assertions.assertEquals("Produce failed due to internal error: Some random error.", pe.getMessage());
        verify(emitter, never()).emit(anyBoolean(), anyLong());
    }

    @Test
    public void testProduceToNonExistingTopic() {
        ProducerMetricsEmitter emitter = getMetricEmitter(topic, project, region);
        Message msg1 = getMessage(0, 1, null, 0);
        VaradhiTopic vt = getTopic(topic, project, region);
        doReturn(producer).when(producerFactory).newProducer(any());
        doThrow(new ResourceNotFoundException("Topic doesn't exists.")).when(topicProvider).get(vt.getName());
        Assertions.assertThrows(
            ResourceNotFoundException.class,
            () -> service.produceToTopic(msg1, VaradhiTopic.buildTopicName(project.getName(), topic), emitter)
        );
        verify(producer, never()).produceAsync(any());
    }

    @Test
    public void testProduceWithUnknownExceptionInGetTopic() {
        ProducerMetricsEmitter emitter = getMetricEmitter(topic, project, region);
        Message msg1 = getMessage(0, 1, null, 0);
        VaradhiTopic vt = getTopic(topic, project, region);
        doReturn(producer).when(producerFactory).newProducer(any());
        doThrow(new RuntimeException("Unknown error.")).when(topicProvider).get(vt.getName());
        ProduceException e = Assertions.assertThrows(
            ProduceException.class,
            () -> service.produceToTopic(msg1, VaradhiTopic.buildTopicName(project.getName(), topic), emitter)
        );
        Assertions.assertEquals("Failed to get produce Topic(project1.topic1). Unknown error.", e.getMessage());
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

    public void produceNotAllowedTopicState(TopicState topicState, ProduceStatus produceStatus, String message)
        throws InterruptedException {
        ProducerMetricsEmitter emitter = getMetricEmitter(topic, project, region);
        Message msg1 = getMessage(0, 1, null, 0);
        VaradhiTopic vt = getTopic(topicState, topic, project, region);
        doReturn(vt).when(topicProvider).get(vt.getName());
        doReturn(producer).when(producerFactory).newProducer(any());
        CompletableFuture<ProduceResult> result = service.produceToTopic(
            msg1,
            VaradhiTopic.buildTopicName(project.getName(), topic),
            emitter
        );
        ResultCapture rc = getResult(result);
        Assertions.assertNotNull(rc.produceResult);
        Assertions.assertNull(rc.throwable);
        Assertions.assertEquals(produceStatus, rc.produceResult.getProduceStatus());
        Assertions.assertEquals(message, rc.produceResult.getFailureReason());
        verify(producer, never()).produceAsync(any());
    }

    @Test
    public void testProduceWithUnknownExceptionInGetProducer() {
        ProducerMetricsEmitter emitter = getMetricEmitter(topic, project, region);
        Message msg1 = getMessage(0, 1, null, 0);
        VaradhiTopic vt = getTopic(topic, project, region);
        doReturn(vt).when(topicProvider).get(vt.getName());
        doThrow(new RuntimeException("Unknown Error.")).when(producerFactory).newProducer(any());
        ProduceException pe = Assertions.assertThrows(
            ProduceException.class,
            () -> service.produceToTopic(msg1, VaradhiTopic.buildTopicName(project.getName(), topic), emitter)
        );
        Assertions.assertEquals(
            "Failed to create Pulsar producer for Topic(project1.topic1). Unknown Error.",
            pe.getMessage()
        );
    }

    @Test
    public void testProduceWithKnownExceptionInGetProducer() {
        ProducerMetricsEmitter emitter = getMetricEmitter(topic, project, region);
        Message msg1 = getMessage(0, 1, null, 0);
        VaradhiTopic vt = getTopic(topic, project, region);
        doReturn(vt).when(topicProvider).get(vt.getName());
        doThrow(new RuntimeException("Topic doesn't exists.")).when(producerFactory).newProducer(any());
        RuntimeException re = Assertions.assertThrows(
            RuntimeException.class,
            () -> service.produceToTopic(msg1, VaradhiTopic.buildTopicName(project.getName(), topic), emitter)
        );
        verify(producer, never()).produceAsync(any());
        Assertions.assertEquals(
            "Failed to create Pulsar producer for Topic(project1.topic1). Topic doesn't exists.",
            re.getMessage()
        );
    }

    @Test
    public void testProduceWithProducerFailure() throws InterruptedException {
        ProducerMetricsEmitter emitter = getMetricEmitter(topic, project, region);
        Message msg1 = getMessage(0, 1, UnsupportedOperationException.class.getName(), 0);
        VaradhiTopic vt = getTopic(topic, project, region);
        doReturn(vt).when(topicProvider).get(vt.getName());
        doReturn(producer).when(producerFactory).newProducer(any());

        CompletableFuture<ProduceResult> result = service.produceToTopic(
            msg1,
            VaradhiTopic.buildTopicName(project.getName(), topic),
            emitter
        );

        ResultCapture rc = getResult(result);
        Assertions.assertNotNull(rc.produceResult);
        Assertions.assertNull(rc.throwable);
        Assertions.assertEquals(ProduceStatus.Failed, rc.produceResult.getProduceStatus());
        Assertions.assertEquals(
            "Produce failure from messaging stack for Topic/Queue. null",
            rc.produceResult.getFailureReason()
        );
        verify(producerFactory, times(1)).newProducer(any());
    }

    @Test
    public void testMetricEmitFailureNotIgnored() throws InterruptedException {
        ProducerMetricsEmitter emitter = mock(ProducerMetricsEmitter.class);
        doThrow(new RuntimeException("Failed to send metric.")).when(emitter).emit(anyBoolean(), anyLong());
        Message msg1 = getMessage(0, 1, null, 10);
        VaradhiTopic vt = getTopic(topic, project, region);
        doReturn(vt).when(topicProvider).get(vt.getName());
        doReturn(producer).when(producerFactory).newProducer(any());
        CompletableFuture<ProduceResult> result = service.produceToTopic(
            msg1,
            VaradhiTopic.buildTopicName(project.getName(), topic),
            emitter
        );
        ResultCapture rc = getResult(result);
        Assertions.assertNull(rc.produceResult);
        Assertions.assertNotNull(rc.throwable);
        verify(producer, times(1)).produceAsync(eq(msg1));
        verify(emitter, times(1)).emit(anyBoolean(), anyLong());
        // Exception gets wrapped in CompletionException.
        Assertions.assertEquals("Failed to send metric.", rc.throwable.getCause().getMessage());
    }

    public VaradhiTopic getTopic(String name, Project project, String region) {
        return getTopic(TopicState.Producing, name, project, region);
    }

    public VaradhiTopic getTopic(TopicState state, String name, Project project, String region) {
        VaradhiTopic topic = VaradhiTopic.of(
            project.getName(),
            name,
            false,
            null,
            LifecycleStatus.ActorCode.SYSTEM_ACTION
        );
        StorageTopic st = new DummyStorageTopic(topic.getName(), 0);
        InternalCompositeTopic ict = InternalCompositeTopic.of(st);
        ict.setTopicState(state);
        topic.addInternalTopic(region, ict);
        return topic;
    }

    public Message getMessage(int sleepMs, int offset, String exceptionClass, int payloadSize) {
        Multimap<String, String> headers = ArrayListMultimap.create();
        headers.put(StandardHeaders.MESSAGE_ID, getMessageId());
        headers.put(PRODUCE_IDENTITY, ANONYMOUS_IDENTITY);
        headers.put(PRODUCE_REGION, region);
        headers.put(PRODUCE_TIMESTAMP, System.currentTimeMillis() + "");
        byte[] payload = null;
        if (payloadSize > 0) {
            payload = new byte[payloadSize];
            random.nextBytes(payload);
        }
        DummyProducer.DummyMessage message = new DummyProducer.DummyMessage(sleepMs, offset, exceptionClass, payload);
        return new ProducerMessage(JsonMapper.jsonSerialize(message).getBytes(), headers);
    }

    public ProducerMetricsEmitter getMetricEmitter(String topic, Project project, String region) {
        Map<String, String> produceAttributes = new HashMap<>();
        produceAttributes.put(TAG_REGION, region);
        produceAttributes.put(TAG_ORG, project.getOrg());
        produceAttributes.put(TAG_TEAM, project.getTeam());
        produceAttributes.put(TAG_PROJECT, project.getName());
        produceAttributes.put(TAG_TOPIC, topic);
        produceAttributes.put(TAG_IDENTITY, ANONYMOUS_IDENTITY);
        produceAttributes.put(TAG_REMOTE_HOST, "remoteHost");
        return new ProducerMetricsEmitterImpl(meterRegistry, 0, produceAttributes);
    }

    public String getMessageId() {
        byte[] messageId = new byte[30];
        random.nextBytes(messageId);
        return new String(messageId);
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

    public static class TopicProvider {
        public VaradhiTopic get(String topicName) {
            return null;
        }
    }


    static class ResultCapture {
        ProduceResult produceResult;
        Throwable throwable;
    }


    public static class DummyStorageTopic extends StorageTopic {
        public DummyStorageTopic(String name, int version) {
            super(name, version, Constants.DEFAULT_TOPIC_CAPACITY);
        }
    }
}
