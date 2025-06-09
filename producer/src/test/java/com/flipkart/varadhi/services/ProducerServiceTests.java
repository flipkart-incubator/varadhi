package com.flipkart.varadhi.services;

import com.flipkart.varadhi.common.ResourceReadCache;
import com.flipkart.varadhi.common.SimpleMessage;
import com.flipkart.varadhi.common.exceptions.ProduceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.JsonMapper;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.produce.ProduceResult;
import com.flipkart.varadhi.produce.config.ProducerOptions;
import com.flipkart.varadhi.produce.telemetry.ProducerMetrics;
import com.flipkart.varadhi.produce.ProducerService;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.services.DummyProducer;
import com.flipkart.varadhi.spi.services.Producer;
import com.flipkart.varadhi.spi.services.ProducerFactory;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ProducerServiceTests {
    ProducerService service;
    ProducerFactory<StorageTopic> producerFactory;
    MeterRegistry meterRegistry;
    MetaStore metaStore;
    Producer producer;
    ResourceReadCache<Resource.EntityResource<Project>> projectCache;
    ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicReadCache;
    ResourceReadCache<OrgDetails> orgCache;
    Random random;
    String topic = "topic1";
    Project project = Project.of("project1", "", "team1", "org1");
    String region = "region1";

    @BeforeAll
    static void setup() {
        StdHeaders.init(TestStdHeaders.get());
    }

    @BeforeEach
    void preTest() {
        producerFactory = mock(ProducerFactory.class);
        meterRegistry = new OtlpMeterRegistry();
        metaStore = mock(MetaStore.class);
        topicReadCache = mock(ResourceReadCache.class);
        projectCache = mock(ResourceReadCache.class);
        orgCache = mock(ResourceReadCache.class);
        producer = spy(new DummyProducer(JsonMapper.getMapper()));
        when(producerFactory.newProducer(any(), any())).thenReturn(producer);

        service = new ProducerService(region, producerFactory::newProducer, orgCache, projectCache, topicReadCache);
        random = new Random();
    }

    @Test
    void testProduceMessage() throws InterruptedException {
        Message msg1 = getMessage(0, 1, null, 10);
        Resource.EntityResource<VaradhiTopic> vt = getTopic(topic, project, region);

        when(topicReadCache.get(any())).thenReturn(Optional.of(vt));

        doReturn(producer).when(producerFactory).newProducer(any(), any());
        CompletableFuture<ProduceResult> result = service.produceToTopic(
            msg1,
            VaradhiTopic.fqn(project.getName(), topic)
        );
        ResultCapture rc = getResult(result);
        Assertions.assertNotNull(rc.produceResult);
        Assertions.assertNull(rc.throwable);
        verify(producer, times(1)).produceAsync(eq(msg1));

        Message msg2 = getMessage(100, 1, null, 2000);
        result = service.produceToTopic(msg2, VaradhiTopic.fqn(project.getName(), topic));
        rc = getResult(result);
        Assertions.assertNotNull(rc.produceResult);
        Assertions.assertNull(rc.throwable);
        verify(producer, times(1)).produceAsync(msg2);
        verify(producerFactory, times(1)).newProducer(any(), any());
        verify(topicReadCache, times(3)).get(vt.getName());
    }

    @Test
    void testProduceWhenProduceAsyncThrows() {
        ProducerMetrics emitter = mock(ProducerMetrics.class);
        Message msg1 = getMessage(0, 1, null, 10);
        Resource.EntityResource<VaradhiTopic> vt = getTopic(topic, project, region);
        when(topicReadCache.get(vt.getName())).thenReturn(Optional.of(vt));
        doReturn(producer).when(producerFactory).newProducer(any(), any());
        doThrow(new RuntimeException("Some random error.")).when(producer).produceAsync(msg1);
        // This is testing Producer.ProduceAsync(), throwing an exception which is handled in produce service.
        // This is not expected in general.
        CompletableFuture<ProduceResult> future = service.produceToTopic(
            msg1,
            VaradhiTopic.fqn(project.getName(), topic)
        );
        CompletionException exception = Assertions.assertThrows(CompletionException.class, future::join);
        assertTrue(exception.getCause() instanceof RuntimeException);
        Assertions.assertEquals("Some random error.", exception.getCause().getMessage());
        //        verify(emitter, never()).emit(anyBoolean(), anyLong(), anyLong(), anyInt(), anyBoolean(), any());
    }

    @Test
    void testProduceToNonExistingTopic() {
        Message msg1 = getMessage(0, 1, null, 0);
        String topicName = VaradhiTopic.fqn(project.getName(), topic);
        doReturn(producer).when(producerFactory).newProducer(any(), any());
        when(topicReadCache.get(topicName)).thenReturn(Optional.empty());
        ResourceNotFoundException ex = Assertions.assertThrows(
            ResourceNotFoundException.class,
            () -> service.produceToTopic(msg1, topicName)
        );

        Assertions.assertEquals("Topic(project1.topic1) does not exist", ex.getMessage());
        verify(producer, never()).produceAsync(any());
    }

    @Test
    void produceToBlockedTopic() throws InterruptedException {
        produceNotAllowedTopicState(
            TopicState.Blocked,
            ProduceStatus.Blocked,
            "Topic/Queue is blocked. Unblock the Topic/Queue before produce."
        );
    }

    @Test
    void produceToThrottledTopic() throws InterruptedException {
        produceNotAllowedTopicState(
            TopicState.Throttled,
            ProduceStatus.Throttled,
            "Produce to Topic/Queue is currently rate limited, try again after sometime."
        );
    }

    @Test
    void produceToReplicatingTopic() throws InterruptedException {
        produceNotAllowedTopicState(
            TopicState.Replicating,
            ProduceStatus.NotAllowed,
            "Produce is not allowed for replicating Topic/Queue."
        );
    }

    public void produceNotAllowedTopicState(TopicState topicState, ProduceStatus produceStatus, String message)
        throws InterruptedException {
        Message msg1 = getMessage(0, 1, null, 0);
        VaradhiTopic vt = getTopic(topicState, topic, project, region);
        when(topicReadCache.get(vt.getName())).thenReturn(Optional.of(Resource.of(vt, ResourceType.TOPIC)));
        doReturn(producer).when(producerFactory).newProducer(any(), any());
        CompletableFuture<ProduceResult> result = service.produceToTopic(
            msg1,
            VaradhiTopic.fqn(project.getName(), topic)
        );
        ResultCapture rc = getResult(result);
        Assertions.assertNotNull(rc.produceResult);
        Assertions.assertNull(rc.throwable);
        Assertions.assertEquals(produceStatus, rc.produceResult.getProduceStatus());
        Assertions.assertEquals(message, rc.produceResult.getFailureReason());
        verify(producer, never()).produceAsync(any());
    }

    @Test
    void testProduceWithUnknownExceptionInGetProducer() {
        Message msg1 = getMessage(0, 1, null, 0);
        Resource.EntityResource<VaradhiTopic> vt = getTopic(topic, project, region);
        when(topicReadCache.get(vt.getName())).thenReturn(Optional.of(vt));
        ProducerFactory<StorageTopic> failingProducerProvider = (st, c) -> {
            throw new RuntimeException("Unknown Error.");
        };
        ProducerService failingService = new ProducerService(
            region,
            failingProducerProvider,
            orgCache,
            projectCache,
            topicReadCache
        );
        CompletableFuture<ProduceResult> future = failingService.produceToTopic(
            msg1,
            VaradhiTopic.fqn(project.getName(), topic)
        );
        CompletionException exception = Assertions.assertThrows(CompletionException.class, future::join);
        assertTrue(exception.getCause() instanceof ProduceException);
        assertTrue(exception.getCause().getMessage().contains("Error getting producer for Topic(project1.topic1)"));
        assertTrue(exception.getCause().getMessage().contains("Unknown Error."));
    }

    @Test
    void testProduceWithKnownExceptionInGetProducer() {
        Message msg1 = getMessage(0, 1, null, 0);
        Resource.EntityResource<VaradhiTopic> vt = getTopic(topic, project, region);
        when(topicReadCache.get(vt.getName())).thenReturn(Optional.of(vt));
        ProducerFactory<StorageTopic> failingProducerProvider = (st, c) -> {
            throw new RuntimeException("Topic doesn't exist.");
        };
        ProducerService failingService = new ProducerService(
            region,
            failingProducerProvider,
            orgCache,
            projectCache,
            topicReadCache
        );
        CompletableFuture<ProduceResult> future = failingService.produceToTopic(
            msg1,
            VaradhiTopic.fqn(project.getName(), topic)
        );
        CompletionException exception = Assertions.assertThrows(CompletionException.class, future::join);
        verify(producer, never()).produceAsync(any());
        assertTrue(exception.getCause() instanceof ProduceException);
        assertTrue(exception.getCause().getMessage().contains("Error getting producer for Topic(project1.topic1)"));
        assertTrue(exception.getCause().getMessage().contains("Topic doesn't exist."));
    }

    @Test
    void testProduceWithProducerFailure() throws InterruptedException {
        Message msg1 = getMessage(0, 1, UnsupportedOperationException.class.getName(), 0);
        Resource.EntityResource<VaradhiTopic> vt = getTopic(topic, project, region);
        when(topicReadCache.get(vt.getName())).thenReturn(Optional.of(vt));
        doReturn(producer).when(producerFactory).newProducer(any(), any());

        CompletableFuture<ProduceResult> result = service.produceToTopic(
            msg1,
            VaradhiTopic.fqn(project.getName(), topic)
        );

        ResultCapture rc = getResult(result);
        Assertions.assertNotNull(rc.produceResult);
        Assertions.assertNull(rc.throwable);
        Assertions.assertEquals(ProduceStatus.Failed, rc.produceResult.getProduceStatus());
        Assertions.assertEquals(
            "Produce failure from messaging stack for Topic/Queue. null",
            rc.produceResult.getFailureReason()
        );
        verify(producerFactory, times(1)).newProducer(any(), any());
    }

    // TODO: this test expectation looks wrong. the metric emit failure should not lead to api failure.
    @Test
    void testMetricEmitFailureNotIgnored() throws InterruptedException {
        ProducerMetrics emitter = mock(ProducerMetrics.class);
        doThrow(new RuntimeException("Failed to send metric.")).when(emitter).accepted(any(), any());

        service = new ProducerService(
            region,
            producerFactory::newProducer,
            orgCache,
            projectCache,
            topicReadCache,
            t -> emitter,
            ProducerOptions.defaultOptions()
        );
        Message msg1 = getMessage(0, 1, null, 10);
        Resource.EntityResource<VaradhiTopic> vt = getTopic(topic, project, region);
        when(topicReadCache.get(vt.getName())).thenReturn(Optional.of(vt));
        doReturn(producer).when(producerFactory).newProducer(any(), any());

        CompletableFuture<ProduceResult> result = service.produceToTopic(
            msg1,
            VaradhiTopic.fqn(project.getName(), topic)
        );
        ResultCapture rc = getResult(result);
        Assertions.assertNull(rc.produceResult);
        Assertions.assertNotNull(rc.throwable);
        verify(producer, times(1)).produceAsync(eq(msg1));
        verify(emitter, times(1)).accepted(any(), isNull());
        // Exception gets wrapped in CompletionException.
        Assertions.assertEquals("Failed to send metric.", rc.throwable.getCause().getMessage());
    }

    public Resource.EntityResource<VaradhiTopic> getTopic(String name, Project project, String region) {
        return Resource.of(getTopic(TopicState.Producing, name, project, region), ResourceType.TOPIC);
    }

    public VaradhiTopic getTopic(TopicState state, String name, Project project, String region) {
        VaradhiTopic topic = VaradhiTopic.of(
            project.getName(),
            name,
            false,
            null,
            LifecycleStatus.ActorCode.SYSTEM_ACTION
        );
        topic.markCreated();

        StorageTopic st = new DummyStorageTopic(topic.getName());
        SegmentedStorageTopic ict = SegmentedStorageTopic.of(st);
        ict.setTopicState(state);
        topic.addInternalTopic(region, ict);
        return topic;
    }

    public Message getMessage(int sleepMs, int offset, String exceptionClass, int payloadSize) {
        Multimap<String, String> headers = ArrayListMultimap.create();
        headers.put(StdHeaders.get().msgId(), getMessageId());
        headers.put(StdHeaders.get().producerIdentity(), "ANONYMOUS");
        headers.put(StdHeaders.get().produceRegion(), region);
        headers.put(StdHeaders.get().produceTimestamp(), System.currentTimeMillis() + "");
        byte[] payload = null;
        if (payloadSize > 0) {
            payload = new byte[payloadSize];
            random.nextBytes(payload);
        }
        DummyProducer.DummyMessage message = new DummyProducer.DummyMessage(sleepMs, offset, exceptionClass, payload);
        return new SimpleMessage(JsonMapper.jsonSerialize(message).getBytes(), headers);
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

    static class ResultCapture {
        ProduceResult produceResult;
        Throwable throwable;
    }


    public static class DummyStorageTopic extends StorageTopic {
        public DummyStorageTopic(String name) {
            super(0, name);
        }
    }
}
