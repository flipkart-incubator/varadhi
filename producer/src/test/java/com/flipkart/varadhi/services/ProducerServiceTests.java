package com.flipkart.varadhi.services;

import com.flipkart.varadhi.common.Constants;
import com.flipkart.varadhi.common.EntityReadCache;
import com.flipkart.varadhi.common.SimpleMessage;
import com.flipkart.varadhi.common.exceptions.ProduceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.common.utils.JsonMapper;
import com.flipkart.varadhi.entities.InternalCompositeTopic;
import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.ProduceStatus;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.StdHeaders;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TestStdHeaders;
import com.flipkart.varadhi.entities.TopicState;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.produce.ProduceResult;
import com.flipkart.varadhi.produce.config.ProducerMetricsConfig;
import com.flipkart.varadhi.produce.otel.ProducerMetricsEmitter;
import com.flipkart.varadhi.produce.otel.ProducerMetricsEmitterImpl;
import com.flipkart.varadhi.produce.services.ProducerService;
import com.flipkart.varadhi.pulsar.entities.PulsarOffset;
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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import static com.flipkart.varadhi.common.Constants.Tags.TAG_IDENTITY;
import static com.flipkart.varadhi.common.Constants.Tags.TAG_ORG;
import static com.flipkart.varadhi.common.Constants.Tags.TAG_PROJECT;
import static com.flipkart.varadhi.common.Constants.Tags.TAG_REGION;
import static com.flipkart.varadhi.common.Constants.Tags.TAG_REMOTE_HOST;
import static com.flipkart.varadhi.common.Constants.Tags.TAG_TEAM;
import static com.flipkart.varadhi.common.Constants.Tags.TAG_TOPIC;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyLong;
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
    EntityReadCache<VaradhiTopic> topicReadCache;
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
        topicReadCache = mock(EntityReadCache.class);

        producer = spy(new DummyProducer(JsonMapper.getMapper()));
        when(producerFactory.newProducer(any())).thenReturn(producer);

        service = new ProducerService(region, producerFactory::newProducer, topicReadCache);
        random = new Random();
    }

    @Test
    void testProduceMessage() throws InterruptedException {
        ProducerMetricsEmitter emitter = getMetricEmitter(topic, project, region);
        Message msg1 = getMessage(0, 1, null, 10);
        VaradhiTopic vt = getTopic(topic, project, region);

        when(topicReadCache.get(vt.getName())).thenReturn(Optional.of(vt));

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
        verify(topicReadCache, times(2)).get(vt.getName());
    }

    @Test
    void testProduceWhenProduceAsyncThrows() {
        ProducerMetricsEmitter emitter = mock(ProducerMetricsEmitter.class);
        Message msg1 = getMessage(0, 1, null, 10);
        VaradhiTopic vt = getTopic(topic, project, region);
        when(topicReadCache.get(vt.getName())).thenReturn(Optional.of(vt));
        doReturn(producer).when(producerFactory).newProducer(any());
        doThrow(new RuntimeException("Some random error.")).when(producer).produceAsync(msg1);
        // This is testing Producer.ProduceAsync(), throwing an exception which is handled in produce service.
        // This is not expected in general.
        CompletableFuture<ProduceResult> future = service.produceToTopic(
            msg1,
            VaradhiTopic.buildTopicName(project.getName(), topic),
            emitter
        );
        CompletionException exception = Assertions.assertThrows(CompletionException.class, future::join);
        assertTrue(exception.getCause() instanceof RuntimeException);
        Assertions.assertEquals("Some random error.", exception.getCause().getMessage());
        verify(emitter, never()).emit(anyBoolean(), anyLong(), anyLong(), anyInt(), anyBoolean(), any());
    }

    @Test
    void testProduceToNonExistingTopic() {
        ProducerMetricsEmitter emitter = getMetricEmitter(topic, project, region);
        Message msg1 = getMessage(0, 1, null, 0);
        String topicName = VaradhiTopic.buildTopicName(project.getName(), topic);
        doReturn(producer).when(producerFactory).newProducer(any());
        when(topicReadCache.get(topicName)).thenReturn(Optional.empty());
        ResourceNotFoundException ex = Assertions.assertThrows(
            ResourceNotFoundException.class,
            () -> service.produceToTopic(msg1, topicName, emitter)
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
        ProducerMetricsEmitter emitter = getMetricEmitter(topic, project, region);
        Message msg1 = getMessage(0, 1, null, 0);
        VaradhiTopic vt = getTopic(topicState, topic, project, region);
        when(topicReadCache.get(vt.getName())).thenReturn(Optional.of(vt));
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
    void testProduceWithUnknownExceptionInGetProducer() {
        ProducerMetricsEmitter emitter = getMetricEmitter(topic, project, region);
        Message msg1 = getMessage(0, 1, null, 0);
        VaradhiTopic vt = getTopic(topic, project, region);
        when(topicReadCache.get(vt.getName())).thenReturn(Optional.of(vt));
        Function<StorageTopic, Producer> failingProducerProvider = storageTopic -> {
            throw new RuntimeException("Unknown Error.");
        };
        ProducerService failingService = new ProducerService(region, failingProducerProvider, topicReadCache);
        CompletableFuture<ProduceResult> future = failingService.produceToTopic(
            msg1,
            VaradhiTopic.buildTopicName(project.getName(), topic),
            emitter
        );
        CompletionException exception = Assertions.assertThrows(CompletionException.class, future::join);
        assertTrue(exception.getCause() instanceof ProduceException);
        assertTrue(exception.getCause().getMessage().contains("Error getting producer for Topic(project1.topic1)"));
        assertTrue(exception.getCause().getMessage().contains("Unknown Error."));
    }

    @Test
    void testProduceWithKnownExceptionInGetProducer() {
        ProducerMetricsEmitter emitter = getMetricEmitter(topic, project, region);
        Message msg1 = getMessage(0, 1, null, 0);
        VaradhiTopic vt = getTopic(topic, project, region);
        when(topicReadCache.get(vt.getName())).thenReturn(Optional.of(vt));
        Function<StorageTopic, Producer> failingProducerProvider = st -> {
            throw new RuntimeException("Topic doesn't exist.");
        };
        ProducerService failingService = new ProducerService(region, failingProducerProvider, topicReadCache);
        CompletableFuture<ProduceResult> future = failingService.produceToTopic(
            msg1,
            VaradhiTopic.buildTopicName(project.getName(), topic),
            emitter
        );
        CompletionException exception = Assertions.assertThrows(CompletionException.class, future::join);
        verify(producer, never()).produceAsync(any());
        assertTrue(exception.getCause() instanceof ProduceException);
        assertTrue(exception.getCause().getMessage().contains("Error getting producer for Topic(project1.topic1)"));
        assertTrue(exception.getCause().getMessage().contains("Topic doesn't exist."));
    }

    @Test
    void testProduceWithProducerFailure() throws InterruptedException {
        ProducerMetricsEmitter emitter = getMetricEmitter(topic, project, region);
        Message msg1 = getMessage(0, 1, UnsupportedOperationException.class.getName(), 0);
        VaradhiTopic vt = getTopic(topic, project, region);
        when(topicReadCache.get(vt.getName())).thenReturn(Optional.of(vt));
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
    void testMetricEmitFailureNotIgnored() throws InterruptedException {
        ProducerMetricsEmitter emitter = mock(ProducerMetricsEmitter.class);
        doThrow(new RuntimeException("Failed to send metric.")).when(emitter)
                                                               .emit(
                                                                   anyBoolean(),
                                                                   anyLong(),
                                                                   anyLong(),
                                                                   anyInt(),
                                                                   anyBoolean(),
                                                                   any()
                                                               );
        Message msg1 = getMessage(0, 1, null, 10);
        VaradhiTopic vt = getTopic(topic, project, region);
        when(topicReadCache.get(vt.getName())).thenReturn(Optional.of(vt));
        doReturn(producer).when(producerFactory).newProducer(any());

        PulsarOffset offset = mock(PulsarOffset.class);
        when(offset.getStorageLatencyMs()).thenReturn(0L);

        CompletableFuture<ProduceResult> result = service.produceToTopic(
            msg1,
            VaradhiTopic.buildTopicName(project.getName(), topic),
            emitter
        );
        ResultCapture rc = getResult(result);
        Assertions.assertNull(rc.produceResult);
        Assertions.assertNotNull(rc.throwable);
        verify(producer, times(1)).produceAsync(eq(msg1));
        verify(emitter, times(1)).emit(anyBoolean(), anyLong(), anyLong(), anyInt(), anyBoolean(), any());
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
        topic.markCreated();

        StorageTopic st = new DummyStorageTopic(topic.getName());
        InternalCompositeTopic ict = InternalCompositeTopic.of(st);
        ict.setTopicState(state);
        topic.addInternalTopic(region, ict);
        return topic;
    }

    public Message getMessage(int sleepMs, int offset, String exceptionClass, int payloadSize) {
        Multimap<String, String> headers = ArrayListMultimap.create();
        headers.put(StdHeaders.get().msgId(), getMessageId());
        headers.put(StdHeaders.get().produceIdentity(), "ANONYMOUS");
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

    public ProducerMetricsEmitter getMetricEmitter(String topic, Project project, String region) {
        Map<String, String> produceAttributes = new HashMap<>();
        produceAttributes.put(TAG_REGION, region);
        produceAttributes.put(TAG_ORG, project.getOrg());
        produceAttributes.put(TAG_TEAM, project.getTeam());
        produceAttributes.put(TAG_PROJECT, project.getName());
        produceAttributes.put(TAG_TOPIC, topic);
        produceAttributes.put(TAG_IDENTITY, "ANONYMOUS");
        produceAttributes.put(TAG_REMOTE_HOST, "remoteHost");
        return new ProducerMetricsEmitterImpl(meterRegistry, ProducerMetricsConfig.getDefault(), produceAttributes);
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

    //    public static class TopicProvider {
    //        public VaradhiTopic get(String topicName) {
    //            return null;
    //        }
    //    }


    static class ResultCapture {
        ProduceResult produceResult;
        Throwable throwable;
    }


    public static class DummyStorageTopic extends StorageTopic {
        public DummyStorageTopic(String name) {
            super(name, Constants.DEFAULT_TOPIC_CAPACITY);
        }
    }
}
