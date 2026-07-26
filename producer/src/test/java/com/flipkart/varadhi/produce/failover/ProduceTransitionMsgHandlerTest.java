package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.cluster.controller.TransitionApi;
import com.flipkart.varadhi.core.cluster.events.EventType;
import com.flipkart.varadhi.core.cluster.events.ResourceEvent;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.entities.SegmentedStorageTopic;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.VaradhiTopicName;
import com.flipkart.varadhi.entities.cluster.failover.TransitionAck;
import com.flipkart.varadhi.entities.cluster.failover.TransitionEvent;
import com.flipkart.varadhi.entities.cluster.failover.TransitionParticipation;
import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import com.flipkart.varadhi.produce.ProducerService;
import com.flipkart.varadhi.spi.services.Producer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ProduceTransitionMsgHandler} driven against a <b>real</b>
 * {@link ResourceReadCache} (no mock of the cache or its resources) — versions are seeded by
 * firing the same {@link ResourceEvent}s the L1 propagation pipeline would.
 */
class ProduceTransitionMsgHandlerTest {

    private static final String OP_ID = "op-1";
    private static final String PROJECT = "proj";
    private static final String TOPIC = "topic1";
    private static final String FQN = PROJECT + "." + TOPIC;
    private static final VaradhiTopicName TOPIC_NAME = VaradhiTopicName.of(PROJECT, TOPIC);
    private static final String DEPLOYED_REGION = "region-a";
    private static final RegionName TARGET_REGION = new RegionName("region-b");
    private static final int TARGET_STORAGE_TOPIC_ID = 7;

    private Vertx vertx;
    private ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache;
    private ProducerService producerService;
    private CapturingControllerClient acker;
    private ScheduledExecutorService scheduler;
    private TransitionMetrics metrics;

    @BeforeEach
    void setup() throws Exception {
        vertx = Vertx.vertx();
        topicCache = ResourceReadCache.<Resource.EntityResource<VaradhiTopic>>create(
            ResourceType.TOPIC,
            List::of,
            vertx
        ).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        producerService = mock(ProducerService.class);
        when(producerService.deployedRegion()).thenReturn(DEPLOYED_REGION);
        when(producerService.hasProducer(anyString(), anyInt(), anyString())).thenReturn(true);
        when(producerService.getProducerForRegion(any(VaradhiTopic.class), any(RegionName.class))).thenReturn(
            CompletableFuture.completedFuture(mock(Producer.class))
        );
        when(producerService.loadProducer(any(VaradhiTopicName.class), anyInt())).thenReturn(
            CompletableFuture.completedFuture(null)
        );
        scheduler = Executors.newSingleThreadScheduledExecutor();
        metrics = new TransitionMetrics(new SimpleMeterRegistry());
    }

    @AfterEach
    void tearDown() throws Exception {
        scheduler.shutdownNow();
        vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    private ProduceTransitionMsgHandler handler(PodTransitionConfig config) {
        return handler(config, 1);
    }

    private ProduceTransitionMsgHandler handler(PodTransitionConfig config, int expectedAcks) {
        acker = new CapturingControllerClient(expectedAcks);
        return new ProduceTransitionMsgHandler(
            "host-1",
            topicCache,
            acker,
            producerService,
            config,
            scheduler,
            metrics
        );
    }

    private void seed(int version) {
        VaradhiTopic topic = VaradhiTopic.of(
            PROJECT,
            TOPIC,
            false,
            new TopicCapacityPolicy(100, 400, 2, 2),
            LifecycleStatus.ActionCode.SYSTEM_ACTION
        );
        topic = topic.withStorageTopic(SegmentedStorageTopic.of(new StorageTopic(0, FQN) {}))
                     .withProduceRegion(RegionName.of(DEPLOYED_REGION));
        topic.setVersion(version);
        topicCache.onChange(
            new ResourceEvent<>(
                ResourceType.TOPIC,
                FQN,
                EventType.UPSERT,
                Resource.of(topic, ResourceType.TOPIC),
                version,
                null
            )
        );
    }

    @Test
    void prepareAcksOkWhenCaughtUpToCurrentVersion() throws Exception {
        seed(10);
        ProduceTransitionMsgHandler h = handler(PodTransitionConfig.defaultConfig());

        h.handle(
            ClusterMessage.of(
                TransitionEvent.of(
                    OP_ID,
                    TOPIC_NAME,
                    TransitionType.TOPIC_FAILOVER,
                    TransitionStage.PREPARE,
                    true,
                    10,
                    new TransitionEvent.Target.Region(TARGET_REGION)
                )
            )
        );

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        TransitionAck ack = acker.acks.get(0);
        assertEquals(TransitionStage.PREPARE, ack.stage());
        assertEquals(TOPIC_NAME, ack.topicFqn());
        assertEquals(TransitionType.TOPIC_FAILOVER, ack.transitionType());
        assertEquals(TransitionParticipation.INVOLVED, ack.participation());
        assertTrue(ack.isSuccess());
        verify(producerService).getProducerForRegion(any(VaradhiTopic.class), eq(TARGET_REGION));
    }

    @Test
    void storageMigrationPrepareWarmsTargetStorageTopicAndAcksOk() throws Exception {
        seed(10);
        ProduceTransitionMsgHandler h = handler(PodTransitionConfig.defaultConfig());

        h.handle(
            ClusterMessage.of(
                TransitionEvent.of(
                    OP_ID,
                    TOPIC_NAME,
                    TransitionType.STORAGE_MIGRATION,
                    TransitionStage.PREPARE,
                    true,
                    10,
                    new TransitionEvent.Target.StorageTopic(TARGET_STORAGE_TOPIC_ID)
                )
            )
        );

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        TransitionAck ack = acker.acks.get(0);
        assertEquals(TransitionStage.PREPARE, ack.stage());
        assertEquals(TOPIC_NAME, ack.topicFqn());
        assertEquals(TransitionType.STORAGE_MIGRATION, ack.transitionType());
        assertEquals(TransitionParticipation.INVOLVED, ack.participation());
        assertTrue(ack.isSuccess());
        verify(producerService).loadProducer(TOPIC_NAME, TARGET_STORAGE_TOPIC_ID);
        verify(producerService, never()).getProducerForRegion(any(VaradhiTopic.class), any(RegionName.class));
    }

    @Test
    void prepareAcksFailureWhenWarmFails() throws Exception {
        seed(10);
        when(producerService.getProducerForRegion(any(VaradhiTopic.class), any(RegionName.class))).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("broker unreachable"))
        );
        ProduceTransitionMsgHandler h = handler(PodTransitionConfig.defaultConfig());

        h.handle(
            ClusterMessage.of(
                TransitionEvent.of(
                    OP_ID,
                    TOPIC_NAME,
                    TransitionType.TOPIC_FAILOVER,
                    TransitionStage.PREPARE,
                    true,
                    10,
                    new TransitionEvent.Target.Region(TARGET_REGION)
                )
            )
        );

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        TransitionAck ack = acker.acks.get(0);
        assertEquals(TransitionStage.PREPARE, ack.stage());
        assertEquals(TransitionParticipation.INVOLVED, ack.participation());
        assertFalse(ack.isSuccess());
        assertTrue(ack.errorMsg().contains("prepare warm failed"));
    }

    @Test
    void prepareAcksOkWithoutWarmingWhenPodNotInvolved() throws Exception {
        seed(10);
        when(producerService.hasProducer(anyString(), anyInt(), anyString())).thenReturn(false);
        ProduceTransitionMsgHandler h = handler(PodTransitionConfig.defaultConfig());

        h.handle(
            ClusterMessage.of(
                TransitionEvent.of(
                    OP_ID,
                    TOPIC_NAME,
                    TransitionType.TOPIC_FAILOVER,
                    TransitionStage.PREPARE,
                    true,
                    10,
                    new TransitionEvent.Target.Region(TARGET_REGION)
                )
            )
        );

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        TransitionAck ack = acker.acks.get(0);
        assertEquals(TransitionStage.PREPARE, ack.stage());
        assertEquals(TransitionParticipation.NOT_INVOLVED, ack.participation());
        assertTrue(ack.isSuccess());
        verify(producerService, never()).getProducerForRegion(any(VaradhiTopic.class), any(RegionName.class));
        verify(producerService, never()).loadProducer(any(VaradhiTopicName.class), anyInt());
    }

    @Test
    void switchEchoesParticipationDecidedAtPrepare() throws Exception {
        seed(10);
        when(producerService.hasProducer(anyString(), anyInt(), anyString())).thenReturn(false);
        ProduceTransitionMsgHandler h = handler(PodTransitionConfig.defaultConfig(), 2);

        h.handle(
            ClusterMessage.of(
                TransitionEvent.of(
                    OP_ID,
                    TOPIC_NAME,
                    TransitionType.TOPIC_FAILOVER,
                    TransitionStage.PREPARE,
                    true,
                    10,
                    new TransitionEvent.Target.Region(TARGET_REGION)
                )
            )
        );
        seed(11);
        h.handle(
            ClusterMessage.of(
                TransitionEvent.of(
                    OP_ID,
                    TOPIC_NAME,
                    TransitionType.TOPIC_FAILOVER,
                    TransitionStage.SWITCH,
                    true,
                    11,
                    null
                )
            )
        );
        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        assertEquals(TransitionParticipation.NOT_INVOLVED, acker.acks.get(0).participation());
        TransitionAck switchAck = acker.acks.get(1);
        assertEquals(TransitionStage.SWITCH, switchAck.stage());
        assertEquals(TransitionParticipation.NOT_INVOLVED, switchAck.participation());
        assertTrue(switchAck.isSuccess());
    }

    @Test
    void prepareAcksFailureWhenStaleOrUnreachable() throws Exception {
        ProduceTransitionMsgHandler h = handler(new PodTransitionConfig(60L, 10L, 0L));

        h.handle(
            ClusterMessage.of(
                TransitionEvent.of(
                    OP_ID,
                    TOPIC_NAME,
                    TransitionType.TOPIC_FAILOVER,
                    TransitionStage.PREPARE,
                    true,
                    10,
                    new TransitionEvent.Target.Region(TARGET_REGION)
                )
            )
        );

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        TransitionAck ack = acker.acks.get(0);
        assertEquals(TransitionStage.PREPARE, ack.stage());
        assertFalse(ack.isSuccess());
        assertTrue(ack.errorMsg().contains("timeout"));
    }

    @Test
    void switchAcksOkWhenVersionAlreadyPresent() throws Exception {
        seed(11);
        ProduceTransitionMsgHandler h = handler(PodTransitionConfig.defaultConfig());

        h.handle(
            ClusterMessage.of(
                TransitionEvent.of(
                    OP_ID,
                    TOPIC_NAME,
                    TransitionType.TOPIC_FAILOVER,
                    TransitionStage.SWITCH,
                    true,
                    11,
                    null
                )
            )
        );

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        TransitionAck ack = acker.acks.get(0);
        assertEquals(TransitionStage.SWITCH, ack.stage());
        assertEquals(TransitionParticipation.INVOLVED, ack.participation());
        assertTrue(ack.isSuccess());
        verify(producerService, never()).getProducerForRegion(any(VaradhiTopic.class), any(RegionName.class));
        verify(producerService, never()).loadProducer(any(VaradhiTopicName.class), anyInt());
    }

    @Test
    void switchAcksOkWhenVersionArrivesLater() throws Exception {
        ProduceTransitionMsgHandler h = handler(new PodTransitionConfig(2000L, 5L, 0L));
        scheduler.schedule(() -> seed(11), 40, TimeUnit.MILLISECONDS);

        h.handle(
            ClusterMessage.of(
                TransitionEvent.of(
                    OP_ID,
                    TOPIC_NAME,
                    TransitionType.TOPIC_FAILOVER,
                    TransitionStage.SWITCH,
                    true,
                    11,
                    null
                )
            )
        );

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        assertTrue(acker.acks.get(0).isSuccess());
    }

    @Test
    void switchAcksFailureOnTimeout() throws Exception {
        ProduceTransitionMsgHandler h = handler(new PodTransitionConfig(60L, 10L, 0L));

        h.handle(
            ClusterMessage.of(
                TransitionEvent.of(
                    OP_ID,
                    TOPIC_NAME,
                    TransitionType.TOPIC_FAILOVER,
                    TransitionStage.SWITCH,
                    true,
                    11,
                    null
                )
            )
        );

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        TransitionAck ack = acker.acks.get(0);
        assertFalse(ack.isSuccess());
        assertTrue(ack.errorMsg().contains("timeout"));
    }

    @Test
    void switchAcksFailureWhenVersionOvershoots() throws Exception {
        seed(12);
        ProduceTransitionMsgHandler h = handler(PodTransitionConfig.defaultConfig());

        h.handle(
            ClusterMessage.of(
                TransitionEvent.of(
                    OP_ID,
                    TOPIC_NAME,
                    TransitionType.TOPIC_FAILOVER,
                    TransitionStage.SWITCH,
                    true,
                    11,
                    null
                )
            )
        );

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        TransitionAck ack = acker.acks.get(0);
        assertEquals(TransitionStage.SWITCH, ack.stage());
        assertFalse(ack.isSuccess());
        assertTrue(ack.errorMsg().contains("overshot"));
    }

    @Test
    void prepareAcksFailureWhenVersionOvershoots() throws Exception {
        seed(11);
        ProduceTransitionMsgHandler h = handler(PodTransitionConfig.defaultConfig());

        h.handle(
            ClusterMessage.of(
                TransitionEvent.of(
                    OP_ID,
                    TOPIC_NAME,
                    TransitionType.TOPIC_FAILOVER,
                    TransitionStage.PREPARE,
                    true,
                    10,
                    new TransitionEvent.Target.Region(TARGET_REGION)
                )
            )
        );

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        TransitionAck ack = acker.acks.get(0);
        assertEquals(TransitionStage.PREPARE, ack.stage());
        assertFalse(ack.isSuccess());
        assertTrue(ack.errorMsg().contains("overshot"));
    }

    @Test
    void completedAcksOkImmediately() throws Exception {
        ProduceTransitionMsgHandler h = handler(PodTransitionConfig.defaultConfig());

        h.handle(
            ClusterMessage.of(
                TransitionEvent.of(
                    OP_ID,
                    TOPIC_NAME,
                    TransitionType.TOPIC_FAILOVER,
                    TransitionStage.COMPLETED,
                    false,
                    0,
                    null
                )
            )
        );

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        TransitionAck ack = acker.acks.get(0);
        assertEquals(TransitionStage.COMPLETED, ack.stage());
        assertTrue(ack.isSuccess());
    }

    @Test
    void abortedAcksOkImmediately() throws Exception {
        ProduceTransitionMsgHandler h = handler(PodTransitionConfig.defaultConfig());

        h.handle(
            ClusterMessage.of(
                TransitionEvent.of(
                    OP_ID,
                    TOPIC_NAME,
                    TransitionType.TOPIC_FAILOVER,
                    TransitionStage.ABORTED,
                    false,
                    0,
                    null
                )
            )
        );

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        TransitionAck ack = acker.acks.get(0);
        assertEquals(TransitionStage.ABORTED, ack.stage());
        assertTrue(ack.isSuccess());
    }

    @Test
    void pendingAcksOkImmediatelyWithoutVersionWait() throws Exception {
        ProduceTransitionMsgHandler h = handler(PodTransitionConfig.defaultConfig());

        h.handle(
            ClusterMessage.of(
                TransitionEvent.of(
                    OP_ID,
                    TOPIC_NAME,
                    TransitionType.TOPIC_FAILOVER,
                    TransitionStage.PENDING,
                    false,
                    0,
                    null
                )
            )
        );

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        TransitionAck ack = acker.acks.get(0);
        assertEquals(TransitionStage.PENDING, ack.stage());
        assertTrue(ack.isSuccess());
    }

    private static final class CapturingControllerClient implements TransitionApi {
        private final CopyOnWriteArrayList<TransitionAck> acks = new CopyOnWriteArrayList<>();
        private final CountDownLatch latch;

        CapturingControllerClient(int expected) {
            this.latch = new CountDownLatch(expected);
        }

        @Override
        public CompletableFuture<Void> sendEvent(TransitionEvent event) {
            throw new UnsupportedOperationException("sendEvent is controller-local");
        }

        @Override
        public CompletableFuture<Void> ack(TransitionAck ack) {
            acks.add(ack);
            latch.countDown();
            return CompletableFuture.completedFuture(null);
        }
    }
}
