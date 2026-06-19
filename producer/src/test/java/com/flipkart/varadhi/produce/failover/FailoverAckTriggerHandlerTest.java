package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.cluster.failover.FailoverAck;
import com.flipkart.varadhi.entities.cluster.failover.FailoverEvent;
import com.flipkart.varadhi.entities.cluster.failover.FailoverStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FailoverAckTriggerHandlerTest {

    private static final String OP_ID = "op-1";
    private static final String FQN = "proj.topic1";
    private static final String TARGET_REGION = "region-b";
    private static final String TARGET_STORAGE_TOPIC_ID = "7";

    private ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache;
    private CapturingAckClient acker;
    private RecordingWarmer warmer;
    private RecordingWarmer storageWarmer;
    private ScheduledExecutorService scheduler;

    @SuppressWarnings ("unchecked")
    @BeforeEach
    void setup() {
        topicCache = mock(ResourceReadCache.class);
        warmer = new RecordingWarmer();
        storageWarmer = new RecordingWarmer();
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    private FailoverAckTriggerHandler handler(PodFailoverConfig config) {
        acker = new CapturingAckClient(1);
        return new FailoverAckTriggerHandler(
            "host-1",
            topicCache,
            acker,
            Map.of(
                TransitionType.TOPIC_FAILOVER,
                warmer,
                TransitionType.STORAGE_MIGRATION,
                storageWarmer
            ),
            config,
            scheduler
        );
    }

    @Test
    void prepareAcksOkWhenCaughtUpToCurrentVersion() throws Exception {
        Resource.EntityResource<VaradhiTopic> atV10 = topicAtVersion(10);
        when(topicCache.get(FQN)).thenReturn(Optional.of(atV10));
        FailoverAckTriggerHandler h = handler(PodFailoverConfig.defaultConfig());

        h.handle(ClusterMessage.of(FailoverEvent.forPrepare(OP_ID, FQN, 10, TARGET_REGION)));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        FailoverAck ack = acker.acks.get(0);
        assertEquals(FailoverStage.PREPARE, ack.stage());
        assertTrue(ack.success());
        assertTrue(
            warmer.warmed.contains(FQN + "@" + TARGET_REGION),
            "PREPARE should pre-warm the target region producer"
        );
    }

    @Test
    void storageMigrationPrepareWarmsTargetStorageTopicAndAcksOk() throws Exception {
        Resource.EntityResource<VaradhiTopic> atV10 = topicAtVersion(10);
        when(topicCache.get(FQN)).thenReturn(Optional.of(atV10));
        FailoverAckTriggerHandler h = handler(PodFailoverConfig.defaultConfig());

        h.handle(
            ClusterMessage.of(
                FailoverEvent.forPrepare(OP_ID, FQN, 10, TARGET_STORAGE_TOPIC_ID, TransitionType.STORAGE_MIGRATION)
            )
        );

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        FailoverAck ack = acker.acks.get(0);
        assertEquals(FailoverStage.PREPARE, ack.stage());
        assertTrue(ack.success());
        assertTrue(
            storageWarmer.warmed.contains(FQN + "@" + TARGET_STORAGE_TOPIC_ID),
            "STORAGE_MIGRATION PREPARE should pre-warm the target storage-topic producer"
        );
        assertTrue(warmer.warmed.isEmpty(), "failover warmer must not run for a storage migration");
    }

    @Test
    void prepareAcksFailureWhenWarmThrows() throws Exception {
        Resource.EntityResource<VaradhiTopic> atV10 = topicAtVersion(10);
        when(topicCache.get(FQN)).thenReturn(Optional.of(atV10));
        warmer.toThrow = new RuntimeException("broker unreachable");
        FailoverAckTriggerHandler h = handler(PodFailoverConfig.defaultConfig());

        h.handle(ClusterMessage.of(FailoverEvent.forPrepare(OP_ID, FQN, 10, TARGET_REGION)));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        FailoverAck ack = acker.acks.get(0);
        assertEquals(FailoverStage.PREPARE, ack.stage());
        assertFalse(ack.success());
        assertTrue(ack.errorMsg().contains("prepare warm failed"));
    }

    @Test
    void prepareAcksFailureWhenStaleOrUnreachable() throws Exception {
        when(topicCache.get(FQN)).thenReturn(Optional.empty());
        FailoverAckTriggerHandler h = handler(new PodFailoverConfig(60L, 10L));

        h.handle(ClusterMessage.of(FailoverEvent.forPrepare(OP_ID, FQN, 10, TARGET_REGION)));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        FailoverAck ack = acker.acks.get(0);
        assertEquals(FailoverStage.PREPARE, ack.stage());
        assertFalse(ack.success());
        assertTrue(ack.errorMsg().contains("timeout"));
    }

    @Test
    void switchAcksOkWhenVersionAlreadyPresent() throws Exception {
        Resource.EntityResource<VaradhiTopic> atV11 = topicAtVersion(11);
        when(topicCache.get(FQN)).thenReturn(Optional.of(atV11));
        FailoverAckTriggerHandler h = handler(PodFailoverConfig.defaultConfig());

        h.handle(ClusterMessage.of(FailoverEvent.forSwitch(OP_ID, FQN, 11)));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        FailoverAck ack = acker.acks.get(0);
        assertEquals(FailoverStage.SWITCH, ack.stage());
        assertTrue(ack.success());
        assertTrue(warmer.warmed.isEmpty(), "SWITCH must not warm the producer");
    }

    @Test
    void switchAcksOkWhenVersionArrivesLater() throws Exception {
        Resource.EntityResource<VaradhiTopic> atV11 = topicAtVersion(11);
        when(topicCache.get(FQN)).thenReturn(Optional.empty(), Optional.empty(), Optional.of(atV11));
        FailoverAckTriggerHandler h = handler(new PodFailoverConfig(2000L, 5L));

        h.handle(ClusterMessage.of(FailoverEvent.forSwitch(OP_ID, FQN, 11)));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        assertTrue(acker.acks.get(0).success());
    }

    @Test
    void switchAcksFailureOnTimeout() throws Exception {
        when(topicCache.get(FQN)).thenReturn(Optional.empty());
        FailoverAckTriggerHandler h = handler(new PodFailoverConfig(60L, 10L));

        h.handle(ClusterMessage.of(FailoverEvent.forSwitch(OP_ID, FQN, 11)));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        FailoverAck ack = acker.acks.get(0);
        assertFalse(ack.success());
        assertTrue(ack.errorMsg().contains("timeout"));
    }

    @Test
    void switchAcksFailureWhenVersionOvershoots() throws Exception {
        // Cache skipped past the coordinated version (concurrent modification) => fail fast.
        Resource.EntityResource<VaradhiTopic> atV12 = topicAtVersion(12);
        when(topicCache.get(FQN)).thenReturn(Optional.of(atV12));
        FailoverAckTriggerHandler h = handler(PodFailoverConfig.defaultConfig());

        h.handle(ClusterMessage.of(FailoverEvent.forSwitch(OP_ID, FQN, 11)));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        FailoverAck ack = acker.acks.get(0);
        assertEquals(FailoverStage.SWITCH, ack.stage());
        assertFalse(ack.success());
        assertTrue(ack.errorMsg().contains("overshot"));
    }

    @Test
    void completedAcksOkImmediately() throws Exception {
        FailoverAckTriggerHandler h = handler(PodFailoverConfig.defaultConfig());

        h.handle(ClusterMessage.of(FailoverEvent.forStage(OP_ID, FQN, FailoverStage.COMPLETED)));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        FailoverAck ack = acker.acks.get(0);
        assertEquals(FailoverStage.COMPLETED, ack.stage());
        assertTrue(ack.success());
    }

    @Test
    void abortedAcksOkImmediately() throws Exception {
        FailoverAckTriggerHandler h = handler(PodFailoverConfig.defaultConfig());

        h.handle(ClusterMessage.of(FailoverEvent.forStage(OP_ID, FQN, FailoverStage.ABORTED)));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        FailoverAck ack = acker.acks.get(0);
        assertEquals(FailoverStage.ABORTED, ack.stage());
        assertTrue(ack.success());
    }

    @Test
    void drainAcksOkImmediatelyWithoutVersionWait() throws Exception {
        // No version to await => acks without ever consulting the topic cache.
        FailoverAckTriggerHandler h = handler(PodFailoverConfig.defaultConfig());

        h.handle(ClusterMessage.of(FailoverEvent.forStage(OP_ID, FQN, FailoverStage.DRAIN)));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        FailoverAck ack = acker.acks.get(0);
        assertEquals(FailoverStage.DRAIN, ack.stage());
        assertTrue(ack.success());
    }

    @SuppressWarnings ("unchecked")
    private Resource.EntityResource<VaradhiTopic> topicAtVersion(int version) {
        Resource.EntityResource<VaradhiTopic> res = mock(Resource.EntityResource.class);
        when(res.getVersion()).thenReturn(version);
        return res;
    }

    private static final class RecordingWarmer implements BiConsumer<String, String> {
        private final CopyOnWriteArrayList<String> warmed = new CopyOnWriteArrayList<>();
        private volatile RuntimeException toThrow;

        @Override
        public void accept(String topicFqn, String target) {
            if (toThrow != null) {
                throw toThrow;
            }
            warmed.add(topicFqn + "@" + target);
        }
    }


    private static final class CapturingAckClient implements FailoverAckClient {
        private final CopyOnWriteArrayList<FailoverAck> acks = new CopyOnWriteArrayList<>();
        private final CountDownLatch latch;

        CapturingAckClient(int expected) {
            this.latch = new CountDownLatch(expected);
        }

        @Override
        public void ack(FailoverAck update) {
            acks.add(update);
            latch.countDown();
        }
    }
}
