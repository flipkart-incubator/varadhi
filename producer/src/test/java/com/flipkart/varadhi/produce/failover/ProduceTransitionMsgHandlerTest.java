package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.cluster.events.EventType;
import com.flipkart.varadhi.core.cluster.events.ResourceEvent;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.cluster.failover.TransitionAck;
import com.flipkart.varadhi.entities.cluster.failover.TransitionEvent;
import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    private static final String TARGET_REGION = "region-b";
    private static final String TARGET_STORAGE_TOPIC_ID = "7";

    private Vertx vertx;
    private ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache;
    private CapturingAckClient acker;
    private RecordingWarmer warmer;
    private RecordingWarmer storageWarmer;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setup() throws Exception {
        vertx = Vertx.vertx();
        topicCache = ResourceReadCache.<Resource.EntityResource<VaradhiTopic>>create(
            ResourceType.TOPIC,
            List::of,
            vertx
        ).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        warmer = new RecordingWarmer();
        storageWarmer = new RecordingWarmer();
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterEach
    void tearDown() throws Exception {
        scheduler.shutdownNow();
        vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    private ProduceTransitionMsgHandler handler(PodTransitionConfig config) {
        acker = new CapturingAckClient(1);
        return new ProduceTransitionMsgHandler(
            "host-1",
            topicCache,
            acker,
            Map.of(TransitionType.TOPIC_FAILOVER, warmer, TransitionType.STORAGE_MIGRATION, storageWarmer),
            config,
            scheduler
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
            ClusterMessage.of(TransitionEvent.forPrepare(OP_ID, FQN, 10, TARGET_REGION, TransitionType.TOPIC_FAILOVER))
        );

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        TransitionAck ack = acker.acks.get(0);
        assertEquals(TransitionStage.PREPARE, ack.stage());
        assertTrue(ack.success());
        assertTrue(
            warmer.warmed.contains(FQN + "@" + TARGET_REGION),
            "PREPARE should pre-warm the target region producer"
        );
    }

    @Test
    void storageMigrationPrepareWarmsTargetStorageTopicAndAcksOk() throws Exception {
        seed(10);
        ProduceTransitionMsgHandler h = handler(PodTransitionConfig.defaultConfig());

        h.handle(
            ClusterMessage.of(
                TransitionEvent.forPrepare(OP_ID, FQN, 10, TARGET_STORAGE_TOPIC_ID, TransitionType.STORAGE_MIGRATION)
            )
        );

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        TransitionAck ack = acker.acks.get(0);
        assertEquals(TransitionStage.PREPARE, ack.stage());
        assertTrue(ack.success());
        assertTrue(
            storageWarmer.warmed.contains(FQN + "@" + TARGET_STORAGE_TOPIC_ID),
            "STORAGE_MIGRATION PREPARE should pre-warm the target storage-topic producer"
        );
        assertTrue(warmer.warmed.isEmpty(), "failover warmer must not run for a storage migration");
    }

    @Test
    void prepareAcksFailureWhenWarmFails() throws Exception {
        seed(10);
        warmer.toFail = new RuntimeException("broker unreachable");
        ProduceTransitionMsgHandler h = handler(PodTransitionConfig.defaultConfig());

        h.handle(
            ClusterMessage.of(TransitionEvent.forPrepare(OP_ID, FQN, 10, TARGET_REGION, TransitionType.TOPIC_FAILOVER))
        );

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        TransitionAck ack = acker.acks.get(0);
        assertEquals(TransitionStage.PREPARE, ack.stage());
        assertFalse(ack.success());
        assertTrue(ack.errorMsg().contains("prepare warm failed"));
    }

    @Test
    void prepareAcksFailureWhenStaleOrUnreachable() throws Exception {
        // Topic never appears in the cache -> times out.
        ProduceTransitionMsgHandler h = handler(new PodTransitionConfig(60L, 10L));

        h.handle(
            ClusterMessage.of(TransitionEvent.forPrepare(OP_ID, FQN, 10, TARGET_REGION, TransitionType.TOPIC_FAILOVER))
        );

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        TransitionAck ack = acker.acks.get(0);
        assertEquals(TransitionStage.PREPARE, ack.stage());
        assertFalse(ack.success());
        assertTrue(ack.errorMsg().contains("timeout"));
    }

    @Test
    void switchAcksOkWhenVersionAlreadyPresent() throws Exception {
        seed(11);
        ProduceTransitionMsgHandler h = handler(PodTransitionConfig.defaultConfig());

        h.handle(ClusterMessage.of(TransitionEvent.forSwitch(OP_ID, FQN, 11, TransitionType.TOPIC_FAILOVER)));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        TransitionAck ack = acker.acks.get(0);
        assertEquals(TransitionStage.SWITCH, ack.stage());
        assertTrue(ack.success());
        assertTrue(warmer.warmed.isEmpty(), "SWITCH must not warm the producer");
    }

    @Test
    void switchAcksOkWhenVersionArrivesLater() throws Exception {
        ProduceTransitionMsgHandler h = handler(new PodTransitionConfig(2000L, 5L));
        // The version is not present at first; it propagates into the cache shortly after.
        scheduler.schedule(() -> seed(11), 40, TimeUnit.MILLISECONDS);

        h.handle(ClusterMessage.of(TransitionEvent.forSwitch(OP_ID, FQN, 11, TransitionType.TOPIC_FAILOVER)));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        assertTrue(acker.acks.get(0).success());
    }

    @Test
    void switchAcksFailureOnTimeout() throws Exception {
        ProduceTransitionMsgHandler h = handler(new PodTransitionConfig(60L, 10L));

        h.handle(ClusterMessage.of(TransitionEvent.forSwitch(OP_ID, FQN, 11, TransitionType.TOPIC_FAILOVER)));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        TransitionAck ack = acker.acks.get(0);
        assertFalse(ack.success());
        assertTrue(ack.errorMsg().contains("timeout"));
    }

    @Test
    void switchAcksFailureWhenVersionOvershoots() throws Exception {
        // Cache skipped past the coordinated version (concurrent modification) => fail fast.
        seed(12);
        ProduceTransitionMsgHandler h = handler(PodTransitionConfig.defaultConfig());

        h.handle(ClusterMessage.of(TransitionEvent.forSwitch(OP_ID, FQN, 11, TransitionType.TOPIC_FAILOVER)));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        TransitionAck ack = acker.acks.get(0);
        assertEquals(TransitionStage.SWITCH, ack.stage());
        assertFalse(ack.success());
        assertTrue(ack.errorMsg().contains("overshot"));
    }

    @Test
    void completedAcksOkImmediately() throws Exception {
        ProduceTransitionMsgHandler h = handler(PodTransitionConfig.defaultConfig());

        h.handle(
            ClusterMessage.of(
                TransitionEvent.forStage(OP_ID, FQN, TransitionStage.COMPLETED, TransitionType.TOPIC_FAILOVER)
            )
        );

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        TransitionAck ack = acker.acks.get(0);
        assertEquals(TransitionStage.COMPLETED, ack.stage());
        assertTrue(ack.success());
    }

    @Test
    void abortedAcksOkImmediately() throws Exception {
        ProduceTransitionMsgHandler h = handler(PodTransitionConfig.defaultConfig());

        h.handle(
            ClusterMessage.of(
                TransitionEvent.forStage(OP_ID, FQN, TransitionStage.ABORTED, TransitionType.TOPIC_FAILOVER)
            )
        );

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        TransitionAck ack = acker.acks.get(0);
        assertEquals(TransitionStage.ABORTED, ack.stage());
        assertTrue(ack.success());
    }

    @Test
    void drainAcksOkImmediatelyWithoutVersionWait() throws Exception {
        ProduceTransitionMsgHandler h = handler(PodTransitionConfig.defaultConfig());

        h.handle(
            ClusterMessage.of(
                TransitionEvent.forStage(OP_ID, FQN, TransitionStage.DRAIN, TransitionType.TOPIC_FAILOVER)
            )
        );

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        TransitionAck ack = acker.acks.get(0);
        assertEquals(TransitionStage.DRAIN, ack.stage());
        assertTrue(ack.success());
    }

    /** Records warm requests; returns a future that fails when {@code toFail} is set. */
    private static final class RecordingWarmer implements BiFunction<String, String, CompletableFuture<Void>> {
        private final CopyOnWriteArrayList<String> warmed = new CopyOnWriteArrayList<>();
        private volatile RuntimeException toFail;

        @Override
        public CompletableFuture<Void> apply(String topicFqn, String target) {
            if (toFail != null) {
                return CompletableFuture.failedFuture(toFail);
            }
            warmed.add(topicFqn + "@" + target);
            return CompletableFuture.completedFuture(null);
        }
    }


    private static final class CapturingAckClient implements TransitionAckClient {
        private final CopyOnWriteArrayList<TransitionAck> acks = new CopyOnWriteArrayList<>();
        private final CountDownLatch latch;

        CapturingAckClient(int expected) {
            this.latch = new CountDownLatch(expected);
        }

        @Override
        public void ack(TransitionAck ack) {
            acks.add(ack);
            latch.countDown();
        }
    }
}
