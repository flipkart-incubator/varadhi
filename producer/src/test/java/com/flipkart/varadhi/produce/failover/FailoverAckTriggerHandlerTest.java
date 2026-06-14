package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.cluster.failover.FailoverStage;
import com.flipkart.varadhi.entities.cluster.failover.FailoverStageEvent;
import com.flipkart.varadhi.entities.cluster.failover.FailoverStatusUpdate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FailoverAckTriggerHandlerTest {

    private static final String OP_ID = "op-1";
    private static final String FQN = "proj.topic1";

    private ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache;
    private CapturingAcker acker;
    private ScheduledExecutorService scheduler;

    @SuppressWarnings ("unchecked")
    @BeforeEach
    void setup() {
        topicCache = mock(ResourceReadCache.class);
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    private FailoverAckTriggerHandler handler(PodFailoverConfig config) {
        acker = new CapturingAcker(1);
        return new FailoverAckTriggerHandler("host-1", topicCache, acker, config, scheduler);
    }

    @Test
    void prepareAcksOkWhenCaughtUpToCurrentVersion() throws Exception {
        Resource.EntityResource<VaradhiTopic> atV10 = topicAtVersion(10);
        when(topicCache.get(FQN)).thenReturn(Optional.of(atV10));
        FailoverAckTriggerHandler h = handler(PodFailoverConfig.defaultConfig());

        h.handle(ClusterMessage.of(FailoverStageEvent.forPrepare(OP_ID, FQN, 10)));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        FailoverStatusUpdate ack = acker.acks.get(0);
        assertEquals(FailoverStage.PREPARE, ack.stage());
        assertTrue(ack.success());
    }

    @Test
    void prepareAcksFailureWhenStaleOrUnreachable() throws Exception {
        when(topicCache.get(FQN)).thenReturn(Optional.empty());
        FailoverAckTriggerHandler h = handler(new PodFailoverConfig(60L, 10L));

        h.handle(ClusterMessage.of(FailoverStageEvent.forPrepare(OP_ID, FQN, 10)));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        FailoverStatusUpdate ack = acker.acks.get(0);
        assertEquals(FailoverStage.PREPARE, ack.stage());
        assertFalse(ack.success());
        assertTrue(ack.errorMsg().contains("timeout"));
    }

    @Test
    void switchAcksOkWhenVersionAlreadyPresent() throws Exception {
        Resource.EntityResource<VaradhiTopic> atV11 = topicAtVersion(11);
        when(topicCache.get(FQN)).thenReturn(Optional.of(atV11));
        FailoverAckTriggerHandler h = handler(PodFailoverConfig.defaultConfig());

        h.handle(ClusterMessage.of(FailoverStageEvent.forSwitch(OP_ID, FQN, 11)));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        FailoverStatusUpdate ack = acker.acks.get(0);
        assertEquals(FailoverStage.SWITCH, ack.stage());
        assertTrue(ack.success());
    }

    @Test
    void switchAcksOkWhenVersionArrivesLater() throws Exception {
        Resource.EntityResource<VaradhiTopic> atV11 = topicAtVersion(11);
        when(topicCache.get(FQN)).thenReturn(Optional.empty(), Optional.empty(), Optional.of(atV11));
        FailoverAckTriggerHandler h = handler(new PodFailoverConfig(2000L, 5L));

        h.handle(ClusterMessage.of(FailoverStageEvent.forSwitch(OP_ID, FQN, 11)));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        assertTrue(acker.acks.get(0).success());
    }

    @Test
    void switchAcksFailureOnTimeout() throws Exception {
        when(topicCache.get(FQN)).thenReturn(Optional.empty());
        FailoverAckTriggerHandler h = handler(new PodFailoverConfig(60L, 10L));

        h.handle(ClusterMessage.of(FailoverStageEvent.forSwitch(OP_ID, FQN, 11)));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        FailoverStatusUpdate ack = acker.acks.get(0);
        assertFalse(ack.success());
        assertTrue(ack.errorMsg().contains("timeout"));
    }

    @Test
    void completedAcksOkImmediately() throws Exception {
        FailoverAckTriggerHandler h = handler(PodFailoverConfig.defaultConfig());

        h.handle(ClusterMessage.of(FailoverStageEvent.forStage(OP_ID, FQN, FailoverStage.COMPLETED)));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        FailoverStatusUpdate ack = acker.acks.get(0);
        assertEquals(FailoverStage.COMPLETED, ack.stage());
        assertTrue(ack.success());
    }

    @Test
    void abortedAcksOkImmediately() throws Exception {
        FailoverAckTriggerHandler h = handler(PodFailoverConfig.defaultConfig());

        h.handle(ClusterMessage.of(FailoverStageEvent.forStage(OP_ID, FQN, FailoverStage.ABORTED)));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        FailoverStatusUpdate ack = acker.acks.get(0);
        assertEquals(FailoverStage.ABORTED, ack.stage());
        assertTrue(ack.success());
    }

    @Test
    void drainAcksOkImmediatelyWithoutVersionWait() throws Exception {
        // No version to await => acks without ever consulting the topic cache.
        FailoverAckTriggerHandler h = handler(PodFailoverConfig.defaultConfig());

        h.handle(ClusterMessage.of(FailoverStageEvent.forStage(OP_ID, FQN, FailoverStage.DRAIN)));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        FailoverStatusUpdate ack = acker.acks.get(0);
        assertEquals(FailoverStage.DRAIN, ack.stage());
        assertTrue(ack.success());
    }

    @SuppressWarnings ("unchecked")
    private Resource.EntityResource<VaradhiTopic> topicAtVersion(int version) {
        Resource.EntityResource<VaradhiTopic> res = mock(Resource.EntityResource.class);
        when(res.getVersion()).thenReturn(version);
        return res;
    }

    private static final class CapturingAcker implements FailoverAcker {
        private final CopyOnWriteArrayList<FailoverStatusUpdate> acks = new CopyOnWriteArrayList<>();
        private final CountDownLatch latch;

        CapturingAcker(int expected) {
            this.latch = new CountDownLatch(expected);
        }

        @Override
        public void ack(FailoverStatusUpdate update) {
            acks.add(update);
            latch.countDown();
        }
    }
}
