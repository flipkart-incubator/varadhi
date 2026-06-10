package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.cluster.failover.FailoverStage;
import com.flipkart.varadhi.entities.cluster.failover.FailoverStageEvent;
import com.flipkart.varadhi.entities.cluster.failover.FailoverStatusUpdate;
import com.flipkart.varadhi.spi.services.BrokerWarmer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FailoverAckTriggerHandlerTest {

    private static final String OP_ID = "op-1";
    private static final String FQN = "proj.topic1";
    private static final long FENCE = 7L;

    private ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache;
    private BrokerWarmer brokerWarmer;
    private CapturingAcker acker;
    private ScheduledExecutorService scheduler;

    @SuppressWarnings ("unchecked")
    @BeforeEach
    void setup() {
        topicCache = mock(ResourceReadCache.class);
        brokerWarmer = mock(BrokerWarmer.class);
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    private FailoverAckTriggerHandler handler(String localRegion, PodFailoverConfig config) {
        acker = new CapturingAcker(1);
        return new FailoverAckTriggerHandler("host-1", localRegion, topicCache, brokerWarmer, acker, config, scheduler);
    }

    @Test
    void prepareInTargetRegionWarmsThenAcksOk() throws Exception {
        when(brokerWarmer.warm(FQN, "us-east")).thenReturn(CompletableFuture.completedFuture(null));
        FailoverAckTriggerHandler h = handler("us-east", PodFailoverConfig.defaultConfig());

        h.handle(ClusterMessage.of(FailoverStageEvent.forPrepare(OP_ID, FQN, FENCE, "us-east")));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        verify(brokerWarmer, times(1)).warm(FQN, "us-east");
        FailoverStatusUpdate ack = acker.acks.get(0);
        assertEquals(FailoverStage.PREPARE, ack.stage());
        assertEquals(FENCE, ack.fenceVersion());
        assertTrue(ack.ok());
    }

    @Test
    void prepareInOtherRegionAcksOkWithoutWarming() throws Exception {
        FailoverAckTriggerHandler h = handler("ap-south", PodFailoverConfig.defaultConfig());

        h.handle(ClusterMessage.of(FailoverStageEvent.forPrepare(OP_ID, FQN, FENCE, "us-east")));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        verify(brokerWarmer, never()).warm(any(), any());
        assertTrue(acker.acks.get(0).ok());
    }

    @Test
    void prepareAcksFailureWhenWarmFails() throws Exception {
        when(brokerWarmer.warm(FQN, "us-east")).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("cold"))
        );
        FailoverAckTriggerHandler h = handler("us-east", PodFailoverConfig.defaultConfig());

        h.handle(ClusterMessage.of(FailoverStageEvent.forPrepare(OP_ID, FQN, FENCE, "us-east")));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        assertFalse(acker.acks.get(0).ok());
    }

    @Test
    void switchAcksOkWhenVersionAlreadyPresent() throws Exception {
        Resource.EntityResource<VaradhiTopic> atV11 = topicAtVersion(11);
        when(topicCache.get(FQN)).thenReturn(Optional.of(atV11));
        FailoverAckTriggerHandler h = handler("us-east", PodFailoverConfig.defaultConfig());

        h.handle(ClusterMessage.of(FailoverStageEvent.forSwitch(OP_ID, FQN, FENCE, 11)));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        FailoverStatusUpdate ack = acker.acks.get(0);
        assertEquals(FailoverStage.SWITCH, ack.stage());
        assertTrue(ack.ok());
    }

    @Test
    void switchAcksOkWhenVersionArrivesLater() throws Exception {
        Resource.EntityResource<VaradhiTopic> atV11 = topicAtVersion(11);
        when(topicCache.get(FQN)).thenReturn(Optional.empty(), Optional.empty(), Optional.of(atV11));
        FailoverAckTriggerHandler h = handler("us-east", new PodFailoverConfig(2000L, 5L));

        h.handle(ClusterMessage.of(FailoverStageEvent.forSwitch(OP_ID, FQN, FENCE, 11)));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        assertTrue(acker.acks.get(0).ok());
    }

    @Test
    void switchAcksFailureOnTimeout() throws Exception {
        when(topicCache.get(FQN)).thenReturn(Optional.empty());
        FailoverAckTriggerHandler h = handler("us-east", new PodFailoverConfig(60L, 10L));

        h.handle(ClusterMessage.of(FailoverStageEvent.forSwitch(OP_ID, FQN, FENCE, 11)));

        assertTrue(acker.latch.await(2, TimeUnit.SECONDS));
        FailoverStatusUpdate ack = acker.acks.get(0);
        assertFalse(ack.ok());
        assertTrue(ack.errorMsg().contains("timeout"));
    }

    @Test
    void terminalStageDoesNotAck() throws Exception {
        FailoverAckTriggerHandler h = handler("us-east", PodFailoverConfig.defaultConfig());

        h.handle(ClusterMessage.of(FailoverStageEvent.forTerminal(OP_ID, FQN, FailoverStage.COMPLETED, FENCE)));

        // No ack expected for terminal stages; give the scheduler a brief window.
        assertFalse(acker.latch.await(200, TimeUnit.MILLISECONDS));
        assertEquals(0, acker.acks.size());
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
        private final AtomicInteger count = new AtomicInteger();

        CapturingAcker(int expected) {
            this.latch = new CountDownLatch(expected);
        }

        @Override
        public void ack(FailoverStatusUpdate update) {
            acks.add(update);
            count.incrementAndGet();
            latch.countDown();
        }
    }
}
