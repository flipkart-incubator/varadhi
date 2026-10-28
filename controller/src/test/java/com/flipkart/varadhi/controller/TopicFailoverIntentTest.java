package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.core.cluster.consumer.ConsumerClientFactory;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.db.OpStoreImpl;
import com.flipkart.varadhi.db.TransitionStoreImpl;
import com.flipkart.varadhi.db.ZKMetaStore;
import com.flipkart.varadhi.entities.ProduceConfig;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.TopicState;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.VaradhiTopicTestUtils;
import com.flipkart.varadhi.entities.cluster.Operation;
import com.flipkart.varadhi.entities.cluster.transition.failover.TopicFailoverOperation;
import com.flipkart.varadhi.entities.cluster.transition.failover.ProduceFailoverTransition;
import com.flipkart.varadhi.entities.cluster.transition.failover.TopicFailoverRequest;
import com.flipkart.varadhi.entities.cluster.transition.TransitionEvent;
import com.flipkart.varadhi.entities.cluster.transition.TransitionStage;
import com.flipkart.varadhi.spi.db.OpStore;
import com.flipkart.varadhi.spi.db.RegionStore;
import com.flipkart.varadhi.spi.db.TopicStore;
import com.flipkart.varadhi.spi.db.TransitionStore;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.concurrent.CompletionException;

import static com.flipkart.varadhi.core.cluster.transition.TransitionBusAddress.ROUTE_TOPIC_TRANSITION;
import static com.flipkart.varadhi.core.cluster.transition.TransitionBusAddress.STAGE_BROADCAST_API;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * PR 345 intent path: real ZooKeeper for the lock and the op, topic/region reads stubbed.
 */
class TopicFailoverIntentTest {

    private TestingServer zkServer;
    private CuratorFramework curator;
    private TransitionStore transitions;
    private OpStore opStore;
    private MessageExchange messageExchange;
    private SubscriptionService service;
    private VaradhiTopic topic;

    @BeforeEach
    void setUp() throws Exception {
        zkServer = new TestingServer();
        curator = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new ExponentialBackoffRetry(1000, 1));
        curator.start();
        ZKMetaStore zk = new ZKMetaStore(curator);
        transitions = new TransitionStoreImpl(zk);
        opStore = new OpStoreImpl(zk);
        topic = VaradhiTopicTestUtils.getNewTopic(
            Map.of(RegionName.of("r1"), ProduceConfig.producing(), RegionName.of("r2"), ProduceConfig.blocked())
        );
        TopicStore topics = mock(TopicStore.class);
        when(topics.get(topic.getName())).thenReturn(topic);
        RegionStore regions = mock(RegionStore.class);
        when(regions.exists("r1")).thenReturn(true);
        when(regions.exists("r2")).thenReturn(true);
        messageExchange = mock(MessageExchange.class);
        service = new SubscriptionService(
            new OperationMgr(2, opStore, new RetryPolicy(0, 1, 1, 1)),
            mock(AssignmentManager.class),
            mock(com.flipkart.varadhi.spi.db.SubscriptionStore.class),
            mock(ConsumerClientFactory.class),
            transitions,
            topics,
            regions,
            mock(StorageTopicService.class),
            mock(VaradhiClusterManager.class),
            messageExchange,
            RegionName.of("r1")
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        curator.close();
        zkServer.close();
    }

    @Test
    void createGetThenAbort_leavesCompletedOpAndNoActiveLock() {
        TopicFailoverRequest request = new TopicFailoverRequest(RegionName.of("r1"), RegionName.of("r2"), true);
        TopicFailoverOperation created = service.createTopicFailover(topic.getName(), request, "thanos").join();

        assertEquals(Operation.State.IN_PROGRESS, created.getState());
        assertNull(created.getErrorMsg());
        assertEquals("thanos", created.getRequestedBy());

        ProduceFailoverTransition live = transitions.get(topic.getName());
        assertEquals(created.getId(), live.getOperationId());
        assertEquals(TransitionStage.PENDING, live.getCurrentStage());
        assertEquals(RegionName.of("r1"), live.getSourceRegion());
        assertEquals(RegionName.of("r2"), live.getTargetRegion());
        assertTrue(live.isWaitForReplicationLagToClear());
        assertTrue(live.getStageHistory().isEmpty());

        assertEquals(created.getId(), service.getTopicFailover(topic.getName()).join().getId());

        CompletionException duplicate = assertThrows(
            CompletionException.class,
            () -> service.createTopicFailover(topic.getName(), request, "thanos").join()
        );
        assertTrue(duplicate.getCause().getMessage().contains("active failover already exists"));

        TopicFailoverOperation aborted = service.abortTopicFailover(topic.getName(), "thanos").join();
        assertEquals(Operation.State.COMPLETED, aborted.getState());
        assertEquals(Operation.State.COMPLETED, opStore.getTopicFailoverOp(created.getId()).getState());
        assertFalse(transitions.exists(topic.getName()));
        assertThrows(CompletionException.class, () -> service.getTopicFailover(topic.getName()).join());

        ArgumentCaptor<ClusterMessage> published = ArgumentCaptor.forClass(ClusterMessage.class);
        verify(messageExchange).publish(
            org.mockito.ArgumentMatchers.eq(ROUTE_TOPIC_TRANSITION),
            org.mockito.ArgumentMatchers.eq(STAGE_BROADCAST_API),
            published.capture()
        );
        TransitionEvent event = published.getValue().getData(TransitionEvent.class);
        assertEquals(TransitionStage.ABORTED, event.stage());
        assertEquals(created.getId(), event.opId());
        assertEquals(TopicState.Producing, topic.getProduceConfig(RegionName.of("r1")).orElseThrow().getState());
    }

    @Test
    void abortRefusedWhenStageIsMigrate() {
        TopicFailoverRequest request = new TopicFailoverRequest(null, RegionName.of("r2"), false);
        TopicFailoverOperation created = service.createTopicFailover(topic.getName(), request, "thanos").join();
        ProduceFailoverTransition live = transitions.get(topic.getName());
        assertEquals(RegionName.of("r1"), live.getSourceRegion());
        live.advanceTo(TransitionStage.MIGRATE, 1L, Map.of(), null);
        transitions.update(live);

        CompletionException refused = assertThrows(
            CompletionException.class,
            () -> service.abortTopicFailover(topic.getName(), "thanos").join()
        );
        assertTrue(refused.getCause().getMessage().contains("not abortable"));
        assertEquals(TransitionStage.MIGRATE, transitions.get(topic.getName()).getCurrentStage());
        assertEquals(Operation.State.IN_PROGRESS, opStore.getTopicFailoverOp(created.getId()).getState());
    }
}
