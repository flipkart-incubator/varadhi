package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.entities.ProduceConfig;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.VaradhiTopicTestUtils;
import com.flipkart.varadhi.entities.cluster.Operation;
import com.flipkart.varadhi.entities.cluster.transition.TransitionStage;
import com.flipkart.varadhi.entities.cluster.transition.failover.ProduceFailoverTransition;
import com.flipkart.varadhi.entities.cluster.transition.failover.TopicFailoverOperation;
import com.flipkart.varadhi.entities.cluster.transition.failover.TopicFailoverRequest;
import com.flipkart.varadhi.spi.db.OpStore;
import com.flipkart.varadhi.spi.db.RegionStore;
import com.flipkart.varadhi.spi.db.TopicStore;
import com.flipkart.varadhi.spi.db.TransitionStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TransitionServiceTest {

    private TransitionStore transitions;
    private OpStore opStore;
    private OperationMgr operationMgr;
    private MessageExchange messageExchange;
    private TransitionService service;
    private VaradhiTopic topic;

    @BeforeEach
    void setUp() {
        topic = VaradhiTopicTestUtils.getNewTopic(
            Map.of(RegionName.of("r1"), ProduceConfig.producing(), RegionName.of("r2"), ProduceConfig.blocked())
        );
        TopicStore topics = mock(TopicStore.class);
        when(topics.get(topic.getName())).thenReturn(topic);
        RegionStore regions = mock(RegionStore.class);
        when(regions.exists("r1")).thenReturn(true);
        when(regions.exists("r2")).thenReturn(true);
        transitions = mock(TransitionStore.class);
        opStore = mock(OpStore.class);
        when(opStore.getTopicFailoverOp(any())).thenAnswer(inv -> {
            // persistTopicFailoverOp re-reads; create path only hits this on failure
            return TopicFailoverOperation.of(topic.getName(), "thanos");
        });
        operationMgr = new OperationMgr(2, opStore, new RetryPolicy(0, 1, 1, 1));
        messageExchange = mock(MessageExchange.class);
        service = new TransitionService(
            messageExchange,
            operationMgr,
            transitions,
            topics,
            regions,
            RegionName.of("r1")
        );
    }

    @Test
    void createPersistsOpBeforeTransition() {
        when(transitions.exists(topic.getName())).thenReturn(false);
        TopicFailoverRequest request = new TopicFailoverRequest(RegionName.of("r1"), RegionName.of("r2"), true);

        TopicFailoverOperation created = service.createTopicFailover(topic.getName(), request, "thanos").join();

        assertEquals(Operation.State.IN_PROGRESS, created.getState());
        InOrder order = inOrder(opStore, transitions);
        order.verify(opStore).createTopicFailoverOp(any(TopicFailoverOperation.class));
        order.verify(transitions).create(any(ProduceFailoverTransition.class));
    }

    @Test
    void getReturnsTransitionNotOp() {
        ProduceFailoverTransition live = ProduceFailoverTransition.forFailover(
            "op-1",
            topic.getName(),
            RegionName.of("r1"),
            RegionName.of("r2"),
            false
        );
        when(transitions.exists(topic.getName())).thenReturn(true);
        when(transitions.get(topic.getName())).thenReturn(live);

        assertSame(live, service.getTopicFailover(topic.getName()).join());
    }

    @Test
    void abortRecordsRequestWithoutDeletingOrBroadcasting() {
        ProduceFailoverTransition live = ProduceFailoverTransition.forFailover(
            "op-1",
            topic.getName(),
            RegionName.of("r1"),
            RegionName.of("r2"),
            false
        );
        when(transitions.exists(topic.getName())).thenReturn(true);
        when(transitions.get(topic.getName())).thenReturn(live);

        ProduceFailoverTransition aborted = service.abortTopicFailover(topic.getName(), "thanos").join();

        assertTrue(aborted.isAbortRequested());
        verify(transitions).update(live);
        verify(transitions, never()).delete(topic.getName());
        verify(messageExchange, never()).publish(any(), any(), any());
    }

    @Test
    void abortRefusedWhenStageIsMigrate() {
        ProduceFailoverTransition live = ProduceFailoverTransition.forFailover(
            "op-1",
            topic.getName(),
            RegionName.of("r1"),
            RegionName.of("r2"),
            false
        );
        live.advanceTo(TransitionStage.MIGRATE, 1L, Map.of(), null);
        when(transitions.exists(topic.getName())).thenReturn(true);
        when(transitions.get(topic.getName())).thenReturn(live);

        CompletionException refused = assertThrows(
            CompletionException.class,
            () -> service.abortTopicFailover(topic.getName(), "thanos").join()
        );
        assertTrue(refused.getCause() instanceof InvalidOperationForResourceException);
        assertTrue(refused.getCause().getMessage().contains("not abortable"));
        verify(transitions, never()).update(any());
        assertFalse(live.isAbortRequested());
    }

    @Test
    void listReturnsStoreContents() {
        ProduceFailoverTransition live = ProduceFailoverTransition.forFailover(
            "op-1",
            topic.getName(),
            RegionName.of("r1"),
            RegionName.of("r2"),
            false
        );
        when(transitions.listActive()).thenReturn(List.of(live));
        assertEquals(List.of(live), service.getActiveFailovers().join());
    }
}
