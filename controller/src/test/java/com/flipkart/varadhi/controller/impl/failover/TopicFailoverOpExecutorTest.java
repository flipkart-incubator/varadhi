package com.flipkart.varadhi.controller.impl.failover;

import com.flipkart.varadhi.controller.OperationMgr;
import com.flipkart.varadhi.core.cluster.ComponentKind;
import com.flipkart.varadhi.core.cluster.MemberInfo;
import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.core.cluster.failover.TransitionBusAddress;
import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.ProduceConfig;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.SegmentedStorageTopic;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.TopicState;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.cluster.TopicFailoverOperation;
import com.flipkart.varadhi.entities.cluster.failover.TransitionMaster;
import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.spi.db.TopicStore;
import com.flipkart.varadhi.spi.db.TransitionStore;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import io.vertx.core.Future;
import lombok.EqualsAndHashCode;
import org.mockito.ArgumentCaptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TopicFailoverOpExecutorTest {

    private static final String FQN = "proj.topic";
    private static final RegionName SOURCE = RegionName.of("r1");
    private static final RegionName TARGET = RegionName.of("r2");

    @EqualsAndHashCode (callSuper = true)
    private static final class DummyStorageTopic extends StorageTopic {
        private DummyStorageTopic(String name) {
            super(0, name);
        }
    }

    private OperationMgr operationMgr;
    private TransitionStore transitionStore;
    private TopicStore topicStore;
    private MessageExchange messageExchange;
    private StageAwaiter stageAwaiter;
    private VaradhiClusterManager clusterManager;
    private StorageTopicService storageTopicService;
    private TopicFailoverOpExecutor executor;

    private VaradhiTopic topic;
    private TransitionMaster transition;
    private TopicFailoverOperation op;

    @BeforeEach
    void setup() {
        operationMgr = mock(OperationMgr.class);
        transitionStore = mock(TransitionStore.class);
        topicStore = mock(TopicStore.class);
        messageExchange = mock(MessageExchange.class);
        stageAwaiter = mock(StageAwaiter.class);
        clusterManager = mock(VaradhiClusterManager.class);
        storageTopicService = mock(StorageTopicService.class);
        when(storageTopicService.getReplicationLag(any(), any(), any())).thenReturn(0L);

        topic = VaradhiTopic.of(
            "proj",
            "topic",
            false,
            new TopicCapacityPolicy(100, 400, 2, 2),
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            null,
            VaradhiTopic.TopicCategory.TOPIC,
            null,
            null,
            null,
            SegmentedStorageTopic.of(new DummyStorageTopic(FQN)),
            false,
            Map.of(SOURCE, ProduceConfig.producing(), TARGET, ProduceConfig.blocked())
        );
        topic.setVersion(1);

        op = TopicFailoverOperation.of(FQN, SOURCE, TARGET, false, "tester");
        transition = TransitionMaster.forFailover(op.getId(), FQN, SOURCE, TARGET);

        AtomicReference<VaradhiTopic> topicRef = new AtomicReference<>(topic);
        when(topicStore.get(FQN)).thenAnswer(invocation -> topicRef.get());
        doAnswer(invocation -> {
            topicRef.set(invocation.getArgument(0));
            return null;
        }).when(topicStore).update(any(VaradhiTopic.class));
        when(transitionStore.exists(FQN)).thenReturn(true);
        when(transitionStore.get(FQN)).thenReturn(transition);
        when(stageAwaiter.expect(anyString(), any(), anySet(), anyLong())).thenReturn(
            CompletableFuture.completedFuture(null)
        );
        when(clusterManager.getAllMembers()).thenReturn(
            Future.succeededFuture(
                List.of(new MemberInfo("host-1", "", 0, new ComponentKind[] {ComponentKind.Server}, null, null))
            )
        );

        executor = new TopicFailoverOpExecutor(
            operationMgr,
            transitionStore,
            topicStore,
            storageTopicService,
            messageExchange,
            stageAwaiter,
            clusterManager,
            TopicFailoverConfig.defaultConfig(),
            SOURCE
        );
    }

    @Test
    void happyPathSwitchesProduceAndCompletes() throws Exception {
        executor.execute(op).get(5, TimeUnit.SECONDS);

        ArgumentCaptor<VaradhiTopic> updated = ArgumentCaptor.forClass(VaradhiTopic.class);
        verify(topicStore, times(2)).update(updated.capture());
        VaradhiTopic afterSwitch = updated.getAllValues().get(0);
        assertEquals(TopicState.Fenced, afterSwitch.getProduceConfig(SOURCE).orElseThrow().getState());
        assertEquals(TopicState.Fenced, afterSwitch.getProduceConfig(TARGET).orElseThrow().getState());
        VaradhiTopic afterComplete = updated.getAllValues().get(1);
        assertEquals(TopicState.Blocked, afterComplete.getProduceConfig(SOURCE).orElseThrow().getState());
        assertEquals(TopicState.Producing, afterComplete.getProduceConfig(TARGET).orElseThrow().getState());
        assertEquals(TransitionStage.COMPLETED, transition.getCurrentStage());
        verify(transitionStore).delete(FQN);

        verify(stageAwaiter).expect(anyString(), eq(TransitionStage.PREPARE), anySet(), anyLong());
        verify(stageAwaiter).expect(anyString(), eq(TransitionStage.SWITCH), anySet(), anyLong());
        verify(stageAwaiter, times(0)).expect(anyString(), eq(TransitionStage.DRAIN), anySet(), anyLong());
        verify(messageExchange, times(4)).publish(
            eq(TransitionBusAddress.ROUTE_TOPIC_TRANSITION),
            eq(TransitionBusAddress.STAGE_BROADCAST_API),
            any()
        );

        verify(operationMgr, times(5)).updateTopicFailoverOp(op);
        assertEquals(com.flipkart.varadhi.entities.cluster.Operation.State.COMPLETED, op.getState());
        assertFalse(op.getStageHistory().isEmpty());
        assertEquals(TransitionStage.COMPLETED, op.getStageHistory().get(op.getStageHistory().size() - 1).getStage());
    }

    @Test
    void noActiveTransitionFailsFast() {
        when(transitionStore.exists(FQN)).thenReturn(false);

        CompletableFuture<Void> result = executor.execute(op);

        assertEquals(true, result.isCompletedExceptionally());
    }
}
