package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.controller.impl.failover.StageAwaiter;
import com.flipkart.varadhi.controller.impl.failover.TopicFailoverConfig;
import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.core.cluster.consumer.ConsumerClientFactory;
import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.SegmentedStorageTopic;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.cluster.TopicFailoverOperation;
import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TopicFailoverRequest;
import com.flipkart.varadhi.entities.cluster.failover.TransitionObject;
import com.flipkart.varadhi.spi.db.SubscriptionStore;
import com.flipkart.varadhi.spi.db.TopicStore;
import com.flipkart.varadhi.spi.db.TransitionStore;
import lombok.EqualsAndHashCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ControllerApiMgrFailoverTest {

    private static final String FQN = "proj.topic";
    private static final RegionName SOURCE = RegionName.of("r1");
    private static final RegionName TARGET = RegionName.of("r2");

    @EqualsAndHashCode (callSuper = true)
    private static final class DummyStorageTopic extends StorageTopic {
        private DummyStorageTopic(String name) {
            super(0, name);
        }
    }

    private TransitionStore transitionStore;
    private TopicStore topicStore;
    private OperationMgr operationMgr;
    private ControllerApiMgr apiMgr;

    @BeforeEach
    void setup() {
        operationMgr = mock(OperationMgr.class);
        transitionStore = mock(TransitionStore.class);
        topicStore = mock(TopicStore.class);
        apiMgr = new ControllerApiMgr(
            operationMgr,
            mock(AssignmentManager.class),
            mock(SubscriptionStore.class),
            mock(ConsumerClientFactory.class),
            transitionStore,
            topicStore,
            mock(VaradhiClusterManager.class),
            mock(MessageExchange.class),
            new StageAwaiter(),
            TopicFailoverConfig.defaultConfig()
        );
    }

    private VaradhiTopic topicWithRegions() {
        VaradhiTopic topic = VaradhiTopic.of(
            "proj",
            "topic",
            false,
            new TopicCapacityPolicy(100, 400, 2, 2),
            LifecycleStatus.ActionCode.SYSTEM_ACTION
        );
        topic.addInternalTopic(SOURCE.value(), SegmentedStorageTopic.of(new DummyStorageTopic(FQN + SOURCE.value())));
        topic.addInternalTopic(TARGET.value(), SegmentedStorageTopic.of(new DummyStorageTopic(FQN + TARGET.value())));
        return topic;
    }

    @Test
    void createPersistsTransitionAndEnqueues() throws Exception {
        when(topicStore.get(FQN)).thenReturn(topicWithRegions());
        when(transitionStore.exists(FQN)).thenReturn(false);

        TopicFailoverRequest request = new TopicFailoverRequest(SOURCE, TARGET, false, "tester");
        TopicFailoverOperation op = apiMgr.createTopicFailover(FQN, request).get();

        assertEquals(FQN, op.getTopicFqn());
        assertEquals(SOURCE, op.getSourceRegion());
        assertEquals(TARGET, op.getTargetRegion());
        verify(transitionStore).create(any(TransitionObject.class));
        verify(operationMgr).createAndEnqueueTopicFailover(eq(op), any());
    }

    @Test
    void createRejectsWhenActiveFailoverExists() {
        when(topicStore.get(FQN)).thenReturn(topicWithRegions());
        when(transitionStore.exists(FQN)).thenReturn(true);

        ExecutionException ex = assertThrows(
            ExecutionException.class,
            () -> apiMgr.createTopicFailover(FQN, new TopicFailoverRequest(SOURCE, TARGET, false, "t")).get()
        );
        assertInstanceOf(InvalidOperationForResourceException.class, ex.getCause());
    }

    @Test
    void createRejectsUnknownRegion() {
        when(topicStore.get(FQN)).thenReturn(topicWithRegions());

        ExecutionException ex = assertThrows(
            ExecutionException.class,
            () -> apiMgr.createTopicFailover(FQN, new TopicFailoverRequest(SOURCE, RegionName.of("nope"), false, "t"))
                        .get()
        );
        assertInstanceOf(IllegalArgumentException.class, ex.getCause());
    }

    @Test
    void createRejectsSameSourceAndTarget() {
        when(topicStore.get(FQN)).thenReturn(topicWithRegions());

        ExecutionException ex = assertThrows(
            ExecutionException.class,
            () -> apiMgr.createTopicFailover(FQN, new TopicFailoverRequest(SOURCE, SOURCE, false, "t")).get()
        );
        assertInstanceOf(IllegalArgumentException.class, ex.getCause());
    }

    @Test
    void createRejectsWhenSourceNotActiveRegion() {
        when(topicStore.get(FQN)).thenReturn(topicWithRegions());

        ExecutionException ex = assertThrows(
            ExecutionException.class,
            () -> apiMgr.createTopicFailover(FQN, new TopicFailoverRequest(TARGET, SOURCE, false, "t")).get()
        );
        assertInstanceOf(IllegalArgumentException.class, ex.getCause());
        assertTrue(ex.getCause().getMessage().contains("activeRegion"));
    }

    @Test
    void abortRejectsWhenNoActiveFailover() {
        when(transitionStore.exists(FQN)).thenReturn(false);

        ExecutionException ex = assertThrows(
            ExecutionException.class,
            () -> apiMgr.abortTopicFailover(FQN, "tester").get()
        );
        assertInstanceOf(ResourceNotFoundException.class, ex.getCause());
    }

    @Test
    void abortRejectsWhenNotAbortable() {
        TransitionObject transition = TransitionObject.forFailover("op", FQN, SOURCE, TARGET);
        transition.advanceTo(TransitionStage.SWITCH, 1L); // past the abortable window
        when(transitionStore.exists(FQN)).thenReturn(true);
        when(transitionStore.get(FQN)).thenReturn(transition);

        ExecutionException ex = assertThrows(
            ExecutionException.class,
            () -> apiMgr.abortTopicFailover(FQN, "tester").get()
        );
        assertInstanceOf(InvalidOperationForResourceException.class, ex.getCause());
    }

    @Test
    void abortDeletesTransitionWhenAbortable() throws Exception {
        TransitionObject transition = TransitionObject.forFailover("op", FQN, SOURCE, TARGET);
        transition.advanceTo(TransitionStage.PREPARE, 1L);
        when(transitionStore.exists(FQN)).thenReturn(true);
        when(transitionStore.get(FQN)).thenReturn(transition);

        apiMgr.abortTopicFailover(FQN, "tester").get();

        verify(transitionStore).delete(FQN);
    }
}
