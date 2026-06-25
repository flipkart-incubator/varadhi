package com.flipkart.varadhi.failover;

import com.flipkart.varadhi.controller.OperationMgr;
import com.flipkart.varadhi.controller.impl.failover.StageAwaiter;
import com.flipkart.varadhi.controller.impl.failover.TopicFailoverConfig;
import com.flipkart.varadhi.controller.impl.failover.TopicFailoverOpExecutor;
import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.cluster.ComponentKind;
import com.flipkart.varadhi.core.cluster.MemberInfo;
import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.MessageRouter;
import com.flipkart.varadhi.core.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.core.cluster.controller.ControllerApi;
import com.flipkart.varadhi.core.cluster.events.EventType;
import com.flipkart.varadhi.core.cluster.events.ResourceEvent;
import com.flipkart.varadhi.core.cluster.failover.TransitionBusAddress;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.entities.TopicState;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.VaradhiTopicName;
import com.flipkart.varadhi.entities.cluster.Operation;
import com.flipkart.varadhi.entities.cluster.TopicFailoverOperation;
import com.flipkart.varadhi.entities.cluster.failover.TransitionAck;
import com.flipkart.varadhi.entities.cluster.failover.TransitionObject;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import com.flipkart.varadhi.produce.failover.ControllerTransitionAckClient;
import com.flipkart.varadhi.produce.failover.ProduceTransitionMsgHandler;
import com.flipkart.varadhi.produce.failover.PodTransitionConfig;
import com.flipkart.varadhi.produce.failover.TransitionMetrics;
import com.flipkart.varadhi.produce.failover.TransitionPrepareResult;
import com.flipkart.varadhi.spi.db.TopicStore;
import com.flipkart.varadhi.spi.db.TransitionStore;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * In-JVM integration test of the topic-failover control loop over a <b>real Vert.x event bus</b>:
 * the controller {@link TopicFailoverOpExecutor} broadcasts stage events that real pod
 * {@link ProduceTransitionMsgHandler}s react to, ack back over the bus, and the {@link StageAwaiter}
 * gates each stage. No Pulsar/ZK: stores are in-memory and pod producer-warm is recorded.
 *
 * <p>This exercises the forward leg ({@code publish}), the pod version-convergence + ack, the return
 * leg ({@code send} -> controller ack handler -> {@link StageAwaiter#recordAck}), and the full
 * PREPARE -> SWITCH -> COMPLETED progression including the traffic-affecting SWITCH topic
 * write that pods converge on.
 */
class TopicFailoverIntegrationTest {

    private static final String PROJECT = "proj";
    private static final String TOPIC = "topic";
    private static final String SOURCE = "r1";
    private static final String TARGET = "r2";

    private Vertx vertx;
    private EventBus eventBus;
    private MessageExchange exchange;
    private MessageRouter router;
    private ScheduledExecutorService podScheduler;

    private InMemTopicStore topicStore;
    private InMemTransitionStore transitionStore;
    private StageAwaiter stageAwaiter;
    private OperationMgr operationMgr;
    private VaradhiClusterManager clusterManager;

    private VaradhiTopic topic;
    private String fqn;

    private final List<ResourceReadCache<Resource.EntityResource<VaradhiTopic>>> podCaches =
        new CopyOnWriteArrayList<>();
    private final List<String> warmed = new CopyOnWriteArrayList<>();
    private static final java.util.concurrent.CompletableFuture<TransitionPrepareResult> WARM_OK =
        java.util.concurrent.CompletableFuture.completedFuture(TransitionPrepareResult.WARMED);

    @BeforeEach
    void setup() {
        vertx = Vertx.vertx();
        eventBus = vertx.eventBus();
        DeliveryOptions deliveryOptions = new DeliveryOptions();
        exchange = new MessageExchange(eventBus, deliveryOptions);
        router = new MessageRouter(eventBus, deliveryOptions);
        podScheduler = Executors.newScheduledThreadPool(2);

        topicStore = new InMemTopicStore();
        transitionStore = new InMemTransitionStore();
        stageAwaiter = new StageAwaiter();
        operationMgr = mock(OperationMgr.class);
        clusterManager = mock(VaradhiClusterManager.class);

        topic = MultiRegionTopicFixture.create(PROJECT, TOPIC, SOURCE, TARGET);
        // A live topic has seen prior writes; a non-zero version makes PREPARE version-gated (so pods
        // run the readiness + producer pre-warm path, which is skipped when awaiting version 0).
        topic.setVersion(5);
        fqn = topic.getName();
        topicStore.put(topic);
        // L1 fan-out simulation: when the controller writes the topic, push the new version into
        // every pod's TopicCache so pods can converge on it (as the real ResourceEvent pipeline would).
        topicStore.setOnUpdate(this::fanOutToPodCaches);

        // Controller-side return leg: pod acks land here and feed the stage barrier.
        router.sendHandler(
            ControllerApi.ROUTE_CONTROLLER,
            TransitionBusAddress.STAGE_ACK_API,
            msg -> stageAwaiter.recordAck(msg.getData(TransitionAck.class))
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        podScheduler.shutdownNow();
        vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    @Test
    void failoverDrivesAllPodsThroughStagesAndSwitchesProduce() throws Exception {
        registerPod("pod-1");
        registerPod("pod-2");
        registerPod("pod-3");
        expectServerHosts("pod-1", "pod-2", "pod-3");

        TopicFailoverOperation op = startFailover();
        TopicFailoverOpExecutor executor = newExecutor(new TopicFailoverConfig(5000, 5000, 5000));

        executor.execute(op).get(10, TimeUnit.SECONDS);

        // SWITCH flipped the per-region produce gate; COMPLETED marked the old source a replica.
        assertEquals(TopicState.Replicating, topic.getTopicState(RegionName.of(SOURCE)));
        assertEquals(TopicState.Producing, topic.getTopicState(RegionName.of(TARGET)));

        assertEquals(Operation.State.COMPLETED, op.getState());
        assertFalse(transitionStore.exists(fqn), "transition master should be deleted on completion");

        // Every pod pre-warmed the target-region producer during PREPARE.
        assertEquals(3, warmed.size());
        assertTrue(
            warmed.stream().allMatch(w -> w.endsWith("@" + TARGET)),
            "each pod should warm the target region, got " + warmed
        );
    }

    @Test
    void prepareTimesOutWhenAPodIsSilentAndNoSwitchHappens() {
        registerPod("pod-1");
        // pod-2 is expected by the controller but never registers a handler -> never acks.
        expectServerHosts("pod-1", "pod-2");

        TopicFailoverOperation op = startFailover();
        TopicFailoverOpExecutor executor = newExecutor(new TopicFailoverConfig(500, 500, 500));

        ExecutionException ex = assertThrows(
            ExecutionException.class,
            () -> executor.execute(op).get(10, TimeUnit.SECONDS)
        );
        assertTrue(ex.getCause().getMessage().toLowerCase().contains("timed out"), ex.getCause().getMessage());

        // No SWITCH write occurred: both regions remain in their pre-failover Producing state and the
        // transition master is still present (not completed).
        assertEquals(TopicState.Producing, topic.getTopicState(RegionName.of(SOURCE)));
        assertEquals(TopicState.Producing, topic.getTopicState(RegionName.of(TARGET)));
        assertEquals(5, topic.getVersion(), "topic must not have been written during a failed PREPARE");
        assertTrue(transitionStore.exists(fqn), "transition master should remain on failure");
    }

    private TopicFailoverOperation startFailover() {
        TopicFailoverOperation op = TopicFailoverOperation.of(fqn, SOURCE, TARGET, false, "tester");
        transitionStore.create(TransitionObject.forFailover(op.getId(), fqn, SOURCE, TARGET));
        return op;
    }

    private TopicFailoverOpExecutor newExecutor(TopicFailoverConfig config) {
        return new TopicFailoverOpExecutor(
            operationMgr,
            transitionStore,
            topicStore,
            exchange,
            stageAwaiter,
            clusterManager,
            config
        );
    }

    private void registerPod(String hostname) {
        ResourceReadCache<Resource.EntityResource<VaradhiTopic>> cache = newSeededCache();
        podCaches.add(cache);
        BiFunction<VaradhiTopicName, String, java.util.concurrent.CompletableFuture<TransitionPrepareResult>> warmer = (
            topicFqn,
            target
        ) -> {
            warmed.add(hostname + "@" + target);
            return WARM_OK;
        };
        ProduceTransitionMsgHandler handler = new ProduceTransitionMsgHandler(
            hostname,
            cache,
            new ControllerTransitionAckClient(exchange),
            Map.of(TransitionType.TOPIC_FAILOVER, warmer),
            new PodTransitionConfig(3000, 10),
            podScheduler,
            TransitionMetrics.NOOP
        );
        router.publishHandler(
            TransitionBusAddress.ROUTE_TOPIC_TRANSITION,
            TransitionBusAddress.STAGE_BROADCAST_API,
            handler
        );
    }

    private ResourceReadCache<Resource.EntityResource<VaradhiTopic>> newSeededCache() {
        try {
            ResourceReadCache<Resource.EntityResource<VaradhiTopic>> cache =
                ResourceReadCache.<Resource.EntityResource<VaradhiTopic>>create(
                    ResourceType.TOPIC,
                    () -> List.of(Resource.of(topic, ResourceType.TOPIC)),
                    vertx
                ).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            return cache;
        } catch (Exception e) {
            throw new RuntimeException("failed to seed pod cache", e);
        }
    }

    private void fanOutToPodCaches(VaradhiTopic updated) {
        for (ResourceReadCache<Resource.EntityResource<VaradhiTopic>> cache : podCaches) {
            cache.onChange(
                new ResourceEvent<>(
                    ResourceType.TOPIC,
                    updated.getName(),
                    EventType.UPSERT,
                    Resource.of(updated, ResourceType.TOPIC),
                    updated.getVersion(),
                    null
                )
            );
        }
    }

    private void expectServerHosts(String... hostnames) {
        List<MemberInfo> members = new ArrayList<>();
        for (String h : hostnames) {
            members.add(new MemberInfo(h, "", 0, new ComponentKind[] {ComponentKind.Server}, null, null));
        }
        when(clusterManager.getAllMembers()).thenReturn(Future.succeededFuture(members));
    }

    private static final class InMemTopicStore implements TopicStore {
        private final Map<String, VaradhiTopic> topics = new java.util.concurrent.ConcurrentHashMap<>();
        private volatile Consumer<VaradhiTopic> onUpdate = t -> {
        };

        void put(VaradhiTopic t) {
            topics.put(t.getName(), t);
        }

        void setOnUpdate(Consumer<VaradhiTopic> listener) {
            this.onUpdate = listener;
        }

        @Override
        public void create(VaradhiTopic t) {
            topics.put(t.getName(), t);
        }

        @Override
        public VaradhiTopic get(String topicName) {
            VaradhiTopic t = topics.get(topicName);
            if (t == null) {
                throw new ResourceNotFoundException("Topic(%s) not found".formatted(topicName));
            }
            return t;
        }

        @Override
        public List<String> getAllNames(String projectName) {
            return new ArrayList<>(topics.keySet());
        }

        @Override
        public List<VaradhiTopic> getAll() {
            return new ArrayList<>(topics.values());
        }

        @Override
        public boolean exists(String topicName) {
            return topics.containsKey(topicName);
        }

        @Override
        public void update(VaradhiTopic t) {
            t.setVersion(t.getVersion() + 1); // mirror metastore: a tracked write bumps the version
            topics.put(t.getName(), t);
            onUpdate.accept(t);
        }

        @Override
        public void delete(String topicName) {
            topics.remove(topicName);
        }
    }


    private static final class InMemTransitionStore implements TransitionStore {
        private final Map<String, TransitionObject> map = new java.util.concurrent.ConcurrentHashMap<>();

        @Override
        public void create(TransitionObject transition) {
            if (map.putIfAbsent(transition.getTopicFqn(), transition) != null) {
                throw new IllegalStateException("transition already exists for " + transition.getTopicFqn());
            }
        }

        @Override
        public TransitionObject get(String topicFqn) {
            return map.get(topicFqn);
        }

        @Override
        public boolean exists(String topicFqn) {
            return map.containsKey(topicFqn);
        }

        @Override
        public void update(TransitionObject transition) {
            map.put(transition.getTopicFqn(), transition);
        }

        @Override
        public void delete(String topicFqn) {
            map.remove(topicFqn);
        }

        @Override
        public List<TransitionObject> listActive() {
            return new ArrayList<>(map.values());
        }
    }
}
