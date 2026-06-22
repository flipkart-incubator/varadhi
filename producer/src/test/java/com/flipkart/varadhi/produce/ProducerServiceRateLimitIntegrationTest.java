package com.flipkart.varadhi.produce;

import com.flipkart.varadhi.common.MockTicker;
import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.cluster.ClusterMembershipView;
import com.flipkart.varadhi.core.cluster.ComponentKind;
import com.flipkart.varadhi.core.cluster.InMemoryVaradhiClusterManager;
import com.flipkart.varadhi.core.cluster.MemberInfo;
import com.flipkart.varadhi.core.cluster.NodeCapacity;
import com.flipkart.varadhi.core.cluster.PodCountProvider;
import com.flipkart.varadhi.core.config.ProducerOptions;
import com.flipkart.varadhi.entities.JsonMapper;
import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.ProduceStatus;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.RateLimiterMode;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.entities.SegmentedStorageTopic;
import com.flipkart.varadhi.entities.SimpleMessage;
import com.flipkart.varadhi.entities.StdHeaders;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TestStdHeaders;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.TopicState;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.produce.ratelimit.EvenSplitPerPodTopicQuotaProvider;
import com.flipkart.varadhi.produce.ratelimit.ProduceRateLimiter;
import com.flipkart.varadhi.produce.ratelimit.RateLimitTelemetry;
import com.flipkart.varadhi.produce.telemetry.ProducerMetrics;
import com.flipkart.varadhi.produce.telemetry.ProducerMetricsImpl;
import com.flipkart.varadhi.spi.mock.DummyProducer;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProducerServiceRateLimitIntegrationTest {

    private static final String REGION = "default";
    private static final String TOPIC = "topic1";
    private static final Project PROJECT = Project.of("project1", "", "team1", "org1");

    private static Vertx vertx;
    private static ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache;

    private final MockTicker ticker = new MockTicker(0L);
    private SimpleMeterRegistry meterRegistry;
    private ProducerService service;

    @BeforeAll
    static void startVertx() {
        if (!StdHeaders.isGlobalInstanceInitialized()) {
            StdHeaders.init(TestStdHeaders.get());
        }
        vertx = Vertx.vertx();
    }

    @AfterAll
    static void stopVertx() {
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().join();
        }
    }

    @BeforeEach
    void setUp() {
        ticker.setNanos(0L);
        meterRegistry = new SimpleMeterRegistry();
        VaradhiTopic topic = rateLimitTopic(RateLimiterMode.enforced, 1);
        topicCache = cache(ResourceType.TOPIC, List.of(Resource.of(topic, ResourceType.TOPIC)));

        Map<String, ProducerMetrics> metricsByTopic = new ConcurrentHashMap<>();
        Function<String, ProducerMetrics> metricsProvider = fqn -> metricsByTopic.computeIfAbsent(
            fqn,
            ignored -> new ProducerMetricsImpl(meterRegistry, fqn, REGION)
        );

        service = new ProducerService(
            REGION,
            (storageTopic, config) -> new DummyProducer(com.flipkart.varadhi.entities.JsonMapper.getMapper()),
            cache(ResourceType.ORG, List.of()),
            cache(ResourceType.PROJECT, List.of()),
            topicCache,
            metricsProvider,
            ProducerOptions.defaultOptions(),
            rateLimiter(RateLimiterMode.enforced, metricsProvider)
        );
    }

    @Test
    void enforcedMode_OverQuota_ReturnsThrottledWithoutPublishing() throws Exception {
        assertEquals(ProduceStatus.Success, result(message(1)).getProduceStatus());
        assertEquals(ProduceStatus.Throttled, result(message(2)).getProduceStatus());
        assertEquals(1.0, meterRegistry.get("producer.rejected.count").tag("shadow", "false").counter().count());
    }

    @Test
    void enforcedMode_AfterRefill_AllowsAgain() throws Exception {
        assertEquals(ProduceStatus.Success, result(message(1)).getProduceStatus());
        assertEquals(ProduceStatus.Throttled, result(message(2)).getProduceStatus());

        ticker.advance(1, TimeUnit.SECONDS);
        assertEquals(ProduceStatus.Success, result(message(3)).getProduceStatus());
    }

    @Test
    void shadowMode_OverQuota_StillProduces() throws Exception {
        VaradhiTopic topic = rateLimitTopic(RateLimiterMode.shadow, 1);
        topicCache = cache(ResourceType.TOPIC, List.of(Resource.of(topic, ResourceType.TOPIC)));
        service = serviceWithMode(RateLimiterMode.shadow);
        assertEquals(ProduceStatus.Success, result(message(1)).getProduceStatus());
        assertEquals(ProduceStatus.Success, result(message(2)).getProduceStatus());
        assertEquals(1.0, meterRegistry.get("producer.rejected.count").tag("shadow", "true").counter().count());
    }

    @Test
    void disabledTopicMode_IgnoresLimiter() throws Exception {
        VaradhiTopic topic = rateLimitTopic(RateLimiterMode.disabled, 1);
        topicCache = cache(ResourceType.TOPIC, List.of(Resource.of(topic, ResourceType.TOPIC)));
        service = serviceWithMode(RateLimiterMode.enforced);

        assertEquals(ProduceStatus.Success, result(message(1)).getProduceStatus());
        assertEquals(ProduceStatus.Success, result(message(2)).getProduceStatus());
    }

    @Test
    void killSwitchDisabledLimiter_NeverThrottles() throws Exception {
        service = new ProducerService(
            REGION,
            (storageTopic, config) -> new DummyProducer(com.flipkart.varadhi.entities.JsonMapper.getMapper()),
            cache(ResourceType.ORG, List.of()),
            cache(ResourceType.PROJECT, List.of()),
            topicCache,
            ignored -> ProducerMetrics.NOOP,
            ProducerOptions.defaultOptions(),
            ProduceRateLimiter.disabled()
        );

        assertEquals(ProduceStatus.Success, result(message(1)).getProduceStatus());
        assertEquals(ProduceStatus.Success, result(message(2)).getProduceStatus());
    }

    @Test
    void largeMessage_FitsByteBudgetThenPaces() throws Exception {
        VaradhiTopic topic = rateLimitTopic(RateLimiterMode.enforced, 1, 1);
        topicCache = cache(ResourceType.TOPIC, List.of(Resource.of(topic, ResourceType.TOPIC)));
        service = serviceWithMode(RateLimiterMode.enforced);

        assertTrue(result(messageWithPayload(64)).isSuccess());
        assertEquals(ProduceStatus.Throttled, result(messageWithPayload(64)).getProduceStatus());

        ticker.advance(1, TimeUnit.SECONDS);
        assertTrue(result(messageWithPayload(64)).isSuccess());
    }

    private ProducerService serviceWithMode(RateLimiterMode mode) {
        Map<String, ProducerMetrics> metricsByTopic = new ConcurrentHashMap<>();
        Function<String, ProducerMetrics> metricsProvider = fqn -> metricsByTopic.computeIfAbsent(
            fqn,
            ignored -> new ProducerMetricsImpl(meterRegistry, fqn, REGION)
        );
        return new ProducerService(
            REGION,
            (storageTopic, config) -> new DummyProducer(com.flipkart.varadhi.entities.JsonMapper.getMapper()),
            cache(ResourceType.ORG, List.of()),
            cache(ResourceType.PROJECT, List.of()),
            topicCache,
            metricsProvider,
            ProducerOptions.defaultOptions(),
            rateLimiter(mode, metricsProvider)
        );
    }

    private static <T extends Resource> ResourceReadCache<T> cache(ResourceType type, List<T> resources) {
        return ResourceReadCache.create(type, () -> resources, vertx).toCompletionStage().toCompletableFuture().join();
    }

    private ProduceRateLimiter rateLimiter(
        RateLimiterMode defaultMode,
        Function<String, ProducerMetrics> metricsProvider
    ) {
        InMemoryVaradhiClusterManager clusterManager = new InMemoryVaradhiClusterManager();
        clusterManager.replaceMembers(Map.of("server-1", server("server-1")));
        ClusterMembershipView membership = new ClusterMembershipView(clusterManager);
        membership.start();
        PodCountProvider podCount = PodCountProvider.withRole(membership, ComponentKind.Server, 1);
        EvenSplitPerPodTopicQuotaProvider quotaProvider = new EvenSplitPerPodTopicQuotaProvider(
            REGION,
            0.0,
            1,
            podCount
        );
        return new ProduceRateLimiter(
            defaultMode,
            quotaProvider,
            1,
            ticker,
            podCount,
            new RateLimitTelemetry(metricsProvider)
        );
    }

    private ProduceResult result(Message message) throws Exception {
        return service.produceToTopic(message, VaradhiTopic.fqn(PROJECT.getName(), TOPIC)).get();
    }

    private static Message message(int id) {
        return messageWithPayload(1);
    }

    private static Message messageWithPayload(int payloadBytes) {
        byte[] random = new byte[Math.max(0, payloadBytes - 32)];
        DummyProducer.DummyMessage dummyMessage = new DummyProducer.DummyMessage(0, 1, null, random);
        byte[] payload = JsonMapper.jsonSerialize(dummyMessage).getBytes();
        Multimap<String, String> headers = ArrayListMultimap.create();
        headers.put(StdHeaders.get().msgId(), "msg-" + payloadBytes);
        return new SimpleMessage(payload, headers);
    }

    private static VaradhiTopic rateLimitTopic(RateLimiterMode mode, int qps) {
        return rateLimitTopic(mode, qps, 1024);
    }

    private static VaradhiTopic rateLimitTopic(RateLimiterMode mode, int qps, int throughputKBps) {
        VaradhiTopic topic = VaradhiTopic.of(
            PROJECT.getName(),
            TOPIC,
            false,
            new TopicCapacityPolicy(qps, throughputKBps, 1, 2),
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            null,
            VaradhiTopic.TopicCategory.TOPIC,
            Map.of(REGION, 1.0),
            null,
            mode
        );
        topic.markCreated();
        StorageTopic storageTopic = new DummyStorageTopic(topic.getName());
        SegmentedStorageTopic internal = SegmentedStorageTopic.of(storageTopic);
        internal.setTopicState(TopicState.Producing);
        topic.addInternalTopic(REGION, internal);
        return topic;
    }

    private static MemberInfo server(String hostname) {
        return new MemberInfo(
            hostname,
            "127.0.0.1",
            8080,
            new ComponentKind[] {ComponentKind.Server},
            new NodeCapacity(1000, 1000),
            RegionName.BOOTSTRAP_REGION
        );
    }

    private static final class DummyStorageTopic extends StorageTopic {
        private DummyStorageTopic(String name) {
            super(0, name);
        }
    }
}
