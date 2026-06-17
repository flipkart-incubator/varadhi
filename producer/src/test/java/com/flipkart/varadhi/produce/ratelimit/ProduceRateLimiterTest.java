package com.flipkart.varadhi.produce.ratelimit;

import com.flipkart.varadhi.common.MockTicker;
import com.flipkart.varadhi.core.cluster.ClusterMembershipView;
import com.flipkart.varadhi.core.cluster.ComponentKind;
import com.flipkart.varadhi.core.cluster.FakeVaradhiClusterManager;
import com.flipkart.varadhi.core.cluster.MemberInfo;
import com.flipkart.varadhi.core.cluster.NodeCapacity;
import com.flipkart.varadhi.core.cluster.PodCountProvider;
import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.RateLimiterMode;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.produce.telemetry.ProducerMetrics;
import com.flipkart.varadhi.produce.telemetry.ProducerMetricsImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProduceRateLimiterTest {

    private static final String REGION = "local";

    @Test
    void disabled_NeverThrottles() {
        assertFalse(ProduceRateLimiter.disabled().check(topic(RateLimiterMode.enforced), 1));
    }

    @Test
    void check_EnforcedMode_ThrottlesWhenOverQuota() {
        MockTicker ticker = new MockTicker(0L);
        ProduceRateLimiter limiter = enforcedLimiter(ticker);
        VaradhiTopic topic = topic(RateLimiterMode.enforced);

        assertFalse(limiter.check(topic, 1));
        assertTrue(limiter.check(topic, 1));
    }

    @Test
    void check_ShadowMode_AllowsWhenOverQuota() {
        MockTicker ticker = new MockTicker(0L);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ProduceRateLimiter limiter = shadowLimiter(ticker, registry);
        VaradhiTopic topic = topic(RateLimiterMode.shadow);

        assertFalse(limiter.check(topic, 1));
        assertFalse(limiter.check(topic, 1));
        assertEquals(1.0, registry.get("producer.rejected.count").tag("shadow", "true").counter().count());
    }

    @Test
    void check_DisabledMode_PassThroughEvenWhenOverQuota() {
        MockTicker ticker = new MockTicker(0L);
        ProduceRateLimiter limiter = enforcedLimiter(ticker);
        VaradhiTopic topic = topic(RateLimiterMode.disabled);

        limiter.check(topic, 1);
        assertFalse(limiter.check(topic, 1));
    }

    @Test
    void resolveLimiter_ReusesRegistryEntryPerTopic() {
        ProduceRateLimiter facade = facadeWithPodCount(2);
        VaradhiTopic topic = topic(null);

        TopicRateLimiter first = facade.resolveLimiter(topic);
        TopicRateLimiter second = facade.resolveLimiter(topic);

        assertSame(first, second);
    }

    @Test
    void resolveLimiter_RefreshesQuotaLazilyOnMembershipChange() {
        FakeVaradhiClusterManager clusterManager = new FakeVaradhiClusterManager();
        clusterManager.replaceMembers(Map.of("server-1", server("server-1"), "server-2", server("server-2")));
        PodCountProvider podCount = startServerPodCount(clusterManager);

        EvenSplitPerPodTopicQuotaProvider quotaProvider = new EvenSplitPerPodTopicQuotaProvider(
            REGION,
            0.25,
            1,
            podCount
        );
        ProduceRateLimiter facade = new ProduceRateLimiter(
            RateLimiterMode.disabled,
            quotaProvider,
            1,
            new MockTicker(0L),
            podCount,
            noMetricsTelemetry()
        );
        VaradhiTopic topic = topic(null);

        PerPodTopicQuota before = facade.resolveLimiter(topic).lastQuota();

        clusterManager.simulateJoin("server-3", server("server-3"));
        PerPodTopicQuota after = facade.resolveLimiter(topic).lastQuota();

        assertNotEquals(before, after);
    }

    @Test
    void removeTopic_DropsRegistryEntry() {
        MockTicker ticker = new MockTicker(0L);
        ProduceRateLimiter limiter = enforcedLimiter(ticker);
        VaradhiTopic topic = topic(RateLimiterMode.enforced);

        TopicRateLimiter before = limiter.resolveLimiter(topic);
        limiter.removeTopic(topic.getName());
        TopicRateLimiter after = limiter.resolveLimiter(topic);

        assertNotSame(before, after);
    }

    private static RateLimitTelemetry noMetricsTelemetry() {
        return new RateLimitTelemetry(ignored -> ProducerMetrics.NOOP);
    }

    private static ProduceRateLimiter enforcedLimiter(MockTicker ticker) {
        FakeVaradhiClusterManager clusterManager = new FakeVaradhiClusterManager();
        clusterManager.replaceMembers(Map.of("server-1", server("server-1")));
        PodCountProvider podCount = startServerPodCount(clusterManager);
        EvenSplitPerPodTopicQuotaProvider quotaProvider = new EvenSplitPerPodTopicQuotaProvider(
            REGION,
            0.0,
            1,
            podCount
        );
        return new ProduceRateLimiter(
            RateLimiterMode.enforced,
            quotaProvider,
            1,
            ticker,
            podCount,
            noMetricsTelemetry()
        );
    }

    private static ProduceRateLimiter shadowLimiter(MockTicker ticker, SimpleMeterRegistry registry) {
        FakeVaradhiClusterManager clusterManager = new FakeVaradhiClusterManager();
        clusterManager.replaceMembers(Map.of("server-1", server("server-1")));
        PodCountProvider podCount = startServerPodCount(clusterManager);
        EvenSplitPerPodTopicQuotaProvider quotaProvider = new EvenSplitPerPodTopicQuotaProvider(
            REGION,
            0.0,
            1,
            podCount
        );
        Map<String, ProducerMetrics> cache = new ConcurrentHashMap<>();
        Function<String, ProducerMetrics> metricsProvider = fqn -> cache.computeIfAbsent(
            fqn,
            ignored -> new ProducerMetricsImpl(registry, fqn, REGION)
        );
        return new ProduceRateLimiter(
            RateLimiterMode.shadow,
            quotaProvider,
            1,
            ticker,
            podCount,
            new RateLimitTelemetry(metricsProvider)
        );
    }

    private static ProduceRateLimiter facadeWithPodCount(int servers) {
        FakeVaradhiClusterManager clusterManager = new FakeVaradhiClusterManager();
        Map<String, MemberInfo> members = new java.util.HashMap<>();
        for (int i = 0; i < servers; i++) {
            members.put("server-" + i, server("server-" + i));
        }
        clusterManager.replaceMembers(members);
        PodCountProvider podCount = startServerPodCount(clusterManager);
        EvenSplitPerPodTopicQuotaProvider quotaProvider = new EvenSplitPerPodTopicQuotaProvider(
            REGION,
            0.25,
            1,
            podCount
        );
        return new ProduceRateLimiter(
            RateLimiterMode.disabled,
            quotaProvider,
            1,
            new MockTicker(0L),
            podCount,
            noMetricsTelemetry()
        );
    }

    private static PodCountProvider startServerPodCount(FakeVaradhiClusterManager clusterManager) {
        ClusterMembershipView membership = new ClusterMembershipView(clusterManager);
        membership.start();
        return PodCountProvider.withRole(membership, ComponentKind.Server, 1);
    }

    private static VaradhiTopic topic(RateLimiterMode mode) {
        return VaradhiTopic.of(
            "project",
            "topic",
            false,
            new TopicCapacityPolicy(1, 1, 1, 2),
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            null,
            VaradhiTopic.TopicCategory.TOPIC,
            Map.of(REGION, 1.0),
            null,
            mode
        );
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
}
