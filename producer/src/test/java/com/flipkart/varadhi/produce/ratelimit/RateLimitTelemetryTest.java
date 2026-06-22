package com.flipkart.varadhi.produce.ratelimit;

import com.flipkart.varadhi.common.MockTicker;
import com.flipkart.varadhi.core.cluster.ClusterMembershipView;
import com.flipkart.varadhi.core.cluster.ComponentKind;
import com.flipkart.varadhi.core.cluster.InMemoryVaradhiClusterManager;
import com.flipkart.varadhi.core.cluster.MemberInfo;
import com.flipkart.varadhi.core.cluster.NodeCapacity;
import com.flipkart.varadhi.core.cluster.PodCountProvider;
import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.RateLimiterMode;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

class RateLimitTelemetryTest {

    private static final String REGION = "local";

    @Test
    void shadowMode_RecordsShadowRejectionMeters() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Function<String, ProducerMetrics> metricsProvider = metricsProvider(registry);
        ProduceRateLimiter limiter = shadowLimiter(new MockTicker(0L), metricsProvider);

        VaradhiTopic topic = topic(RateLimiterMode.shadow);
        assertFalse(limiter.check(topic, 1));
        assertFalse(limiter.check(topic, 1));

        assertEquals(1.0, registry.get("producer.rejected.count").tag("shadow", "true").counter().count());
        assertEquals(1.0, registry.get("producer.rejected.bytes").tag("shadow", "true").counter().count());
    }

    @Test
    void enforcedMode_DoesNotRecordAtLimiterCheck() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Function<String, ProducerMetrics> metricsProvider = metricsProvider(registry);
        ProduceRateLimiter limiter = enforcedLimiter(new MockTicker(0L), metricsProvider);

        VaradhiTopic topic = topic(RateLimiterMode.enforced);
        assertFalse(limiter.check(topic, 1));
        assertTrue(limiter.check(topic, 1));

        assertEquals(0, registry.find("producer.rejected.count").counters().size());
    }

    private static Function<String, ProducerMetrics> metricsProvider(SimpleMeterRegistry registry) {
        Map<String, ProducerMetrics> cache = new ConcurrentHashMap<>();
        return topicFqn -> cache.computeIfAbsent(topicFqn, fqn -> new ProducerMetricsImpl(registry, fqn, REGION));
    }

    private static ProduceRateLimiter shadowLimiter(
        MockTicker ticker,
        Function<String, ProducerMetrics> metricsProvider
    ) {
        return limiter(ticker, RateLimiterMode.shadow, metricsProvider);
    }

    private static ProduceRateLimiter enforcedLimiter(
        MockTicker ticker,
        Function<String, ProducerMetrics> metricsProvider
    ) {
        return limiter(ticker, RateLimiterMode.enforced, metricsProvider);
    }

    private static ProduceRateLimiter limiter(
        MockTicker ticker,
        RateLimiterMode mode,
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
            mode,
            quotaProvider,
            1,
            ticker,
            podCount,
            new RateLimitTelemetry(metricsProvider)
        );
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
