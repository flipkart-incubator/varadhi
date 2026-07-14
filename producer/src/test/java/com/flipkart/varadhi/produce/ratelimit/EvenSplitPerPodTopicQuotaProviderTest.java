package com.flipkart.varadhi.produce.ratelimit;

import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.MessageSizeProfile;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.VaradhiTopic;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EvenSplitPerPodTopicQuotaProviderTest {

    private static final String REGION = "local";

    private final AtomicInteger podCount = new AtomicInteger(2);

    private EvenSplitPerPodTopicQuotaProvider provider() {
        return new EvenSplitPerPodTopicQuotaProvider(REGION, 0.25, 1, podCount::get);
    }

    @Test
    void quotaFor_EvenSplitWithFallbackBuffer() {
        VaradhiTopic topic = topic(new TopicCapacityPolicy(100, 1000, 1, 2), Map.of(REGION, 1.0), null);

        PerPodTopicQuota quota = provider().quotaFor(topic);

        assertEquals(63, quota.qpsQuota());
        assertEquals(640_000L, quota.bytesQuota());
    }

    @Test
    void quotaFor_AppliesMinPodQpsFloor() {
        VaradhiTopic topic = topic(new TopicCapacityPolicy(1, 100, 1, 2), Map.of(REGION, 1.0), null);

        PerPodTopicQuota quota = provider().quotaFor(topic);

        assertEquals(1, quota.qpsQuota());
    }

    @Test
    void quotaFor_AppliesDimensionCouplingFloor() {
        VaradhiTopic topic = topic(
            new TopicCapacityPolicy(100, 1, 1, 2),
            Map.of(REGION, 1.0),
            new MessageSizeProfile(1024, 2048)
        );

        PerPodTopicQuota quota = provider().quotaFor(topic);

        assertTrue(quota.bytesQuota() >= quota.qpsQuota() * 2048);
    }

    @Test
    void quotaFor_AppliesRegionWeight() {
        VaradhiTopic topic = topic(new TopicCapacityPolicy(100, 1000, 1, 2), Map.of(REGION, 0.5, "other", 0.5), null);

        PerPodTopicQuota quota = provider().quotaFor(topic);

        assertEquals(32, quota.qpsQuota());
    }

    @Test
    void quotaFor_RejectsMissingDeploymentRegion() {
        VaradhiTopic topic = topic(new TopicCapacityPolicy(100, 1000, 1, 2), Map.of("other", 1.0), null);

        assertThrows(IllegalArgumentException.class, () -> provider().quotaFor(topic));
    }

    @Test
    void quotaFor_RecomputesWhenPodCountChanges() {
        VaradhiTopic topic = topic(new TopicCapacityPolicy(100, 1000, 1, 2), Map.of(REGION, 1.0), null);
        EvenSplitPerPodTopicQuotaProvider provider = provider();

        PerPodTopicQuota atTwoPods = provider.quotaFor(topic);

        podCount.set(4);
        PerPodTopicQuota atFourPods = provider.quotaFor(topic);

        assertTrue(atTwoPods.qpsQuota() > atFourPods.qpsQuota());
    }

    private static VaradhiTopic topic(
        TopicCapacityPolicy capacity,
        Map<String, Double> regionWeights,
        MessageSizeProfile messageSizeProfile
    ) {
        return VaradhiTopic.of(
            "project",
            "topic",
            false,
            capacity,
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            null,
            VaradhiTopic.TopicCategory.TOPIC,
            regionWeights,
            messageSizeProfile,
            null
        );
    }
}
