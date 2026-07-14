package com.flipkart.varadhi.produce.ratelimit;

import com.flipkart.varadhi.entities.MessageSizeProfile;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.VaradhiTopic;

import java.util.Map;
import java.util.function.IntSupplier;

/**
 * VIP §9 static even split: {@code regionBudget / podCount × (1 + fallbackBuffer)}, with
 * {@code minPodQps} floor and bytes/qps dimension coupling. Fractional shares are ceiled so each
 * pod errs permissive (less rate limiting).
 * <p>
 * Missing {@code perRegionQuotaWeights} (null or empty) is treated as a single-region topic with
 * weight {@code 1.0}. A non-empty map must include the deployment region.
 */
public final class EvenSplitPerPodTopicQuotaProvider implements PerPodTopicQuotaProvider {

    private final String deploymentRegion;
    private final double fallbackBuffer;
    private final int minPodQps;
    private final IntSupplier podCountSupplier;

    public EvenSplitPerPodTopicQuotaProvider(
        String deploymentRegion,
        double fallbackBuffer,
        int minPodQps,
        IntSupplier podCountSupplier
    ) {
        this.deploymentRegion = deploymentRegion;
        this.fallbackBuffer = fallbackBuffer;
        this.minPodQps = minPodQps;
        this.podCountSupplier = podCountSupplier;
    }

    @Override
    public PerPodTopicQuota quotaFor(VaradhiTopic topic) {
        TopicCapacityPolicy capacity = topic.getCapacity();
        double regionWeight = resolveRegionWeight(topic.getPerRegionQuotaWeights());
        int podCount = Math.max(1, podCountSupplier.getAsInt());
        double splitFactor = (1.0 + fallbackBuffer) / podCount;

        int qpsQuota = Math.max(minPodQps, ceilToInt(capacity.getQps() * regionWeight * splitFactor));

        long bytesQuota = ceilToLong(capacity.getThroughputKBps() * 1024L * regionWeight * splitFactor);
        int maxMsgSize = maxMsgSizeBytes(topic.getMessageSizeProfile());
        bytesQuota = Math.max(bytesQuota, (long)qpsQuota * maxMsgSize);

        return new PerPodTopicQuota(qpsQuota, bytesQuota);
    }

    private double resolveRegionWeight(Map<String, Double> perRegionQuotaWeights) {
        if (perRegionQuotaWeights == null || perRegionQuotaWeights.isEmpty()) {
            return 1.0;
        }
        Double weight = perRegionQuotaWeights.get(deploymentRegion);
        if (weight == null) {
            throw new IllegalArgumentException(
                "No perRegionQuotaWeights entry for deployment region: " + deploymentRegion
            );
        }
        return weight;
    }

    /** Null or absent profile → no dimension-coupling floor ({@code maxMsgSize = 0}). */
    private static int maxMsgSizeBytes(MessageSizeProfile profile) {
        return profile == null ? 0 : profile.getMaxMsgSizeBytes();
    }

    /** Fractional split shares are ceiled — bounded by topic capacity inputs, errs permissive. */
    private static int ceilToInt(double value) {
        if (value <= 0.0) {
            return 0;
        }
        double ceiled = Math.ceil(value);
        return ceiled > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int)ceiled;
    }

    private static long ceilToLong(double value) {
        if (value <= 0.0) {
            return 0L;
        }
        double ceiled = Math.ceil(value);
        return ceiled > Long.MAX_VALUE ? Long.MAX_VALUE : (long)ceiled;
    }
}
