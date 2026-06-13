package com.flipkart.varadhi.produce.ratelimit;

import com.flipkart.varadhi.core.cluster.PodCountProvider;
import com.flipkart.varadhi.entities.VaradhiTopic;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Facade for per-topic rate limiting on the produce path (VIP-0001).
 * <p>
 * Phase 2: registry, lazy {@link TopicRateLimiter} creation, and membership-driven quota refresh.
 * Mode gating, kill switch, and {@code check(...)} throttling are added in Phase 3.
 */
public final class ProduceRateLimiter {

    private static final class RegistryEntry {
        final TopicRateLimiter limiter;
        volatile long appliedQuotaEpoch;

        RegistryEntry(TopicRateLimiter limiter, long appliedQuotaEpoch) {
            this.limiter = limiter;
            this.appliedQuotaEpoch = appliedQuotaEpoch;
        }
    }

    private final PerPodTopicQuotaProvider quotaProvider;
    private final int windowSecs;
    private final LongSupplier nanoTime;
    private final ConcurrentHashMap<String, RegistryEntry> registry = new ConcurrentHashMap<>();
    private volatile long quotaEpoch;

    public ProduceRateLimiter(
        PerPodTopicQuotaProvider quotaProvider,
        int windowSecs,
        LongSupplier nanoTime,
        PodCountProvider podCountProvider
    ) {
        this.quotaProvider = quotaProvider;
        this.windowSecs = windowSecs;
        this.nanoTime = nanoTime;
        podCountProvider.addCountChangeListener(this::markQuotasStale);
    }

    /**
     * Returns the limiter for {@code topic}, refreshing its quota when membership (or other global
     * quota inputs) have changed since the last access.
     */
    TopicRateLimiter resolveLimiter(VaradhiTopic topic) {
        RegistryEntry entry = registry.computeIfAbsent(topic.getName(), ignored -> createEntry(topic));
        refreshQuotaIfStale(entry, topic);
        return entry.limiter;
    }

    private void markQuotasStale() {
        quotaEpoch++;
    }

    private RegistryEntry createEntry(VaradhiTopic topic) {
        long epoch = quotaEpoch;
        PerPodTopicQuota quota = quotaProvider.quotaFor(topic);
        TopicRateLimiter limiter = new TopicRateLimiter(nanoTime, windowSecs, quota);
        return new RegistryEntry(limiter, epoch);
    }

    private void refreshQuotaIfStale(RegistryEntry entry, VaradhiTopic topic) {
        long epoch = quotaEpoch;
        if (entry.appliedQuotaEpoch >= epoch) {
            return;
        }
        synchronized (entry) {
            if (entry.appliedQuotaEpoch < epoch) {
                entry.limiter.applyQuota(quotaProvider.quotaFor(topic));
                entry.appliedQuotaEpoch = epoch;
            }
        }
    }
}
