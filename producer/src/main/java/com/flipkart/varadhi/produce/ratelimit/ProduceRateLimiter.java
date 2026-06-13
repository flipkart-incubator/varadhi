package com.flipkart.varadhi.produce.ratelimit;

import com.flipkart.varadhi.core.cluster.PodCountProvider;
import com.flipkart.varadhi.entities.RateLimiterMode;
import com.flipkart.varadhi.entities.VaradhiTopic;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Facade for per-topic rate limiting on the produce path (VIP-0001).
 */
@Slf4j
public final class ProduceRateLimiter {

    private static final ProduceRateLimiter DISABLED = new ProduceRateLimiter(
        false,
        RateLimiterMode.disabled,
        null,
        0,
        () -> 0L,
        null,
        RateLimitTelemetry.NOOP
    );

    private static final class RegistryEntry {
        final TopicRateLimiter limiter;
        volatile long appliedQuotaEpoch;

        RegistryEntry(TopicRateLimiter limiter, long appliedQuotaEpoch) {
            this.limiter = limiter;
            this.appliedQuotaEpoch = appliedQuotaEpoch;
        }
    }

    private final boolean enabled;
    private final RateLimiterMode defaultMode;
    private final PerPodTopicQuotaProvider quotaProvider;
    private final int windowSecs;
    private final LongSupplier nanoTime;
    private final RateLimitTelemetry telemetry;
    private final ConcurrentHashMap<String, RegistryEntry> registry = new ConcurrentHashMap<>();
    private volatile long quotaEpoch;

    public static ProduceRateLimiter disabled() {
        return DISABLED;
    }

    public ProduceRateLimiter(
        RateLimiterMode defaultMode,
        PerPodTopicQuotaProvider quotaProvider,
        int windowSecs,
        LongSupplier nanoTime,
        PodCountProvider podCountProvider
    ) {
        this(defaultMode, quotaProvider, windowSecs, nanoTime, podCountProvider, RateLimitTelemetry.NOOP);
    }

    ProduceRateLimiter(
        RateLimiterMode defaultMode,
        PerPodTopicQuotaProvider quotaProvider,
        int windowSecs,
        LongSupplier nanoTime,
        PodCountProvider podCountProvider,
        RateLimitTelemetry telemetry
    ) {
        this(true, defaultMode, quotaProvider, windowSecs, nanoTime, podCountProvider, telemetry);
    }

    private ProduceRateLimiter(
        boolean enabled,
        RateLimiterMode defaultMode,
        PerPodTopicQuotaProvider quotaProvider,
        int windowSecs,
        LongSupplier nanoTime,
        PodCountProvider podCountProvider,
        RateLimitTelemetry telemetry
    ) {
        this.enabled = enabled;
        this.defaultMode = defaultMode;
        this.quotaProvider = quotaProvider;
        this.windowSecs = windowSecs;
        this.nanoTime = nanoTime;
        this.telemetry = telemetry;
        if (enabled) {
            podCountProvider.addCountChangeListener(this::markQuotasStale);
        }
    }

    /**
     * @return {@code true} if produce should be throttled (429)
     */
    public boolean check(VaradhiTopic topic, long messageBytes) {
        if (!enabled) {
            return false;
        }
        try {
            RateLimiterMode mode = resolveMode(topic);
            if (mode == RateLimiterMode.disabled) {
                return false;
            }
            TopicRateLimiter limiter = resolveLimiter(topic);
            if (limiter.tryAcquire(messageBytes)) {
                return false;
            }
            if (mode == RateLimiterMode.shadow) {
                telemetry.wouldHaveThrottled();
                return false;
            }
            telemetry.enforcedThrottled();
            return true;
        } catch (RuntimeException e) {
            // TODO: see if telemetry is required here.
            log.warn("Rate limit check failed open for topic {}", topic.getName(), e);
            return false;
        }
    }

    public void removeTopic(String topicFqn) {
        if (enabled) {
            registry.remove(topicFqn);
        }
    }

    TopicRateLimiter resolveLimiter(VaradhiTopic topic) {
        RegistryEntry entry = registry.computeIfAbsent(topic.getName(), ignored -> createEntry(topic));
        refreshQuotaIfStale(entry, topic);
        return entry.limiter;
    }

    private RateLimiterMode resolveMode(VaradhiTopic topic) {
        RateLimiterMode mode = topic.getRateLimiterMode();
        return mode != null ? mode : defaultMode;
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
