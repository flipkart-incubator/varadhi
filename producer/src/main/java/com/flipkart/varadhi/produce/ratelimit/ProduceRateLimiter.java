package com.flipkart.varadhi.produce.ratelimit;

import com.flipkart.varadhi.core.cluster.PodCountProvider;
import com.flipkart.varadhi.entities.RateLimiterMode;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.produce.telemetry.ProducerMetrics;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Facade for per-topic rate limiting on the produce path (VIP-0001).
 */
@Slf4j
public final class ProduceRateLimiter {

    /** Never invoked; {@link #disabled()} returns before rate-limit work runs. */
    private static final RateLimitTelemetry UNUSED_TELEMETRY = new RateLimitTelemetry(ignored -> ProducerMetrics.NOOP);

    private static final ProduceRateLimiter DISABLED = new ProduceRateLimiter(
        false,
        RateLimiterMode.disabled,
        null,
        0,
        () -> 0L,
        null,
        UNUSED_TELEMETRY
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
        this.telemetry = Objects.requireNonNull(telemetry);
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
                telemetry.shadowRejected(topic, messageBytes);
                return false;
            }
            return true;
        } catch (RuntimeException e) {
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
