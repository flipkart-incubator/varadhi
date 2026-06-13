package com.flipkart.varadhi.produce.ratelimit;

import java.util.function.LongSupplier;

/**
 * Per-topic, per-pod limiter with coupled qps and bytes buckets.
 * <p>
 * Mode ({@code disabled} / {@code shadow} / {@code enforced}) is handled by the facade: this class
 * only answers whether buckets admit the message. {@link #applyQuota(PerPodTopicQuota)} updates
 * refill rates and retains existing token state so in-flight debt/refill is not reset on membership
 * change.
 *
 * <h2>Preconditions</h2>
 * <ul>
 *   <li>{@code messageBytes} is the full produce cost (payload + headers), non-negative.</li>
 *   <li>One {@code TopicRateLimiter} per topic per pod, shared across event-loop threads.</li>
 * </ul>
 *
 * <h2>Concurrency</h2>
 * <p>
 * Admission is both-or-neither: both buckets must have credit, then both are debited; on reject,
 * neither is debited. Qps and bytes are <em>not</em> updated in a single atomic step — another
 * thread may change bucket state between the two checks. Combined with per-bucket check-then-debit
 * in {@link TokenBucket}, this yields slightly permissive guard-rail behaviour under concurrent
 * load; see VIP §7.
 * <p>
 * {@link #applyQuota(PerPodTopicQuota)} publishes {@code lastQuota} before retargeting the buckets,
 * and the two bucket updates are not jointly atomic; a reader may briefly see a new {@code lastQuota}
 * while the buckets still refill at the old rate. Quota changes are rare, so this race is accepted.
 *
 * <h2>Hot path</h2>
 * <p>
 * {@link #tryAcquire(long)} reads the monotonic clock once and shares that instant across both
 * credit checks and both debits, avoiding redundant {@code nanoTime} calls (the dominant per-message
 * cost) and giving a consistent check-then-debit instant.
 */
public final class TopicRateLimiter {

    private final LongSupplier nanoTime;
    private final TokenBucket qpsBucket;
    private final TokenBucket bytesBucket;
    private volatile PerPodTopicQuota lastQuota;

    public TopicRateLimiter(
        LongSupplier nanoTime,
        int windowSecs,
        PerPodTopicQuota initialQuota
    ) {
        this.nanoTime = nanoTime;
        this.qpsBucket = new TokenBucket(nanoTime, windowSecs, initialQuota.qpsQuota());
        this.bytesBucket = new TokenBucket(nanoTime, windowSecs, initialQuota.bytesQuota());
        this.lastQuota = initialQuota;
    }

    public PerPodTopicQuota lastQuota() {
        return lastQuota;
    }

    public void applyQuota(PerPodTopicQuota quota) {
        this.lastQuota = quota;
        qpsBucket.updateRate(quota.qpsQuota());
        bytesBucket.updateRate(quota.bytesQuota());
    }

    /**
     * @param messageBytes full produce cost (payload + headers); a negative value is clamped to
     *                     zero to keep it from crediting the bytes bucket
     * @return {@code true} if admitted (both buckets debited); {@code false} if rejected (neither
     * debited)
     */
    public boolean tryAcquire(long messageBytes) {
        long bytesCost = Math.max(0L, messageBytes);
        long now = nanoTime.getAsLong();
        if (qpsBucket.hasPositiveCredit(now) && bytesBucket.hasPositiveCredit(now)) {
            qpsBucket.debit(1, now);
            bytesBucket.debit(bytesCost, now);
            return true;
        }
        return false;
    }
}
