package com.flipkart.varadhi.produce.ratelimit;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * Daemonless token bucket: refill lazily, admit on positive credit, persist tokens on debit.
 * <p>
 * All token counts and rates are whole units ({@code long}). Sub-second refill uses integer
 * nanosecond math — no {@code double} state.
 *
 * <h2>Preconditions</h2>
 * <ul>
 *   <li>{@code nanoTime} must be monotonic (e.g. {@link System#nanoTime()}), never wall-clock. The
 *       {@code now} passed to the {@link #hasPositiveCredit(long)} / {@link #debit(long, long)}
 *       overloads must come from that same monotonic source.</li>
 *   <li>{@code ratePerSecond} and {@code cost} are non-negative whole units (permits/sec or
 *       bytes/sec from {@link PerPodTopicQuota}).</li>
 *   <li>A debit is only valid after a successful credit check on the same bucket in the same
 *       admission attempt (see concurrency caveats).</li>
 * </ul>
 *
 * <h2>Thread safety and usage</h2>
 * <p>
 * Lock-free ({@code AtomicReference} + CAS). Safe to share one instance across Vert.x event-loop
 * threads (typical deployment: a small pool per pod). Hot path must stay in-memory — no I/O or
 * blocking here.
 * <p>
 * Under concurrent access, several threads may each observe positive credit and debit; worst-case
 * over-allow is bounded by {@code (concurrent admits) × cost}. This is accepted for a
 * protection guard-rail (VIP §7). {@link #hasPositiveCredit()} is an allocation-free read; only
 * {@link #debit(long)} persists state, where CAS retries add spin + a {@link BucketState}
 * allocation. Contention is per-topic, not global.
 *
 * <h2>Assumptions and behaviour</h2>
 * <ul>
 *   <li>Approximate enforcement is sufficient; {@code capacity = rate × windowSecs}, clamped to
 *       {@link Long#MAX_VALUE} on overflow so an extreme rate fails open rather than wrapping
 *       negative and rejecting everything.</li>
 *   <li>A bucket starts full (tokens = capacity) so a freshly created/rebalanced limiter does not
 *       cold-start by rejecting traffic.</li>
 *   <li>{@code rate == 0} yields {@code capacity == 0} and never admits — a deliberate fail-closed
 *       for a zero quota (the only case where this guard-rail rejects by construction).</li>
 *   <li>{@link #updateRate(long)} is rare (membership/quota change), not per message. A brief race
 *       with in-flight refills is acceptable. {@code rate} and {@code capacity} are separate
 *       volatiles, so a concurrent refill may briefly observe one updated and the other not.</li>
 *   <li>On a downward rate change, tokens already above the new (lower) capacity persist until the
 *       next timed refill re-clamps them; enforcement is briefly permissive after a shrink.</li>
 *   <li>Idle elapsed beyond one window is capped — refill cannot add more than {@code capacity}
 *       per update.</li>
 * </ul>
 */
public final class TokenBucket {

    private static final long NS_PER_SEC = 1_000_000_000L;

    private final LongSupplier nanoTime;
    private final int windowSecs;

    private volatile long ratePerSecond;
    private volatile long capacity;

    private final AtomicReference<BucketState> state = new AtomicReference<>(new BucketState(0L, 0L));

    /**
     * Creates a bucket that starts full (tokens = capacity).
     *
     * @param nanoTime     monotonic clock source (see preconditions)
     * @param windowSecs   burst window in seconds; {@code capacity = ratePerSecond × windowSecs}
     * @param ratePerSecond refill rate in whole units/sec; negative values are clamped to zero
     */
    public TokenBucket(LongSupplier nanoTime, int windowSecs, long ratePerSecond) {
        this.nanoTime = nanoTime;
        this.windowSecs = windowSecs;
        updateRate(ratePerSecond);
        long now = nanoTime.getAsLong();
        state.set(new BucketState(now, capacity));
    }

    public long ratePerSecond() {
        return ratePerSecond;
    }

    public long capacity() {
        return capacity;
    }

    /**
     * Updates the refill rate and capacity in place, retaining current token state so in-flight
     * debt/credit survives a membership or quota change. A negative rate is clamped to zero. See
     * the class-level notes on the downward-change transient and the rate/capacity volatile tear.
     */
    public void updateRate(long newRatePerSecond) {
        this.ratePerSecond = Math.max(0L, newRatePerSecond);
        long cap = this.ratePerSecond * windowSecs;
        // Guard against overflow for extreme rates: clamp to Long.MAX_VALUE so the guard-rail
        // stays fail-open (a wrapped negative capacity would reject all traffic).
        boolean overflowed = windowSecs != 0 && cap / windowSecs != this.ratePerSecond;
        this.capacity = overflowed ? Long.MAX_VALUE : cap;
    }

    /**
     * Computes refilled tokens without mutating state and returns whether they are strictly
     * positive. Persisting the refill here is unnecessary — {@link #debit(long)} recomputes from
     * {@code lastNano} — so this stays an allocation-free read on the hot path.
     */
    public boolean hasPositiveCredit() {
        return hasPositiveCredit(nanoTime.getAsLong());
    }

    /**
     * Variant taking a caller-supplied {@code now} so a single admission can share one clock read
     * across both credit checks and debits (see {@link TopicRateLimiter}); avoids redundant
     * {@code nanoTime} calls and gives a consistent instant for check-then-debit.
     */
    public boolean hasPositiveCredit(long now) {
        return refill(state.get(), now) > 0L;
    }

    /** Debits after a successful {@link #hasPositiveCredit()} check; may go negative (bounded debt). */
    public void debit(long cost) {
        debit(cost, nanoTime.getAsLong());
    }

    /** Variant taking a caller-supplied {@code now}; see {@link #hasPositiveCredit(long)}. */
    public void debit(long cost, long now) {
        while (true) {
            BucketState current = state.get();
            // A stale (smaller) now from a racing thread that won a later CAS must not move lastNano
            // backward, or the elapsed window would be recounted on the next refill (over-refill).
            long effectiveNow = Math.max(current.lastNano, now);
            BucketState updated = new BucketState(effectiveNow, refill(current, effectiveNow) - cost);
            if (state.compareAndSet(current, updated)) {
                return;
            }
        }
    }

    long tokensForTest() {
        BucketState current = state.get();
        return refill(current, nanoTime.getAsLong());
    }

    long lastNanoForTest() {
        return state.get().lastNano;
    }

    private long refill(BucketState current, long now) {
        if (now <= current.lastNano) {
            return current.tokens;
        }
        long elapsedNanos = Math.min(now - current.lastNano, (long)windowSecs * NS_PER_SEC);
        long added = tokensForElapsed(elapsedNanos, ratePerSecond);
        // added is non-negative; saturate on overflow so an extreme capacity stays fail-open
        // (a wrapped-negative sum would falsely reject all traffic).
        long refilled = current.tokens + added;
        if (refilled < current.tokens) {
            refilled = Long.MAX_VALUE;
        }
        return Math.min(capacity, refilled);
    }

    /**
     * Computes {@code floor(elapsedNanos × ratePerSecond / NS_PER_SEC)} using an integer
     * decomposition that avoids 64-bit overflow for any rate whose {@code capacity} fits in a
     * {@code long}. The naive product would overflow once {@code ratePerSecond} exceeds ~9.2e9
     * (bytes/sec), so both the elapsed nanos and the rate are split around {@link #NS_PER_SEC}:
     * the sub-second contribution becomes {@code remainderNanos × rateHi + remainderNanos ×
     * rateLo / NS_PER_SEC}, where each factor is below {@code NS_PER_SEC} in the divided term.
     */
    private static long tokensForElapsed(long elapsedNanos, long ratePerSecond) {
        if (elapsedNanos <= 0L || ratePerSecond <= 0L) {
            return 0L;
        }
        long wholeSeconds = elapsedNanos / NS_PER_SEC;
        long remainderNanos = elapsedNanos % NS_PER_SEC;
        long rateHi = ratePerSecond / NS_PER_SEC;
        long rateLo = ratePerSecond % NS_PER_SEC;
        long subSecond = remainderNanos * rateHi + (remainderNanos * rateLo) / NS_PER_SEC;
        return wholeSeconds * ratePerSecond + subSecond;
    }

    private record BucketState(long lastNano, long tokens) {
    }
}
