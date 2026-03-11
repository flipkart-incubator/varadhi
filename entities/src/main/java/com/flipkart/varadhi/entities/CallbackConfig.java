package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import java.util.Collections;
import java.util.Set;

/**
 * Configuration for callback-style subscriptions (e.g. Queue delivery).
 * Defines which HTTP response code ranges trigger callback behaviour and optional timeout.
 * <p>
 * Used when the subscription is queue-like: delivery endpoint is taken from message headers,
 * and callbacks are retried according to code ranges (e.g. 2xx = success, 5xx = retry).
 * </p>
 *
 * @see VaradhiSubscription#isCallbackStyle()
 */
@Getter
public class CallbackConfig {

    private final Set<CodeRange> codeRanges;
    private final long timeoutMs;

    @JsonCreator
    public CallbackConfig(
        @JsonProperty("codeRanges") Set<CodeRange> codeRanges,
        @JsonProperty("timeoutMs") long timeoutMs
    ) {
        this.codeRanges = codeRanges != null ? Set.copyOf(codeRanges) : Collections.emptySet();
        this.timeoutMs = timeoutMs > 0 ? timeoutMs : 0;
    }

    public CallbackConfig(Set<CodeRange> codeRanges) {
        this(codeRanges, 0);
    }

    /**
     * Factory to get callback config for a subscription that supports callback-style delivery.
     *
     * @param sub the Varadhi subscription (e.g. queue-backed with callback code ranges)
     * @return CallbackConfig built from the subscription's callback fields, or null if the
     *         subscription is not callback-style (no code ranges configured)
     */
    public static CallbackConfig getCallbackConfig(VaradhiSubscription sub) {
        if (sub == null || !sub.isCallbackStyle()) {
            return null;
        }
        Set<CodeRange> ranges = sub.getCallbackCodeRanges();
        if (ranges == null || ranges.isEmpty()) {
            return null;
        }
        long timeout = sub.getCallbackTimeoutMs() > 0 ? sub.getCallbackTimeoutMs() : 0;
        return new CallbackConfig(ranges, timeout);
    }

    /**
     * Returns true if the given HTTP response code matches any configured code range
     * (e.g. for deciding whether to trigger callback or retry).
     */
    public boolean matches(int code) {
        if (codeRanges == null) {
            return false;
        }
        for (CodeRange range : codeRanges) {
            if (range.inRange(code)) {
                return true;
            }
        }
        return false;
    }
}
