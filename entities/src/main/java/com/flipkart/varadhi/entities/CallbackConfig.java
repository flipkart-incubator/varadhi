package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Configuration for callback-style subscriptions (e.g. Queue delivery).
 * List of callback code ranges: defines which HTTP response code ranges trigger callback behaviour.
 *
 * @see VaradhiSubscription#getCallbackConfig()
 */
@NoArgsConstructor
@EqualsAndHashCode
public class CallbackConfig {

    /**
     * Set of code ranges on which callback should be applied (closed range).
     * Mutable internally via {@link #addRange(CodeRange)}; exposed as unmodifiable.
     */
    private final Set<CodeRange> codeRanges = new HashSet<>();

    @JsonCreator
    public CallbackConfig(@JsonProperty ("codeRanges") Set<CodeRange> codeRanges) {
        if (codeRanges != null) {
            this.codeRanges.addAll(codeRanges);
        }
    }

    /** Returns an unmodifiable view of the code ranges. */
    public Set<CodeRange> getCodeRanges() {
        return Collections.unmodifiableSet(codeRanges);
    }

    public void addRange(CodeRange range) {
        if (range != null) {
            codeRanges.add(range);
        }
    }

    /**
     * Returns true if the given HTTP response code falls in any configured code range
     * (e.g. for deciding whether to trigger callback or retry).
     */
    public boolean shouldCallback(int responseCode) {
        return codeRanges.stream().anyMatch(r -> r.inRange(responseCode));
    }
}
