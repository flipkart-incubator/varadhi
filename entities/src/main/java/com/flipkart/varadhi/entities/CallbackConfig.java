package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.Sets;
import lombok.*;

import java.util.Set;

/**
 * Immutable configuration for callback-style subscriptions (e.g. Queue delivery).
 * List of callback code ranges: defines which HTTP response code ranges trigger callback behaviour.
 * Structure aligned with QueueCallbackConfig in the request/API layer.
 *
 * @see VaradhiSubscription#getCallbackConfig()
 */
@Getter
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties (ignoreUnknown = true)
public class CallbackConfig {

    private @With Set<CodeRange> codeRanges = Sets.newHashSet();

    /**
     * Returns true if the given HTTP response code falls in any configured code range
     * (e.g. for deciding whether to trigger callback or retry).
     */
    public boolean shouldCallback(int responseCode) {
        return codeRanges.stream().anyMatch(r -> r.inRange(responseCode));
    }
}
