package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOG = LoggerFactory.getLogger(CallbackConfig.class);

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

    /**
     * Parses request JSON (list of code ranges) into CallbackConfig.
     * Expects format: [{"from": 200, "to": 299}, {"from": 400, "to": 404}]
     *
     * @param requestJson JSON string (e.g. from HTTP header); null or empty returns null
     * @return CallbackConfig or null if input is null/empty or parsing fails
     */
    public static CallbackConfig fromJson(String requestJson) {
        if (requestJson == null || requestJson.isBlank()) {
            return null;
        }
        try {
            Set<CodeRange> codeRanges = JsonMapper.getMapper()
                                                  .readValue(requestJson, new TypeReference<Set<CodeRange>>() {
                                                  });
            return new CallbackConfig(codeRanges);
        } catch (Exception e) {
            LOG.error("Unable to deserialize callback code ranges from json: {} - {}", requestJson, e.getMessage());
            return null;
        }
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
