package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Configuration for callback-style subscriptions (e.g. Queue delivery).
 * List of callback code ranges: defines which HTTP response code ranges trigger callback behaviour.
 * <p>
 * Aligned with QueueCallbackConfig: mutable set of code ranges, fromJson (header/request), addRange, shouldCallback.
 * Optional timeoutMs for request timeout; 0 means not set.
 * </p>
 *
 * @see VaradhiSubscription#getCallbackConfig()
 */
@Getter
@Setter
@NoArgsConstructor
public class CallbackConfig {

    private static final Logger LOG = LoggerFactory.getLogger(CallbackConfig.class);

    /**
     * Set of code ranges on which callback should be applied (closed range).
     * Mutable so ranges can be added via {@link #addRange(CodeRange)} or parsed from JSON.
     */
    private final Set<CodeRange> codeRanges = new HashSet<>();

    @JsonCreator
    public CallbackConfig(
        @JsonProperty("codeRanges") Set<CodeRange> codeRanges
    ) {
        if (codeRanges != null) {
            this.codeRanges.addAll(codeRanges);
        }
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
            Set<CodeRange> codeRanges = JsonMapper.getMapper().readValue(
                requestJson,
                new TypeReference<Set<CodeRange>>() {}
            );
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
        for (CodeRange range : codeRanges) {
            if (range.inRange(responseCode)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Same as {@link #shouldCallback(int)}; kept for backward compatibility.
     */
    public boolean matches(int code) {
        return shouldCallback(code);
    }

    /**
     * Factory: callback config from a Varadhi subscription that has callback config.
     *
     * @param sub Varadhi subscription (e.g. queue-backed with callback config)
     * @return sub.getCallbackConfig() or null if sub or sub.getCallbackConfig() is null
     */
    public static CallbackConfig getCallbackConfig(VaradhiSubscription sub) {
        return sub == null ? null : sub.getCallbackConfig();
    }

    /**
     * Get callback config for the message. If not available, get the config from queue config
     *
     * @param message   being processed
     * @param topicName to get callback config for
     *
     * @return the callback config if queue and is configured. Else, return null
     */
    CallbackConfig getCallbackConfig(final Message message, final String topicName) {
        final String callbackCodes = message.getHeader(StdHeaders.get().callbackCodes());
        // here we're getting callback code from message
        // thus even if we remove all failure callback codes from queue callback config
        // we can never be sure that a failure code will never lead to a callback
        // maybe we can remove any failure codes from the callback config here
        CallbackConfig callbackConfig = CallbackConfig.fromJson(callbackCodes);
        return callbackConfig;
    }
}
