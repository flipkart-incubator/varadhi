package com.flipkart.varadhi.core.config;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.varadhi.entities.StdHeaders;
import com.google.common.collect.Multimap;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;

@Getter
public class MessageConfiguration {

    @NotNull
    private final StdHeaders stdHeaders;

    @NotNull
    private final int maxIdHeaderSize;

    @NotNull
    private final int maxRequestSize;

    @NotNull
    private final boolean filterNonCompliantHeaders;

    @JsonCreator
    public MessageConfiguration(
        @JsonProperty ("headers") StdHeaders stdHeaders,
        @JsonProperty ("maxIdHeaderSize") int maxIdHeaderSize,
        @JsonProperty ("maxRequestSize") int maxRequestSize,
        @JsonProperty ("filterNonCompliantHeaders") boolean filterNonCompliantHeaders
    ) {
        this.stdHeaders = stdHeaders;
        this.maxIdHeaderSize = maxIdHeaderSize;
        this.maxRequestSize = maxRequestSize;
        this.filterNonCompliantHeaders = filterNonCompliantHeaders;
        validate();
    }

    private void validate() {
        List<String> allowedPrefixList = stdHeaders.allowedPrefix();
        for (String prefix : allowedPrefixList) {
            if (prefix.isBlank()) {
                throw new IllegalArgumentException("Header prefix cannot be blank");
            }
            if (!prefix.equals(prefix.toUpperCase())) {
                throw new IllegalArgumentException("Header prefix must be in uppercase: " + prefix);
            }
        }

        for (String header : stdHeaders.getAllHeaderNames()) {
            if (header == null || header.isEmpty() || !startsWithValidPrefix(header)) {
                throw new IllegalArgumentException(
                    "Invalid header name " + header + "for header" + header
                                                   + "' is either null, empty, or does not start with a valid prefix."
                );
            }
            if (!header.equals(header.toUpperCase())) {
                throw new IllegalArgumentException("Header name must be in uppercase: " + header);
            }
        }
    }

    private boolean startsWithValidPrefix(String value) {
        return stdHeaders.allowedPrefix().stream().anyMatch(value::startsWith);
    }

    /**
     * @param headers which must be normalized
     */
    public void ensureRequiredHeaders(Multimap<String, String> headers, boolean queueBackedTopic) {
        List<String> missingHeaders = new ArrayList<>();
        List<String> requiredHeaderNames = queueBackedTopic ?
            stdHeaders.getHeaderNamesRequiredForQueueProduce() :
            stdHeaders.getHeaderNamesRequiredForProduce();
        ;
        for (String key : requiredHeaderNames) {
            if (!headers.containsKey(key)) {
                missingHeaders.add(key);
            }
        }

        if (!missingHeaders.isEmpty()) {
            throw new IllegalArgumentException("Missing required headers: " + String.join(", ", missingHeaders));
        }
    }
}
