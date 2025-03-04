package com.flipkart.varadhi.config;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.varadhi.entities.StdHeaders;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import jakarta.validation.constraints.NotNull;
import lombok.Builder;

@Builder
public record MessageConfiguration(
    @NotNull StdHeaders stdHeaders,
    @NotNull int maxHeaderValueSize,
    @NotNull int maxRequestSize,
    @NotNull boolean filterNonCompliantHeaders
) {
    @JsonCreator
    public MessageConfiguration(
        @JsonProperty ("headers") StdHeaders stdHeaders,
        @JsonProperty ("maxHeaderValueSize") int maxHeaderValueSize,
        @JsonProperty ("maxRequestSize") int maxRequestSize,
        @JsonProperty ("filterNonCompliantHeaders") boolean filterNonCompliantHeaders
    ) {
        this.stdHeaders = stdHeaders;
        this.maxHeaderValueSize = maxHeaderValueSize;
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
        }

        for (String header : stdHeaders.getAllHeaderNames()) {
            if (header == null || header.isEmpty() || !startsWithValidPrefix(header)) {
                throw new IllegalArgumentException(
                    "Invalid header name " + header + "for header" + header
                                                   + "' is either null, empty, or does not start with a valid prefix."
                );
            }
        }
    }

    private boolean startsWithValidPrefix(String value) {
        return stdHeaders.allowedPrefix().stream().anyMatch(value::startsWith);
    }

    public List<String> getRequiredHeaders() {
        return List.of(stdHeaders.msgId());
    }

    public void ensureRequiredHeaders(Multimap<String, String> headers) {
        getRequiredHeaders().forEach(key -> {
            if (!headers.containsKey(key)) {
                throw new IllegalArgumentException(String.format("Missing required header %s", key));
            }
        });
    }

    /**
     * The method needs to return a copy.
     * @param headers
     * @return
     */
    public Multimap<String, String> filterCompliantHeaders(Multimap<String, String> headers) {
        Multimap<String, String> copy = ArrayListMultimap.create();

        if (!filterNonCompliantHeaders) {
            copy.putAll(headers);
            return copy;
        }

        for (Map.Entry<String, String> entry : headers.entries()) {
            String key = entry.getKey();
            boolean validPrefix = stdHeaders.allowedPrefix().stream().anyMatch(key::startsWith);
            if (validPrefix) {
                copy.put(key, entry.getValue());
            }
        }
        return copy;
    }
}
