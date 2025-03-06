package com.flipkart.varadhi.config;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.varadhi.entities.StdHeaders;

import jakarta.validation.constraints.NotNull;
import lombok.Builder;

@Builder
public record MessageConfiguration(
    @NotNull StdHeaders stdHeaders,
    @NotNull int maxHeaderIdSize,
    @NotNull int maxRequestSize,
    @NotNull boolean filterNonCompliantHeaders
) {
    @JsonCreator
    public MessageConfiguration(
        @JsonProperty ("headers") StdHeaders stdHeaders,
        @JsonProperty ("maxHeaderIdSize") int maxHeaderIdSize,
        @JsonProperty ("maxRequestSize") int maxRequestSize,
        @JsonProperty ("filterNonCompliantHeaders") boolean filterNonCompliantHeaders
    ) {
        this.stdHeaders = stdHeaders;
        this.maxHeaderIdSize = maxHeaderIdSize;
        this.maxRequestSize = maxRequestSize;
        this.filterNonCompliantHeaders = filterNonCompliantHeaders;
        validate();
    }

    private void validate() {
        List<String> allowedPrefixList = stdHeaders.allowedPrefix();
        for (String prefix : allowedPrefixList) {
            if (prefix.isBlank() || prefix.isEmpty()) {
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
}
