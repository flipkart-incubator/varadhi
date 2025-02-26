package com.flipkart.varadhi.entities.config;

import com.flipkart.varadhi.entities.constants.MessageHeaders;
import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Builder
public record MessageHeaderConfiguration(@NotNull Map<MessageHeaders, String> mapping, @NotNull List<String> allowedPrefix, @NotNull int headerValueSizeMax, @NotNull int maxRequestSize, @NotNull boolean filterNonCompliantHeaders) {
    @JsonCreator
    public MessageHeaderConfiguration(@JsonProperty ("mapping")
    Map<MessageHeaders, String> mapping, @JsonProperty ("allowedPrefix")
    List<String> allowedPrefix, @JsonProperty ("headerValueSizeMax")
    int headerValueSizeMax, @JsonProperty ("maxRequestSize")
    int maxRequestSize, @JsonProperty ("filterNonCompliantHeaders")
    boolean filterNonCompliantHeaders) {
        this.mapping = mapping;
        this.allowedPrefix = allowedPrefix;
        this.headerValueSizeMax = headerValueSizeMax;
        this.maxRequestSize = maxRequestSize;
        this.filterNonCompliantHeaders = filterNonCompliantHeaders;
        validate();
    }

    private void validate() {
        List<String> allowedPrefixList = allowedPrefix();
        for (String prefix : allowedPrefixList) {
            if (prefix.isEmpty() || prefix.isBlank()) {
                throw new IllegalArgumentException("Header prefix cannot be blank");
            }
        }

        for (MessageHeaders header : MessageHeaders.values()) {
            String value = mapping.get(header);

            // Combined check for null/empty and valid prefix
            if (value == null || value.isEmpty() || !startsWithValidPrefix(value)) {
                throw new IllegalArgumentException(
                    "Invalid header name" + value + "for header" + header
                                                   + "' is either null, empty, or does not start with a valid prefix."
                );
            }
        }

    }

    private boolean startsWithValidPrefix(String value) {
        return allowedPrefix.stream().anyMatch(value::startsWith);
    }
}
