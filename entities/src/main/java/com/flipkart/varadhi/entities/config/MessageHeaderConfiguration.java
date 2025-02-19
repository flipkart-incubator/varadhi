package com.flipkart.varadhi.entities.config;

import com.flipkart.varadhi.entities.Validatable;
import com.flipkart.varadhi.entities.constants.StandardHeaders;
import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.util.List;
import java.util.Map;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
@Builder
public class MessageHeaderConfiguration implements Validatable {
    @NotNull
    private final Map<StandardHeaders, String> mapping;
    @NotNull
    private final List<String> allowedPrefix;
    @NotNull
    private final Integer headerValueSizeMax;
    @NotNull
    private final Integer maxRequestSize;
    @JsonCreator
    public MessageHeaderConfiguration(
            @JsonProperty("mapping") Map<StandardHeaders, String> mapping,
            @JsonProperty("allowedPrefix") List<String> allowedPrefix,
            @JsonProperty("headerValueSizeMax") Integer headerValueSizeMax,
            @JsonProperty("maxRequestSize") Integer maxRequestSize) {
        this.mapping = mapping;
        this.allowedPrefix = allowedPrefix;
        this.headerValueSizeMax = headerValueSizeMax;
        this.maxRequestSize = maxRequestSize;
    }


    @SneakyThrows
    @Override
    public void validate() {
        List<String> allowedPrefixList = getAllowedPrefix();
        for (String prefix : allowedPrefixList) {
            if (prefix.isEmpty() || prefix.isBlank()) {
                throw new IllegalArgumentException("Header prefix cannot be blank");
            }
        }

        for (StandardHeaders header : StandardHeaders.values()) {
            String value = mapping.get(header);

            // Combined check for null/empty and valid prefix
            if (value == null || value.isEmpty() || !startsWithValidPrefix(allowedPrefixList, value)) {
                throw new IllegalArgumentException(
                        "Invalid value for header " + header + ": value '" + value +
                                "' is either null, empty, or does not start with a valid prefix.");
            }
        }

    }

    private boolean startsWithValidPrefix(List<String> allowedPrefixes, String value) {
        for (String prefix : allowedPrefixes) {
            if (value.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }
}





