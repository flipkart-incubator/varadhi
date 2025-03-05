package com.flipkart.varadhi.config;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.varadhi.entities.StdHeaders;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;

@Getter
public class MessageConfiguration {

    @NotNull
    private final StdHeaders stdHeaders;

    @NotNull
    private final int maxHeaderValueSize;

    @NotNull
    private final int maxRequestSize;

    @NotNull
    private final boolean filterNonCompliantHeaders;

    private final List<String> requiredHeaders;

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
        this.requiredHeaders = List.of(stdHeaders.msgId());
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

    /**
     * @param headers which must be normalized
     */
    public void ensureRequiredHeaders(Multimap<String, String> headers) {
        getRequiredHeaders().forEach(key -> {
            if (!headers.containsKey(key)) {
                throw new IllegalArgumentException(String.format("Missing required header %s", key));
            }
        });
    }

    /**
     * The method needs to return a copy with normalized casing of headers.
     *
     * @param headers
     * @return
     */
    public Multimap<String, String> filterCompliantHeaders(Multimap<String, String> headers) {
        Multimap<String, String> copy = ArrayListMultimap.create();

        if (!filterNonCompliantHeaders) {
            for (Map.Entry<String, String> entry : headers.entries()) {
                copy.put(entry.getKey().toUpperCase(), entry.getValue());
            }
            return copy;
        }

        for (Map.Entry<String, String> entry : headers.entries()) {
            String key = entry.getKey().toUpperCase();
            boolean validPrefix = stdHeaders.allowedPrefix().stream().anyMatch(key::startsWith);
            if (validPrefix) {
                copy.put(key, entry.getValue());
            }
        }
        return copy;
    }

    public StdHeaders stdHeaders() {
        return stdHeaders;
    }

    public int maxHeaderValueSize() {
        return maxHeaderValueSize;
    }

    public int maxRequestSize() {
        return maxRequestSize;
    }

    public boolean filterNonCompliantHeaders() {
        return filterNonCompliantHeaders;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        var that = (MessageConfiguration)obj;
        return Objects.equals(this.stdHeaders, that.stdHeaders) && this.maxHeaderValueSize == that.maxHeaderValueSize
               && this.maxRequestSize == that.maxRequestSize && this.filterNonCompliantHeaders
                                                                == that.filterNonCompliantHeaders;
    }

    @Override
    public int hashCode() {
        return Objects.hash(stdHeaders, maxHeaderValueSize, maxRequestSize, filterNonCompliantHeaders);
    }

    @Override
    public String toString() {
        return "MessageConfiguration[" + "stdHeaders=" + stdHeaders + ", " + "maxHeaderValueSize=" + maxHeaderValueSize
               + ", " + "maxRequestSize=" + maxRequestSize + ", " + "filterNonCompliantHeaders="
               + filterNonCompliantHeaders + ']';
    }
}
