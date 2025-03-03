package com.flipkart.varadhi.entities.config;

import com.flipkart.varadhi.entities.constants.MessageHeaders;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
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

    public List<String> getRequiredHeaders() {
        return List.of(mapping().get(MessageHeaders.MSG_ID));
    }

    public void ensureRequiredHeaders(Multimap<String, String> headers) {
        getRequiredHeaders().forEach(key -> {
            if (!headers.containsKey(key)) {
                throw new IllegalArgumentException(String.format("Missing required header %s", key));
            }
        });
    }

    public Multimap<String, String> returnVaradhiRecognizedHeaders(Multimap<String, String> headers) {
        Multimap<String, String> varadhiHeaders = ArrayListMultimap.create();
        for (Map.Entry<String, String> entry : headers.entries()) {
            String key = entry.getKey();
            if (filterNonCompliantHeaders()) {
                boolean validPrefix = allowedPrefix().stream().anyMatch(key::startsWith);
                if (validPrefix) {
                    varadhiHeaders.put(key.toUpperCase(), entry.getValue());
                }
            } else {
                varadhiHeaders.put(key.toUpperCase(), entry.getValue());
            }
        }
        return varadhiHeaders;
    }

    public String getMsgIdHeaderKey() {
        return mapping.get(MessageHeaders.MSG_ID);
    }

    public String getGroupIdHeaderKey() {
        return mapping.get(MessageHeaders.GROUP_ID);
    }

    public String getCallbackCodesKey() {
        return mapping.get(MessageHeaders.CALLBACK_CODE);
    }

    public String getRequestTimeoutKey() {
        return mapping.get(MessageHeaders.REQUEST_TIMEOUT);
    }

    public String getReplyToHttpUriHeaderKey() {
        return mapping.get(MessageHeaders.REPLY_TO_HTTP_URI);
    }

    public String getReplyToHttpMethodHeaderKey() {
        return mapping.get(MessageHeaders.REPLY_TO_HTTP_METHOD);
    }

    public String getReplyToHeaderKey() {
        return mapping.get(MessageHeaders.REPLY_TO);
    }

    public String getHttpUriHeaderKey() {
        return mapping.get(MessageHeaders.HTTP_URI);
    }

    public String getHttpMethodHeaderKey() {
        return mapping.get(MessageHeaders.HTTP_METHOD);
    }

    public String getHttpContentTypeKey() {
        return mapping.get(MessageHeaders.CONTENT_TYPE);
    }

    public String getProduceIdentityKey() {
        return mapping.get(MessageHeaders.PRODUCE_IDENTITY);
    }

    public String getProduceRegionKey() {
        return mapping.get(MessageHeaders.PRODUCE_REGION);
    }

    public String getProduceTimestampKey() {
        return mapping.get(MessageHeaders.PRODUCE_TIMESTAMP);
    }


}
