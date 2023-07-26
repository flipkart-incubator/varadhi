package com.flipkart.varadhi.entities;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
public class Message {
    private final String messageId;
    private final String groupId;
    private final byte[] payload;
    private final Map<String, String> headers;

    public Message(
            String messageId,
            String groupId,
            byte[] payload,
            Map<String, String> headers
    ) {
        this.messageId = messageId;
        this.groupId = groupId;
        this.payload = payload;
        this.headers = null == headers ? new HashMap<>() : headers;
    }

    public void addHeader(String headerName, String headerValue) {
        // add only valid headers and values.
        if (isValid(headerName) && isValid(headerValue)) {
            this.headers.put(headerName, headerValue);
        }
    }

    private boolean isValid(String keyOrValue) {
        return null != keyOrValue && !keyOrValue.isEmpty();
    }

    public void addHeaders(Map<String, String> headersToAdd) {
        this.headers.putAll(headersToAdd.
                entrySet()
                .stream()
                .filter((e) -> isValid(e.getKey()) && isValid(e.getValue()))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()))
        );
    }

    public String getHeader(String headerName) {
        //TODO:: 1. check the locale semantics. 2. Is case semantics ok.
        return this.headers.get(headerName.toUpperCase());
    }

}
