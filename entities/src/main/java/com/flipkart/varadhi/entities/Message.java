package com.flipkart.varadhi.entities;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

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
        this.headers.put(headerName, headerValue);
    }

    public void addHeaders(Map<String, String> headersToAdd) {
        this.headers.putAll(headersToAdd);
    }

    public String getHeader(String headerName) {
        //TODO:: 1. check the locale semantics. 2. Is case semantics ok.
        return this.headers.get(headerName.toUpperCase());
    }

}
