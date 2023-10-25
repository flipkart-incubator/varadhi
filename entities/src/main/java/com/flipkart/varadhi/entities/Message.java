package com.flipkart.varadhi.entities;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import lombok.Getter;

import java.util.List;

import static com.flipkart.varadhi.MessageConstants.Headers.MESSAGE_ID;

@Getter
public class Message {
    private final byte[] payload;
    private final ArrayListMultimap<String, String> requestHeaders;

    public Message(
            byte[] payload,
            Multimap<String, String> requestHeaders
    ) {
        this.payload = payload;
        this.requestHeaders = ArrayListMultimap.create(requestHeaders);
    }

    // TODO:: This will affect json, verify it.
    public String getMessageId() {
        return getHeader(MESSAGE_ID);
    }

    public boolean hasHeader(String key) {
        return requestHeaders.containsKey(key);
    }

    public String getHeader(String key) {
        return requestHeaders.get(key).get(0);
    }

    public List<String> getHeaders(String key) {
        return (requestHeaders).get(key);
    }
}
