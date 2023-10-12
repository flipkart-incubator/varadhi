package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.exceptions.ArgumentException;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import lombok.Getter;

import java.util.List;

import static com.flipkart.varadhi.MessageConstants.Headers.MESSAGE_ID;
import static com.flipkart.varadhi.MessageConstants.Headers.REQUIRED_HEADERS;

@Getter
public class Message {
    private final byte[] payload;
    private final Multimap<String, String> requestHeaders;

    public Message(
            byte[] payload,
            Multimap<String, String> requestHeaders
    ) {
        checkRequiredHeaders(requestHeaders);
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
        return ((ArrayListMultimap<String, String>) requestHeaders).get(key).get(0);
    }

    public List<String> getHeaders(String key) {
        return ((ArrayListMultimap<String, String>) requestHeaders).get(key);
    }

    private void checkRequiredHeaders(Multimap<String, String> requestHeaders) {
        REQUIRED_HEADERS.forEach(key -> {
            if (!requestHeaders.containsKey(key)) {
                throw new ArgumentException(String.format("Missing required header %s", key));
            }
        });
    }

}
