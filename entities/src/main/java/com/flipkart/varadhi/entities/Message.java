package com.flipkart.varadhi.entities;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import lombok.Getter;

import static com.flipkart.varadhi.MessageConstants.Headers.MESSAGE_ID;
import static com.flipkart.varadhi.MessageConstants.Headers.REQUIRED_HEADERS;

@Getter
public class Message {
    private final byte[] payload;
    private final ArrayListMultimap<String, String> headers;

    public Message(
            byte[] payload,
            Multimap<String, String> headers
    ) {
        checkRequiredHeaders(headers);
        this.payload = payload;
        this.headers = ArrayListMultimap.create(headers);
    }

    // TODO:: This will affect json, verify it.
    public String getMessageId() {
        return headers.get(MESSAGE_ID).get(0);
    }

    private void checkRequiredHeaders(Multimap<String, String> headers) {
        REQUIRED_HEADERS.forEach(key -> {
            if (!headers.containsKey(key)) {
                throw new IllegalArgumentException(String.format("Missing required header %s", key));
            }
        });
    }

}
