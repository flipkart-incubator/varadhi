package com.flipkart.varadhi.entities;

import com.google.common.collect.Multimap;

import java.util.List;

public interface Message {

    String getMessageId();

    String getGroupId();

    boolean hasHeader(String key);

    String getHeader(String key);

    List<String> getHeaders(String key);

    byte[] getPayload();

    Multimap<String, String> getHeaders();

    default Message withHeader(String key, String value) {
        getHeaders().put(key, value);
        return this;
    }

    default Message withoutHeader(String key) {
        getHeaders().removeAll(key);
        return this;
    }
}
