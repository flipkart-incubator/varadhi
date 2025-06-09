package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Utf8;
import com.google.common.collect.Multimap;

import java.util.List;
import java.util.Map;

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

    // TODO: add support for caching the calculated size.
    @JsonIgnore
    default int getTotalSizeBytes() {
        int headersAndBodySize = 0;
        for (Map.Entry<String, String> entry : getHeaders().entries()) {
            String key = entry.getKey();
            String value = entry.getValue();
            int byteLength = Utf8.encodedLength(key) + Utf8.encodedLength(value);
            headersAndBodySize += byteLength;
        }
        headersAndBodySize += getPayload().length;
        return headersAndBodySize;
    }
}
