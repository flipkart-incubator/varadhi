package com.flipkart.varadhi.common;

import java.util.List;

import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.StdHeaders;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;


public class SimpleMessage implements Message {

    private final byte[] payload;
    private final ArrayListMultimap<String, String> requestHeaders;

    public SimpleMessage(byte[] payload, Multimap<String, String> requestHeaders) {
        this.payload = payload;
        this.requestHeaders = ArrayListMultimap.create(requestHeaders);
    }

    // TODO:: This will affect json, verify it.
    @Override
    public String getMessageId() {
        return getHeader(StdHeaders.get().msgId());
    }

    @Override
    public String getGroupId() {
        return getHeader(StdHeaders.get().groupId());
    }

    @Override
    public boolean hasHeader(String key) {
        return requestHeaders.containsKey(key);
    }

    @Override
    public String getHeader(String key) {
        return requestHeaders.get(key).getFirst();
    }

    @Override
    public List<String> getHeaders(String key) {
        return requestHeaders.get(key);
    }

    @Override
    public byte[] getPayload() {
        return payload;
    }

    @Override
    public Multimap<String, String> getHeaders() {
        return requestHeaders;
    }
}
