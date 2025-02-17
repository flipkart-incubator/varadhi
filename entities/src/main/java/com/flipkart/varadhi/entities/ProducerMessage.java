package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.entities.config.MessageHeaderConfiguration;
import com.flipkart.varadhi.entities.constants.StandardHeaders;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.util.List;


public class ProducerMessage implements Message {
    private final byte[] payload;
    private final ArrayListMultimap<String, String> requestHeaders;

    public ProducerMessage(
            byte[] payload,
            Multimap<String, String> requestHeaders
    ) {
        this.payload = payload;
        this.requestHeaders = ArrayListMultimap.create(requestHeaders);
    }

    // TODO:: This will affect json, verify it.
    @Override
    public String getMessageId() {
        return getHeader(StandardHeaders.msgIdHeader);
    }

    @Override
    public String getGroupId() {
        return getHeader(StandardHeaders.groupIdHeader);
    }

    @Override
    public boolean hasHeader(String key) {
        return requestHeaders.containsKey(key);
    }

    @Override
    public String getHeader(String key) {
        return requestHeaders.get(key).get(0);
    }

    @Override
    public List<String> getHeaders(String key) {
        return (requestHeaders).get(key);
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
