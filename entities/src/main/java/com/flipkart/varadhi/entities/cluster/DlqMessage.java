package com.flipkart.varadhi.entities.cluster;

import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.entities.StandardHeaders;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.List;

public record DlqMessage(byte[] payload, ArrayListMultimap<String, String> requestHeaders, Offset offset,
                         int partitionIndex) implements Message {

    @Override
    public String getMessageId() {
        return getHeader(StandardHeaders.MESSAGE_ID);
    }

    @Override
    public String getGroupId() {
        return getHeader(StandardHeaders.GROUP_ID);
    }

    @Override
    public boolean hasHeader(String key) {
        return requestHeaders.containsKey(key);
    }

    @Override
    public String getHeader(String key) {
        return !requestHeaders.containsKey(key) || requestHeaders.get(key).size() == 0 ? null :
                requestHeaders.get(key).get(0);
    }

    @Override
    public List<String> getHeaders(String key) {
        return new ArrayList<>(requestHeaders.get(key));
    }

    @Override
    public byte[] getPayload() {
        return payload;
    }

    @Override
    public Multimap<String, String> getRequestHeaders() {
        return requestHeaders;
    }
}
