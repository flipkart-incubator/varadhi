package com.flipkart.varadhi.pulsar.entities;

import com.flipkart.varadhi.entities.StandardHeaders;
import com.flipkart.varadhi.pulsar.util.PropertyHelper;
import com.flipkart.varadhi.spi.services.PolledMessage;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.MessageIdImpl;

import java.util.List;
import java.util.Map;

public class PulsarMessage implements PolledMessage<PulsarOffset> {

    private final Message<byte[]> msg;
    private final ArrayListMultimap<String, String> requestHeaders = ArrayListMultimap.create();

    public PulsarMessage(Message<byte[]> msg) {
        this.msg = msg;
        populateRequestHeaders();
    }

    private void populateRequestHeaders() {
        for (Map.Entry<String, String> entry : msg.getProperties().entrySet()) {
            requestHeaders.putAll(entry.getKey(), PropertyHelper.decodePropertyValues(entry.getValue()));
        }
    }

    @Override
    public String getTopicName() {
        return msg.getTopicName();
    }

    @Override
    public int getPartition() {
        return ((MessageIdImpl) msg.getMessageId()).getPartitionIndex();
    }

    @Override
    public PulsarOffset getOffset() {
        return new PulsarOffset(msg.getMessageId());
    }

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
        return requestHeaders.get(key).get(0);
    }

    @Override
    public List<String> getHeaders(String key) {
        return requestHeaders.get(key);
    }

    @Override
    public byte[] getPayload() {
        return msg.getValue();
    }

    @Override
    public Multimap<String, String> getRequestHeaders() {
        return requestHeaders;
    }

    @Override
    public void release() {
        msg.release();
    }
}
