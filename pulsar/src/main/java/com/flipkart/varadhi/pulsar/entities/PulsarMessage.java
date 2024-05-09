package com.flipkart.varadhi.pulsar.entities;

import com.flipkart.varadhi.spi.services.PolledMessage;
import com.google.common.collect.Multimap;
import lombok.RequiredArgsConstructor;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.MessageIdImpl;

import java.util.List;

@RequiredArgsConstructor
public class PulsarMessage implements PolledMessage<PulsarOffset> {

    private final Message<byte[]> msg;

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
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String getGroupId() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean hasHeader(String key) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String getHeader(String key) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<String> getHeaders(String key) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public byte[] getPayload() {
        return msg.getValue();
    }

    @Override
    public Multimap<String, String> getRequestHeaders() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void release() {
        msg.release();
    }
}
