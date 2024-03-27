package com.flipkart.varadhi.pulsar.entities;

import com.flipkart.varadhi.spi.services.PolledMessage;
import lombok.RequiredArgsConstructor;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.MessageIdImpl;

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
    public byte[] getPayload() {
        return msg.getValue();
    }

    @Override
    public void release() {
        msg.release();
    }
}
