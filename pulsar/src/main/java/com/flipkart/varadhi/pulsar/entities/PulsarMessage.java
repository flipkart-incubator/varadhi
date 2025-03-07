package com.flipkart.varadhi.pulsar.entities;

import java.util.List;
import java.util.Map;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageIdImpl;

import com.flipkart.varadhi.entities.StdHeaders;
import com.flipkart.varadhi.pulsar.util.PropertyHelper;
import com.flipkart.varadhi.spi.services.PolledMessage;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class PulsarMessage implements PolledMessage<PulsarOffset> {

    private final Message<byte[]> msg;
    private ArrayListMultimap<String, String> requestHeaders = null;

    public PulsarMessage(Message<byte[]> msg) {
        this.msg = msg;
    }

    private ArrayListMultimap<String, String> requestHeaders() {
        if (requestHeaders == null) {
            requestHeaders = computeRequestHeaders();
        }
        return requestHeaders;
    }

    private ArrayListMultimap<String, String> computeRequestHeaders() {
        ArrayListMultimap<String, String> headers = ArrayListMultimap.create();
        for (Map.Entry<String, String> entry : msg.getProperties().entrySet()) {
            headers.putAll(entry.getKey(), PropertyHelper.decodePropertyValues(entry.getValue()));
        }
        return headers;
    }

    @Override
    public long getProducedTimestampMs() {
        return msg.getPublishTime();
    }

    @Override
    public String getTopicName() {
        return msg.getTopicName();
    }

    @Override
    public int getPartition() {
        if (msg.getMessageId() instanceof MessageIdImpl) {
            return ((MessageIdImpl)msg.getMessageId()).getPartitionIndex();
        } else if (msg.getMessageId() instanceof TopicMessageIdImpl) {
            MessageIdImpl innerMessageId = (MessageIdImpl)((TopicMessageIdImpl)msg.getMessageId()).getInnerMessageId();
            return innerMessageId.getPartitionIndex();
        } else {
            throw new IllegalStateException("Unknown message id type: " + msg.getMessageId().getClass());
        }
    }

    @Override
    public PulsarOffset getOffset() {
        return new PulsarOffset(msg.getMessageId());
    }

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
        return requestHeaders().containsKey(key);
    }

    @Override
    public String getHeader(String key) {
        return requestHeaders().get(key).get(0);
    }

    @Override
    public List<String> getHeaders(String key) {
        return requestHeaders().get(key);
    }

    @Override
    public byte[] getPayload() {
        return msg.getValue();
    }

    @Override
    public Multimap<String, String> getHeaders() {
        return requestHeaders();
    }

    @Override
    public void release() {
        msg.release();
    }
}
