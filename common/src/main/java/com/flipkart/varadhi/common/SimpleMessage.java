package com.flipkart.varadhi.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.io.Serializable;
import java.util.List;

import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.StdHeaders;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;


public class SimpleMessage implements Message, Serializable {

    @Serial
    private static final long serialVersionUID = 30387897349875L;

    private final byte[] payload;
    private final ArrayListMultimap<String, String> requestHeaders;

    public SimpleMessage(byte[] payload, Multimap<String, String> requestHeaders) {
        this.payload = payload;
        this.requestHeaders = ArrayListMultimap.create(requestHeaders);
    }

    public SimpleMessage(Message msg) {
        this.payload = msg.getPayload();
        this.requestHeaders = ArrayListMultimap.create(msg.getHeaders());
        StdHeaders headers = StdHeaders.get();
        if (msg.getMessageId() != null && !this.requestHeaders.containsKey(headers.msgId())) {
            this.requestHeaders.put(headers.msgId(), msg.getMessageId());
        }
        if (msg.getGroupId() != null && !this.requestHeaders.containsKey(headers.groupId())) {
            this.requestHeaders.put(headers.groupId(), msg.getGroupId());
        }
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

    public byte[] serialize() throws IOException {
        try (
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bos)
        ) {
            out.writeObject(this);
            return bos.toByteArray();
        }
    }

    public static SimpleMessage deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        try (
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream in = new ObjectInputStream(bis)
        ) {
            return (SimpleMessage)in.readObject();
        }
    }
}
