package com.flipkart.varadhi.spi.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.Offset;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class DummyProducer implements Producer {

    private final ObjectMapper objectMapper;

    public DummyProducer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public CompletableFuture<Offset> produceAsync(Message message) {
        byte[] payload = message.getPayload();
        DummyMessage dm = toDummyMsg(payload);

        return CompletableFuture.supplyAsync(() -> {
            if (dm.sleepMillis > 0) {
                try {
                    Thread.sleep(dm.sleepMillis);
                } catch (Exception e) {
                    //ignore for now.
                }
            }
            if (null != dm.exceptionClass && !dm.exceptionClass.isBlank()) {
                throw loadClass(dm.exceptionClass);
            }
            return new DummyOffset(dm.offSet);
        });
    }

    private DummyMessage toDummyMsg(byte[] payload) {
        DummyMessage dm;
        try {
            dm = objectMapper.readValue(payload, DummyMessage.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return dm;
    }

    private RuntimeException loadClass(String className) {
        try {
            Class<RuntimeException> pluginClass = (Class<RuntimeException>)Class.forName(className);
            return pluginClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Fail to load class %s.", className), e);
        }
    }

    public record DummyMessage(int sleepMillis, int offSet, String exceptionClass, byte[] randomData) {
    }


    public static class DummyOffset implements Offset {
        int offset;

        public DummyOffset(int offset) {
            this.offset = offset;
        }

        @Override
        public int compareTo(Offset o) {
            return offset - ((DummyOffset)o).offset;
        }
    }

}
