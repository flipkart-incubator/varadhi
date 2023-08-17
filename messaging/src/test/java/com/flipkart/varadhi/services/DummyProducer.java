package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.ProducerResult;
import com.flipkart.varadhi.spi.services.Producer;
import com.flipkart.varadhi.utils.JsonMapper;

import java.util.concurrent.CompletableFuture;

public class DummyProducer implements Producer {

    @Override
    public CompletableFuture<ProducerResult> ProduceAsync(Message message) {
        byte[] payload = message.getPayload();
        DummyMessage dm = JsonMapper.jsonDeserialize(new String(payload), DummyMessage.class);

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
            return new DummyProducerResult(dm.offSet);
        });
    }

    private RuntimeException loadClass(String className) {
        try {
            Class<RuntimeException> pluginClass = (Class<RuntimeException>) Class.forName(className);
            return pluginClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Fail to load class %s.", className), e);
        }
    }

    public record DummyMessage(int sleepMillis, int offSet, String exceptionClass, byte[] randomData) {
    }

    public static class DummyProducerResult extends ProducerResult {
        int offset;

        DummyProducerResult(int offset) {
            this.offset = offset;
        }

        @Override
        public int compareTo(ProducerResult o) {
            return offset - ((DummyProducerResult) o).offset;
        }
    }

}
