package com.flipkart.varadhi.pulsar.consumer;

import com.flipkart.varadhi.pulsar.entities.PulsarMessages;
import com.flipkart.varadhi.pulsar.entities.PulsarOffset;
import com.flipkart.varadhi.spi.services.Consumer;
import com.flipkart.varadhi.spi.services.PolledMessages;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor
public class PulsarConsumer implements Consumer<PulsarOffset> {

    private final org.apache.pulsar.client.api.Consumer<byte[]> pulsarConsumer;

    @Override
    public CompletableFuture<PolledMessages<PulsarOffset>> receiveAsync() {
        return pulsarConsumer.batchReceiveAsync().thenApply(PulsarMessages::new);
    }

    @Override
    public CompletableFuture<Void> commitCumulativeAsync(String topic, int partition, PulsarOffset offset) {
        return pulsarConsumer.acknowledgeCumulativeAsync(offset.getMessageId());
    }

    @Override
    public void close() throws Exception {
        pulsarConsumer.close();
    }
}
