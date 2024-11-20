package com.flipkart.varadhi.pulsar.consumer;

import com.flipkart.varadhi.pulsar.entities.PulsarMessages;
import com.flipkart.varadhi.pulsar.entities.PulsarOffset;
import com.flipkart.varadhi.spi.services.Consumer;
import com.flipkart.varadhi.spi.services.PolledMessage;
import com.flipkart.varadhi.spi.services.PolledMessages;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor
public class PulsarConsumer implements Consumer<PulsarOffset> {

    private final org.apache.pulsar.client.api.Consumer<byte[]> pulsarConsumer;

    @Override
    public CompletableFuture<PolledMessages<PulsarOffset>> receiveAsync() {
        CompletableFuture<PolledMessages<PulsarOffset>> promise = new CompletableFuture<>();
        receiveNonZeroMessages(promise);
        return promise;
    }

    private void receiveNonZeroMessages(CompletableFuture<PolledMessages<PulsarOffset>> promise) {
        pulsarConsumer.batchReceiveAsync().thenAccept(m -> {
            if (m.size() == 0) {
                receiveNonZeroMessages(promise);
            } else {
                promise.complete(new PulsarMessages(m));
            }
        });
    }

    @Override
    public CompletableFuture<Void> commitCumulativeAsync(PolledMessage<PulsarOffset> message) {
        return pulsarConsumer.acknowledgeCumulativeAsync(message.getOffset().getMessageId());
    }

    @Override
    public CompletableFuture<Void> commitIndividualAsync(PolledMessage<PulsarOffset> message) {
        return pulsarConsumer.acknowledgeAsync(message.getOffset().getMessageId());
    }

    @Override
    public void close() throws IOException {
        pulsarConsumer.close();
    }
}
