package com.flipkart.varadhi.pulsar.consumer;

import com.flipkart.varadhi.pulsar.config.ConsumerOptions;
import com.flipkart.varadhi.pulsar.config.TelemetryOptions;
import com.flipkart.varadhi.pulsar.entities.PulsarMessages;
import com.flipkart.varadhi.pulsar.entities.PulsarOffset;
import com.flipkart.varadhi.spi.services.Consumer;
import com.flipkart.varadhi.spi.services.PolledMessage;
import com.flipkart.varadhi.spi.services.PolledMessages;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class PulsarConsumer implements Consumer<PulsarOffset> {
    private final org.apache.pulsar.client.api.Consumer<byte[]> pulsarConsumer;

    public PulsarConsumer(
        PulsarClient pulsarClient,
        ConsumerOptions consumerOptions,
        Set<String> topicNames,
        String subscriptionName,
        String consumerName,
        TelemetryOptions telemetryOptions
    ) throws PulsarClientException {
        this.pulsarConsumer = getPulsarConsumer(
            pulsarClient,
            topicNames,
            subscriptionName,
            consumerName,
            consumerOptions
        );
        if (telemetryOptions != null) {
            telemetryOptions.recordTelemetry(pulsarConsumer);
        }
    }

    @Override
    public CompletableFuture<PolledMessages<PulsarOffset>> receiveAsync() {
        CompletableFuture<PolledMessages<PulsarOffset>> promise = new CompletableFuture<>();
        receiveNonZeroMessages(promise);
        return promise;
    }

    private void receiveNonZeroMessages(CompletableFuture<PolledMessages<PulsarOffset>> promise) {
        pulsarConsumer.batchReceiveAsync().thenAccept(m -> {
            if (promise.isCancelled()) {
                return;
            }
            if (m.size() > 0) {
                promise.complete(new PulsarMessages(m));
            } else {
                receiveNonZeroMessages(promise);
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

    private org.apache.pulsar.client.api.Consumer<byte[]> getPulsarConsumer(
        PulsarClient pulsarClient,
        Set<String> topicNames,
        String subscriptionName,
        String consumerName,
        ConsumerOptions consumerOptions
    ) throws PulsarClientException {
        return pulsarClient.newConsumer(Schema.BYTES)
                           .loadConf(consumerOptions.asMap())
                           .topics(new ArrayList<>(topicNames))
                           .subscriptionName(subscriptionName)
                           .consumerName(consumerName)
                           .subscribe();
    }
}
