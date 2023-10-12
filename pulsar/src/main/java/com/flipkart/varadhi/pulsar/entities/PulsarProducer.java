package com.flipkart.varadhi.pulsar.entities;

import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.ProducerResult;
import com.flipkart.varadhi.pulsar.clients.ClientProvider;
import com.flipkart.varadhi.pulsar.config.ProducerOptions;
import com.flipkart.varadhi.spi.services.Producer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.flipkart.varadhi.pulsar.Constants.Producer.*;

@Slf4j
public class PulsarProducer implements Producer {

    private final ClientProvider clientProvider;
    private org.apache.pulsar.client.api.Producer<byte[]> pulsarProducer;

    public PulsarProducer(
            ClientProvider clientProvider, PulsarStorageTopic storageTopic, ProducerOptions producerOptions,
            String hostName
    )
            throws PulsarClientException {
        this.clientProvider = clientProvider;
        this.pulsarProducer = getProducer(storageTopic, producerOptions, hostName);
    }

    @Override
    public CompletableFuture<ProducerResult> ProduceAsync(String partitioningKey, Message message) {

        CompletableFuture<ProducerResult> produceFuture = new CompletableFuture<>();

        TypedMessageBuilder<byte[]> messageBuilder =
                pulsarProducer.newMessage().key(partitioningKey).value(message.getPayload());

        message.getRequestHeaders().asMap()
                .forEach((key, values) -> values.forEach(value -> messageBuilder.property(key, value)));

        messageBuilder.sendAsync().whenComplete((producerResult, throwable) -> {
            if (producerResult != null) {
                produceFuture.complete(new PulsarProducerResult(producerResult));
            } else {
                // In general Pulsar client and producer, auto-reconnects so this should be fine.Might need to
                // refresh/re-create producer (and possibly client) if there are fatal errors, currently these
                // failures are unknown.
                produceFuture.completeExceptionally(throwable);
            }
        });
        return produceFuture;
    }


    private org.apache.pulsar.client.api.Producer<byte[]> getProducer(
            PulsarStorageTopic topic, ProducerOptions options, String hostname
    )
            throws PulsarClientException {
        Map<String, Object> producerConfig = getProducerConfig(topic, options, hostname);
        return clientProvider.getPulsarClient().newProducer().loadConf(producerConfig).create();
    }

    private Map<String, Object> getProducerConfig(PulsarStorageTopic topic, ProducerOptions options, String hostName) {

        // System Configured::
        // sendTimeoutMs
        // blockIfQueueFull
        // compressionType
        // batchingEnabled
        // batchingMaxPublishDelay

        // Fixed::
        // name = topic + hostname
        // topic = topic name
        // access mode -- shared.
        //
        // Default Values::
        // enableChunking: false
        // autoUpdatePartitions, autoUpdatePartitionsInterval
        //
        // Calculated:: Topic capacity dependent, (primitive calculation by Varadhi).
        // maxPendingMessages, maxPendingMessagesAcrossPartitions
        // batchingMaxPublishDelay, batchingMaxMessages, batchingMaxBytes,

        Map<String, Object> producerConfig = options.asConfigMap();
        producerConfig.put("topicName", topic.getName());
        producerConfig.put("producerName", getProducerName(topic.getName(), hostName));
        producerConfig.put("accessMode", ProducerAccessMode.Shared);

        int topicMaxQps = topic.getMaxQPS();
        int maxPendingMessages = getMaxPendingMessages(topicMaxQps);
        int batchingMaxMessages = getBatchMaxMessages(topicMaxQps, options.getBatchingMaxPublishDelayMs());
        producerConfig.put("maxPendingMessages", maxPendingMessages);
        // maxPendingMessages and maxPendingMessagesAcrossPartitions are kept same assuming worst case.
        producerConfig.put("maxPendingMessagesAcrossPartitions", maxPendingMessages);
        producerConfig.put("batchingMaxMessages", batchingMaxMessages);
        producerConfig.put("batchingMaxBytes", batchingMaxMessages * getBatchingMaxBytes(batchingMaxMessages, topic));
        return producerConfig;
    }

    private String getProducerName(String topicName, String hostName) {
        String topicSuffix = topicName.split("/")[4];
        return String.format("%s.%s", topicSuffix, hostName);
    }

    private int getMaxPendingMessages(int topicMaxQps) {
        // Assumption: Don't allow queue to build more than 1 second worth of messages.
        // with bound [min, max]
        // TODO:: This impacts memory so Discuss and close it.
        return Math.min(MAX_PENDING_MESSAGES, Math.max(MIN_PENDING_MESSAGES, topicMaxQps));
    }

    private int getBatchMaxMessages(int topicMaxQps, int maxPublishDelayMs) {
        return Math.min(MAX_BATCH_SIZE, Math.max(MIN_BATCH_SIZE, ((topicMaxQps * maxPublishDelayMs) / 1000)));
    }

    private int getBatchingMaxBytes(int batchingMaxMessages, PulsarStorageTopic topic) {
        return batchingMaxMessages * (topic.getMaxThroughputKBps() * 1000 / topic.getMaxQPS());
    }

}
