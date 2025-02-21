package com.flipkart.varadhi.pulsar.producer;

import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.entities.utils.HeaderUtils;
import com.flipkart.varadhi.entities.constants.MessageHeaders;
import com.flipkart.varadhi.pulsar.entities.PulsarOffset;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.pulsar.config.ProducerOptions;
import com.flipkart.varadhi.pulsar.util.PropertyHelper;
import com.flipkart.varadhi.spi.services.Producer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.flipkart.varadhi.Constants.RANDOM_PARTITION_KEY_LENGTH;
import static com.flipkart.varadhi.pulsar.Constants.Producer.*;
import static org.apache.commons.text.CharacterPredicates.DIGITS;
import static org.apache.commons.text.CharacterPredicates.LETTERS;

@Slf4j
public class PulsarProducer implements Producer {
    private final RandomStringGenerator stringGenerator;
    private org.apache.pulsar.client.api.Producer<byte[]> pulsarProducer;

    public PulsarProducer(
        PulsarClient pulsarClient,
        PulsarStorageTopic storageTopic,
        ProducerOptions producerOptions,
        String hostName
    ) throws PulsarClientException {
        this.stringGenerator = new RandomStringGenerator.Builder().withinRange('0', 'z')
                                                                  .filteredBy(DIGITS, LETTERS)
                                                                  .build();
        this.pulsarProducer = getProducer(pulsarClient, storageTopic, producerOptions, hostName);
    }

    public static String getProducerName(String topicName, String hostName) {
        return String.format("%s.%s", topicName, hostName);
    }

    public static int getMaxPendingMessages(int topicMaxQps) {
        // Assumption:
        // 1. Don't allow queue to build more than 1 second worth of messages.
        // with bound [min, max]
        // 2. It also assumes worst case in terms of distribution i.e. all messages are landing to the same producer.
        // TODO::
        // 1. This impacts memory so Discuss and close it.
        // 2. This needs further tuning based on benchmarking and further understanding of the behavior.
        return Math.min(MAX_PENDING_MESSAGES, Math.max(MIN_PENDING_MESSAGES, topicMaxQps));
    }

    public static int getBatchMaxMessages(int topicMaxQps, int maxPublishDelayMs) {
        return Math.min(MAX_BATCH_SIZE, Math.max(MIN_BATCH_SIZE, ((topicMaxQps * maxPublishDelayMs) / 1000)));
    }

    public static int getBatchingMaxBytes(int batchingMaxMessages, PulsarStorageTopic topic) {
        return batchingMaxMessages * (topic.getCapacity().getThroughputKBps() * 1000 / topic.getCapacity().getQps());
    }

    @Override
    public CompletableFuture<Offset> produceAsync(Message message) {

        String partitioningKey = getPartitioningKey(message);

        TypedMessageBuilder<byte[]> messageBuilder = pulsarProducer.newMessage()
                                                                   .key(partitioningKey)
                                                                   .value(message.getPayload());

        message.getHeaders()
               .asMap()
               .forEach((key, values) -> messageBuilder.property(key, PropertyHelper.encodePropertyValues(values)));

        // In general Pulsar client and producer, auto-reconnects so this should be fine.Might need to
        // refresh/re-create producer (and possibly client) if there are fatal errors, currently these
        // failures are unknown.
        return messageBuilder.sendAsync().thenApply(PulsarOffset::new);
    }

    private String getPartitioningKey(Message message) {
        if (message.hasHeader(HeaderUtils.getHeader(MessageHeaders.GROUP_ID))) {
            return message.getHeader(HeaderUtils.getHeader(MessageHeaders.GROUP_ID));
        }
        return stringGenerator.generate(RANDOM_PARTITION_KEY_LENGTH);
    }

    private org.apache.pulsar.client.api.Producer<byte[]> getProducer(
        PulsarClient pulsarClient,
        PulsarStorageTopic topic,
        ProducerOptions options,
        String hostname
    ) throws PulsarClientException {
        Map<String, Object> producerConfig = getProducerConfig(topic, options, hostname);

        return pulsarClient.newProducer().loadConf(producerConfig).create();
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
        // batchingMaxMessages, batchingMaxBytes,

        Map<String, Object> producerConfig = options.asConfigMap();
        producerConfig.put("topicName", topic.getName());
        producerConfig.put("producerName", getProducerName(topic.getName(), hostName));
        producerConfig.put("accessMode", ProducerAccessMode.Shared);

        int topicMaxQps = topic.getCapacity().getQps();
        int maxPendingMessages = getMaxPendingMessages(topicMaxQps);
        int batchingMaxMessages = getBatchMaxMessages(topicMaxQps, options.getBatchingMaxPublishDelayMs());
        producerConfig.put("maxPendingMessages", maxPendingMessages);
        // maxPendingMessages and maxPendingMessagesAcrossPartitions are kept same assuming worst case.
        producerConfig.put("maxPendingMessagesAcrossPartitions", maxPendingMessages);
        producerConfig.put("batchingMaxMessages", batchingMaxMessages);
        producerConfig.put("batchingMaxBytes", getBatchingMaxBytes(batchingMaxMessages, topic));
        return producerConfig;
    }

}
