package com.flipkart.varadhi.pulsar.consumer;

import com.flipkart.varadhi.pulsar.config.ConsumerOptions;
import com.flipkart.varadhi.pulsar.config.TelemetryOptions;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.spi.services.Consumer;
import com.flipkart.varadhi.spi.services.ConsumerFactory;
import com.flipkart.varadhi.spi.services.MessagingException;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicPartitions;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.naming.TopicName;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class PulsarConsumerFactory implements ConsumerFactory {

    private final PulsarClient pulsarClient;
    private final TelemetryOptions telemetryOptions;
    private final ConsumerOptions consumerOptions;

    public PulsarConsumerFactory(
        PulsarClient pulsarClient,
        ConsumerOptions defaultConsumerProperties,
        TelemetryOptions telemetryOptions
    ) {
        this.pulsarClient = pulsarClient;
        this.telemetryOptions = telemetryOptions;
        this.consumerOptions = defaultConsumerProperties;
    }

    @Override
    public Consumer<? extends Offset> newConsumer(
        Collection<TopicPartitions<? extends StorageTopic>> _topics,
        String subscriptionName,
        String consumerName,
        Map<String, Object> properties
    ) throws MessagingException {

        try {

            Set<String> topicNames = new HashSet<>();

            for (var _topic : _topics) {
                TopicPartitions<PulsarStorageTopic> topic = _topic.lift(PulsarStorageTopic.class);
                if (!topic.hasSpecificPartitions()) {
                    topicNames.add(topic.getTopic().getName());
                } else {
                    for (int partition : topic.getPartitions()) {
                        String partitionName = topic.getTopic().getName() + TopicName.PARTITIONED_TOPIC_SUFFIX
                                               + partition;
                        topicNames.add(TopicName.get(partitionName).toString());
                    }
                }
            }

            Map<String, Object> config = new HashMap<>(consumerOptions.asMap());
            config.putAll(properties);
            config.remove("topicNames");

            int maxPollRecords = getInt(config, "maxPollRecords", 2000);
            int fetchMaxBytes = getInt(config, "fetchMaxBytes", 5 * 1024 * 1024);
            int fetchMaxWaitMs = getInt(config, "fetchMaxWaitMs", 200);
            Integer receiverQueueSize = getInt(config, "receiverQueueSize");
            Integer maxTotalReceiverQueueSizeAcrossPartitions = getInt(
                config,
                "maxTotalReceiverQueueSizeAcrossPartitions"
            );
            Long acknowledgementGroupTimeMicros = getLong(config, "acknowledgementsGroupTimeMicros");

            var builder = pulsarClient.newConsumer(Schema.BYTES)
                                      .topics(new ArrayList<>(topicNames))
                                      .subscriptionName(subscriptionName)
                                      .subscriptionType(SubscriptionType.Exclusive)
                                      .subscriptionMode(SubscriptionMode.Durable)
                                      .consumerName(consumerName)
                                      .poolMessages(true)
                                      .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                                      .batchReceivePolicy(
                                          BatchReceivePolicy.builder()
                                                            .maxNumMessages(maxPollRecords)
                                                            .maxNumBytes(fetchMaxBytes)
                                                            .timeout(fetchMaxWaitMs, TimeUnit.MILLISECONDS)
                                                            .build()
                                      )
                                      .acknowledgmentGroupTime(
                                          acknowledgementGroupTimeMicros != null ?
                                              acknowledgementGroupTimeMicros :
                                              1_000_000L,
                                          TimeUnit.MICROSECONDS
                                      );
            if (receiverQueueSize != null) {
                builder = builder.receiverQueueSize(receiverQueueSize);
            }
            if (maxTotalReceiverQueueSizeAcrossPartitions != null) {
                builder = builder.maxTotalReceiverQueueSizeAcrossPartitions(maxTotalReceiverQueueSizeAcrossPartitions);
            }

            return new PulsarConsumer(builder.subscribe(), telemetryOptions);
        } catch (PulsarClientException e) {
            throw new MessagingException("Error creating consumer", e);
        }
    }

    private static Integer getInt(Map<String, Object> config, String key) {
        Object v = config.get(key);
        if (v == null)
            return null;
        if (v instanceof Number)
            return ((Number)v).intValue();
        return null;
    }

    private static int getInt(Map<String, Object> config, String key, int defaultValue) {
        Integer v = getInt(config, key);
        return v != null ? v : defaultValue;
    }

    private static Long getLong(Map<String, Object> config, String key) {
        Object v = config.get(key);
        if (v == null)
            return null;
        if (v instanceof Number)
            return ((Number)v).longValue();
        return null;
    }
}
