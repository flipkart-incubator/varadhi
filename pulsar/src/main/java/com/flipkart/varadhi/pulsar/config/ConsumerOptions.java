package com.flipkart.varadhi.pulsar.config;

import lombok.Data;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Pulsar consumer configuration options.
 * <p>
 * These options map to {@link org.apache.pulsar.client.api.ConsumerBuilder} and
 * {@link org.apache.pulsar.client.api.BatchReceivePolicy} settings and control
 * batch receive, receiver queue size, and acknowledgment grouping.
 */
@Data
public class ConsumerOptions {

    /**
     * Max number of messages to fetch per batch.
     * <p>{@link org.apache.pulsar.client.api.BatchReceivePolicy#maxNumMessages}
     * Can be overridden by subscription property.
     */
    private int maxPollRecords = 1000;

    /**
     * Max bytes to fetch from Pulsar per batch.
     * <p>{@link org.apache.pulsar.client.api.BatchReceivePolicy#maxNumBytes}
     */
    private Integer fetchMaxBytes = 52428800;

    /**
     * Max time to wait when fetching messages for a batch (milliseconds).
     * <p>{@link org.apache.pulsar.client.api.BatchReceivePolicy#timeout}
     */
    private Integer fetchMaxWaitMs = 500;

    /**
     * Time to group consumer acknowledgments before sending to broker (microseconds).
     * <p>{@link org.apache.pulsar.client.api.ConsumerBuilder#acknowledgmentGroupTime}
     */
    private Long acknowledgementsGroupTimeMicros = 100000L;

    /**
     * Size of the consumer receive queue.
     * <p>Can be overridden by {@link #maxTotalReceiverQueueSizeAcrossPartitions}.
     * <p>{@link org.apache.pulsar.client.api.ConsumerBuilder#receiverQueueSize}
     */
    private Integer receiverQueueSize = 2000;

    /**
     * Max total receiver queue size across all partitions.
     * <p>{@link org.apache.pulsar.client.api.ConsumerBuilder#maxTotalReceiverQueueSizeAcrossPartitions}
     */
    private Integer maxTotalReceiverQueueSizeAcrossPartitions = 10000;

    /**
     * Max timeout to wait when polling a message source (milliseconds).
     */
    private Integer messageSourcePollMaxWaitMs = 1000;

    /**
     * Max number of messages to hold in memory for a subscription.
     */
    private Integer maxInMemoryMessages = 2000;

    private SubscriptionType subscriptionType = SubscriptionType.Exclusive;

    private SubscriptionMode subscriptionMode = SubscriptionMode.Durable;

    /**
     * Where to start consuming when a subscription is created.
     * <p>{@link org.apache.pulsar.client.api.ConsumerBuilder#subscriptionInitialPosition}
     */
    private SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.Earliest;

    public synchronized Map<String, Object> asMap() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("acknowledgementsGroupTimeMicros", this.acknowledgementsGroupTimeMicros);
        configMap.put("receiverQueueSize", this.receiverQueueSize);
        configMap.put("maxTotalReceiverQueueSizeAcrossPartitions", this.maxTotalReceiverQueueSizeAcrossPartitions);
        configMap.put("subscriptionType", this.subscriptionType);
        configMap.put("subscriptionMode", this.subscriptionMode);
        configMap.put("subscriptionInitialPosition", this.subscriptionInitialPosition.name());
        return configMap;
    }

    /** Build policy for {@link org.apache.pulsar.client.api.ConsumerBuilder#loadConf} batchReceivePolicy. */
    public BatchReceivePolicy buildBatchReceivePolicy() {
        int maxBytes = this.fetchMaxBytes != null ? this.fetchMaxBytes : 5 * 1024 * 1024;
        int timeoutMs = this.fetchMaxWaitMs != null ? this.fetchMaxWaitMs : 200;
        return BatchReceivePolicy.builder()
                                 .maxNumMessages(this.maxPollRecords)
                                 .maxNumBytes(maxBytes)
                                 .timeout(timeoutMs, TimeUnit.MILLISECONDS)
                                 .build();
    }
}
