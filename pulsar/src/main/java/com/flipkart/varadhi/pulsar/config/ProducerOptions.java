package com.flipkart.varadhi.pulsar.config;

import lombok.Data;
import org.apache.pulsar.client.api.CompressionType;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;

/**
 * Pulsar producer configuration options.
 * <p>
 * These options map to {@link org.apache.pulsar.client.api.ProducerBuilder} settings and control
 * send timeouts, pending queue behaviour, batching, and compression.
 */
@Data
public class ProducerOptions {

    /** Timeout in milliseconds for a send to be acknowledged by the broker; send fails after this. */
    @NotNull
    private Long sendTimeoutMs = 10000L;

    /** If true, {@code send()} / {@code sendAsync()} block when the pending queue is full; if false, they fail. */
    @NotNull
    private Boolean blockIfQueueFull = true;

    /** Maximum number of messages pending acknowledgment from the broker for this producer. */
    @NotNull
    private Integer maxPendingMessages = 200;

    /** Maximum total messages pending across all partitions (for partitioned producers). */
    @NotNull
    private Integer maxPendingMessagesAcrossPartitions = 500;

    /** Max time in microseconds to wait before publishing a batch when batching is enabled. */
    @NotNull
    private Long batchingMaxPublishDelayMicros = 20000L;

    /** Maximum number of messages to include in a single batch when batching is enabled. */
    @NotNull
    private Integer batchingMaxMessages = 100;

    /** Whether to batch multiple messages into a single publish to the broker. */
    @NotNull
    private Boolean batchingEnabled = true;

    /** Compression algorithm to use for message payloads (e.g. NONE, LZ4, ZSTD). */
    @NotNull
    private CompressionType compressionType = CompressionType.SNAPPY;


    public synchronized Map<String, Object> asMap() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("sendTimeoutMs", this.sendTimeoutMs);
        configMap.put("blockIfQueueFull", this.blockIfQueueFull);
        configMap.put("maxPendingMessages", this.maxPendingMessages);
        configMap.put("maxPendingMessagesAcrossPartitions", this.maxPendingMessagesAcrossPartitions);
        configMap.put("batchingMaxPublishDelayMicros", this.batchingMaxPublishDelayMicros);
        configMap.put("batchingMaxMessages", this.batchingMaxMessages);
        configMap.put("batchingEnabled", this.batchingEnabled);
        configMap.put("compressionType", this.compressionType);
        return configMap;
    }
}
