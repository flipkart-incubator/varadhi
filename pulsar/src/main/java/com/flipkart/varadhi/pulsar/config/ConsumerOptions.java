package com.flipkart.varadhi.pulsar.config;

import lombok.Data;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Data
public class ConsumerOptions {
    private int receiverQueueSize = 1000;
    private int maxTotalReceiverQueueSizeAcrossPartitions = 50000;
    private int batchReceiveMaxNumMessages = 100;
    private int batchReceiveTimeoutMs = 100;
    private int maxUnackedMessages = 10000;
    private int ackTimeoutMs = 30000;
    private boolean enableRetry = true;
    private int maxRetries = 3;
    private int negativeAckRedeliveryDelayMs = 60000;

    /**s
     * Converts this configuration to a Map suitable for Pulsar consumer builder's loadConf method.
     *
     * @return an unmodifiable map of configuration properties
     */
    public Map<String, Object> asMap() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("receiverQueueSize", receiverQueueSize);
        configMap.put("batchReceiveMaxNumMessages", batchReceiveMaxNumMessages);
        configMap.put("batchReceiveTimeoutMs", batchReceiveTimeoutMs);
        configMap.put("maxUnackedMessages", maxUnackedMessages);
        configMap.put("ackTimeoutMs", ackTimeoutMs);
        configMap.put("enableRetry", enableRetry);
        configMap.put("maxRetries", maxRetries);
        configMap.put("negativeAckRedeliveryDelayMs", negativeAckRedeliveryDelayMs);
        return Collections.unmodifiableMap(configMap);
    }
}
