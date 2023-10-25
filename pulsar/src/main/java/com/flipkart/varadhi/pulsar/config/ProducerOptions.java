package com.flipkart.varadhi.pulsar.config;

import lombok.Data;
import org.apache.pulsar.client.api.CompressionType;

import java.util.HashMap;
import java.util.Map;

@Data
public class ProducerOptions {
    private int sendTimeoutMs = 1000;
    private boolean blockIfQueueFull = true;
    private CompressionType compressionType = CompressionType.SNAPPY;
    private boolean batchingEnabled = true;
    private int batchingMaxPublishDelayMs = 10;

    public Map<String, Object> asConfigMap() {
        HashMap<String, Object> config = new HashMap<>();
        config.put("sendTimeoutMs", sendTimeoutMs);
        config.put("blockIfQueueFull", blockIfQueueFull);
        config.put("compressionType", compressionType);
        config.put("batchingEnabled", batchingEnabled);
        config.put("batchingMaxPublishDelayMicros", batchingMaxPublishDelayMs * 1000);
        return config;
    }
}
