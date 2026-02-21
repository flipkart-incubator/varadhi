package com.flipkart.varadhi.pulsar.config;

import lombok.Data;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Data
public class ConsumerOptions {
    private int receiverQueueSize = 1000;

    /**s
     * Converts this configuration to a Map suitable for Pulsar consumer builder's loadConf method.
     *
     * @return an unmodifiable map of configuration properties
     */
    public Map<String, Object> asMap() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("receiverQueueSize", receiverQueueSize);
        return Collections.unmodifiableMap(configMap);
    }
}
