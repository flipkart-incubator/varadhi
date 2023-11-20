package com.flipkart.varadhi.pulsar.config;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class PulsarClientOptions {
    // Reference
    // org.apache.pulsar.client.api.ClientBuilder
    // org.apache.pulsar.client.impl.conf.ClientConfigurationData
    private String serviceUrl;
    private int keepAliveIntervalSecs = 30; // Pulsar default (30 secs)
    private int ioThreads = 2;   // start with 2, might need further tuning (Pulsar default is 1).
    private int connectionsPerBroker = 1; // Pulsar default is 1.
    private int maxConcurrentLookupRequests = 5000; // Pulsar default is 5000.
    private int maxLookupRequests = 50000; // Pulsar default is 50000.
    private int maxLookupRedirects = 20; // Pulsar default is 20.
    private int maxNumberOfRejectedRequestPerConnection = 50; // Pulsar default is 50.
    private long memoryLimit = 0;  // Pulsar default is no limit (0).
    private int operationTimeoutMs = 2000;  // keep it low to fail fast. Pulsar default is 30 Secs.
    private int connectionTimeoutMs = 2000; //  keep it low to fail fast. Pulsar default is 10 Secs.
    private int lookupTimeoutMs = 10000;
    // 5x operationTimeout -- Pulsar recommendation. Pulsar default is operationTimeout
    private long initialBackoffIntervalMs = 500; // some random value.
    private long maxBackoffIntervalMs = operationTimeoutMs / 2;
    // set it half of operation timeout, to allow for minimum 1 retry.

    // Not enabled/configured below client options for now
    // enableTcpNoDelay  -- default is false. Keep it as it is. Revisit later if needed.
    // enableBusyWait --  default is false, keep it as it is.
    // listenerName -- consumption path setting.
    // listenerThreads -- consumption path setting.
    // statsInterval -- to enable producer client side metric, currently no mechanism to export these.

    public Map<String, Object> asConfigMap() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("serviceUrl", serviceUrl);
        configMap.put("operationTimeoutMs", operationTimeoutMs);
        configMap.put("lookupTimeoutMs", lookupTimeoutMs);
        configMap.put("numIoThreads", ioThreads);
        configMap.put("connectionsPerBroker", connectionsPerBroker);
        configMap.put("concurrentLookupRequest", maxConcurrentLookupRequests);
        configMap.put("maxLookupRedirects", maxLookupRedirects);
        configMap.put("maxNumberOfRejectedRequestPerConnection", maxNumberOfRejectedRequestPerConnection);
        configMap.put("keepAliveIntervalSeconds", keepAliveIntervalSecs);
        configMap.put("connectionTimeoutMs", connectionTimeoutMs);
        configMap.put("memoryLimitBytes", memoryLimit);
        configMap.put("initialBackoffIntervalNanos", initialBackoffIntervalMs * 1000 * 1000);
        configMap.put("maxBackoffIntervalNanos", maxBackoffIntervalMs * 1000 * 1000);
        return configMap;
    }
}
