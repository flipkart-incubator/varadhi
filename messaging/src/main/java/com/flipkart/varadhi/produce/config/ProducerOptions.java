package com.flipkart.varadhi.produce.config;


import lombok.Data;

@Data
public class ProducerOptions {

    // Guava cache spec as defined at com.google.common.cache.CacheBuilderSpec.
    String producerCacheBuilderSpec;
    String topicCacheBuilderSpec;
    boolean metricEnabled;
}
