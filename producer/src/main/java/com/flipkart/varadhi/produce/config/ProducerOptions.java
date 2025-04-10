package com.flipkart.varadhi.produce.config;


import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class ProducerOptions {
    // Guava cache spec as defined at com.google.common.cache.CacheBuilderSpec.
    @NotNull
    String producerCacheBuilderSpec = "";

    @NotNull
    String topicCacheBuilderSpec = "";

    boolean metricEnabled;
}
