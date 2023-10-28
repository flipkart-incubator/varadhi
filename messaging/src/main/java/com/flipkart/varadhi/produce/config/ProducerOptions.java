package com.flipkart.varadhi.produce.config;


import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class ProducerOptions {
    @NotNull
    String producerCacheBuilderSpec = "expireAfterAccess=3600s";

    @NotNull
    String topicCacheBuilderSpec = "expireAfterAccess=3600s";
}
