package com.flipkart.varadhi.config;

import com.flipkart.varadhi.produce.config.ProducerOptions;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class VaradhiOptions {
    @NotBlank
    private String deployedRegion;
    @NotNull
    private String projectCacheBuilderSpec = "expireAfterWrite=3600s";
    @NotNull
    private ProducerOptions producerOptions;
}
