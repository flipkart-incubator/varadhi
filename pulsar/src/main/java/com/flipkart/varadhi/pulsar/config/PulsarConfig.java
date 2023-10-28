package com.flipkart.varadhi.pulsar.config;


import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class PulsarConfig {
    @NotNull
    private PulsarAdminOptions pulsarAdminOptions;
    @NotNull
    private PulsarClientOptions pulsarClientOptions;
    private ProducerOptions producerOptions;
}
