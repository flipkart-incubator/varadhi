package com.flipkart.varadhi.pulsar.config;


import lombok.Data;

@Data
public class PulsarConfig {
    private PulsarAdminOptions pulsarAdminOptions;
    private PulsarClientOptions pulsarClientOptions;
    private ProducerOptions producerOptions;
}
