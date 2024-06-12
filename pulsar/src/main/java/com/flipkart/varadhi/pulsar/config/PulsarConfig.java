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
    private int maxQPSPerPartition = 1000;
    private int maxKBpsPerPartition = 4000;
    private int minPartitionPerTopic = 1;
    private int maxPartitionPerTopic = 32;

    private int maxQpsPerShard = 1000;
    private int maxKBpsPerShard = 4000;

    private int maxFanOutPerBroker = 3;
    private int shardMultiples = 2;
    private int maxShardPerSubscription = 4;
    private double growthMultiplier = 1.5;
}
