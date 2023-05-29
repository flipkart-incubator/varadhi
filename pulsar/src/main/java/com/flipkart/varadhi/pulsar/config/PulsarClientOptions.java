package com.flipkart.varadhi.pulsar.config;

import lombok.Data;

@Data
public class PulsarClientOptions {
    private String pulsarUrl;
    private int connectTimeout;
    private int readTimeout;
    private int requestTimeout;
}
