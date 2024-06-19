package com.flipkart.varadhi.pulsar.config;


import lombok.Data;

@Data
public class PulsarAdminOptions {
    private String serviceHttpUrl;
    private int connectionTimeoutMs = 2000;
    private int readTimeoutMs = 30000;
    private int requestTimeoutMs = 30000;
}
