package com.flipkart.varadhi.pulsar.config;


import lombok.Data;

@Data
public class PulsarAdminOptions {
    private String serviceHttpUrl;
    private int connectionTimeoutMs = 2000;
    private int readTimeoutMs = 2000;
    private int requestTimeoutMs = 2000;
}
