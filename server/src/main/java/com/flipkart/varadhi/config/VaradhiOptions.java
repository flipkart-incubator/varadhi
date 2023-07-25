package com.flipkart.varadhi.config;

import lombok.Data;

@Data
public class VaradhiOptions {
    private String deployedZone;
    private ProducerOptions producerOptions;
}
