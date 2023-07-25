package com.flipkart.varadhi.config;

import lombok.Data;

@Data
public class VaradhiOptions {
    private String deployedRegion;
    private ProducerOptions producerOptions;
}
