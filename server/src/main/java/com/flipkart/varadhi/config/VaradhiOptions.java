package com.flipkart.varadhi.config;

import com.flipkart.varadhi.produce.config.ProducerOptions;
import lombok.Data;

@Data
public class VaradhiOptions {
    private String deployedRegion;
    private ProducerOptions producerOptions;
}
