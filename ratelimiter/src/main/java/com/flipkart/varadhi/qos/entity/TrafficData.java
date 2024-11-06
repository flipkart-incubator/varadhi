package com.flipkart.varadhi.qos.entity;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * Single topic's traffic data
 */
@Getter
@Setter
@Builder
public class TrafficData {
    private String topic;
    private long bytesIn;
    private long rateIn;

    @Override
    public String toString() {
        return "\nTrafficData{" +
                "topic='" + topic + '\'' +
                ", bytesIn=" + bytesIn +
                ", rateIn=" + rateIn +
                "}\n";
    }

}
