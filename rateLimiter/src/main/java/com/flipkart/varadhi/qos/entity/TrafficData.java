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
// TODO(rl): not in RL module, move to entity module
public class TrafficData {
    private String topic;
    private long bytesIn;
    private long rateIn;

    @Override
    public String toString() {
        return "\nTrafficData{" +
                "throughputIn=" + bytesIn +
                ", rateIn=" + rateIn +
                "}";
    }

}
