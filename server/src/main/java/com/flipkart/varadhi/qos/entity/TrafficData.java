package com.flipkart.varadhi.qos.entity;

import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * Single topic's traffic data
 */
@Getter
@Setter
@Builder
public class TrafficData implements Comparable<TrafficData> {
    // TODO(rl): make them adders
    Long throughputIn;
    Long rateIn;

    @Override
    public String toString() {
        return "\nTrafficData{" +
                "throughputIn=" + throughputIn +
                ", rateIn=" + rateIn +
                "}";
    }

    @Override
    public int compareTo(TrafficData trafficData) {
        return compare(this, trafficData);
    }

    public static int compare(@NotNull TrafficData x, @NotNull TrafficData y) {
        if (x.throughputIn < y.throughputIn) {
            return -1;
        }
        if (x.throughputIn > y.throughputIn) {
            return 1;
        }
        if (x.rateIn < y.rateIn) {
            return -1;
        }
        return 1;
    }
}
