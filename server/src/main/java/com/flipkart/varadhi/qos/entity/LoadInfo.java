package com.flipkart.varadhi.qos.entity;


import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@AllArgsConstructor
public class LoadInfo implements Comparable<LoadInfo> {
    // current millis
    private String clientId;
    private long from;
    private long to;
    private Map<String, TrafficData> topicUsageMap; // topic to incoming traffic map

    @Override
    public String toString() {
        return "\nLoadInfo{" +
                "clientId=" + clientId +
                "from=" + from +
                ", to=" + to +
                ", topicUsageMap=" + topicUsageMap +
                "}";
    }

    @Override
    public int compareTo(LoadInfo loadInfo) {
        return compare(this, loadInfo);
    }

    public static int compare(@NotNull LoadInfo x, @NotNull LoadInfo y) {

        if(x.clientId != null && y.clientId != null && x.clientId.compareTo(y.clientId) == 0) {
            return 0;
        }

        long x_from = x.from;
        long x_to = x.to;
        long y_from = y.from;
        long y_to = y.to;

        int c1 = Long.compare(x_from, y_from);
        if (c1 != 0) {
            return c1;
        }
        int c2 = Long.compare(x_to, y_to);
        if (c2 != 0) {
            return c2;
        }
        // we never says that two LoadInfo objects are equal unless their UUID matches
        return 1;
    }

}