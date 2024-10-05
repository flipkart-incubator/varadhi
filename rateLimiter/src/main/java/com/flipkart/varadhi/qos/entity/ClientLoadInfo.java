package com.flipkart.varadhi.qos.entity;


import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class ClientLoadInfo {
    // current millis
    private String clientId;
    private long from;
    private long to;
    private List<TrafficData> topicUsageList; // topic to incoming traffic map

    @Override
    public String toString() {
        return "\nLoadInfo{" +
                "clientId='" + clientId + '\'' +
                ", from=" + from +
                ", to=" + to +
                ", topicUsageMap=" + topicUsageList +
                "}\n";
    }

}
