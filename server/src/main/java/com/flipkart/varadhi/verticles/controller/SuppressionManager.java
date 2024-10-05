package com.flipkart.varadhi.verticles.controller;

import com.flipkart.varadhi.qos.entity.ClientHistory;
import com.flipkart.varadhi.qos.entity.SuppressionFactor;
import com.flipkart.varadhi.qos.entity.TopicLoadInfo;
import com.flipkart.varadhi.services.VaradhiTopicService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

// TODO(rl): try to move business logic to RL module
@Slf4j
public class SuppressionManager {
    Map<String, ClientHistory<TopicLoadInfo>> topicTrafficDataMap; // topic to client load info
    int windowSize;
    List<String> clientIdList;
    VaradhiTopicService varadhiTopicService;

    public SuppressionManager(int windowSize, VaradhiTopicService varadhiTopicService) {
        this.topicTrafficDataMap = new HashMap<>();
        this.windowSize = windowSize;
        this.varadhiTopicService = varadhiTopicService;
        this.clientIdList = Collections.synchronizedList(new ArrayList<>());
    }

    // Adds throughput for current client and returns the updated suppression factor for the topic
    // TODO(rl): add NFR tests
    // TODO(rl): cache for specific clientId and maintain a running agg.
    public SuppressionFactor addTrafficData(String clientId, TopicLoadInfo topicLoadInfo) {

        // check if clientId is already present in the list
        ensureClientIdPresent(clientId);
        String topic = topicLoadInfo.getTopicLoad().getTopic();

        if (!topicTrafficDataMap.containsKey(topic)) {
            topicTrafficDataMap.put(topic, new ClientHistory<>(windowSize));
        }

        topicTrafficDataMap.get(topic).add(clientId, topicLoadInfo);

        Double actualThroughout =
                clientIdList.stream().map(client -> getThroughput(client, topic)).reduce(0.0, Double::sum);
        int throughputBPS = varadhiTopicService.get(topic).getCapacity().getThroughputKBps()*1024;
        return new SuppressionFactor(calculateSuppressionFactor(throughputBPS, actualThroughout));
    }

    private void ensureClientIdPresent(String clientId) {
        if (!clientIdList.contains(clientId)) {
            clientIdList.add(clientId);
        }
    }

    // TODO(rl): remove client from here
    public void removeClient(String clientId) {
        clientIdList.remove(clientId);
    }

    private Double calculateSuppressionFactor(double limit, double actual) {
        return Math.max(0, 1.0 - (limit / actual));
    }

    private Double getThroughput(String clientId, String topic) {
        if (topicTrafficDataMap.containsKey(topic)) {
            // convert bytes to Bps and add to the load
            TopicLoadInfo topicLoadInfo = topicTrafficDataMap.get(topic).getRecentRecord(clientId);
            double windowSizeInSeconds = (double) (topicLoadInfo.getTo() - topicLoadInfo.getFrom()) / 1000;
            return topicLoadInfo.getTopicLoad().getBytesIn()/windowSizeInSeconds;
        }
        return 0.0;
    }

}
