package com.flipkart.varadhi.qos.server;

import com.flipkart.varadhi.services.VaradhiTopicService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SuppressionManager {
    Map<String, ClientLoad<Long>> topicThroughputLoadMap; // topic to client load info
    Map<String, ClientLoad<Long>> topicQPSLoadMap; // topic to client load info
    int windowSize;
    List<String> clientIdList;
    VaradhiTopicService varadhiTopicService;

    public SuppressionManager(int windowSize, VaradhiTopicService varadhiTopicService) {
        this.topicThroughputLoadMap = new HashMap<>();
        this.topicQPSLoadMap = new HashMap<>();
        this.windowSize = windowSize;
        this.varadhiTopicService = varadhiTopicService;
        this.clientIdList = new ArrayList<>();
    }

    // Adds throughput for current client and returns the updated suppression factor for the topic
    public Float addThroughput(String clientId, String topic, Long throughput) {

        // check if clientId is already present in the list
        checkNewClient(clientId);

        if (!topicThroughputLoadMap.containsKey(topic)) {
            topicThroughputLoadMap.put(topic, new ClientLoad<Long>(windowSize));
        }
        topicThroughputLoadMap.get(topic).addLoad(clientId, throughput);

        Long actualThroughout = clientIdList.stream().map(client -> {
            Long thrpt = getThroughout(client, topic);
            log.info("Client: {}, Throughput: {}", client, thrpt);
            return thrpt;
        }).reduce(0L, Long::sum);
        int throughputBPS = varadhiTopicService.get(topic).getCapacity().getThroughputKBps()*1024;
        return calculateSuppressionFactor(throughputBPS, actualThroughout);
    }

    public Float addQPS(String clientId, String topic, Long qps) {

        // check if clientId is already present in the list
        checkNewClient(clientId);

        if (!topicQPSLoadMap.containsKey(topic)) {
            topicQPSLoadMap.put(topic, new ClientLoad<Long>(windowSize));
        }
        topicQPSLoadMap.get(topic).addLoad(clientId, qps);

        Long actualQPS = clientIdList.stream().map(client -> {
            Long _qps = getQPS(client, topic);
            log.info("Client: {}, QPS: {}", client, _qps);
            return _qps;
        }).reduce(0L, Long::sum);
        int QPS = varadhiTopicService.get(topic).getCapacity().getQps();
        return calculateSuppressionFactor(QPS, actualQPS);
    }

    private void checkNewClient(String clientId) {
        if (!clientIdList.contains(clientId)) {
            clientIdList.add(clientId);
        }
    }

    private Float calculateSuppressionFactor(float limit, float actual) {
        return Math.max(0f, 1.0f - (limit / actual));
    }

    private Long getThroughout(String clientId, String topic) {
        if (topicThroughputLoadMap.containsKey(topic)) {
            return topicThroughputLoadMap.get(topic).getLastLoad(clientId);
        }
        return 0L;
    }

    private Long getQPS(String clientId, String topic) {
        if (topicQPSLoadMap.containsKey(topic)) {
            return topicQPSLoadMap.get(topic).getLastLoad(clientId);
        }
        return 0L;
    }



}
