package com.flipkart.varadhi.qos.server;

import com.flipkart.varadhi.services.VaradhiTopicService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SuppressionManager {
    Map<String, ClientLoad<Double>> topicThroughputLoadMap; // topic to client load info
    Map<String, ClientLoad<Double>> topicQPSLoadMap; // topic to client load info
    int windowSize;
    List<String> clientIdList;
    VaradhiTopicService varadhiTopicService;

    public SuppressionManager(int windowSize, VaradhiTopicService varadhiTopicService) {
        this.topicThroughputLoadMap = new HashMap<>();
        this.topicQPSLoadMap = new HashMap<>();
        this.windowSize = windowSize;
        this.varadhiTopicService = varadhiTopicService;
        this.clientIdList = Collections.synchronizedList(new ArrayList<>());
    }

    // Adds throughput for current client and returns the updated suppression factor for the topic
    public Double addThroughput(String clientId, String topic, Double throughput) {

        // check if clientId is already present in the list
        ensureClientIdPresent(clientId);

        if (!topicThroughputLoadMap.containsKey(topic)) {
            topicThroughputLoadMap.put(topic, new ClientLoad<Double>(windowSize));
        }
        topicThroughputLoadMap.get(topic).addLoad(clientId, throughput);

        Double actualThroughout = clientIdList.stream().map(client -> {
            Double thrpt = getThroughout(client, topic);
            log.info("Client: {}, Throughput: {}", client, thrpt);
            return thrpt;
        }).reduce(0.0, Double::sum);
        int throughputBPS = varadhiTopicService.get(topic).getCapacity().getThroughputKBps()*1024;
        return calculateSuppressionFactor(throughputBPS, actualThroughout);
    }

    public Double addQPS(String clientId, String topic, Double qps) {

        // check if clientId is already present in the list
        ensureClientIdPresent(clientId);

        if (!topicQPSLoadMap.containsKey(topic)) {
            topicQPSLoadMap.put(topic, new ClientLoad<Double>(windowSize));
        }
        topicQPSLoadMap.get(topic).addLoad(clientId, qps);

        Double actualQPS = clientIdList.stream().map(client -> {
            Double _qps = getQPS(client, topic);
            log.info("Client: {}, QPS: {}", client, _qps);
            return _qps;
        }).reduce(0.0, Double::sum);
        int QPS = varadhiTopicService.get(topic).getCapacity().getQps();
        return calculateSuppressionFactor(QPS, actualQPS);
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

    private Double getThroughout(String clientId, String topic) {
        if (topicThroughputLoadMap.containsKey(topic)) {
            return topicThroughputLoadMap.get(topic).getLastLoad(clientId);
        }
        return 0.0;
    }

    private Double getQPS(String clientId, String topic) {
        if (topicQPSLoadMap.containsKey(topic)) {
            return topicQPSLoadMap.get(topic).getLastLoad(clientId);
        }
        return 0.0;
    }



}
