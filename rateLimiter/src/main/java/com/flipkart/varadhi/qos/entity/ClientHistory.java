package com.flipkart.varadhi.qos.entity;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ClientHistory {
    Map<String, Deque<TopicLoadInfo>> clientHistoryMap; // clientId to history records
    private final int slots;
    LoadPrediction loadPredictor;

    public ClientHistory(int historySlots, LoadPrediction loadPredictor) {
        this.clientHistoryMap = new ConcurrentHashMap<>();
        this.slots = historySlots;
        this.loadPredictor = loadPredictor;
    }

    public List<TopicLoadInfo> getTotalLoad() {
        // returns the best possible prediction of load
        return loadPredictor.predictLoad(clientHistoryMap);
    }

    public void add(String clientId, TopicLoadInfo load) {
        // first time for client, create a new history queue
        if(!clientHistoryMap.containsKey(clientId)) {
            addClient(clientId);
//            // other option to throw exception and handle client addition somewhere else
//            throw new IllegalArgumentException("Client not found");
        }

        // if history queue is full, remove the oldest data
        if(clientHistoryMap.get(clientId).size() == slots)
            clientHistoryMap.get(clientId).poll();

        // add new data
        clientHistoryMap.get(clientId).add(load);
    }

    public void addClient(String clientId) {
        clientHistoryMap.put(clientId, new ArrayDeque<>(slots));
    }

    // TODO(rl): remove not active clients
    public void removeClient(String clientId) {
        clientHistoryMap.remove(clientId);
    }

}
