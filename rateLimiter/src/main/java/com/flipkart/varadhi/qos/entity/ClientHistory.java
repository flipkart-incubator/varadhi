package com.flipkart.varadhi.qos.entity;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;

import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ClientHistory<T> {
    Map<String, Deque<T>> clientHistoryMap; // clientId to history records
    private final int slots;

    public ClientHistory(int historySlots) {
        this.clientHistoryMap = new ConcurrentHashMap<>();
        this.slots = historySlots;
    }

    public T getRecentRecord(String clientId) {
        var load = clientHistoryMap.get(clientId);
        if(ObjectUtils.isEmpty(load)) {
            return null;
        }
        return load.peekLast();
    }

    public List<T> getRecentRecordForAll() {
        // for each client, get the most recent record and return it
        List<T> recentRecordMap = new ArrayList<>();
        clientHistoryMap.forEach((clientId, history) -> {
            if(!history.isEmpty()) {
                T t = history.peekLast();
                if(t != null) {
                    recentRecordMap.add(t);
                }
            }
        });
        return recentRecordMap;
    }

    public void add(String clientId, T load) {
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
