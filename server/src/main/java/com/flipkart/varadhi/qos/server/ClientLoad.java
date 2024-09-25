package com.flipkart.varadhi.qos.server;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ClientLoad<T> {
    Map<String, Queue<T>> clientLoad; // clientId to load info (list to keep history data)
    Map<String, T> lastDataCache; // client's last known throughput
    private final int slots;

    public ClientLoad(int historySlots) {
        this.clientLoad = new ConcurrentHashMap<>();
        this.lastDataCache = new ConcurrentHashMap<>();
        this.slots = historySlots;
    }

    public T getLoad(String clientId) {
        var load = clientLoad.get(clientId);
        if(ObjectUtils.isEmpty(load)) {
            return null;
        }
        return load.peek();
    }

    public void addLoad(String clientId, T load) {
        // first time for client, create a new history queue
        if(!clientLoad.containsKey(clientId)) {
            addClient(clientId);
//            // other option to throw exception and handle client addition somewhere else
//            throw new IllegalArgumentException("Client not found");
        }

        // if history queue is full, remove the oldest data
        if(clientLoad.get(clientId).size() == slots)
            clientLoad.get(clientId).poll();

        // add new data
        clientLoad.get(clientId).add(load);
        lastDataCache.put(clientId, load); // cache the data
    }

    public void addClient(String clientId) {
        clientLoad.put(clientId, new ArrayDeque<>(slots));
        lastDataCache.put(clientId, null);
    }

    // TODO(rl): remove not active clients
    public void removeClient(String clientId) {
        clientLoad.remove(clientId);
        lastDataCache.remove(clientId);
    }

    public T getLastLoad(String clientId) {
        // get from cache if available
        return lastDataCache.getOrDefault(clientId, getLoad(clientId));
    }

}
