package com.flipkart.varadhi.qos.entity;

import java.time.Clock;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ClientHistory implements LoadPredictor {
    private final Map<String, Deque<TopicLoadInfo>> clientHistoryMap; // clientId to history records
    private final int slots;
    private final int slotDuration;
    private final Clock clock;

    public ClientHistory(int historySlots, int slotDuration, Clock clock) {//, LoadPredictor loadPredictor) {
        this.clientHistoryMap = new ConcurrentHashMap<>();
        this.slots = historySlots;
        this.slotDuration = slotDuration;
        this.clock = clock;
    }

    @Override
    public List<TopicLoadInfo> predictLoad() {
        List<TopicLoadInfo> recentRecords = new ArrayList<>();
        clientHistoryMap.forEach((clientId, history) -> {
            if(!history.isEmpty()) {
                TopicLoadInfo topicLoadInfo = history.peekLast();
                if(topicLoadInfo != null) {
                    if(!isExpired(topicLoadInfo.to())) {
                        recentRecords.add(topicLoadInfo);
                    } else {
                        // most recent record of given client is outside of window
                        removeClient(clientId);
                    }
                }
            }
        });

        // returns the best possible prediction of load
        return recentRecords;
    }

    @Override
    public void add(String clientId, TopicLoadInfo load) {
        // first time for client, create a new history queue
        clientHistoryMap.compute(clientId, (k, v) -> {
            // add a new client
            if (v == null) {
                v = new ArrayDeque<>(slots);
            }
            if(v.size() == slots) {
                v.poll();
            }
            v.add(load);
            return v;
        });
    }

    private void removeClient(String clientId) {
        clientHistoryMap.remove(clientId);
    }

    /**
     * check if record is expired (older than number of slots maintained)
     * @param time record time
     * @return true if record is older than windowSize
     */
    private boolean isExpired(long time) {
        return (clock.millis() - (long) slots * slotDuration) > time;
    }

}
