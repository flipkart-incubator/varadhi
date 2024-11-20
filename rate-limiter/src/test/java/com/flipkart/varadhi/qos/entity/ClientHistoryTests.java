package com.flipkart.varadhi.qos.entity;

import static org.junit.jupiter.api.Assertions.*;

import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.util.List;

public class ClientHistoryTests {

    private ClientHistory clientHistory;
    private Clock clock;
    private final int historySlots = 5;
    private final int slotDuration = 1000; // 1 second

    @BeforeEach
    public void setUp() {
        clock = Clock.systemUTC();
        clientHistory = new ClientHistory(historySlots, slotDuration, clock);
    }

    @Test
    public void testInitializationParams() {
        assertTrue(clientHistory.predictLoad().isEmpty());
        assertThrows(IllegalArgumentException.class, () -> new ClientHistory(0, 1, clock));
        assertThrows(IllegalArgumentException.class, () -> new ClientHistory(1, 0, clock));
        assertDoesNotThrow(() -> new ClientHistory(1, 1, clock));
    }

    @Test
    public void testAddAndPredictLoadForSingleClient() {
        String clientId = "client1";
        String topic = "topic1";
        long currentTime = Instant.now(clock).toEpochMilli();
        TopicLoadInfo loadInfo = new TopicLoadInfo(clientId, currentTime, currentTime+slotDuration, new TrafficData(topic, 100, 100));

        clientHistory.add(clientId, loadInfo);
        List<TopicLoadInfo> predictedLoad = clientHistory.predictLoad();

        assertEquals(1, predictedLoad.size());
        assertEquals(loadInfo, predictedLoad.get(0));
        assertEquals(clientId, predictedLoad.get(0).clientId());
        assertEquals(currentTime, predictedLoad.get(0).from());
        assertEquals(currentTime+slotDuration, predictedLoad.get(0).to());
        assertEquals(topic, predictedLoad.get(0).topicLoad().topic());
        assertEquals(100, predictedLoad.get(0).topicLoad().bytesIn());
        assertEquals(100, predictedLoad.get(0).topicLoad().rateIn());
    }

    @Test
    public void testAddMultipleLoads() {
        String clientId = "client";
        String topic = "topic1";
        long currentTime = Instant.now(clock).toEpochMilli();
        for (int i = 0; i < historySlots; i++) {
            TopicLoadInfo loadInfo = new TopicLoadInfo(clientId+i, currentTime,
                    currentTime + slotDuration, new TrafficData(topic, 100, 100)
            );
            clientHistory.add(clientId+i, loadInfo);
        }
        List<TopicLoadInfo> predictedLoad = clientHistory.predictLoad();
        assertEquals(historySlots, predictedLoad.size());
    }

    @SneakyThrows
    @Test
    public void testExpiredLoad() {
        String clientId = "client1";
        String topic = "topic1";
        long currentTime = Instant.now(clock).toEpochMilli();
        TopicLoadInfo loadInfo = new TopicLoadInfo(clientId, currentTime, currentTime+slotDuration, new TrafficData(topic, 100, 100));

        clientHistory.add(clientId, loadInfo);
        assertEquals(1, clientHistory.predictLoad().size());
        Thread.sleep((historySlots + 1) * slotDuration);
        assertTrue(clientHistory.predictLoad().isEmpty());
    }

}
