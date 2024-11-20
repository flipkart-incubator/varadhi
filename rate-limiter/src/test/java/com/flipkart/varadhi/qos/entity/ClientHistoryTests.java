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
    private final String DEFAULT_CLIENT_ID = "client1";
    private final String DEFAULT_TOPIC = "topic1";

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
        long currentTime = Instant.now(clock).toEpochMilli();
        TopicLoadInfo loadInfo = new TopicLoadInfo(DEFAULT_CLIENT_ID, currentTime, currentTime + slotDuration,
                new TrafficData(DEFAULT_TOPIC, 100, 100)
        );

        clientHistory.add(DEFAULT_CLIENT_ID, loadInfo);
        List<TopicLoadInfo> predictedLoad = clientHistory.predictLoad();

        assertEquals(1, predictedLoad.size());
        assertEquals(loadInfo, predictedLoad.get(0));
        assertEquals(DEFAULT_CLIENT_ID, predictedLoad.get(0).clientId());
        assertEquals(currentTime, predictedLoad.get(0).from());
        assertEquals(currentTime + slotDuration, predictedLoad.get(0).to());
        assertEquals(DEFAULT_TOPIC, predictedLoad.get(0).topicLoad().topic());
        assertEquals(100, predictedLoad.get(0).topicLoad().bytesIn());
        assertEquals(100, predictedLoad.get(0).topicLoad().rateIn());
    }

    @Test
    public void testAddMultipleLoads() {
        String clientId = "client";
        long currentTime = Instant.now(clock).toEpochMilli();
        for (int i = 0; i < historySlots; i++) {
            TopicLoadInfo loadInfo = new TopicLoadInfo(clientId + i, currentTime,
                    currentTime + slotDuration, new TrafficData(DEFAULT_TOPIC, 100, 100)
            );
            clientHistory.add(clientId + i, loadInfo);
        }
        List<TopicLoadInfo> predictedLoad = clientHistory.predictLoad();
        assertEquals(historySlots, predictedLoad.size());
    }

    @SneakyThrows
    @Test
    public void testExpiredLoad() {
        long currentTime = Instant.now(clock).toEpochMilli();
        TopicLoadInfo loadInfo = new TopicLoadInfo(DEFAULT_CLIENT_ID, currentTime, currentTime + slotDuration,
                new TrafficData(DEFAULT_TOPIC, 100, 100)
        );
        clientHistory.add(DEFAULT_CLIENT_ID, loadInfo);
        assertEquals(1, clientHistory.predictLoad().size());
        // debug statements
        System.out.println("Current time " + clock.millis());
        Thread.sleep((historySlots + 1) * slotDuration);
        System.out.println("New time " + clock.millis());
        assertTrue(clientHistory.predictLoad().isEmpty());
    }

}
