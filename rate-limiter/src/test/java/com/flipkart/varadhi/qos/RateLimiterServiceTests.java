package com.flipkart.varadhi.qos;

import com.flipkart.varadhi.qos.entity.SuppressionData;
import com.flipkart.varadhi.qos.entity.SuppressionFactor;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

public class RateLimiterServiceTests {

    @Mock
    private DistributedRateLimiter distributedRateLimiter;
    @Mock
    private RateLimiterMetrics rateLimiterMetrics;
    private RateLimiterService rateLimiterService;
    private final int frequency = 1;

    @BeforeEach
    public void setUp() throws UnknownHostException {
        MockitoAnnotations.openMocks(this);
        doNothing().when(rateLimiterMetrics).addSuccessRequest(anyString(), anyLong());
        doNothing().when(rateLimiterMetrics).addRateLimitedRequest(anyString(), anyLong());
        doNothing().when(rateLimiterMetrics).registerSuppressionFactorGauge(anyString(), any());
        String clientId = "testClientId";
        rateLimiterService = new RateLimiterService(distributedRateLimiter, rateLimiterMetrics, frequency, clientId);
    }

    @Test
    public void testIsAllowed() {
        String topic = "testTopic";
        long dataSize = 1000L;
        boolean allowed = rateLimiterService.isAllowed(topic, dataSize);
        assertTrue(allowed);
    }

    @SneakyThrows
    @Test
    public void testIsNotAllowed() {
        // creates a spike and wait for next interval to see if next request will get rate limited
        String topic = "testTopic";
        long dataSize = 10000000L;
        SuppressionData suppressionData = new SuppressionData();
        suppressionData.setSuppressionFactor(new HashMap<>(Map.of(topic, new SuppressionFactor(1))));
        when(distributedRateLimiter.addTrafficData(any())).thenReturn(suppressionData);
        rateLimiterService.isAllowed(topic, dataSize);
        Thread.sleep(frequency * 1000);
        boolean allowed = rateLimiterService.isAllowed(topic, dataSize);
        assertFalse(allowed);
    }

}
