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
    private final String DEFAULT_CLIENT_ID = "client1";
    private final String DEFAULT_TOPIC = "topic1";

    @BeforeEach
    public void setUp() throws UnknownHostException {
        MockitoAnnotations.openMocks(this);
        doNothing().when(rateLimiterMetrics).addSuccessRequest(anyString(), anyLong());
        doNothing().when(rateLimiterMetrics).addRateLimitedRequest(anyString(), anyLong());
        doNothing().when(rateLimiterMetrics).registerSuppressionFactorGauge(anyString(), any());
        rateLimiterService = new RateLimiterService(distributedRateLimiter, rateLimiterMetrics, frequency, DEFAULT_CLIENT_ID);
    }

    @Test
    public void testUpdateSuppressionFactor() {
        rateLimiterService.updateSuppressionFactor(DEFAULT_TOPIC, 0.0);
        long dataSize = 1000L;
        assertTrue(rateLimiterService.isAllowed(DEFAULT_TOPIC, dataSize));
        assertTrue(rateLimiterService.isAllowed(DEFAULT_TOPIC, dataSize));
        rateLimiterService.updateSuppressionFactor(DEFAULT_TOPIC, 1.0);
        assertFalse(rateLimiterService.isAllowed(DEFAULT_TOPIC, dataSize));
        assertFalse(rateLimiterService.isAllowed(DEFAULT_TOPIC, dataSize));
    }

    @Test
    public void testIsAllowed() {
        long dataSize = 1000L;
        boolean allowed = rateLimiterService.isAllowed(DEFAULT_TOPIC, dataSize);
        assertTrue(allowed);
        rateLimiterService.isAllowed(DEFAULT_TOPIC, dataSize);
    }

    @SneakyThrows
    @Test
    public void testIsNotAllowed() {
        // creates a spike and wait for next interval to see if next request will get rate limited
        long dataSize = 10000000L;
        SuppressionData suppressionData = new SuppressionData();
        suppressionData.setSuppressionFactor(new HashMap<>(Map.of(DEFAULT_TOPIC, new SuppressionFactor(1))));
        when(distributedRateLimiter.addTrafficData(any())).thenReturn(suppressionData);
        rateLimiterService.isAllowed(DEFAULT_TOPIC, dataSize);
        Thread.sleep(frequency * 1000);
        boolean allowed = rateLimiterService.isAllowed(DEFAULT_TOPIC, dataSize);
        assertFalse(allowed);
    }

}
