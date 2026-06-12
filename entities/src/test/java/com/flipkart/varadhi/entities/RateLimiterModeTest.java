package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RateLimiterModeTest {

    @Test
    void jsonRoundTrip_PreservesModeByName() {
        for (RateLimiterMode mode : RateLimiterMode.values()) {
            String json = JsonMapper.jsonSerialize(mode);
            RateLimiterMode deserialized = JsonMapper.jsonDeserialize(json, RateLimiterMode.class);
            assertEquals(mode, deserialized);
        }
    }
}
