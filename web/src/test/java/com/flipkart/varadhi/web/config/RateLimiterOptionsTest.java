package com.flipkart.varadhi.web.config;

import com.flipkart.varadhi.common.utils.YamlLoader;
import com.flipkart.varadhi.entities.RateLimiterMode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class RateLimiterOptionsTest {

    @Test
    void defaults_MatchLld() {
        RateLimiterOptions options = new RateLimiterOptions();

        assertAll(
            () -> assertFalse(options.isEnabled()),
            () -> assertEquals(RateLimiterMode.disabled, options.getDefaultMode()),
            () -> assertEquals(0.25, options.getFallbackBuffer()),
            () -> assertEquals(1.0, options.getBurstSeconds()),
            () -> assertEquals(1.0, options.getMinPodShare()),
            () -> assertEquals(3600L, options.getIdleBucketTtlSeconds()),
            () -> assertEquals(1024, options.getDefaultMsgSizeBytes())
        );
    }

    @Test
    void yamlRoundTrip_LoadsRateLimiterOptionsBlock() {
        String yaml = """
            enabled: false
            defaultMode: shadow
            fallbackBuffer: 0.25
            burstSeconds: 1.0
            minPodShare: 1.0
            idleBucketTtlSeconds: 3600
            defaultMsgSizeBytes: 1024
            """;

        RateLimiterOptions options = YamlLoader.loadConfigFromString(yaml, RateLimiterOptions.class, false);

        assertAll(
            () -> assertFalse(options.isEnabled()),
            () -> assertEquals(RateLimiterMode.shadow, options.getDefaultMode()),
            () -> assertEquals(1024, options.getDefaultMsgSizeBytes())
        );
    }

    @Test
    void webConfiguration_WithoutExplicitBlock_UsesDisabledDefaults() {
        WebConfiguration config = new WebConfiguration();

        assertAll(
            () -> assertFalse(config.getRateLimiterOptions().isEnabled()),
            () -> assertEquals(RateLimiterMode.disabled, config.getRateLimiterOptions().getDefaultMode())
        );
    }
}
