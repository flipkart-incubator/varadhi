package com.flipkart.varadhi.core.topic;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProduceRegionWeightsResolverTest {

    @Test
    void resolve_PartialMap_DefaultsUnsetRegions() {
        Map<String, Double> resolved = ProduceRegionWeightsResolver.resolve(
            Map.of("region-a", 0.6),
            Set.of("region-a", "region-b")
        );

        assertEquals(Map.of("region-a", 0.6, "region-b", 0.4), resolved);
    }

    @Test
    void resolve_EmptyExplicitMap_EvenSplitsAcrossRegions() {
        Map<String, Double> resolved = ProduceRegionWeightsResolver.resolve(null, Set.of("region-a", "region-b"));

        assertEquals(0.5, resolved.get("region-a"));
        assertEquals(0.5, resolved.get("region-b"));
    }

    @Test
    void resolve_RejectsWeightsAboveOne() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> ProduceRegionWeightsResolver.resolve(Map.of("region-a", 1.2), Set.of("region-a"))
        );
        assertTrue(ex.getMessage().contains("exceeds 1"));
    }
}
