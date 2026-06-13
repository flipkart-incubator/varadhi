package com.flipkart.varadhi.core.topic;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PerRegionQuotaWeightsResolverTest {

    @Test
    void resolve_PartialMap_DefaultsUnsetRegions() {
        Map<String, Double> resolved = PerRegionQuotaWeightsResolver.resolve(
            Map.of("region-a", 0.6),
            Set.of("region-a", "region-b")
        );

        assertEquals(Map.of("region-a", 0.6, "region-b", 0.4), resolved);
    }

    @Test
    void resolve_EmptyExplicitMap_EvenSplitsAcrossRegions() {
        Map<String, Double> resolved = PerRegionQuotaWeightsResolver.resolve(null, Set.of("region-a", "region-b"));

        assertEquals(0.5, resolved.get("region-a"));
        assertEquals(0.5, resolved.get("region-b"));
    }

    @Test
    void resolve_RejectsWeightsAboveOne() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> PerRegionQuotaWeightsResolver.resolve(Map.of("region-a", 1.2), Set.of("region-a"))
        );
        assertTrue(ex.getMessage().contains("must be in [0, 1]"));
    }

    @Test
    void resolve_RejectsExplicitWeightsSummingAboveOne() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> PerRegionQuotaWeightsResolver.resolve(
                Map.of("region-a", 0.7, "region-b", 0.6),
                Set.of("region-a", "region-b")
            )
        );
        assertTrue(ex.getMessage().contains("exceeds 1"));
    }

    @Test
    void resolve_RejectsNonFiniteWeights() {
        assertThrows(
            IllegalArgumentException.class,
            () -> PerRegionQuotaWeightsResolver.resolve(Map.of("region-a", Double.NaN), Set.of("region-a"))
        );
    }

    @Test
    void resolve_EmptyProduceRegions_ReturnsEmptyMap() {
        assertEquals(Map.of(), PerRegionQuotaWeightsResolver.resolve(null, Set.of()));
    }

    @Test
    void resolve_RejectsUnknownRegion() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> PerRegionQuotaWeightsResolver.resolve(Map.of("unknown", 0.5), Set.of("region-a"))
        );
        assertTrue(ex.getMessage().contains("Unknown produce region"));
    }

    @Test
    void resolve_RejectsWhenExplicitWeightsSumBelowOneWithNoUnsetRegions() {
        assertThrows(
            IllegalArgumentException.class,
            () -> PerRegionQuotaWeightsResolver.resolve(Map.of("region-a", 0.5), Set.of("region-a"))
        );
    }

    @Test
    void resolve_RejectsWhenRemainingWeightTooSmallToSplit() {
        assertThrows(
            IllegalArgumentException.class,
            () -> PerRegionQuotaWeightsResolver.resolve(
                Map.of("region-a", 1.0 - 1e-12),
                Set.of("region-a", "region-b")
            )
        );
    }

    @Test
    void resolve_RejectsNegativeWeights() {
        assertThrows(
            IllegalArgumentException.class,
            () -> PerRegionQuotaWeightsResolver.resolve(
                Map.of("region-a", -0.5, "region-b", 0.5),
                Set.of("region-a", "region-b")
            )
        );
    }
}
