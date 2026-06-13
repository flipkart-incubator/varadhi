package com.flipkart.varadhi.core.topic;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Resolves {@code perRegionQuotaWeights} with partial-map semantics: explicit weights must sum to at most 1,
 * and any unset produce regions receive an even share of the remainder.
 */
final class PerRegionQuotaWeightsResolver {

    private static final double WEIGHT_SUM_EPSILON = 1e-9;

    private PerRegionQuotaWeightsResolver() {
    }

    static Map<String, Double> resolve(Map<String, Double> explicitWeights, Set<String> produceRegions) {
        if (produceRegions.isEmpty()) {
            return Map.of();
        }
        if (explicitWeights == null || explicitWeights.isEmpty()) {
            return evenSplit(produceRegions);
        }

        for (Map.Entry<String, Double> entry : explicitWeights.entrySet()) {
            if (!produceRegions.contains(entry.getKey())) {
                throw new IllegalArgumentException(
                    "Unknown produce region in perRegionQuotaWeights: " + entry.getKey()
                );
            }
            double weight = entry.getValue();
            // Guard before summing: a NaN slips past both the sum and total checks (every NaN
            // comparison is false), and a negative/>1 weight yields an invalid quota downstream.
            if (!Double.isFinite(weight) || weight < 0.0 || weight > 1.0) {
                throw new IllegalArgumentException(
                    "perRegionQuotaWeights value for " + entry.getKey() + " must be in [0, 1]: " + weight
                );
            }
        }

        double explicitSum = explicitWeights.values().stream().mapToDouble(Double::doubleValue).sum();
        if (explicitSum > 1.0 + WEIGHT_SUM_EPSILON) {
            throw new IllegalArgumentException("perRegionQuotaWeights sum exceeds 1");
        }

        Map<String, Double> resolved = new HashMap<>(explicitWeights);
        Set<String> unsetRegions = produceRegions.stream()
                                                 .filter(region -> !explicitWeights.containsKey(region))
                                                 .collect(java.util.stream.Collectors.toSet());

        if (!unsetRegions.isEmpty()) {
            double remaining = 1.0 - explicitSum;
            if (remaining <= WEIGHT_SUM_EPSILON) {
                throw new IllegalArgumentException("No weight remaining for unset produce regions");
            }
            double shareEach = remaining / unsetRegions.size();
            unsetRegions.forEach(region -> resolved.put(region, shareEach));
        }

        double total = resolved.values().stream().mapToDouble(Double::doubleValue).sum();
        if (Math.abs(total - 1.0) > WEIGHT_SUM_EPSILON) {
            throw new IllegalArgumentException("perRegionQuotaWeights must sum to 1");
        }
        return resolved;
    }

    private static Map<String, Double> evenSplit(Set<String> produceRegions) {
        double weightEach = 1.0 / produceRegions.size();
        Map<String, Double> weights = new HashMap<>();
        produceRegions.forEach(region -> weights.put(region, weightEach));
        return weights;
    }
}
