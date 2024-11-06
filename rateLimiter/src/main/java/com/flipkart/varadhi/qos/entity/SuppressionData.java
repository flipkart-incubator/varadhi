package com.flipkart.varadhi.qos.entity;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

/**
 * Suppression data for rate limiting. Stores suppression factor for each topic.
 */
@Getter
@Setter
public class SuppressionData {
    private Map<String, SuppressionFactor> suppressionFactor = new HashMap<>();
}