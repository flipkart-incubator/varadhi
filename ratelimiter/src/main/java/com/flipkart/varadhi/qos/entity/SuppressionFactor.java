package com.flipkart.varadhi.qos.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Suppression factor for a topic. Factor must be between 0 and 1. 0 means no suppression, 1 means full suppression.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class SuppressionFactor {
    private double throughputFactor;
    // can be extended to support suppression factor for QPS, custom measurements etc.
    // double qpsFactor;
}
