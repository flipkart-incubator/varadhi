package com.flipkart.varadhi.entities;

import java.util.Map;

/** Recent produce result counts for this pod. */
public record TopicProduceStats(Map<Long, Long> successCounts, Map<Long, Long> failureCounts) {
}
