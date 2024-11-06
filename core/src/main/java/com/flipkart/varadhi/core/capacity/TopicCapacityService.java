package com.flipkart.varadhi.core.capacity;

public interface TopicCapacityService {
    /**
     * Get the throughput for a topic
     * @param topic varadhi topic name
     * @return throughput in bytes per second
     */
    int getThroughputLimit(String topic);
}
