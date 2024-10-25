package com.flipkart.varadhi.controller;

public interface TopicLimitService {
    /**
     * Get the throughput for a topic
     * @param topic varadhi topic name
     * @return throughput in bytes per second
     */
    int getThroughput(String topic);
}
