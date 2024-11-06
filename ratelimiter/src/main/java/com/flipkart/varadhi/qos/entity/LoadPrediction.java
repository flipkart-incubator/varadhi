package com.flipkart.varadhi.qos.entity;

import java.util.Deque;
import java.util.List;
import java.util.Map;

public interface LoadPrediction {
    List<TopicLoadInfo> predictLoad(Map<String, Deque<TopicLoadInfo>> records);
}
