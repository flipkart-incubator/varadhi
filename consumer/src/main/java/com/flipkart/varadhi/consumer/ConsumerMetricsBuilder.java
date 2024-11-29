package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.InternalQueueType;

public interface ConsumerMetricsBuilder {
    ConsumerMetrics build(String subName, int shardId, InternalQueueType[] queueTypes);
}
