package com.flipkart.varadhi.pulsar.util;

import com.flipkart.varadhi.entities.InternalQueueCategory;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.pulsar.config.PulsarConfig;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TopicPlanner {

    // Evolve Topic's partitions and Subscription's shards planning as needed.
    // Current implementation calculates Partition and Shard counts based on the
    // max Throughput and QPS configured for a partition and shard.
    PulsarConfig config;

    public TopicPlanner(PulsarConfig config) {
        this.config = config;
    }

    public int getPartitionCount(TopicCapacityPolicy ask, InternalQueueCategory category) {
        if (category != InternalQueueCategory.MAIN) {
            return 1;
        }
        TopicCapacityPolicy planned = ask.from(config.getGrowthMultiplier(), ask.getReadFanOut());
        int countFromQps = (int) Math.ceil((double) planned.getQps() / config.getMaxQPSPerPartition());
        int countFromKBps = (int) Math.ceil((double) planned.getThroughputKBps() / config.getMaxKBpsPerPartition());
        int partitionCount = Math.max(countFromQps, countFromKBps);
        int fanOutMultiplier = (int) Math.ceil((double) planned.getReadFanOut() / config.getMaxFanOutPerBroker());
        partitionCount = partitionCount * fanOutMultiplier;
        // need to ensure that partitions are equally distributed across the shards and hence
        // partitionCount will be in multiple of shardCount.
        int shardCount = getShardCount(ask, category);
        int deltaForMultiple = shardCount - (partitionCount % shardCount);
        partitionCount = deltaForMultiple == shardCount ? partitionCount : partitionCount + deltaForMultiple;
        int boundedPartitionCount =
                Math.max(config.getMinPartitionPerTopic(), Math.min(partitionCount, config.getMaxPartitionPerTopic()));
        log.debug("Suggested PartitionCount:{} for capacity:{}", boundedPartitionCount, ask);
        return boundedPartitionCount;
    }

    public int getShardCount(TopicCapacityPolicy ask, InternalQueueCategory category) {
        if (category != InternalQueueCategory.MAIN) {
            return 1;
        }
        TopicCapacityPolicy planned = ask.from(config.getGrowthMultiplier(), ask.getReadFanOut());
        int countFromQps = (int) Math.ceil((double) planned.getQps() / config.getMaxQpsPerShard());
        int countFromKBps = (int) Math.ceil((double) planned.getThroughputKBps() / config.getMaxKBpsPerShard());
        int shardCount = Math.max(countFromQps, countFromKBps);
        int deltaForMultiple = config.getShardMultiples() - (shardCount % config.getShardMultiples());
        shardCount = deltaForMultiple == config.getShardMultiples() ? shardCount : shardCount + deltaForMultiple;
        shardCount = Math.min(shardCount, config.getMaxShardPerSubscription());
        log.debug("Suggested ShardCount:{} for capacity:{}", shardCount, ask);
        return shardCount;
    }
}
