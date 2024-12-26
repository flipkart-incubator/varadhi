package com.flipkart.varadhi.pulsar.util;

import com.flipkart.varadhi.entities.InternalQueueCategory;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.pulsar.config.PulsarConfig;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
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
        int shardCount = getShardCount(ask, partitionCount, category);
        int deltaForMultiple = shardCount - (partitionCount % shardCount);
        partitionCount = deltaForMultiple == shardCount ? partitionCount : partitionCount + deltaForMultiple;
        int boundedPartitionCount =
                Math.max(config.getMinPartitionPerTopic(), Math.min(partitionCount, config.getMaxPartitionPerTopic()));
        if (0 != boundedPartitionCount % shardCount) {
            log.error("Capacity ask:{} Suggested Partition(s):{} Suggested Shard(s):{}", ask, boundedPartitionCount, shardCount);
            throw new IllegalArgumentException("Couldn't partition topic equally into shards.");
        }
        log.debug("Suggested PartitionCount:{} for capacity:{}", boundedPartitionCount, ask);
        return boundedPartitionCount;
    }

    public int getShardCount(PulsarStorageTopic topic, InternalQueueCategory category) {
        return getShardCount(topic.getCapacity(), topic.getPartitionCount(), category);
    }

    private int getShardCount(TopicCapacityPolicy ask, int topicPartitionCount, InternalQueueCategory category) {
        if (category != InternalQueueCategory.MAIN) {
            return 1;
        }
        TopicCapacityPolicy planned = ask.from(config.getGrowthMultiplier(), ask.getReadFanOut());
        int countFromQps = (int) Math.ceil((double) planned.getQps() / config.getMaxQpsPerShard());
        int countFromKBps = (int) Math.ceil((double) planned.getThroughputKBps() / config.getMaxKBpsPerShard());
        int shardCount = Math.max(countFromQps, countFromKBps);
        int deltaForMultiple = config.getShardMultiples() - (shardCount % config.getShardMultiples());
        shardCount = deltaForMultiple == config.getShardMultiples() ? shardCount : shardCount + deltaForMultiple;
        // Limit max shardCount to topic's partition count and max configured shard per subscription.
        shardCount = Math.min(Math.min(shardCount, topicPartitionCount), config.getMaxShardPerSubscription());
        log.debug("Suggested ShardCount:{} for capacity:{}", shardCount, ask);
        return shardCount;
    }
}
