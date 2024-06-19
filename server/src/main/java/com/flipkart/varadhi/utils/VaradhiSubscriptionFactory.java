package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.entities.InternalQueueCategory;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.spi.services.StorageSubscriptionFactory;
import com.flipkart.varadhi.spi.services.StorageTopicFactory;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public final class VaradhiSubscriptionFactory {
    private static final String NAME_SEPARATOR = ".";
    private static final String PART_SEPARATOR = "-";
    private static final String SUB_QUALIFIER = "is";
    private static final String TOPIC_QUALIFIER = "it";
    private static final String SHARD_QUALIFIER = "shard";
    private static final int READ_FAN_OUT_FOR_INTERNAL_QUEUE = 1;
    private static final int MAX_RETRY_COUNT = 3;
    private final String deployedRegion;
    private final StorageSubscriptionFactory<StorageSubscription<StorageTopic>, StorageTopic> subscriptionFactory;
    private final StorageTopicFactory<StorageTopic> topicFactory;
    private final StorageTopicService<StorageTopic> topicService;

    public VaradhiSubscriptionFactory(
            StorageTopicService<StorageTopic> topicService,
            StorageSubscriptionFactory<StorageSubscription<StorageTopic>, StorageTopic> subscriptionFactory,
            StorageTopicFactory<StorageTopic> topicFactory, String deployedRegion
    ) {
        this.topicService = topicService;
        this.subscriptionFactory = subscriptionFactory;
        this.topicFactory = topicFactory;
        this.deployedRegion = deployedRegion;
    }

    public VaradhiSubscription get(SubscriptionResource subscriptionResource, Project subProject, VaradhiTopic topic) {
        String subName = subscriptionResource.getSubscriptionInternalName();
        SubscriptionShards shards =
                getSubscriptionShards(subName, topic, subProject, subscriptionResource.getConsumptionPolicy());
        return VaradhiSubscription.of(
                subName,
                subscriptionResource.getProject(),
                topic.getName(),
                subscriptionResource.getDescription(),
                subscriptionResource.isGrouped(),
                subscriptionResource.getEndpoint(),
                subscriptionResource.getRetryPolicy(),
                subscriptionResource.getConsumptionPolicy(),
                shards
        );
    }


    private SubscriptionShards getSubscriptionShards(
            String subName, VaradhiTopic topic, Project subProject, ConsumptionPolicy consumptionPolicy
    ) {
        StorageTopic subscribedStorageTopic = topic.getProduceTopicForRegion(deployedRegion).getTopicToProduce();
        List<TopicPartitions<StorageTopic>> topicPartitions =
                topicService.shardTopic(subscribedStorageTopic, InternalQueueCategory.MAIN);
        int numShards = topicPartitions.size();
        log.info("Planning {} shards for subscription {}.", topicPartitions.size(), subName);
        TopicCapacityPolicy shardCapacity = getShardCapacity(topic.getCapacity(), numShards);
        if (numShards == 1) {
            return getShard(subName, 0, topicPartitions.get(0), shardCapacity, subProject, consumptionPolicy);
        } else {
            Map<Integer, SubscriptionUnitShard> subShards = new HashMap<>();
            for (int shardId = 0; shardId < numShards; shardId++) {
                subShards.put(
                        shardId, getShard(subName, shardId, topicPartitions.get(shardId), shardCapacity, subProject,
                                consumptionPolicy
                        ));
            }
            return new SubscriptionMultiShard(subShards);
        }
    }

    private TopicCapacityPolicy getShardCapacity(TopicCapacityPolicy topicCapacity, int shardCount) {
        return topicCapacity.from((double) 1 / shardCount, topicCapacity.getReadFanOut());
    }

    private SubscriptionUnitShard getShard(
            String subName, int shardId, TopicPartitions<StorageTopic> shardTopicPartition,
            TopicCapacityPolicy capacity, Project subProject, ConsumptionPolicy consumptionPolicy
    ) {
        //TODO::Take care of region.
        //TODO::Storage Topic/Subscription names needs to be indexed with in Composite topic/subscription.
        InternalCompositeSubscription shardMainSub = getShardMainSub(subName, shardId, shardTopicPartition, subProject);
        RetrySubscription retrySub =
                getRetrySub(subName, shardId, subProject, capacity, consumptionPolicy);
        InternalCompositeSubscription dltSub =
                getDltSub(subName, shardId, subProject, capacity, consumptionPolicy);
        return new SubscriptionUnitShard(shardId, capacity, shardMainSub, retrySub, dltSub);
    }

    private InternalCompositeSubscription getShardMainSub(
            String subscriptionName, int shardId, TopicPartitions<StorageTopic> shardTopicPartition, Project project
    ) {
        String shardSubName = getShardMainSubName(subscriptionName, shardId);
        StorageSubscription<StorageTopic> ss = subscriptionFactory.get(shardSubName, shardTopicPartition, project);
        return InternalCompositeSubscription.of(ss, new InternalQueueType.Main());
    }

    private String getShardMainSubName(String subscriptionName, int shardId) {
        return String.join(
                NAME_SEPARATOR, subscriptionName, SHARD_QUALIFIER, String.valueOf(shardId),
                InternalQueueCategory.MAIN.toString()
        );
    }

    private RetrySubscription getRetrySub(
            String subscriptionName, int shardId, Project project, TopicCapacityPolicy capacity,
            ConsumptionPolicy consumptionPolicy
    ) {
        InternalCompositeSubscription[] retrySubs = new InternalCompositeSubscription[MAX_RETRY_COUNT];
        for (int retryIndex = 0; retryIndex < MAX_RETRY_COUNT; retryIndex++) {
            retrySubs[retryIndex] = getInternalSub(
                    subscriptionName,
                    shardId,
                    new InternalQueueType.Retry(retryIndex + 1),
                    retryIndex,
                    project,
                    capacity,
                    consumptionPolicy
            );
        }
        return new RetrySubscription(retrySubs);
    }

    private InternalCompositeSubscription getDltSub(
            String subscriptionName, int shardId, Project project, TopicCapacityPolicy capacity,
            ConsumptionPolicy consumptionPolicy
    ) {
        return getInternalSub(
                subscriptionName,
                shardId,
                new InternalQueueType.DeadLetter(),
                0,
                project,
                capacity,
                consumptionPolicy
        );
    }

    private InternalCompositeSubscription getInternalSub(
            String subscriptionName, int shardId, InternalQueueType queueType, int queueIndex, Project project,
            TopicCapacityPolicy capacity, ConsumptionPolicy consumptionPolicy
    ) {
        // TODO::handle cases where retry and dlt topic might be on different projects.
        String itSubName = getInternalSubName(subscriptionName, shardId, queueType.getCategory(), queueIndex);
        String itTopicName = getInternalTopicName(subscriptionName, shardId, queueType.getCategory(), queueIndex);
        TopicCapacityPolicy errCapacity =
                capacity.from(consumptionPolicy.getMaxErrorThreshold(), READ_FAN_OUT_FOR_INTERNAL_QUEUE);
        StorageTopic st = topicFactory.getTopic(itTopicName, project, errCapacity, queueType.getCategory());
        TopicPartitions<StorageTopic> tp = TopicPartitions.byTopic(st);
        StorageSubscription<StorageTopic> ss = subscriptionFactory.get(itSubName, tp, project);
        return InternalCompositeSubscription.of(ss, queueType);
    }

    private String getInternalSubName(
            String subscriptionName, int shardId, InternalQueueCategory queueCategory, int index
    ) {
        // is-<subscriptionName>.shard-<shardId>.<queueType>-<queueIndex>
        return String.join(
                NAME_SEPARATOR,
                String.join(PART_SEPARATOR, SUB_QUALIFIER, subscriptionName),
                String.join(PART_SEPARATOR, SHARD_QUALIFIER, String.valueOf(shardId)),
                String.join(PART_SEPARATOR, queueCategory.toString(), String.valueOf(index))
        );
    }

    private String getInternalTopicName(
            String subscriptionName, int shardId, InternalQueueCategory queueCategory, int index
    ) {
        // it-<subscriptionName>.shard-<shardId>.<queueType>-<queueIndex>
        return String.join(
                NAME_SEPARATOR,
                String.join(PART_SEPARATOR, TOPIC_QUALIFIER, subscriptionName),
                String.join(PART_SEPARATOR, SHARD_QUALIFIER, String.valueOf(shardId)),
                String.join(PART_SEPARATOR, queueCategory.toString(), String.valueOf(index))
        );
    }
}
