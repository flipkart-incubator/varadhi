package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.entities.ConsumptionPolicy;
import com.flipkart.varadhi.entities.InternalCompositeSubscription;
import com.flipkart.varadhi.entities.InternalQueueCategory;
import com.flipkart.varadhi.entities.InternalQueueType;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.RetryPolicy;
import com.flipkart.varadhi.entities.RetrySubscription;
import com.flipkart.varadhi.entities.StorageSubscription;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.SubscriptionMultiShard;
import com.flipkart.varadhi.entities.SubscriptionShards;
import com.flipkart.varadhi.entities.SubscriptionUnitShard;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.TopicPartitions;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.spi.services.StorageSubscriptionFactory;
import com.flipkart.varadhi.spi.services.StorageTopicFactory;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import com.flipkart.varadhi.web.entities.SubscriptionResource;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory class for creating Varadhi subscriptions.
 */
@Slf4j
public final class VaradhiSubscriptionFactory {
    private static final String NAME_SEPARATOR = ".";
    private static final String PART_SEPARATOR = "-";
    private static final String SUB_QUALIFIER = "is";
    private static final String TOPIC_QUALIFIER = "it";
    private static final String SHARD_QUALIFIER = "shard";
    private static final int READ_FAN_OUT_FOR_INTERNAL_QUEUE = 1;

    private final String deployedRegion;
    private final StorageSubscriptionFactory<StorageSubscription<StorageTopic>, StorageTopic> subscriptionFactory;
    private final StorageTopicFactory<StorageTopic> topicFactory;
    private final StorageTopicService<StorageTopic> topicService;

    /**
     * Constructs a VaradhiSubscriptionFactory.
     *
     * @param topicService        The service for managing storage topics.
     * @param subscriptionFactory The factory for creating storage subscriptions.
     * @param topicFactory        The factory for creating storage topics.
     * @param deployedRegion      The region where the subscription is deployed.
     */
    public VaradhiSubscriptionFactory(
        StorageTopicService<StorageTopic> topicService,
        StorageSubscriptionFactory<StorageSubscription<StorageTopic>, StorageTopic> subscriptionFactory,
        StorageTopicFactory<StorageTopic> topicFactory,
        String deployedRegion
    ) {
        this.topicService = topicService;
        this.subscriptionFactory = subscriptionFactory;
        this.topicFactory = topicFactory;
        this.deployedRegion = deployedRegion;
    }

    /**
     * Creates a VaradhiSubscription from a SubscriptionResource.
     *
     * @param subscriptionResource The subscription resource.
     * @param subProject           The project associated with the subscription.
     * @param topic                The topic associated with the subscription.
     *
     * @return A VaradhiSubscription instance.
     */
    public VaradhiSubscription get(SubscriptionResource subscriptionResource, Project subProject, VaradhiTopic topic) {
        String subName = subscriptionResource.getSubscriptionInternalName();
        SubscriptionShards shards = getSubscriptionShards(
            subName,
            topic,
            subProject,
            subscriptionResource.getConsumptionPolicy(),
            subscriptionResource.getRetryPolicy()
        );
        return VaradhiSubscription.of(
            subName,
            subscriptionResource.getProject(),
            topic.getName(),
            subscriptionResource.getDescription(),
            subscriptionResource.isGrouped(),
            subscriptionResource.getEndpoint(),
            subscriptionResource.getRetryPolicy(),
            subscriptionResource.getConsumptionPolicy(),
            shards,
            subscriptionResource.getProperties(),
            subscriptionResource.getActorCode()
        );
    }

    /**
     * Gets the subscription shards for a given subscription.
     *
     * @param subName           The name of the subscription.
     * @param topic             The topic associated with the subscription.
     * @param subProject        The project associated with the subscription.
     * @param consumptionPolicy The consumption policy for the subscription.
     * @param retryPolicy       The retry policy for the subscription.
     *
     * @return A SubscriptionShards instance.
     */
    private SubscriptionShards getSubscriptionShards(
        String subName,
        VaradhiTopic topic,
        Project subProject,
        ConsumptionPolicy consumptionPolicy,
        RetryPolicy retryPolicy
    ) {
        StorageTopic subscribedStorageTopic = topic.getProduceTopicForRegion(deployedRegion).getTopicToProduce();
        List<TopicPartitions<StorageTopic>> topicPartitions = topicService.shardTopic(
            subscribedStorageTopic,
            topic.getCapacity(),
            InternalQueueCategory.MAIN
        );
        int numShards = topicPartitions.size();
        log.info("Planning {} shards for subscription {}.", topicPartitions.size(), subName);

        TopicCapacityPolicy shardCapacity = getShardCapacity(topic.getCapacity(), numShards);
        if (numShards == 1) {
            return getSingleShard(
                subName,
                0,
                topicPartitions.getFirst(),
                shardCapacity,
                subProject,
                consumptionPolicy,
                retryPolicy
            );
        } else {
            return getMultiShard(subName, topicPartitions, shardCapacity, subProject, consumptionPolicy, retryPolicy);
        }
    }

    /**
     * Gets a single subscription shard.
     *
     * @param subName             The name of the subscription.
     * @param shardId             The ID of the shard.
     * @param shardTopicPartition The topic partitions for the shard.
     * @param capacity            The capacity policy for the shard.
     * @param subProject          The project associated with the subscription.
     * @param consumptionPolicy   The consumption policy for the subscription.
     * @param retryPolicy         The retry policy for the subscription.
     *
     * @return A SubscriptionUnitShard instance.
     */
    private SubscriptionUnitShard getSingleShard(
        String subName,
        int shardId,
        TopicPartitions<StorageTopic> shardTopicPartition,
        TopicCapacityPolicy capacity,
        Project subProject,
        ConsumptionPolicy consumptionPolicy,
        RetryPolicy retryPolicy
    ) {
        // TODO: Handle region-specific logic.
        // TODO: Ensure that storage topic and subscription names are indexed within composite topics/subscriptions.
        InternalCompositeSubscription shardMainSub = getShardMainSub(subName, shardId, shardTopicPartition, subProject);
        RetrySubscription retrySub = getRetrySub(
            subName,
            shardId,
            subProject,
            capacity,
            consumptionPolicy,
            retryPolicy
        );
        InternalCompositeSubscription dltSub = getDltSub(subName, shardId, subProject, capacity, consumptionPolicy);
        return new SubscriptionUnitShard(shardId, capacity, shardMainSub, retrySub, dltSub);
    }

    /**
     * Gets multiple subscription shards.
     *
     * @param subName           The name of the subscription.
     * @param partitions        The topic partitions for the shards.
     * @param capacity          The capacity policy for the shards.
     * @param subProject        The project associated with the subscription.
     * @param consumptionPolicy The consumption policy for the subscription.
     * @param retryPolicy       The retry policy for the subscription.
     *
     * @return A SubscriptionMultiShard instance.
     */
    private SubscriptionMultiShard getMultiShard(
        String subName,
        List<TopicPartitions<StorageTopic>> partitions,
        TopicCapacityPolicy capacity,
        Project subProject,
        ConsumptionPolicy consumptionPolicy,
        RetryPolicy retryPolicy
    ) {
        Map<Integer, SubscriptionUnitShard> subShards = new HashMap<>();
        for (int shardId = 0; shardId < partitions.size(); shardId++) {
            subShards.put(
                shardId,
                getSingleShard(
                    subName,
                    shardId,
                    partitions.get(shardId),
                    capacity,
                    subProject,
                    consumptionPolicy,
                    retryPolicy
                )
            );
        }
        return new SubscriptionMultiShard(subShards);
    }

    /**
     * Gets the capacity policy for a shard.
     *
     * @param topicCapacity The capacity policy for the topic.
     * @param shardCount    The number of shards.
     *
     * @return A TopicCapacityPolicy instance.
     */
    private TopicCapacityPolicy getShardCapacity(TopicCapacityPolicy topicCapacity, int shardCount) {
        return topicCapacity.from((double)1 / shardCount, topicCapacity.getReadFanOut());
    }

    /**
     * Gets the main subscription for a shard.
     *
     * @param subscriptionName    The name of the subscription.
     * @param shardId             The ID of the shard.
     * @param shardTopicPartition The topic partitions for the shard.
     * @param project             The project associated with the subscription.
     *
     * @return An InternalCompositeSubscription instance.
     */
    private InternalCompositeSubscription getShardMainSub(
        String subscriptionName,
        int shardId,
        TopicPartitions<StorageTopic> shardTopicPartition,
        Project project
    ) {
        String shardSubName = getShardMainSubName(subscriptionName, shardId);
        StorageSubscription<StorageTopic> ss = subscriptionFactory.get(shardSubName, shardTopicPartition, project);
        return InternalCompositeSubscription.of(ss, new InternalQueueType.Main());
    }

    /**
     * Gets the name of the main subscription for a shard.
     *
     * @param subscriptionName The name of the subscription.
     * @param shardId          The ID of the shard.
     *
     * @return The name of the main subscription for the shard.
     */
    private String getShardMainSubName(String subscriptionName, int shardId) {
        return String.join(
            NAME_SEPARATOR,
            subscriptionName,
            SHARD_QUALIFIER,
            String.valueOf(shardId),
            InternalQueueCategory.MAIN.toString()
        );
    }

    /**
     * Gets the retry subscription for a shard.
     *
     * @param subscriptionName  The name of the subscription.
     * @param shardId           The ID of the shard.
     * @param project           The project associated with the subscription.
     * @param capacity          The capacity policy for the shard.
     * @param consumptionPolicy The consumption policy for the subscription.
     * @param retryPolicy       The retry policy for the subscription.
     * @return A RetrySubscription instance.
     */
    private RetrySubscription getRetrySub(
        String subscriptionName,
        int shardId,
        Project project,
        TopicCapacityPolicy capacity,
        ConsumptionPolicy consumptionPolicy,
        RetryPolicy retryPolicy
    ) {
        InternalCompositeSubscription[] retrySubs = new InternalCompositeSubscription[retryPolicy.getRetryAttempts()];
        for (int retryIndex = 0; retryIndex < retryPolicy.getRetryAttempts(); retryIndex++) {
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

    /**
     * Gets the dead letter subscription for a shard.
     *
     * @param subscriptionName  The name of the subscription.
     * @param shardId           The ID of the shard.
     * @param project           The project associated with the subscription.
     * @param capacity          The capacity policy for the shard.
     * @param consumptionPolicy The consumption policy for the subscription.
     * @return An InternalCompositeSubscription instance.
     */
    private InternalCompositeSubscription getDltSub(
        String subscriptionName,
        int shardId,
        Project project,
        TopicCapacityPolicy capacity,
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

    /**
     * Gets an internal subscription for a shard.
     *
     * @param subscriptionName  The name of the subscription.
     * @param shardId           The ID of the shard.
     * @param queueType         The type of the internal queue.
     * @param queueIndex        The index of the internal queue.
     * @param project           The project associated with the subscription.
     * @param capacity          The capacity policy for the shard.
     * @param consumptionPolicy The consumption policy for the subscription.
     * @return An InternalCompositeSubscription instance.
     */
    private InternalCompositeSubscription getInternalSub(
        String subscriptionName,
        int shardId,
        InternalQueueType queueType,
        int queueIndex,
        Project project,
        TopicCapacityPolicy capacity,
        ConsumptionPolicy consumptionPolicy
    ) {
        // TODO: Address scenarios where Retry and DLT topics might belong to different projects.
        String itSubName = getInternalSubName(subscriptionName, shardId, queueType.getCategory(), queueIndex);
        String itTopicName = getInternalTopicName(subscriptionName, shardId, queueType.getCategory(), queueIndex);
        TopicCapacityPolicy errCapacity = capacity.from(
            consumptionPolicy.getMaxErrorThreshold(),
            READ_FAN_OUT_FOR_INTERNAL_QUEUE
        );
        StorageTopic st = topicFactory.getTopic(itTopicName, project, errCapacity, queueType.getCategory());
        List<TopicPartitions<StorageTopic>> topicPartitions = topicService.shardTopic(
            st,
            errCapacity,
            queueType.getCategory()
        );
        if (topicPartitions.size() != 1) {
            throw new IllegalArgumentException("Multi shard internal topics are unsupported for now.");
        }
        StorageSubscription<StorageTopic> ss = subscriptionFactory.get(itSubName, topicPartitions.getFirst(), project);
        return InternalCompositeSubscription.of(ss, queueType);
    }

    /**
     * Gets the name of an internal subscription for a shard.
     *
     * @param subscriptionName The name of the subscription.
     * @param shardId          The ID of the shard.
     * @param queueCategory    The category of the internal queue.
     * @param index            The index of the internal queue.
     * @return The name of the internal subscription for the shard.
     */
    private String getInternalSubName(
        String subscriptionName,
        int shardId,
        InternalQueueCategory queueCategory,
        int index
    ) {
        // Format: is-<subscriptionName>.shard-<shardId>.<queueType>-<queueIndex>
        return String.join(
            NAME_SEPARATOR,
            String.join(PART_SEPARATOR, SUB_QUALIFIER, subscriptionName),
            String.join(PART_SEPARATOR, SHARD_QUALIFIER, String.valueOf(shardId)),
            String.join(PART_SEPARATOR, queueCategory.toString(), String.valueOf(index))
        );
    }

    /**
     * Gets the name of an internal topic for a shard.
     *
     * @param subscriptionName The name of the subscription.
     * @param shardId          The ID of the shard.
     * @param queueCategory    The category of the internal queue.
     * @param index            The index of the internal queue.
     * @return The name of the internal topic for the shard.
     */
    private String getInternalTopicName(
        String subscriptionName,
        int shardId,
        InternalQueueCategory queueCategory,
        int index
    ) {
        // Format: it-<subscriptionName>.shard-<shardId>.<queueType>-<queueIndex>
        return String.join(
            NAME_SEPARATOR,
            String.join(PART_SEPARATOR, TOPIC_QUALIFIER, subscriptionName),
            String.join(PART_SEPARATOR, SHARD_QUALIFIER, String.valueOf(shardId)),
            String.join(PART_SEPARATOR, queueCategory.toString(), String.valueOf(index))
        );
    }
}
