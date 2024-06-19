package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Getter
@AllArgsConstructor
public class InternalCompositeSubscription {
    private final InternalQueueType queueType;
    private StorageSubscription<StorageTopic>[] storageSubscriptions;
    private int produceIndex;
    private int consumeIndex;


    public static InternalCompositeSubscription of(
            StorageSubscription<StorageTopic> storageSubscription, InternalQueueType queueType
    ) {
        return new InternalCompositeSubscription(queueType, new StorageSubscription[]{storageSubscription}, 0, 0);
    }

    @JsonIgnore
    public StorageTopic getTopicToProduce() {
        if (queueType.getCategory() == InternalQueueCategory.MAIN) {
            throw new IllegalArgumentException("Main Subscription does not have a topic to produce");
        }
        return storageSubscriptions[produceIndex].getTopicPartitions().getTopic();
    }

    @JsonIgnore
    public StorageSubscription<StorageTopic> getSubscriptionToConsume() {
        return storageSubscriptions[consumeIndex];
    }

    @JsonIgnore
    public List<StorageSubscription<StorageTopic>> getActiveSubscriptions() {
        return new ArrayList<>(Arrays.asList(storageSubscriptions));
    }
}
