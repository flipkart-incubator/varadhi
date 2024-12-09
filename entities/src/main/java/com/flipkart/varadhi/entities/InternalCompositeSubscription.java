package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Getter;

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
    public StorageTopic getTopicForProduce() {
        if (queueType.getCategory() == InternalQueueCategory.MAIN) {
            throw new IllegalArgumentException("Main Subscription does not have a topic to produce");
        }
        return storageSubscriptions[produceIndex].getStorageTopic();
    }

    @JsonIgnore
    public StorageSubscription<StorageTopic> getSubscriptionForConsume() {
        return storageSubscriptions[consumeIndex];
    }

    @JsonIgnore
    public List<StorageSubscription<StorageTopic>> getActiveSubscriptions() {
        return Lists.newArrayList(storageSubscriptions);
    }
}
