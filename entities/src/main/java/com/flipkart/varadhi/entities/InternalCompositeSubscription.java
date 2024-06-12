package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

@Data
public class InternalCompositeSubscription {
    private final String subRegion;
    private final InternalQueueType queueType;

    /**
     * As of now only 1 is supported, but in future this can be an array where we can add more storage topics.
     */
    private final StorageSubscription<StorageTopic> storageSubscription;
}
