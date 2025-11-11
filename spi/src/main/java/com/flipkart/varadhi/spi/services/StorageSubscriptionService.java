package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.StorageSubscription;
import com.flipkart.varadhi.entities.StorageTopic;

public interface StorageSubscriptionService {

    void create(Project project, StorageSubscription<? extends StorageTopic> subscription);

    void delete(Project project, StorageSubscription<? extends StorageTopic> subscription);

    boolean exists(String subName, String topicName);
}
