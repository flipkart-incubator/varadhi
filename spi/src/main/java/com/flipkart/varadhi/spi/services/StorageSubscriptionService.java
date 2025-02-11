package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.StorageSubscription;

public interface StorageSubscriptionService<S extends StorageSubscription> {

    void create(S subscription, Project project);

    void delete(S subscription, Project project);

    boolean exists(String subName, String topicName);
}
