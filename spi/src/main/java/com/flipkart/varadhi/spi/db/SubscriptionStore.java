package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.VaradhiSubscription;

import java.util.List;

public interface SubscriptionStore {
    void create(VaradhiSubscription subscription);

    VaradhiSubscription get(String subscriptionName);

    List<String> getAllNames();

    List<String> getAllNames(String projectName);

    boolean exists(String subscriptionName);

    void update(VaradhiSubscription subscription);

    void delete(String subscriptionName);
}
