package com.flipkart.varadhi.spi.db.subscription;

import com.flipkart.varadhi.entities.VaradhiSubscription;

import java.util.List;

public interface SubscriptionOperations {
    void createSubscription(VaradhiSubscription subscription);

    VaradhiSubscription getSubscription(String subscriptionName);

    List<String> getAllSubscriptionNames();

    List<String> getSubscriptionNames(String projectName);

    boolean checkSubscriptionExists(String subscriptionName);

    void updateSubscription(VaradhiSubscription subscription);

    void deleteSubscription(String subscriptionName);
}
