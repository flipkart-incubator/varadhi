package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.entities.SubscriptionResource;
import com.flipkart.varadhi.entities.VaradhiSubscription;

import static com.flipkart.varadhi.entities.MetaStoreEntity.NAME_SEPARATOR;
import static com.flipkart.varadhi.entities.MetaStoreEntity.NAME_SEPARATOR_REGEX;

public final class SubscriptionHelper {
    private SubscriptionHelper() {
    }

    public static VaradhiSubscription fromResource(SubscriptionResource subscriptionResource, int version) {
        return new VaradhiSubscription(
                buildSubscriptionName(subscriptionResource.getProject(), subscriptionResource.getName()),
                version,
                subscriptionResource.getProject(),
                buildTopicName(subscriptionResource.getProject(), subscriptionResource.getTopic()),
                subscriptionResource.getDescription(),
                subscriptionResource.isGrouped(),
                subscriptionResource.getEndpoint()
        );
    }

    public static SubscriptionResource toResource(VaradhiSubscription subscription) {
        String subscriptionName = subscription.getName().split(NAME_SEPARATOR_REGEX)[1];
        String topicName = subscription.getTopic().split(NAME_SEPARATOR_REGEX)[1];
        return new SubscriptionResource(
                subscriptionName,
                subscription.getVersion(),
                subscription.getProject(),
                topicName,
                subscription.getDescription(),
                subscription.isGrouped(),
                subscription.getEndpoint()
        );
    }

    public static String buildSubscriptionName(String projectName, String subscriptionName) {
        return String.join(NAME_SEPARATOR, projectName, subscriptionName);
    }

    public static String buildTopicName(String projectName, String topicName) {
        return String.join(NAME_SEPARATOR, projectName, topicName);
    }
}
