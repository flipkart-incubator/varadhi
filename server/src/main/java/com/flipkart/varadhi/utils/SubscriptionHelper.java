package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.entities.SubscriptionResource;
import com.flipkart.varadhi.entities.SubscriptionShard;
import com.flipkart.varadhi.entities.VaradhiSubscription;

import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR;
import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR_REGEX;

public final class SubscriptionHelper {
    private SubscriptionHelper() {
    }

    private static final SubscriptionShard[] dummyShards =
            new SubscriptionShard[]{new SubscriptionShard(0, null, null, null)};

    public static VaradhiSubscription fromResource(SubscriptionResource subscriptionResource, int version) {
        return new VaradhiSubscription(
                buildSubscriptionName(subscriptionResource.getProject(), subscriptionResource.getName()),
                version,
                subscriptionResource.getProject(),
                buildTopicName(subscriptionResource.getTopicProject(), subscriptionResource.getTopic()),
                subscriptionResource.getDescription(),
                subscriptionResource.isGrouped(),
                subscriptionResource.getEndpoint(),
                subscriptionResource.getRetryPolicy(),
                subscriptionResource.getConsumptionPolicy(),
                dummyShards // fixme: this is a hack to make the flow work
        );
    }

    public static SubscriptionResource toResource(VaradhiSubscription subscription) {
        String[] subscriptionNameSegments = subscription.getName().split(NAME_SEPARATOR_REGEX);
        String subscriptionProject = subscriptionNameSegments[0];
        String subscriptionName = subscriptionNameSegments[1];

        String[] topicNameSegments = subscription.getTopic().split(NAME_SEPARATOR_REGEX);
        String topicProject = topicNameSegments[0];
        String topicName = topicNameSegments[1];

        return new SubscriptionResource(
                subscriptionName,
                subscription.getVersion(),
                subscriptionProject,
                topicName,
                topicProject,
                subscription.getDescription(),
                subscription.isGrouped(),
                subscription.getEndpoint(),
                subscription.getRetryPolicy(),
                subscription.getConsumptionPolicy()
        );
    }

    public static String buildSubscriptionName(String projectName, String subscriptionName) {
        return String.join(NAME_SEPARATOR, projectName, subscriptionName);
    }

    public static String buildTopicName(String projectName, String topicName) {
        return String.join(NAME_SEPARATOR, projectName, topicName);
    }
}