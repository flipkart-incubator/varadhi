package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.entities.SubscriptionResource;
import com.flipkart.varadhi.entities.VaradhiSubscription;

public final class SubscriptionFactory {
    private SubscriptionFactory() {
    }

    public static VaradhiSubscription fromResource(SubscriptionResource subscriptionResource, int version) {
        return new VaradhiSubscription(
                subscriptionResource.getName(),
                version,
                subscriptionResource.getProject(),
                subscriptionResource.getTopic(),
                subscriptionResource.getDescription(),
                subscriptionResource.isGrouped(),
                subscriptionResource.getEndpoint()
        );
    }

    public static SubscriptionResource toResource(VaradhiSubscription subscription) {
        return new SubscriptionResource(
                subscription.getName(),
                subscription.getVersion(),
                subscription.getProject(),
                subscription.getTopic(),
                subscription.getDescription(),
                subscription.isGrouped(),
                subscription.getEndpoint()
        );
    }

    public static VaradhiSubscription fromUpdate(VaradhiSubscription existing, SubscriptionResource update) {
        return new VaradhiSubscription(
                existing.getName(),
                existing.getVersion() + 1,
                existing.getProject(),
                existing.getTopic(),
                update.getDescription(),
                update.isGrouped(),
                update.getEndpoint()
        );
    }
}
