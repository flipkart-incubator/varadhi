package com.flipkart.varadhi.services;


import com.flipkart.varadhi.core.cluster.ControllerApi;
import com.flipkart.varadhi.core.cluster.OperationMgr;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.spi.db.MetaStore;

import java.util.List;
import java.util.Objects;

import static com.flipkart.varadhi.entities.VersionedEntity.INITIAL_VERSION;

public class SubscriptionService {
    private final MetaStore metaStore;
    private final ControllerApi controllerApi;
    private final OperationMgr operationMgr;

    public SubscriptionService(ControllerApi controllerApi, OperationMgr operationMgr, MetaStore metaStore) {
        this.metaStore = metaStore;
        this.controllerApi = controllerApi;
        this.operationMgr = operationMgr;
    }

    public List<String> getSubscriptionList(String projectName) {
        return metaStore.getSubscriptionNames(projectName);
    }

    public VaradhiSubscription getSubscription(String subscriptionName) {
        return metaStore.getSubscription(subscriptionName);
    }

    public VaradhiSubscription createSubscription(VaradhiSubscription subscription) {
        validateCreation(subscription);
        subscription.setVersion(INITIAL_VERSION);
        metaStore.createSubscription(subscription);
        return subscription;
    }

    public void start(String subscriptionName, String requestedBy) {
        SubscriptionOperation op = operationMgr.requestSubStart(subscriptionName, requestedBy);
        controllerApi.startSubscription((SubscriptionOperation.StartData) op.getData());
    }

    public void stop(String subscriptionName, String requestedBy) {
        SubscriptionOperation op = operationMgr.requestSubStop(subscriptionName, requestedBy);
        controllerApi.stopSubscription((SubscriptionOperation.StopData) op.getData());
    }

    private void validateCreation(VaradhiSubscription subscription) {
        metaStore.getProject(subscription.getProject());
        VaradhiTopic topic = metaStore.getTopic(subscription.getTopic());
        if (subscription.isGrouped() && !topic.isGrouped()) {
            throw new IllegalArgumentException(
                    "Cannot create grouped Subscription as it's Topic(%s) is not grouped".formatted(
                            subscription.getTopic()));
        }
    }

    public VaradhiSubscription updateSubscription(VaradhiSubscription subscription) {
        VaradhiSubscription existingSubscription = metaStore.getSubscription(subscription.getName());
        validateUpdate(existingSubscription, subscription);
        VaradhiSubscription updatedSubscription = new VaradhiSubscription(
                existingSubscription.getName(),
                existingSubscription.getVersion(),
                existingSubscription.getProject(),
                existingSubscription.getTopic(),
                subscription.getDescription(),
                subscription.isGrouped(),
                subscription.getEndpoint(),
                subscription.getRetryPolicy(),
                subscription.getConsumptionPolicy(),
                existingSubscription.getShards()
        );

        int updatedVersion = metaStore.updateSubscription(updatedSubscription);
        updatedSubscription.setVersion(updatedVersion);

        return updatedSubscription;
    }

    private void validateUpdate(VaradhiSubscription existing, VaradhiSubscription update) {
        if (update.getVersion() != existing.getVersion()) {
            throw new InvalidOperationForResourceException(String.format(
                    "Conflicting update, Subscription(%s) has been modified. Fetch latest and try again.",
                    existing.getName()
            ));
        }

        if (!Objects.equals(update.getTopic(), existing.getTopic())) {
            throw new IllegalArgumentException(
                    "Cannot update Topic of Subscription(%s)".formatted(update.getName()));
        }

        VaradhiTopic topic = metaStore.getTopic(update.getTopic());
        if (update.isGrouped() && !topic.isGrouped()) {
            throw new IllegalArgumentException(
                    "Cannot update Subscription(%s) to grouped as it's Topic(%s) is not grouped".formatted(
                            update.getName(), topic.getName()));
        }
    }

    public void deleteSubscription(String subscriptionName) {
        metaStore.deleteSubscription(subscriptionName);
    }
}
