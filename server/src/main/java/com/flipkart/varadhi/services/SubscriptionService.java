package com.flipkart.varadhi.services;

import com.flipkart.varadhi.core.cluster.ControllerApi;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.utils.ShardProvisioner;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class SubscriptionService {
    private final MetaStore metaStore;
    private final ControllerApi controllerApi;
    private final ShardProvisioner shardProvisioner;

    public SubscriptionService(ShardProvisioner shardProvisioner, ControllerApi controllerApi, MetaStore metaStore) {
        this.shardProvisioner = shardProvisioner;
        this.metaStore = metaStore;
        this.controllerApi = controllerApi;
    }

    public List<String> getSubscriptionList(String projectName) {
        return metaStore.getSubscriptionNames(projectName);
    }

    public VaradhiSubscription getSubscription(String subscriptionName) {
        return metaStore.getSubscription(subscriptionName);
    }

    public VaradhiSubscription createSubscription(
            VaradhiTopic subscribedTopic, VaradhiSubscription subscription, Project subProject
    ) {
        validateCreation(subscribedTopic, subscription);
        metaStore.createSubscription(subscription);
        try {
            shardProvisioner.provision(subscription, subProject);
            subscription.markCreated();
        } catch (Exception e) {
            log.error("create subscription failed", e);
            subscription.markCreateFailed(e.getMessage());
            throw e;
        } finally {
            metaStore.updateSubscription(subscription);
        }
        return subscription;
    }

    public CompletableFuture<SubscriptionOperation> start(String subscriptionName, String requestedBy) {
        VaradhiSubscription subscription = metaStore.getSubscription(subscriptionName);
        if (subscription.isWellProvisioned()) {
            return controllerApi.startSubscription(subscriptionName, requestedBy);
        }
        throw new InvalidOperationForResourceException(
                "Subscription is in state %s. It can't be started/stopped.".formatted(
                        subscription.getStatus().getState()));
    }

    public CompletableFuture<SubscriptionOperation> stop(String subscriptionName, String requestedBy) {
        VaradhiSubscription subscription = metaStore.getSubscription(subscriptionName);
        if (subscription.isWellProvisioned()) {
            return controllerApi.stopSubscription(subscriptionName, requestedBy);
        }
        throw new InvalidOperationForResourceException(
                "Subscription is in state %s. It can't be started/stopped.".formatted(
                        subscription.getStatus().getState()));
    }

    private void validateCreation(VaradhiTopic topic, VaradhiSubscription subscription) {
        if (subscription.isGrouped() && !topic.isGrouped()) {
            throw new IllegalArgumentException(
                    "Cannot create grouped Subscription as it's Topic(%s) is not grouped".formatted(
                            subscription.getTopic()));
        }
    }

    public CompletableFuture<VaradhiSubscription> updateSubscription(
            String subscriptionName, int fromVersion, String description, boolean grouped, Endpoint endpoint,
            RetryPolicy retryPolicy, ConsumptionPolicy consumptionPolicy, String requestedBy
    ) {
        VaradhiSubscription existingSubscription = metaStore.getSubscription(subscriptionName);
        validateForConflictingUpdate(fromVersion, existingSubscription.getVersion());
        VaradhiTopic subscribedTopic = metaStore.getTopic(existingSubscription.getTopic());
        validateForSubscribedTopic(subscribedTopic, grouped);
        return controllerApi.getSubscriptionStatus(subscriptionName, requestedBy).thenApply(ss -> {
            existingSubscription.setGrouped(grouped);
            existingSubscription.setDescription(description);
            existingSubscription.setEndpoint(endpoint);
            existingSubscription.setRetryPolicy(retryPolicy);
            existingSubscription.setConsumptionPolicy(consumptionPolicy);

            metaStore.updateSubscription(existingSubscription);
            return existingSubscription;
        });


    }

    private void validateForConflictingUpdate(int fromVersion, int latestVersion) {
        if (fromVersion != latestVersion) {
            throw new InvalidOperationForResourceException(
                    "Conflicting update, Subscription has been modified. Fetch latest and try again.");
        }
    }

    private void validateForSubscribedTopic(VaradhiTopic subscribedTopic, boolean groupedUpdated) {
        if (groupedUpdated && !subscribedTopic.isGrouped()) {
            throw new IllegalArgumentException(
                    "Cannot update Subscription to grouped as it's Topic(%s) is not grouped".formatted(
                            subscribedTopic.getName()));
        }
    }

    public CompletableFuture<Void> deleteSubscription(String subscriptionName, Project subProject, String requestedBy) {
        VaradhiSubscription subscription = metaStore.getSubscription(subscriptionName);
        return controllerApi.getSubscriptionStatus(subscriptionName, requestedBy).thenAccept(ss -> {
            if (!ss.canDelete()) {
                throw new IllegalArgumentException(
                        String.format("Subscription deletion not allowed in state: %s.", ss.getState()));
            }
            subscription.markDeleting();
            metaStore.updateSubscription(subscription);
            try {
                shardProvisioner.deProvision(subscription, subProject);
                metaStore.deleteSubscription(subscriptionName);
            } catch (Exception e) {
                log.error("Delete failed.", e);
                subscription.markDeleteFailed(e.getMessage());
                metaStore.updateSubscription(subscription);
                throw e;
            }
        });
    }
}
