package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.spi.db.MetaStore;

import java.util.List;
import java.util.Objects;

import static com.flipkart.varadhi.entities.VersionedEntity.INITIAL_VERSION;

public class SubscriptionService {

    private final MetaStore metaStore;

    public SubscriptionService(MetaStore metaStore) {
        this.metaStore = metaStore;
    }

    public List<String> getSubscriptionList(String projectName) {
        return metaStore.getSubscriptionNames(projectName);
    }

    public VaradhiSubscription getSubscription(String subscriptionName) {
        return metaStore.getSubscription(subscriptionName);
    }

    public VaradhiSubscription createSubscription(VaradhiSubscription subscription) {
        // check project and topic exists
        Project project = metaStore.getProject(subscription.getProject());
        VaradhiTopic topic = metaStore.getVaradhiTopic(subscription.getTopic());
        if (subscription.isGrouped() && !topic.isGrouped()) {
            throw new IllegalArgumentException(
                    "Cannot create grouped Subscription as it's Topic(%s:%s) is not grouped".formatted(
                            project.getName(), subscription.getTopic()));
        }

        // check for duplicate subscription
        if (metaStore.checkSubscriptionExists(subscription.getName())) {
            throw new IllegalArgumentException(
                    "Subscription(%s:%s) already exists".formatted(project.getName(), subscription.getName()));
        }

        // ensure version is set correctly
        subscription.setVersion(INITIAL_VERSION);

        // persist
        metaStore.createSubscription(subscription);

        return subscription;
    }

    public VaradhiSubscription updateSubscription(VaradhiSubscription update) {
        String subscriptionName = update.getName();

        VaradhiSubscription existingSubscription = metaStore.getSubscription(subscriptionName);
        if (update.getVersion() != existingSubscription.getVersion()) {
            throw new InvalidOperationForResourceException(String.format(
                    "Conflicting update, Subscription(%s) has been modified. Fetch latest and try again.",
                    existingSubscription.getName()
            ));
        }

        // only allow description, grouped, endpoint
        if (!Objects.equals(update.getTopic(), existingSubscription.getTopic())) {
            throw new IllegalArgumentException(
                    "Cannot update Topic of Subscription(%s)".formatted(subscriptionName));
        }

        // if set grouped to true, but topic is not grouped
        VaradhiTopic topic = metaStore.getVaradhiTopic(update.getTopic());
        if (update.isGrouped() && !topic.isGrouped()) {
            throw new IllegalArgumentException(
                    "Cannot update Subscription(%s) to grouped as it's Topic(%s) is not grouped".formatted(
                            subscriptionName, topic.getName()));
        }

        // update
        VaradhiSubscription updatedSubscription = new VaradhiSubscription(
                existingSubscription.getName(),
                existingSubscription.getVersion() + 1,
                existingSubscription.getProject(),
                existingSubscription.getTopic(),
                update.getDescription(),
                update.isGrouped(),
                update.getEndpoint()
        );

        int updatedVersion = metaStore.updateSubscription(updatedSubscription);
        updatedSubscription.setVersion(updatedVersion);

        return updatedSubscription;
    }

    public void deleteSubscription(String subscriptionName) {
        metaStore.deleteSubscription(subscriptionName);
    }
}
