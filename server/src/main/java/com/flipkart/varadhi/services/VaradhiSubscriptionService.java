package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.SubscriptionResource;
import com.flipkart.varadhi.entities.TopicResource;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.utils.SubscriptionFactory;

import java.util.List;
import java.util.Objects;

import static com.flipkart.varadhi.entities.VersionedEntity.INITIAL_VERSION;

public class VaradhiSubscriptionService {

    private final MetaStore metaStore;

    public VaradhiSubscriptionService(MetaStore metaStore) {
        this.metaStore = metaStore;
    }

    public List<String> getSubscriptionList(String projectName) {
        return metaStore.getVaradhiSubscriptionNames(projectName);
    }

    public VaradhiSubscription getSubscription(String subscriptionName, String projectName) {
        return metaStore.getVaradhiSubscription(subscriptionName, projectName);
    }

    public VaradhiSubscription createSubscription(SubscriptionResource subscription) {
        // check project and topic exists
        Project project = metaStore.getProject(subscription.getProject());
        TopicResource topic = metaStore.getTopicResource(subscription.getTopic(), project.getName());
        if (subscription.isGrouped() && !topic.isGrouped()) {
            throw new IllegalArgumentException(
                    "Cannot create grouped Subscription as it's Topic(%s:%s) is not grouped".formatted(
                            project.getName(), subscription.getTopic()));
        }

        // check for duplicate subscription
        if (metaStore.checkVaradhiSubscriptionExists(subscription.getName(), project.getName())) {
            throw new IllegalArgumentException(
                    "Subscription(%s:%s) already exists".formatted(project.getName(), subscription.getName()));
        }

        // create VaradhiSubscription entity
        VaradhiSubscription createdSub = SubscriptionFactory.fromResource(subscription, INITIAL_VERSION);

        // persist
        metaStore.createVaradhiSubscription(createdSub);

        return createdSub;
    }

    public VaradhiSubscription updateSubscription(SubscriptionResource subscription) {
        String subscriptionName = subscription.getName();
        String projectName = subscription.getProject();

        // check subscription exist
        if (!metaStore.checkVaradhiSubscriptionExists(subscriptionName, projectName)) {
            throw new IllegalArgumentException(
                    "Subscription(%s:%s) does not exist".formatted(projectName, subscriptionName));
        }

        VaradhiSubscription existingSubscription = metaStore.getVaradhiSubscription(subscriptionName, projectName);
        if (subscription.getVersion() != existingSubscription.getVersion()) {
            throw new InvalidOperationForResourceException(String.format(
                    "Conflicting update, Subscription(%s) has been modified. Fetch latest and try again.",
                    existingSubscription.getName()
            ));
        }

        // only allow description, grouped, endpoint
        if (!Objects.equals(subscription.getTopic(), existingSubscription.getTopic())) {
            throw new IllegalArgumentException(
                    "Cannot update Topic of Subscription(%s:%s)".formatted(projectName, subscriptionName));
        }

        // if set grouped to true, but topic is not grouped
        TopicResource topic = metaStore.getTopicResource(subscription.getTopic(), projectName);
        if (subscription.isGrouped() && !topic.isGrouped()) {
            throw new IllegalArgumentException(
                    "Cannot update Subscription(%s:%s) to grouped as it's Topic(%s:%s) is not grouped".formatted(
                            projectName, subscriptionName, projectName, subscription.getTopic()));
        }

        // update
        VaradhiSubscription updatedVaradhiSubscription =
                SubscriptionFactory.fromUpdate(existingSubscription, subscription);

        int updatedVersion = metaStore.updateVaradhiSubscription(updatedVaradhiSubscription);
        updatedVaradhiSubscription.setVersion(updatedVersion);

        return updatedVaradhiSubscription;
    }

    public void deleteSubscription(String subscriptionName, String projectName) {
        if (!metaStore.checkVaradhiSubscriptionExists(subscriptionName, projectName)) {
            throw new IllegalArgumentException(
                    "Subscription(%s:%s) does not exist".formatted(projectName, subscriptionName));
        }

        metaStore.deleteVaradhiSubscription(subscriptionName, projectName);
    }

}
