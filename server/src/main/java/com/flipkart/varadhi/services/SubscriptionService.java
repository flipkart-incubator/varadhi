package com.flipkart.varadhi.services;

import com.flipkart.varadhi.core.cluster.ControllerRestApi;
import com.flipkart.varadhi.entities.ConsumptionPolicy;
import com.flipkart.varadhi.entities.Endpoint;
import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.common.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.web.entities.ResourceActionRequest;
import com.flipkart.varadhi.entities.ResourceDeletionType;
import com.flipkart.varadhi.entities.RetryPolicy;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.utils.ShardProvisioner;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/**
 * Service class for managing subscriptions.
 */
@Slf4j
public class SubscriptionService {
    private final MetaStore metaStore;
    private final ControllerRestApi controllerClient;
    private final ShardProvisioner shardProvisioner;

    /**
     * Constructs a new SubscriptionService instance.
     *
     * @param shardProvisioner the shard provisioner
     * @param controllerClient the controller REST API client
     * @param metaStore        the meta store
     */
    public SubscriptionService(
        ShardProvisioner shardProvisioner,
        ControllerRestApi controllerClient,
        MetaStore metaStore
    ) {
        this.shardProvisioner = shardProvisioner;
        this.metaStore = metaStore;
        this.controllerClient = controllerClient;
    }

    /**
     * Retrieves the list of subscription names for a given project.
     *
     * @param projectName     the name of the project
     * @param includeInactive flag to include inactive or soft-deleted subscriptions
     *
     * @return the list of subscription names
     */
    public List<String> getSubscriptionList(String projectName, boolean includeInactive) {
        return metaStore.getSubscriptionNames(projectName)
                        .stream()
                        .filter(
                            subscriptionName -> includeInactive || metaStore.getSubscription(subscriptionName)
                                                                            .isActive()
                        )
                        .toList();
    }

    /**
     * Retrieves a subscription by its name.
     *
     * @param subscriptionName the name of the subscription
     *
     * @return the subscription
     */
    public VaradhiSubscription getSubscription(String subscriptionName) {
        return metaStore.getSubscription(subscriptionName);
    }

    /**
     * Creates a new subscription.
     *
     * @param subscribedTopic the subscribed topic
     * @param subscription    the subscription to create
     * @param subProject      the project associated with the subscription
     *
     * @return the created subscription
     */
    public VaradhiSubscription createSubscription(
        VaradhiTopic subscribedTopic,
        VaradhiSubscription subscription,
        Project subProject
    ) {
        validateGroupedSubscription(subscribedTopic, subscription);

        try {
            if (!exists(subscription.getName())) {
                metaStore.createSubscription(subscription);
            } else {
                VaradhiSubscription existingSubscription = getSubscription(subscription.getName());
                if (!existingSubscription.isRetriable()) {
                    throw new DuplicateResourceException(
                        String.format("Subscription '%s' already exists.", subscription.getName())
                    );
                }
                shardProvisioner.deProvision(subscription, subProject);
                metaStore.updateSubscription(subscription);
            }

            shardProvisioner.provision(subscription, subProject);
            subscription.markCreated();
        } catch (Exception e) {
            log.error("Failed to create subscription: ", e);
            subscription.markCreateFailed(e.getMessage());
            throw e;
        } finally {
            updateSubscriptionState(subscription);
        }
        return subscription;
    }

    /**
     * Starts a subscription.
     *
     * @param subscriptionName the name of the subscription
     * @param requestedBy      the user requesting the operation
     *
     * @return a CompletableFuture representing the subscription operation
     */
    public CompletableFuture<SubscriptionOperation> start(String subscriptionName, String requestedBy) {
        return performSubscriptionOperation(subscriptionName, requestedBy, controllerClient::startSubscription);
    }

    /**
     * Stops a subscription.
     *
     * @param subscriptionName the name of the subscription
     * @param requestedBy      the user requesting the operation
     *
     * @return a CompletableFuture representing the subscription operation
     */
    public CompletableFuture<SubscriptionOperation> stop(String subscriptionName, String requestedBy) {
        return performSubscriptionOperation(subscriptionName, requestedBy, controllerClient::stopSubscription);
    }

    /**
     * Updates an existing subscription.
     *
     * @param subscriptionName  the name of the subscription
     * @param fromVersion       the current version of the subscription
     * @param description       the new description of the subscription
     * @param grouped           whether the subscription is grouped
     * @param endpoint          the new endpoint of the subscription
     * @param retryPolicy       the new retry policy of the subscription
     * @param consumptionPolicy the new consumption policy of the subscription
     * @param requestedBy       the user requesting the update
     *
     * @return a CompletableFuture representing the updated subscription
     */
    public CompletableFuture<VaradhiSubscription> updateSubscription(
        String subscriptionName,
        int fromVersion,
        String description,
        boolean grouped,
        Endpoint endpoint,
        RetryPolicy retryPolicy,
        ConsumptionPolicy consumptionPolicy,
        String requestedBy
    ) {
        VaradhiSubscription subscription = getValidatedSubscription(subscriptionName);
        validateVersionForUpdate(fromVersion, subscription.getVersion());

        subscription.setGrouped(grouped);
        validateGroupedSubscription(metaStore.getTopic(subscription.getTopic()), subscription);

        return controllerClient.getSubscriptionState(subscriptionName, requestedBy).thenApply(state -> {
            subscription.setGrouped(grouped);
            subscription.setDescription(description);
            subscription.setEndpoint(endpoint);
            subscription.setRetryPolicy(retryPolicy);
            subscription.setConsumptionPolicy(consumptionPolicy);

            metaStore.updateSubscription(subscription);
            return subscription;
        });
    }

    /**
     * Deletes a subscription.
     *
     * @param subscriptionName the name of the subscription to delete
     * @param subProject       the project associated with the subscription
     * @param requestedBy      the user requesting the deletion
     * @param deletionType     the type of deletion (soft or hard)
     * @param actionRequest    the request containing the action code and message for the deletion
     *
     * @return a CompletableFuture representing the deletion operation
     *
     * @throws IllegalArgumentException if the subscription cannot be deleted in its current state
     */
    public CompletableFuture<Void> deleteSubscription(
        String subscriptionName,
        Project subProject,
        String requestedBy,
        ResourceDeletionType deletionType,
        ResourceActionRequest actionRequest
    ) {
        VaradhiSubscription subscription = metaStore.getSubscription(subscriptionName);

        return controllerClient.getSubscriptionState(subscriptionName, requestedBy).thenAccept(state -> {
            if (!state.isStoppedSuccessfully()) {
                throw new IllegalArgumentException(String.format("Cannot delete subscription in state: %s", state));
            }

            if (deletionType.equals(ResourceDeletionType.HARD_DELETE)) {
                handleHardDelete(subscription, subProject, actionRequest);
            } else {
                handleSoftDelete(subscription, actionRequest);
            }
        });
    }

    /**
     * Restores a subscription.
     *
     * @param subscriptionName the name of the subscription to restore
     * @param requestedBy      the user requesting the restoration
     * @param actionRequest    the request containing the actor code and message for the restoration
     *
     * @return a CompletableFuture representing the restored subscription
     *
     * @throws InvalidOperationForResourceException if the subscription is already active or if the restoration is not
     *                                              allowed
     */
    public CompletableFuture<VaradhiSubscription> restoreSubscription(
        String subscriptionName,
        String requestedBy,
        ResourceActionRequest actionRequest
    ) {
        VaradhiSubscription subscription = metaStore.getSubscription(subscriptionName);

        if (subscription.isActive()) {
            throw new InvalidOperationForResourceException(
                "Subscription '%s' is already active.".formatted(subscriptionName)
            );
        }

        if (!subscription.isInactive()) {
            throw new InvalidOperationForResourceException("Only inactive subscriptions can be restored.");
        }

        LifecycleStatus.ActorCode lastAction = subscription.getStatus().getActorCode();
        boolean isVaradhiAdmin = actionRequest.actorCode() == LifecycleStatus.ActorCode.SYSTEM_ACTION || actionRequest
                                                                                                                      .actorCode()
                                                                                                         == LifecycleStatus.ActorCode.ADMIN_ACTION;

        if (!lastAction.isUserAllowed() && !isVaradhiAdmin) {
            throw new InvalidOperationForResourceException(
                "Restoration denied. Only Varadhi Admin can restore this subscription."
            );
        }

        return controllerClient.getSubscriptionState(subscriptionName, requestedBy).thenApply(state -> {
            subscription.restore(actionRequest.actorCode(), actionRequest.message());
            metaStore.updateSubscription(subscription);
            log.info("Subscription '{}' restored successfully.", subscriptionName);
            return subscription;
        });
    }

    /**
     * Retrieves and validates a subscription by its name.
     *
     * @param subscriptionName the name of the subscription
     *
     * @return the validated subscription
     *
     * @throws ResourceNotFoundException if the subscription is not found or in an invalid state
     */
    private VaradhiSubscription getValidatedSubscription(String subscriptionName) {
        VaradhiSubscription subscription = metaStore.getSubscription(subscriptionName);
        if (!subscription.isActive()) {
            throw new ResourceNotFoundException(
                String.format("Subscription '%s' not found or in invalid state.", subscriptionName)
            );
        }
        return subscription;
    }

    /**
     * Validates if a grouped subscription can be created or updated for a given topic.
     *
     * @param topic        the topic to validate against
     * @param subscription the subscription to validate
     *
     * @throws IllegalArgumentException if the subscription is grouped and the topic is not grouped
     */
    private void validateGroupedSubscription(VaradhiTopic topic, VaradhiSubscription subscription) {
        if (subscription.isGrouped() && !topic.isGrouped()) {
            throw new IllegalArgumentException(
                String.format(
                    "Grouped subscription cannot be created or updated for a non-grouped topic '%s'",
                    topic.getName()
                )
            );
        }
    }

    /**
     * Validates the version for updating a subscription.
     *
     * @param fromVersion   the current version of the subscription
     * @param latestVersion the latest version of the subscription
     *
     * @throws InvalidOperationForResourceException if the versions do not match
     */
    private void validateVersionForUpdate(int fromVersion, int latestVersion) {
        if (fromVersion != latestVersion) {
            throw new InvalidOperationForResourceException(
                "Conflicting update detected. Fetch the latest version and try again."
            );
        }
    }

    /**
     * Performs a subscription operation.
     *
     * @param subscriptionName the name of the subscription
     * @param requestedBy      the user requesting the operation
     * @param operation        the operation to perform
     *
     * @return a CompletableFuture representing the subscription operation
     */
    private CompletableFuture<SubscriptionOperation> performSubscriptionOperation(
        String subscriptionName,
        String requestedBy,
        BiFunction<String, String, CompletableFuture<SubscriptionOperation>> operation
    ) {
        VaradhiSubscription subscription = metaStore.getSubscription(subscriptionName);
        if (!subscription.isActive()) {
            throw new InvalidOperationForResourceException(
                String.format("Subscription '%s' is not well-provisioned for this operation.", subscription.getName())
            );
        }
        return operation.apply(subscriptionName, requestedBy);
    }

    /**
     * Handles the hard deletion of a subscription.
     *
     * @param subscription  the subscription to be hard-deleted
     * @param subProject    the project associated with the subscription
     * @param actionRequest the request containing the actor code and message for the deletion
     */
    private void handleHardDelete(
        VaradhiSubscription subscription,
        Project subProject,
        ResourceActionRequest actionRequest
    ) {
        try {
            subscription.markDeleting(actionRequest.actorCode(), actionRequest.message());
            metaStore.updateSubscription(subscription);

            shardProvisioner.deProvision(subscription, subProject);
            metaStore.deleteSubscription(subscription.getName());
            log.info("Subscription '{}' deleted successfully.", subscription.getName());
        } catch (Exception e) {
            log.error("Failed to hard delete subscription '{}'.", subscription.getName(), e);
            subscription.markDeleteFailed(e.getMessage());
            updateSubscriptionState(subscription);
            throw e;
        }
    }

    /**
     * Handles the soft deletion of a subscription.
     *
     * @param subscription  the subscription to be soft-deleted
     * @param actionRequest the request containing the actor code and message for the deletion
     */
    private void handleSoftDelete(VaradhiSubscription subscription, ResourceActionRequest actionRequest) {
        subscription.markInactive(actionRequest.actorCode(), actionRequest.message());
        metaStore.updateSubscription(subscription);
        log.info("Subscription '{}' marked inactive successfully.", subscription.getName());
    }

    /**
     * Updates the state of the given Varadhi subscription in the meta store.
     *
     * @param varadhiSubscription the Varadhi subscription whose state is to be updated
     */
    private void updateSubscriptionState(VaradhiSubscription varadhiSubscription) {
        try {
            metaStore.updateSubscription(varadhiSubscription);
        } catch (Exception e) {
            log.error("Failed to update subscription state: {}", varadhiSubscription.getName(), e);
        }
    }

    /**
     * Checks if a subscription exists.
     *
     * @param subscriptionName the name of the subscription
     *
     * @return true if the subscription exists, false otherwise
     */
    public boolean exists(String subscriptionName) {
        return metaStore.checkSubscriptionExists(subscriptionName);
    }
}
