package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.CoreServices;
import com.flipkart.varadhi.core.cluster.MembershipListener;
import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.MessageRouter;
import com.flipkart.varadhi.core.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.controller.config.OperationsConfig;
import com.flipkart.varadhi.controller.impl.LeastAssignedStrategy;
import com.flipkart.varadhi.core.cluster.consumer.ConsumerClientFactory;
import com.flipkart.varadhi.core.cluster.ComponentKind;
import com.flipkart.varadhi.core.cluster.ConsumerNode;
import com.flipkart.varadhi.core.cluster.MemberInfo;
import com.flipkart.varadhi.entities.cluster.Assignment;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.controller.events.ResourceEventProcessor;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;
import com.flipkart.varadhi.core.cluster.consumer.ConsumerClientFactoryImpl;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.flipkart.varadhi.core.cluster.controller.ControllerApi.ROUTE_CONTROLLER;

@Slf4j
public class ControllerVerticle extends AbstractVerticle {

    private final VaradhiClusterManager clusterManager;
    private final MetaStoreProvider metaStoreProvider;
    private final MeterRegistry meterRegistry;
    private final OperationsConfig controllerConfig;

    private ResourceEventProcessor entityEventProcessor;

    /**
     * Creates a new ControllerVerticle with the specified configuration and services.
     *
     * @param config         the controller configuration
     * @param coreServices   the core services
     * @param clusterManager the cluster manager
     */
    public ControllerVerticle(
        OperationsConfig config,
        CoreServices coreServices,
        VaradhiClusterManager clusterManager
    ) {
        this.controllerConfig = config;
        this.clusterManager = clusterManager;
        this.metaStoreProvider = coreServices.getMetaStoreProvider();
        this.meterRegistry = coreServices.getMeterRegistry();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Initializes the controller components, sets up event processing, and establishes
     * leadership for controller operations.
     */
    @Override
    public void start(Promise<Void> startPromise) {
        // Initialize controller components
        MessageRouter messageRouter = clusterManager.getRouter(vertx);
        MessageExchange messageExchange = clusterManager.getExchange(vertx);

        // Create controller API manager and handler
        ControllerApiMgr controllerApiMgr = createControllerApiMgr(messageExchange);
        ControllerApiHandler apiHandler = new ControllerApiHandler(controllerApiMgr);

        // Assume leadership and initialize event system
        onLeaderElected(controllerApiMgr, apiHandler, messageRouter).compose(v -> initializeEventSystem())
                                                                    .onComplete(ar -> {
                                                                        if (ar.succeeded()) {
                                                                            log.info("Controller started successfully");
                                                                            startPromise.complete();
                                                                        } else {
                                                                            log.error(
                                                                                "Failed to start controller: {}",
                                                                                ar.cause().getMessage()
                                                                            );
                                                                            startPromise.fail(ar.cause());
                                                                        }
                                                                    });
    }

    /**
     * {@inheritDoc}
     * <p>
     * Gracefully shuts down the controller components and releases resources.
     */
    @Override
    public void stop(Promise<Void> stopPromise) {
        if (entityEventProcessor != null) {
            entityEventProcessor.close();
            entityEventProcessor = null;
        }
        stopPromise.complete();
    }

    /**
     * Creates and initializes the EventProcessor for handling entity events.
     *
     * @return a Future that completes with the initialized EventProcessor
     */
    private Future<ResourceEventProcessor> initializeEventSystem() {
        return ResourceEventProcessor.create(
            clusterManager.getExchange(vertx),
            clusterManager,
            metaStoreProvider.getMetaStore(),
            controllerConfig.getEventProcessorConfig()
        ).onSuccess(processor -> this.entityEventProcessor = processor);
    }

    /**
     * Creates and configures the ControllerApiMgr with the necessary components.
     *
     * @param messageExchange the message exchange for internode communication
     * @return the configured ControllerApiMgr
     */
    private ControllerApiMgr createControllerApiMgr(MessageExchange messageExchange) {
        // Create consumer client factory
        ConsumerClientFactory consumerClientFactory = new ConsumerClientFactoryImpl(messageExchange);

        // Create operation manager with retry policy
        OperationMgr operationMgr = new OperationMgr(
            controllerConfig.getMaxConcurrentOps(),
            metaStoreProvider.getOpStore(),
            createRetryPolicy()
        );

        // Create assignment manager
        AssignmentManager assigner = new AssignmentManager(
            new LeastAssignedStrategy(),
            metaStoreProvider.getAssignmentStore(),
            meterRegistry
        );

        return new ControllerApiMgr(
            operationMgr,
            assigner,
            metaStoreProvider.getMetaStore().subscriptions(),
            consumerClientFactory
        );
    }

    /**
     * Creates a retry policy based on the controller configuration.
     *
     * @return the configured RetryPolicy
     */
    private RetryPolicy createRetryPolicy() {
        return new RetryPolicy(
            controllerConfig.getMaxRetryAllowed(),
            controllerConfig.getRetryIntervalInSeconds(),
            controllerConfig.getRetryMinBackoffInSeconds(),
            controllerConfig.getRetryMaxBackOffInSeconds()
        );
    }

    /**
     * Assumes leadership for controller operations by setting up API handlers,
     * registering membership listeners, and restoring controller state.
     *
     * @param controllerApiMgr the controller API manager
     * @param handler          the controller API handler
     * @param messageRouter    the message router for handling API requests
     * @return a Future that completes when leadership is established
     */
    private Future<Void> onLeaderElected(
        ControllerApiMgr controllerApiMgr,
        ControllerApiHandler handler,
        MessageRouter messageRouter
    ) {
        // Set up membership listener for consumer nodes
        // TODO: Handling membership changes during controller bootstrap.
        setupMembershipListener(controllerApiMgr);

        // Get all cluster members and initialize consumer nodes
        return clusterManager.getAllMembers()
                             .compose(allMembers -> initializeConsumerNodes(allMembers, controllerApiMgr))
                             .compose(consumerIds -> {
                                 // Set up API handlers and restore controller state
                                 setupApiHandlers(messageRouter, handler);
                                 restoreControllerState(controllerApiMgr, consumerIds);
                                 return Future.<Void>succeededFuture();
                             })
                             .onFailure(e -> {
                                 abortLeadership();
                             });
    }

    /**
     * Initializes consumer nodes from the list of cluster members.
     *
     * @param allMembers       the list of all cluster members
     * @param controllerApiMgr the controller API manager
     * @return a Future that completes with the list of initialized consumer IDs
     */
    private Future<List<String>> initializeConsumerNodes(
        List<MemberInfo> allMembers,
        ControllerApiMgr controllerApiMgr
    ) {
        // Filter members that have the Consumer role
        List<ConsumerNode> consumerNodes = allMembers.stream()
                                                     .filter(memberInfo -> memberInfo.hasRole(ComponentKind.Consumer))
                                                     .map(ConsumerNode::new)
                                                     .toList();

        if (consumerNodes.isEmpty()) {
            return Future.succeededFuture(List.of());
        }

        // Create CompletableFuture for each consumer node initialization
        List<CompletableFuture<String>> nodeFutures = consumerNodes.stream()
                                                                   .map(controllerApiMgr::addConsumerNode)
                                                                   .toList();

        // Combine all futures and collect results
        CompletableFuture<Void> allFuture = CompletableFuture.allOf(nodeFutures.toArray(CompletableFuture[]::new));

        return Future.fromCompletionStage(allFuture).map(v -> {
            List<String> consumerIds = nodeFutures.stream().map(future -> {
                try {
                    return future.join();
                } catch (Exception e) {
                    log.warn("Failed to initialize consumer node: {}", e.getMessage());
                    return null;
                }
            }).filter(Objects::nonNull).toList();

            log.info("Successfully initialized {} consumer nodes", consumerIds.size());
            return consumerIds;
        });
    }

    /**
     * Restores the controller state by removing unavailable consumers and
     * requeuing in-progress operations.
     *
     * @param controllerApiMgr the controller API manager
     * @param consumerIds      the list of active consumer IDs
     */
    private void restoreControllerState(ControllerApiMgr controllerApiMgr, List<String> consumerIds) {
        // Remove unavailable consumers
        removeUnavailableConsumers(controllerApiMgr, consumerIds);

        // Requeue in-progress operations
        requeueInProgressOperations(controllerApiMgr);

        // TODO - Implementation needed: Add handling for failed operations with proper recovery mechanisms
        // This should include strategies for recovering from failures without requiring controller restart
    }

    /**
     * Removes consumers that are no longer available in the cluster.
     *
     * @param controllerApiMgr the controller API manager
     * @param consumerIds      the list of active consumer IDs
     */
    private void removeUnavailableConsumers(ControllerApiMgr controllerApiMgr, List<String> consumerIds) {
        Set<String> activeConsumerSet = Set.copyOf(consumerIds);

        getUnavailableConsumers(controllerApiMgr, activeConsumerSet).forEach(consumerId -> {
            log.info("Marking consumer {} as left", consumerId);
            controllerApiMgr.consumerNodeLeft(consumerId);
        });
    }

    /**
     * Gets the list of consumer IDs that are no longer available in the cluster.
     *
     * @param controllerApiMgr the controller API manager
     * @param activeConsumers  the set of active consumer IDs
     * @return the list of unavailable consumer IDs
     */
    private List<String> getUnavailableConsumers(ControllerApiMgr controllerApiMgr, Set<String> activeConsumers) {
        List<Assignment> allAssignments = controllerApiMgr.getAllAssignments();
        log.info("Found {} assignments", allAssignments.size());

        List<String> unavailableConsumers = allAssignments.stream()
                                                          .map(Assignment::getConsumerId)
                                                          .filter(consumerId -> !activeConsumers.contains(consumerId))
                                                          .distinct()
                                                          .toList();

        log.info("Found {} unavailable consumers", unavailableConsumers.size());
        return unavailableConsumers;
    }

    /**
     * Requeues in-progress operations to ensure they are completed.
     *
     * @param controllerApiMgr the controller API manager
     */
    private void requeueInProgressOperations(ControllerApiMgr controllerApiMgr) {
        List<SubscriptionOperation> pendingOps = controllerApiMgr.getPendingSubOps();

        if (pendingOps.isEmpty()) {
            log.info("No pending operations to requeue");
            return;
        }

        // Sort operations by start time to maintain order
        pendingOps.stream()
                  .sorted(Comparator.comparing(SubscriptionOperation::getStartTime))
                  .forEach(controllerApiMgr::retryOperation);
        log.info("Requeued {} pending operations", pendingOps.size());
    }

    /**
     * Aborts leadership by throwing an exception.
     * <p>
     * TODO - Implementation needed: A proper leadership handover mechanism that gracefully
     * transfers controller responsibilities to another node without disrupting service.
     */
    private void abortLeadership() {
        throw new IllegalStateException("Failed to initialize controller, aborting leadership");
    }

    /**
     * Sets up API handlers for controller operations.
     *
     * @param messageRouter the message router
     * @param handler       the controller API handler
     */
    private void setupApiHandlers(MessageRouter messageRouter, ControllerApiHandler handler) {
        // Register request handlers for different controller operations
        messageRouter.requestHandler(ROUTE_CONTROLLER, "start", handler::start);
        messageRouter.requestHandler(ROUTE_CONTROLLER, "stop", handler::stop);
        messageRouter.requestHandler(ROUTE_CONTROLLER, "state", handler::status);
        messageRouter.requestHandler(ROUTE_CONTROLLER, "unsideline", handler::unsideline);
        messageRouter.requestHandler(ROUTE_CONTROLLER, "getShards", handler::getShards);

        // Register send handler for updates
        messageRouter.sendHandler(ROUTE_CONTROLLER, "update", handler::update);

        log.info("Controller API handlers registered successfully");
    }

    /**
     * Sets up a membership listener to handle consumer node joins and leaves.
     *
     * @param controllerApiMgr the controller API manager
     */
    private void setupMembershipListener(ControllerApiMgr controllerApiMgr) {
        clusterManager.addMembershipListener(new MembershipListener() {
            @Override
            public CompletableFuture<Void> joined(MemberInfo memberInfo) {
                log.info("Member joined: {}", memberInfo);

                if (memberInfo.hasRole(ComponentKind.Consumer)) {
                    ConsumerNode consumerNode = new ConsumerNode(memberInfo);
                    return controllerApiMgr.consumerNodeJoined(consumerNode);
                }

                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> left(String memberId) {
                log.info("Member left: {}", memberId);

                return controllerApiMgr.consumerNodeLeft(memberId);
            }
        });

        log.info("Membership listener registered successfully");
    }
}
