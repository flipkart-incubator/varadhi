package com.flipkart.varadhi.verticles.controller;

import com.flipkart.varadhi.CoreServices;
import com.flipkart.varadhi.cluster.MembershipListener;
import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.cluster.MessageRouter;
import com.flipkart.varadhi.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.controller.AssignmentManager;
import com.flipkart.varadhi.controller.ControllerApiMgr;
import com.flipkart.varadhi.controller.OperationMgr;
import com.flipkart.varadhi.controller.RetryPolicy;
import com.flipkart.varadhi.controller.config.ControllerConfig;
import com.flipkart.varadhi.controller.impl.LeastAssignedStrategy;
import com.flipkart.varadhi.core.cluster.ConsumerClientFactory;
import com.flipkart.varadhi.core.cluster.entities.ComponentKind;
import com.flipkart.varadhi.core.cluster.entities.ConsumerNode;
import com.flipkart.varadhi.core.cluster.entities.MemberInfo;
import com.flipkart.varadhi.entities.cluster.Assignment;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.events.EventProcessor;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;
import com.flipkart.varadhi.verticles.consumer.ConsumerClientFactoryImpl;
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

import static com.flipkart.varadhi.core.cluster.ControllerRestApi.ROUTE_CONTROLLER;

@Slf4j
public class ControllerVerticle extends AbstractVerticle {

    private final VaradhiClusterManager clusterManager;
    private final MetaStoreProvider metaStoreProvider;
    private final MeterRegistry meterRegistry;
    private final ControllerConfig controllerConfig;

    private EventProcessor eventProcessor;

    /**
     * Creates a new ControllerVerticle with the specified configuration and services.
     *
     * @param config         the controller configuration
     * @param coreServices   the core services
     * @param clusterManager the cluster manager
     * @throws NullPointerException if any parameter is null
     */
    public ControllerVerticle(
        ControllerConfig config,
        CoreServices coreServices,
        VaradhiClusterManager clusterManager
    ) {
        this.controllerConfig = Objects.requireNonNull(config, "Controller config cannot be null");
        this.clusterManager = Objects.requireNonNull(clusterManager, "Cluster manager cannot be null");
        this.metaStoreProvider = Objects.requireNonNull(
            coreServices.getMetaStoreProvider(),
            "MetaStore provider cannot be null"
        );
        this.meterRegistry = Objects.requireNonNull(coreServices.getMeterRegistry(), "Meter registry cannot be null");
    }

    /**
     * {@inheritDoc}
     * <p>
     * Initializes the controller components, sets up event processing, and establishes
     * leadership for controller operations.
     */
    @Override
    public void start(Promise<Void> startPromise) {
        log.info("Starting ControllerVerticle");

        try {
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
                                                                                log.info(
                                                                                    "Controller initialized successfully"
                                                                                );
                                                                                startPromise.complete();
                                                                            } else {
                                                                                log.error(
                                                                                    "Failed to initialize controller",
                                                                                    ar.cause()
                                                                                );
                                                                                startPromise.fail(ar.cause());
                                                                            }
                                                                        });
        } catch (Exception e) {
            log.error("Error during ControllerVerticle startup", e);
            startPromise.fail(e);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Gracefully shuts down the controller components and releases resources.
     */
    @Override
    public void stop(Promise<Void> stopPromise) {
        log.info("Stopping ControllerVerticle");

        try {
            if (eventProcessor != null) {
                eventProcessor.close();
                eventProcessor = null;
            }
            log.info("ControllerVerticle stopped successfully");
            stopPromise.complete();
        } catch (Exception e) {
            log.error("Error during ControllerVerticle shutdown", e);
            stopPromise.fail(e);
        }
    }

    /**
     * Creates and initializes the EventProcessor for handling entity events.
     *
     * @return a Future that completes with the initialized EventProcessor
     */
    private Future<EventProcessor> initializeEventSystem() {
        return EventProcessor.create(
            clusterManager.getExchange(vertx),
            clusterManager,
            metaStoreProvider.getMetaStore(),
            controllerConfig.getEventProcessorConfig()
        ).onSuccess(processor -> {
            this.eventProcessor = processor;
            log.info("Event processor initialized successfully");
        });
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

        return new ControllerApiMgr(operationMgr, assigner, metaStoreProvider.getMetaStore(), consumerClientFactory);
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
                                 log.error("Failed to assume leadership", e);
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

        log.info("Found {} consumer nodes in the cluster", consumerNodes.size());

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
            controllerApiMgr.consumerNodeLeft(consumerId).exceptionally(e -> {
                log.error("Failed to mark consumer {} as left: {}", consumerId, e.getMessage());
                return null;
            });
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
        pendingOps.stream().sorted(Comparator.comparing(SubscriptionOperation::getStartTime)).forEach(op -> {
            try {
                controllerApiMgr.retryOperation(op);
                log.info("Requeued pending operation: {}", op);
            } catch (Exception e) {
                log.error("Failed to requeue operation {}: {}", op, e.getMessage());
            }
        });

        log.info("Requeued {} pending operations", pendingOps.size());
    }

    /**
     * Aborts leadership by throwing an exception.
     * <p>
     * TODO - Implementation needed: A proper leadership handover mechanism that gracefully
     * transfers controller responsibilities to another node without disrupting service.
     */
    private void abortLeadership() {
        log.error("Aborting leadership due to initialization failure");
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
