package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.controller.config.EventProcessorConfig;
import com.flipkart.varadhi.controller.failover.TopicTransitionMetrics;
import com.flipkart.varadhi.core.CoreServices;
import com.flipkart.varadhi.core.cluster.MembershipListener;
import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.MessageRouter;
import com.flipkart.varadhi.core.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.controller.config.OperationsConfig;
import com.flipkart.varadhi.controller.impl.LeastAssignedStrategy;
import com.flipkart.varadhi.controller.impl.failover.StageAwaiter;
import com.flipkart.varadhi.controller.impl.failover.TopicFailoverConfig;
import com.flipkart.varadhi.core.cluster.consumer.ConsumerClientFactory;
import com.flipkart.varadhi.core.cluster.ComponentKind;
import com.flipkart.varadhi.core.cluster.ConsumerNode;
import com.flipkart.varadhi.core.cluster.MemberInfo;
import com.flipkart.varadhi.core.cluster.failover.TransitionBusAddress;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.cluster.Assignment;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.entities.cluster.TopicFailoverOperation;
import com.flipkart.varadhi.controller.events.ResourceEventProcessor;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;
import com.flipkart.varadhi.spi.services.MessagingStackProvider;
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
    private final MessagingStackProvider messagingStackProvider;
    private final MeterRegistry meterRegistry;
    private final OperationsConfig operationsConfig;
    private final EventProcessorConfig eventProcessorConfig;
    private final RegionName deployedRegion;

    private ResourceEventProcessor entityEventProcessor;

    /**
     * Creates a new ControllerVerticle with the specified configuration and services.
     */
    public ControllerVerticle(
        CoreServices coreServices,
        VaradhiClusterManager clusterManager,
        OperationsConfig opsConfig,
        EventProcessorConfig eventProcessorConfig,
        RegionName deployedRegion
    ) {
        this.operationsConfig = opsConfig;
        this.eventProcessorConfig = eventProcessorConfig;
        this.clusterManager = clusterManager;
        this.metaStoreProvider = coreServices.getMetaStoreProvider();
        this.messagingStackProvider = coreServices.getMessagingStackProvider();
        this.meterRegistry = coreServices.getMeterRegistry();
        this.deployedRegion = deployedRegion;
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

        SubscriptionService subscriptionService = createSubscriptionService(messageExchange);
        TransitionService transitionService = new TransitionService(messageExchange);
        ControllerHandler apiHandler = new ControllerHandler(
            subscriptionService,
            transitionService,
            new TopicTransitionMetrics(meterRegistry)
        );

        // Assume leadership and initialize event system
        onLeaderElected(subscriptionService, apiHandler, messageRouter).compose(v -> initializeEventSystem())
                                                                       .onComplete(ar -> {
                                                                           if (ar.succeeded()) {
                                                                               log.info(
                                                                                   "Controller started successfully"
                                                                               );
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
            eventProcessorConfig
        ).onSuccess(processor -> this.entityEventProcessor = processor);
    }

    /**
     * Creates and configures the SubscriptionService with the necessary components.
     *
     * @param messageExchange the message exchange for internode communication
     * @return the configured SubscriptionService
     */
    private SubscriptionService createSubscriptionService(MessageExchange messageExchange) {
        // Create consumer client factory
        ConsumerClientFactory consumerClientFactory = new ConsumerClientFactoryImpl(messageExchange);

        // Create operation manager with retry policy
        OperationMgr operationMgr = new OperationMgr(
            operationsConfig.getMaxConcurrentOps(),
            metaStoreProvider.getOpStore(),
            createRetryPolicy(operationsConfig.getMaxRetryAllowed()),
            createRetryPolicy(operationsConfig.getTopicFailoverMaxRetryAllowed())
        );

        // Create assignment manager
        AssignmentManager assigner = new AssignmentManager(
            new LeastAssignedStrategy(),
            metaStoreProvider.getAssignmentStore(),
            meterRegistry
        );

        return new SubscriptionService(
            operationMgr,
            assigner,
            metaStoreProvider.getMetaStore().subscriptions(),
            consumerClientFactory,
            metaStoreProvider.getTransitionStore(),
            metaStoreProvider.getMetaStore().topics(),
            metaStoreProvider.getMetaStore().regions(),
            messagingStackProvider.getStorageTopicService(),
            clusterManager,
            messageExchange,
            new StageAwaiter(),
            TopicFailoverConfig.defaultConfig(),
            deployedRegion
        );
    }

    /**
     * Creates a retry policy with the given max-retry ceiling and shared backoff settings.
     */
    private RetryPolicy createRetryPolicy(int maxRetryAllowed) {
        return new RetryPolicy(
            maxRetryAllowed,
            operationsConfig.getRetryIntervalInSeconds(),
            operationsConfig.getRetryMinBackoffInSeconds(),
            operationsConfig.getRetryMaxBackOffInSeconds()
        );
    }

    /**
     * Assumes leadership for controller operations by setting up API handlers,
     * registering membership listeners, and restoring controller state.
     *
     * @param subscriptionService the controller API manager
     * @param handler          the controller API handler
     * @param messageRouter    the message router for handling API requests
     * @return a Future that completes when leadership is established
     */
    private Future<Void> onLeaderElected(
        SubscriptionService subscriptionService,
        ControllerHandler handler,
        MessageRouter messageRouter
    ) {
        // Set up membership listener for consumer nodes
        // TODO: Handling membership changes during controller bootstrap.
        setupMembershipListener(subscriptionService);

        // Register API handlers immediately so they are available before consumer-node init
        setupApiHandlers(messageRouter, handler);

        // Get all cluster members and initialize consumer nodes asynchronously
        return clusterManager.getAllMembers()
                             .compose(allMembers -> initializeConsumerNodes(allMembers, subscriptionService))
                             .compose(consumerIds -> {
                                 restoreControllerState(subscriptionService, consumerIds);
                                 return Future.<Void>succeededFuture();
                             })
                             .onFailure(e -> {
                                 log.error(
                                     "Failed to initialize consumer nodes during leader election: {}",
                                     e.getMessage()
                                 );
                                 abortLeadership();
                             });
    }

    /**
     * Initializes consumer nodes from the list of cluster members.
     *
     * @param allMembers       the list of all cluster members
     * @param subscriptionService the controller API manager
     * @return a Future that completes with the list of initialized consumer IDs
     */
    private Future<List<String>> initializeConsumerNodes(
        List<MemberInfo> allMembers,
        SubscriptionService subscriptionService
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
                                                                   .map(subscriptionService::addConsumerNode)
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
     * @param subscriptionService the controller API manager
     * @param consumerIds      the list of active consumer IDs
     */
    private void restoreControllerState(SubscriptionService subscriptionService, List<String> consumerIds) {
        // Remove unavailable consumers
        removeUnavailableConsumers(subscriptionService, consumerIds);

        // Requeue in-progress operations
        requeueInProgressOperations(subscriptionService);

        // Resume in-flight topic failovers from their TransitionMaster stage
        requeueInProgressFailovers(subscriptionService);

        // TODO - Implementation needed: Add handling for failed operations with proper recovery mechanisms
        // This should include strategies for recovering from failures without requiring controller restart
    }

    /**
     * Resumes topic-failover operations that were in flight when the previous leader stopped. Each
     * executor is idempotent and re-enters from {@code TransitionMaster.currentStage}.
     */
    private void requeueInProgressFailovers(SubscriptionService subscriptionService) {
        List<TopicFailoverOperation> pendingFailovers = subscriptionService.getPendingTopicFailoverOps();
        if (pendingFailovers.isEmpty()) {
            log.info("No pending topic failovers to resume");
            return;
        }
        pendingFailovers.stream()
                        .sorted(Comparator.comparing(TopicFailoverOperation::getStartTime))
                        .forEach(subscriptionService::retryTopicFailover);
        log.info("Resumed {} pending topic failover(s)", pendingFailovers.size());
    }

    /**
     * Removes consumers that are no longer available in the cluster.
     *
     * @param subscriptionService the controller API manager
     * @param consumerIds      the list of active consumer IDs
     */
    private void removeUnavailableConsumers(SubscriptionService subscriptionService, List<String> consumerIds) {
        Set<String> activeConsumerSet = Set.copyOf(consumerIds);

        getUnavailableConsumers(subscriptionService, activeConsumerSet).forEach(consumerId -> {
            log.info("Marking consumer {} as left", consumerId);
            subscriptionService.consumerNodeLeft(consumerId);
        });
    }

    /**
     * Gets the list of consumer IDs that are no longer available in the cluster.
     *
     * @param subscriptionService the controller API manager
     * @param activeConsumers  the set of active consumer IDs
     * @return the list of unavailable consumer IDs
     */
    private List<String> getUnavailableConsumers(SubscriptionService subscriptionService, Set<String> activeConsumers) {
        List<Assignment> allAssignments = subscriptionService.getAllAssignments();
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
     * @param subscriptionService the controller API manager
     */
    private void requeueInProgressOperations(SubscriptionService subscriptionService) {
        List<SubscriptionOperation> pendingOps = subscriptionService.getPendingSubOps();

        if (pendingOps.isEmpty()) {
            log.info("No pending operations to requeue");
            return;
        }

        // Sort operations by start time to maintain order
        pendingOps.stream()
                  .sorted(Comparator.comparing(SubscriptionOperation::getStartTime))
                  .forEach(subscriptionService::retryOperation);
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
    private void setupApiHandlers(MessageRouter messageRouter, ControllerHandler handler) {
        // Register request handlers for different controller operations
        messageRouter.requestHandler(ROUTE_CONTROLLER, "start", handler::start);
        messageRouter.requestHandler(ROUTE_CONTROLLER, "stop", handler::stop);
        messageRouter.requestHandler(ROUTE_CONTROLLER, "state", handler::status);
        messageRouter.requestHandler(ROUTE_CONTROLLER, "unsideline", handler::unsideline);
        messageRouter.requestHandler(ROUTE_CONTROLLER, "getShards", handler::getShards);

        // Topic failover lifecycle (web -> controller)
        messageRouter.requestHandler(
            ROUTE_CONTROLLER,
            TransitionBusAddress.CREATE_FAILOVER_API,
            handler::createFailover
        );
        messageRouter.requestHandler(ROUTE_CONTROLLER, TransitionBusAddress.GET_FAILOVER_API, handler::getFailover);
        messageRouter.requestHandler(ROUTE_CONTROLLER, TransitionBusAddress.ABORT_FAILOVER_API, handler::abortFailover);
        messageRouter.requestHandler(ROUTE_CONTROLLER, TransitionBusAddress.LIST_FAILOVERS_API, handler::listFailovers);

        // Register send handlers for pod → controller updates
        messageRouter.sendHandler(ROUTE_CONTROLLER, "update", handler::update);
        messageRouter.sendHandler(ROUTE_CONTROLLER, TransitionBusAddress.STAGE_ACK_API, handler::ack);

        log.info("Controller API handlers registered successfully");
    }

    /**
     * Sets up a membership listener to handle consumer node joins and leaves.
     *
     * @param subscriptionService the controller API manager
     */
    private void setupMembershipListener(SubscriptionService subscriptionService) {
        clusterManager.addMembershipListener(new MembershipListener() {
            @Override
            public CompletableFuture<Void> joined(String nodeId, MemberInfo memberInfo) {
                log.info("Member joined: {} ({})", nodeId, memberInfo);

                if (memberInfo.hasRole(ComponentKind.Consumer)) {
                    ConsumerNode consumerNode = new ConsumerNode(memberInfo);
                    return subscriptionService.consumerNodeJoined(consumerNode);
                }

                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> left(String memberId) {
                log.info("Member left: {}", memberId);

                return subscriptionService.consumerNodeLeft(memberId);
            }
        });

        log.info("Membership listener registered successfully");
    }
}
