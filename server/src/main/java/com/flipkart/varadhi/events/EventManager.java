package com.flipkart.varadhi.events;

import com.flipkart.varadhi.cluster.MessageRouter;
import com.flipkart.varadhi.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.core.cluster.entities.MemberInfo;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.produce.providers.TopicProvider;
import com.flipkart.varadhi.providers.ProjectProvider;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

import static com.flipkart.varadhi.common.Constants.ENTITY_EVENTS_HANDLER;

/**
 * Central manager for entity events and cache management in Varadhi.
 * <p>
 * This class is responsible for:
 * <ul>
 *   <li>Preloading entity caches for improved performance</li>
 *   <li>Setting up event handlers for entity events</li>
 *   <li>Coordinating event distribution across the system</li>
 * </ul>
 * <p>
 * This component is designed to be initialized once at application startup,
 * before any verticles are deployed.
 *
 * @see EntityEventDispatcher
 * @see ProjectProvider
 * @see TopicProvider
 */
@Slf4j
public final class EventManager {

    /**
     * Provider for project entities.
     */
    private final ProjectProvider projectProvider;

    /**
     * Provider for topic entities.
     */
    private final TopicProvider topicProvider;

    /**
     * Cluster manager for inter-node communication.
     */
    private final VaradhiClusterManager clusterManager;

    /**
     * Information about the current cluster member.
     */
    private final MemberInfo memberInfo;

    /**
     * Vert.x instance for event handling.
     */
    private final Vertx vertx;

    /**
     * Creates a new EntityEventManager.
     *
     * @param projectProvider the provider for project entities
     * @param topicProvider   the provider for topic entities
     * @param clusterManager  the cluster manager for internode communication
     * @param memberInfo      information about the current cluster member
     * @param vertx           the Vert.x instance for event handling
     * @throws NullPointerException if any parameter is null
     */
    public EventManager(
        ProjectProvider projectProvider,
        TopicProvider topicProvider,
        VaradhiClusterManager clusterManager,
        MemberInfo memberInfo,
        Vertx vertx
    ) {
        this.projectProvider = Objects.requireNonNull(projectProvider, "ProjectProvider cannot be null");
        this.topicProvider = Objects.requireNonNull(topicProvider, "TopicProvider cannot be null");
        this.clusterManager = Objects.requireNonNull(clusterManager, "ClusterManager cannot be null");
        this.memberInfo = Objects.requireNonNull(memberInfo, "MemberInfo cannot be null");
        this.vertx = Objects.requireNonNull(vertx, "Vertx cannot be null");
    }

    /**
     * Initializes caches and sets up event handlers.
     *
     * @return a future that completes when initialization is finished
     */
    public Future<Void> initialize() {
        log.info("Starting cache and event handler initialization");

        return preloadCaches().compose(v -> setupEventHandlers())
                              .onSuccess(v -> log.info("Initialization completed successfully"))
                              .onFailure(e -> log.error("Initialization failed: {}", e.getMessage(), e));
    }

    /**
     * Preloads entity caches for improved performance.
     *
     * @return a future that completes when cache preloading is finished
     */
    private Future<Void> preloadCaches() {
        log.info("Starting cache preloading");

        return Future.all(projectProvider.preload(), topicProvider.preload()).map(v -> null);

        return projectProvider.preload().compose(v -> {
            log.info("Project cache preloaded successfully");
            return topicProvider.preload();
        })
                              .onSuccess(v -> log.info("All caches preloaded successfully"))
                              .onFailure(e -> log.error("Cache preloading failed: {}", e.getMessage(), e));
    }

    /**
     * Sets up event handlers for entity events.
     *
     * @return a future that completes when event handlers are set up
     */
    private Future<Void> setupEventHandlers() {
        Promise<Void> promise = Promise.promise();

        try {
            log.info("Setting up event handlers");
            String hostname = memberInfo.hostname();

            // Create event dispatcher
            EntityEventDispatcher eventDispatcher = new EntityEventDispatcher.Builder().withListener(
                ResourceType.PROJECT,
                projectProvider
            ).withListener(ResourceType.TOPIC, topicProvider).build();

            // Register event handler with message router
            MessageRouter messageRouter = clusterManager.getRouter(vertx);
            messageRouter.requestHandler(hostname, ENTITY_EVENTS_HANDLER, eventDispatcher::processEvent);

            log.info("Entity event handlers initialized");
            promise.complete();
        } catch (Exception e) {
            log.error("Failed to setup event handlers: {}", e.getMessage(), e);
            promise.fail(e);
        }

        return promise.future();
    }
}
