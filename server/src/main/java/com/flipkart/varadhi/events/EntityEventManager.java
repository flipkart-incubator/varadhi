package com.flipkart.varadhi.events;

import com.flipkart.varadhi.cluster.MessageRouter;
import com.flipkart.varadhi.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.common.EntityReadCache;
import com.flipkart.varadhi.common.EntityReadCacheRegistry;
import com.flipkart.varadhi.core.cluster.entities.MemberInfo;
import com.flipkart.varadhi.entities.auth.ResourceType;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

import static com.flipkart.varadhi.common.Constants.ENTITY_EVENTS_HANDLER;

/**
 * Central manager for entity events and cache management in Varadhi.
 * <p>
 * This class is responsible for:
 * <ul>
 *   <li>Preloading entity caches for improved performance</li>
 *   <li>Type-safe event routing with generic constraints</li>
 * </ul>
 * <p>
 * The manager coordinates entity caches and event handlers across the system, ensuring
 * consistent state management in a distributed environment. It is designed to be initialized
 * once at application startup, before any verticles are deployed.
 *
 * @see EntityEventDispatcher
 * @see EntityReadCache
 * @see EntityReadCacheRegistry
 */
@Slf4j
public final class EntityEventManager {

    /**
     * Registry of entity caches for different resource types.
     */
    private final EntityReadCacheRegistry cacheRegistry;

    /**
     * Cluster manager for internode communication.
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
     * @param cacheRegistry  the registry of entity caches
     * @param clusterManager the cluster manager for internode communication
     * @param memberInfo     information about the current cluster member
     * @param vertx          the Vert.x instance for event handling
     */
    public EntityEventManager(
        EntityReadCacheRegistry cacheRegistry,
        VaradhiClusterManager clusterManager,
        MemberInfo memberInfo,
        Vertx vertx
    ) {
        this.cacheRegistry = cacheRegistry;
        this.clusterManager = clusterManager;
        this.memberInfo = memberInfo;
        this.vertx = vertx;
    }

    /**
     * Initializes the event handling system.
     * <p>
     * This method sets up event handlers for all registered entity caches.
     *
     * @return a future that completes when initialization is finished
     */
    public Future<Void> initialize() {
        return setupEventHandlers();
    }

    /**
     * Sets up event handlers for entity events.
     * <p>
     * This method creates an {@link EntityEventDispatcher} with listeners for all
     * registered entity caches and registers it with the message router for
     * distributed event processing.
     *
     * @return a future that completes when event handlers are set up
     */
    private Future<Void> setupEventHandlers() {
        Promise<Void> promise = Promise.promise();
        String hostname = memberInfo.hostname();

        try {
            // Create event dispatcher builder
            EntityEventDispatcher.Builder builder = new EntityEventDispatcher.Builder();

            // Add listeners for all registered caches
            int listenersAdded = registerCacheListeners(builder);

            if (listenersAdded == 0) {
                log.warn("No cache listeners registered. Event processing will be limited.");
            }

            // Build the dispatcher
            EntityEventDispatcher dispatcher = builder.build();

            // Register event handler with message router
            MessageRouter messageRouter = clusterManager.getRouter(vertx);
            messageRouter.requestHandler(hostname, ENTITY_EVENTS_HANDLER, dispatcher::processEvent);

            log.info("Entity event handlers initialized with {} listeners", listenersAdded);
            promise.complete();
        } catch (Exception e) {
            log.error("Failed to setup event handlers: {}", e.getMessage(), e);
            promise.fail(e);
        }

        return promise.future();
    }

    /**
     * Registers cache listeners with the event dispatcher builder.
     * <p>
     * This method attempts to register a listener for each resource type
     * that has a registered cache in the cache registry.
     *
     * @param builder the event dispatcher builder
     * @return the number of listeners successfully registered
     */
    private int registerCacheListeners(EntityEventDispatcher.Builder builder) {
        int listenersAdded = 0;

        // Register listeners for each resource type with a registered cache
        for (ResourceType type : ResourceType.values()) {
            try {
                EntityReadCache<?> cache = cacheRegistry.getCache(type);
                builder.withListener(type, cache);
                listenersAdded++;
                log.debug("Registered listener for resource type: {}", type);
            } catch (IllegalStateException e) {
                // Skip resource types without registered caches
                log.debug("No cache registered for resource type: {}", type);
            }
        }

        return listenersAdded;
    }
}
