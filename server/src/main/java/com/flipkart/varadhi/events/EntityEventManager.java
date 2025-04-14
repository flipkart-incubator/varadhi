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

import java.util.Objects;

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
     * @param cacheRegistry  the registry of entity caches
     * @param clusterManager the cluster manager for internode communication
     * @param memberInfo     information about the current cluster member
     * @param vertx          the Vert.x instance for event handling
     * @throws NullPointerException if any parameter is null
     */
    public EntityEventManager(
        EntityReadCacheRegistry cacheRegistry,
        VaradhiClusterManager clusterManager,
        MemberInfo memberInfo,
        Vertx vertx
    ) {
        this.cacheRegistry = Objects.requireNonNull(cacheRegistry, "Cache registry cannot be null");
        this.clusterManager = Objects.requireNonNull(clusterManager, "ClusterManager cannot be null");
        this.memberInfo = Objects.requireNonNull(memberInfo, "MemberInfo cannot be null");
        this.vertx = Objects.requireNonNull(vertx, "Vertx cannot be null");
    }

    /**
     * Initializes caches and sets up event handlers.
     * <p>
     * This method performs the following operations in sequence:
     * <ol>
     *   <li>Preloads all entity caches for improved performance</li>
     *   <li>Sets up event handlers for entity events</li>
     * </ol>
     *
     * @return a future that completes when initialization is finished
     */
    public Future<Void> initialize() {
        log.info("Starting cache and event handler initialization");

        return preloadCaches().compose(this::setupEventHandlers)
                              .onSuccess(v -> log.info("Initialization completed successfully"))
                              .onFailure(e -> log.error("Initialization failed: {}", e.getMessage(), e));
    }

    /**
     * Preloads entity caches for improved performance.
     * <p>
     * This method preloads all registered entity caches in parallel. It leverages the cache registry's
     * preloading capabilities and provides detailed logging of the process.
     *
     * @return a future that completes when cache preloading is finished
     */
    private Future<Void> preloadCaches() {
        return cacheRegistry.preloadAll();
    }

    /**
     * Sets up event handlers for entity events.
     * <p>
     * This method creates an {@link EntityEventDispatcher} with listeners for all
     * registered entity caches and registers it with the message router for
     * distributed event processing.
     *
     * @param unused ignored parameter to maintain the compose chain
     * @return a future that completes when event handlers are set up
     */
    private Future<Void> setupEventHandlers(Void unused) {
        Promise<Void> promise = Promise.promise();

        try {
            log.info("Setting up event handlers");
            String hostname = memberInfo.hostname();

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

        // Get all available resource types
        ResourceType[] resourceTypes = ResourceType.values();
        log.debug("Registering listeners for {} resource types", resourceTypes.length);

        // Register listeners for each resource type with a registered cache
        for (ResourceType type : resourceTypes) {
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
