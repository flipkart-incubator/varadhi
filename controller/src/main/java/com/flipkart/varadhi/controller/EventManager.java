package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.controller.events.MetaStoreEntityResult;
import com.flipkart.varadhi.entities.EntityEvent;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.MetaStoreChangeEvent;
import com.flipkart.varadhi.spi.db.MetaStoreEventListener;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
public final class EventManager implements MetaStoreEventListener, AutoCloseable {
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(5);
    private static final String THREAD_NAME = "event-manager";

    private final MetaStore metaStore;
    private final EntityEventProcessor eventProcessor;
    private final ExecutorService executorService;
    private final AtomicBoolean isShutdown;
    private final AtomicBoolean isActive;

    public EventManager(MetaStore metaStore, EntityEventProcessor eventProcessor) {
        this.metaStore = Objects.requireNonNull(metaStore, "metaStore cannot be null");
        this.eventProcessor = Objects.requireNonNull(eventProcessor, "eventProcessor cannot be null");
        this.executorService = createExecutorService();
        this.isShutdown = new AtomicBoolean(false);
        this.isActive = new AtomicBoolean(false);

        initializeEventListener();
        log.info("EventManager initialized successfully");
    }

    private ExecutorService createExecutorService() {
        return Executors.newSingleThreadExecutor(r -> Thread.ofPlatform()
                .name(THREAD_NAME)
                .uncaughtExceptionHandler(this::handleUncaughtException)
                .unstarted(r));
    }

    private void handleUncaughtException(Thread thread, Throwable throwable) {
        log.error("Uncaught exception in thread {}", thread.getName(), throwable);
        if (throwable instanceof InterruptedException) {
            thread.interrupt();
        }
    }

    private void initializeEventListener() {
        boolean registered = metaStore.registerEventListener(this);
        if (!registered) {
            log.info("Failed to register event listener - another controller is already active");
            return;
        }
        isActive.set(true);
        log.info("Successfully registered as active event listener");
    }

    @Override
    public void onEvent(MetaStoreChangeEvent event) {
        if (isShutdown.get() || !isActive.get()) {
            log.warn("Skipping Event processing for {}/{} - Manager is shutdown or inactive",
                    event.getResourceType(), event.getResourceName());
            return;
        }

        CompletableFuture.runAsync(() -> processEvent(event), executorService)
                .exceptionallyAsync(throwable -> {
                    log.error("Unexpected error in event processing future for {}/{}",
                            event.getResourceType(), event.getResourceName(), throwable);
                    return null;
                }, executorService);
    }

    private void processEvent(MetaStoreChangeEvent event) {
        try {
            var entityEvent = createEntityEvent(event);
            if (entityEvent == null) {
                log.error("Unable to process event {}/{} - failed to create entity event",
                        event.getResourceType(), event.getResourceName());
                return;
            }

            eventProcessor.process(entityEvent)
                    .thenAcceptAsync(v -> {
                        event.markAsProcessed();
                        log.info("Successfully processed event for {}/{}",
                                event.getResourceType(), event.getResourceName());
                    }, executorService)
                    .exceptionallyAsync(throwable -> {
                        log.error("Failed to process event for {}/{}",
                                event.getResourceType(), event.getResourceName(), throwable);
                        return null;
                    }, executorService);
        } catch (Exception e) {
            log.error("Failed to create entity event for {}/{}",
                    event.getResourceType(), event.getResourceName(), e);
        }
    }

    private EntityEvent createEntityEvent(MetaStoreChangeEvent event) {
        var result = getMetaStoreEntity(event);
        if (result == null) {
            return null;
        }
        return EntityEvent.of(
            event.getResourceType(),
            event.getResourceName(),
            result.cacheOperation(),
            result.state()
        );
    }

    private MetaStoreEntityResult getMetaStoreEntity(MetaStoreChangeEvent event) {
        return fetchMetaStoreEntity(() ->
                invokeGetMethod(event.getResourceType(), event.getResourceName())
        );
    }

    private Object invokeGetMethod(ResourceType type, String name) {
        try {
            String methodName = "get" + formatMethodName(type.name());
            Method method = metaStore.getClass().getMethod(methodName, String.class);
            return method.invoke(metaStore, name);
        } catch (InvocationTargetException e) {
            throw (RuntimeException) e.getTargetException();
        } catch (Exception e) {
            log.error("Failed to invoke get method for {}/{}", type, name, e);
            return null;
        }
    }

    private String formatMethodName(String typeName) {
        return Arrays.stream(typeName.split("_"))
                .map(part -> Character.toUpperCase(part.charAt(0)) + part.substring(1).toLowerCase())
                .collect(Collectors.joining());
    }

    private MetaStoreEntityResult fetchMetaStoreEntity(Supplier<Object> entitySupplier) {
        try {
            Object entity = entitySupplier.get();
            if (entity == null) {
                return null;
            }
            return MetaStoreEntityResult.of(entity);
        } catch (ResourceNotFoundException e) {
            return MetaStoreEntityResult.notFound();
        } catch (MetaStoreException e) {
            log.error("MetaStore error while fetching entity", e);
            return null;
        }
    }

    @Override
    public void close() {
        if (isShutdown.compareAndSet(false, true)) {
            try {
                log.info("Initiating EventManager shutdown...");
                shutdownExecutor();
                log.info("EventManager shutdown completed");
            } catch (Exception e) {
                log.error("Error during EventManager shutdown", e);
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private void shutdownExecutor() throws InterruptedException {
        executorService.shutdown();
        if (!executorService.awaitTermination(SHUTDOWN_TIMEOUT.toSeconds(), TimeUnit.SECONDS)) {
            log.warn("EventManager executor service did not terminate in time");
            executorService.shutdownNow().forEach(Runnable::run);
        }
    }
}
