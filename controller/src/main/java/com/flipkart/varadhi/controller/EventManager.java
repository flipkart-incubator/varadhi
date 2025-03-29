package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.common.events.EntityEvent;
import com.flipkart.varadhi.controller.config.EventProcessorConfig;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.MetaStoreChangeEvent;
import com.flipkart.varadhi.spi.db.MetaStoreEventListener;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public final class EventManager implements MetaStoreEventListener, AutoCloseable {

    private final MetaStore metaStore;
    private final EntityEventProcessor eventProcessor;
    private final EventProcessorConfig config;
    private final EntityEventFactory eventFactory;
    private final ExecutorService executorService;
    private final AtomicBoolean isShutdown;
    private final AtomicBoolean isActive;

    public EventManager(
        MetaStore metaStore,
        EntityEventProcessor eventProcessor,
        EntityEventFactory eventFactory,
        EventProcessorConfig config
    ) {
        this.metaStore = Objects.requireNonNull(metaStore, "metaStore cannot be null");
        this.eventProcessor = Objects.requireNonNull(eventProcessor, "eventProcessor cannot be null");
        this.eventFactory = Objects.requireNonNull(eventFactory, "eventFactory cannot be null");
        this.config = config != null ? config : EventProcessorConfig.getDefault();
        this.executorService = createExecutorService();
        this.isShutdown = new AtomicBoolean(false);
        this.isActive = new AtomicBoolean(false);

        initializeEventListener();
        log.info("EventManager initialized successfully");
    }

    private ExecutorService createExecutorService() {
        return Executors.newSingleThreadExecutor(
            r -> Thread.ofPlatform()
                       .name(config.getEventManagerThreadName())
                       .uncaughtExceptionHandler(this::handleUncaughtException)
                       .unstarted(r)
        );
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
            log.warn(
                "Skipping Event processing for {}/{} - Manager is shutdown or inactive",
                event.getResourceType(),
                event.getResourceName()
            );
            return;
        }

        CompletableFuture.runAsync(() -> processEventWithRetry(event, 0), executorService).exceptionally(throwable -> {
            log.error(
                "Unexpected error in event processing future for {}/{}",
                event.getResourceType(),
                event.getResourceName(),
                throwable
            );
            scheduleRetry(event, 10);
            return null;
        });
    }

    private void processEventWithRetry(MetaStoreChangeEvent event, int retryCount) {
        try {
            EntityEvent<?> entityEvent;
            try {
                entityEvent = createEntityEvent(event);
            } catch (Exception e) {
                log.error(
                    "Error creating entity event for {}/{}, scheduling retry",
                    event.getResourceType(),
                    event.getResourceName(),
                    e
                );
                scheduleRetry(event, retryCount);
                return;
            }

            eventProcessor.process(entityEvent)
                          .thenCompose(v -> markEventProcessedWithRetry(event))
                          .whenComplete((result, throwable) -> {
                              if (throwable != null) {
                                  log.error(
                                      "Failed to process event for {}/{}, scheduling retry",
                                      event.getResourceType(),
                                      event.getResourceName(),
                                      throwable
                                  );
                                  scheduleRetry(event, retryCount);
                              } else {
                                  log.info(
                                      "Successfully processed event for {}/{}",
                                      event.getResourceType(),
                                      event.getResourceName()
                                  );
                              }
                          });
        } catch (Exception e) {
            log.error("Failed to create entity event for {}/{}", event.getResourceType(), event.getResourceName(), e);
            scheduleRetry(event, retryCount);
        }
    }

    private CompletableFuture<Void> markEventProcessedWithRetry(MetaStoreChangeEvent event) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            event.markAsProcessed();
            future.complete(null);
        } catch (Exception e) {
            log.error("Failed to mark event as processed, will retry", e);
            future.completeExceptionally(e);
        }
        return future;
    }

    private void scheduleRetry(MetaStoreChangeEvent event, int retryCount) {
        if (isShutdown.get() || !isActive.get()) {
            log.warn(
                "Skipping retry for {}/{} - Manager is shutdown or inactive",
                event.getResourceType(),
                event.getResourceName()
            );
            return;
        }

        long delayMillis = config.calculateRetryDelayMs(retryCount);
        CompletableFuture.delayedExecutor(delayMillis, TimeUnit.MILLISECONDS, executorService)
                         .execute(() -> processEventWithRetry(event, retryCount + 1));
    }

    private EntityEvent<?> createEntityEvent(MetaStoreChangeEvent event) {
        ResourceType resourceType = event.getResourceType();
        String resourceName = event.getResourceName();

        return eventFactory.createEvent(resourceType, resourceName, metaStore);
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
        if (!executorService.awaitTermination(config.getShutdownTimeoutSeconds(), TimeUnit.SECONDS)) {
            log.warn("EventManager executor service did not terminate in time");
            executorService.shutdownNow().forEach(Runnable::run);
        }
    }
}
