package com.flipkart.varadhi.events;

import com.flipkart.varadhi.common.events.EntityEvent;
import com.flipkart.varadhi.core.cluster.EntityEventHandler;
import com.flipkart.varadhi.entities.auth.ResourceType;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public final class CompositeEntityEventHandler implements EntityEventHandler {
    private final List<EntityEventHandler> handlers;
    private final Set<ResourceType> supportedTypes;

    public CompositeEntityEventHandler(List<EntityEventHandler> handlers) {
        if (handlers.isEmpty()) {
            throw new IllegalArgumentException("At least one handler must be provided");
        }

        this.handlers = List.copyOf(handlers);
        this.supportedTypes = Collections.unmodifiableSet(
                handlers.stream()
                        .map(EntityEventHandler::getSupportedResourceTypes)
                        .flatMap(Set::stream)
                        .collect(Collectors.toCollection(() -> EnumSet.noneOf(ResourceType.class)))
        );
    }

    @Override
    public void handleEvent(EntityEvent event) {
        Objects.requireNonNull(event, "Event cannot be null");

        if (!supportedTypes.contains(event.resourceType())) {
            log.debug("Skipping unsupported resource type: {}", event.resourceType());
            return;
        }

        handlers.forEach(handler -> {
            try {
                if (handler.getSupportedResourceTypes().contains(event.resourceType())) {
                    handler.handleEvent(event);
                }
            } catch (Exception e) {
                log.error(
                        "Handler {} failed to process event type {}: {}",
                        handler.getClass().getSimpleName(),
                        event.resourceType(),
                        e.getMessage(),
                        e
                );
            }
        });
    }

    @Override
    public Set<ResourceType> getSupportedResourceTypes() {
        return supportedTypes;
    }
}
