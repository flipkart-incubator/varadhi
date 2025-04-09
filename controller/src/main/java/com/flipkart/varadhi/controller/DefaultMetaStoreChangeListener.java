package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.common.events.EntityEvent;
import com.flipkart.varadhi.common.events.EntityEventListener;
import com.flipkart.varadhi.common.events.EventType;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.MetaStoreChangeEvent;
import com.flipkart.varadhi.spi.db.MetaStoreEventListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Default implementation of {@link MetaStoreEventListener} that bridges MetaStore events to EntityEvents.
 * <p>
 * This class acts as an adapter between the MetaStore event system and the EntityEvent system,
 * translating MetaStoreChangeEvents into appropriate EntityEvents and forwarding them to an
 * EntityEventListener. It handles different resource types and manages both update and invalidation
 * scenarios.
 *
 * @see MetaStoreEventListener
 * @see EntityEventListener
 * @see MetaStoreChangeEvent
 * @see EntityEvent
 */
@Slf4j
@RequiredArgsConstructor
public class DefaultMetaStoreChangeListener implements MetaStoreEventListener {

    private final MetaStore metaStore;
    private final EntityEventListener listener;

    /**
     * Handles MetaStore change events by converting them to EntityEvents.
     * <p>
     * This method attempts to retrieve the current state of the resource from the MetaStore.
     * If the resource exists, it creates an UPSERT event with the resource data.
     * If the resource doesn't exist (ResourceNotFoundException), it creates an INVALIDATE event.
     * <p>
     * The method supports TOPIC, SUBSCRIPTION, and PROJECT resource types.
     *
     * @param event the MetaStore change event to process
     * @throws IllegalArgumentException if the resource type is not supported
     */
    @Override
    public void onEvent(MetaStoreChangeEvent event) {
        ResourceType type = event.getResourceType();
        String name = event.getResourceName();

        log.debug("Processing MetaStore change event for {} {}", type, name);

        try {
            switch (type) {
                case TOPIC -> {
                    VaradhiTopic topic = metaStore.getTopic(name);
                    log.debug("Retrieved topic {}, creating UPSERT event", name);
                    listener.onChange(new EntityEvent<>(type, name, EventType.UPSERT, topic, event::markAsProcessed));
                }
                case SUBSCRIPTION -> {
                    VaradhiSubscription subscription = metaStore.getSubscription(name);
                    log.debug("Retrieved subscription {}, creating UPSERT event", name);
                    listener.onChange(
                        new EntityEvent<>(type, name, EventType.UPSERT, subscription, event::markAsProcessed)
                    );
                }
                case PROJECT -> {
                    Project project = metaStore.getProject(name);
                    log.debug("Retrieved project {}, creating UPSERT event", name);
                    listener.onChange(new EntityEvent<>(type, name, EventType.UPSERT, project, event::markAsProcessed));
                }
                default -> throw new IllegalArgumentException("Unsupported resource type: " + type);
            }
        } catch (ResourceNotFoundException e) {
            log.debug("Resource not found, creating INVALIDATE event for {} {}", type, name);
            listener.onChange(new EntityEvent<>(type, name, EventType.INVALIDATE, null, event::markAsProcessed));
        }
    }
}
