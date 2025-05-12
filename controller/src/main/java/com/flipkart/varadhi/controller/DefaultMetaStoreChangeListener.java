package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.common.events.EntityEvent;
import com.flipkart.varadhi.common.events.EntityEventListener;
import com.flipkart.varadhi.common.events.EventType;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.entities.filters.OrgFilters;
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
    private final EntityEventListener<MetaStoreEntity> listener;

    /**
     * Processes a MetaStore change event by translating it into an EntityEvent and forwarding it to the registered listener.
     * <p>
     * For supported resource types (TOPIC, SUBSCRIPTION, PROJECT, ORG), retrieves the current resource state from the MetaStore and emits an UPSERT event with the resource data. For ORG, includes associated filters in the event. If the resource does not exist, emits an INVALIDATE event. Unsupported resource types result in an IllegalArgumentException.
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
                    VaradhiTopic topic = metaStore.topics().get(name);
                    log.debug("Retrieved topic {}, creating UPSERT event", name);
                    listener.onChange(
                        new EntityEvent<>(
                            type,
                            name,
                            EventType.UPSERT,
                            topic,
                            topic.getVersion(),
                            event::markAsProcessed
                        )
                    );
                }
                case SUBSCRIPTION -> {
                    VaradhiSubscription subscription = metaStore.subscriptions().get(name);
                    log.debug("Retrieved subscription {}, creating UPSERT event", name);
                    listener.onChange(
                        new EntityEvent<>(
                            type,
                            name,
                            EventType.UPSERT,
                            subscription,
                            subscription.getVersion(),
                            event::markAsProcessed
                        )
                    );
                }
                case PROJECT -> {
                    Project project = metaStore.projects().get(name);
                    log.debug("Retrieved project {}, creating UPSERT event", name);
                    listener.onChange(
                        new EntityEvent<>(
                            type,
                            name,
                            EventType.UPSERT,
                            project,
                            project.getVersion(),
                            event::markAsProcessed
                        )
                    );
                }
                case ORG -> {
                    Org org = metaStore.orgs().get(name);
                    OrgFilters orgFilters = metaStore.orgs().getFilter(name);
                    log.debug("Retrieved org details {}, creating UPSERT event", name);
                    // Wrap the Org object in OrgDetails
                    OrgDetails orgDetails = new OrgDetails(org, orgFilters);
                    listener.onChange(
                        new EntityEvent<>(
                            type,
                            name,
                            EventType.UPSERT,
                            orgDetails,
                            org.getVersion(),
                            event::markAsProcessed
                        )
                    );
                }
                default -> throw new IllegalArgumentException("Unsupported resource type: " + type);
            }
        } catch (ResourceNotFoundException e) {
            log.debug("Resource not found, creating INVALIDATE event for {} {}", type, name);
            listener.onChange(new EntityEvent<>(type, name, EventType.INVALIDATE, null, 0, event::markAsProcessed));
        }
    }
}
