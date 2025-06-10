package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.core.cluster.events.ResourceEvent;
import com.flipkart.varadhi.core.cluster.events.ResourceEventListener;
import com.flipkart.varadhi.core.cluster.events.EventType;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.MetaStoreEntityType;
import com.flipkart.varadhi.entities.filters.OrgFilters;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.MetaStoreChangeEvent;
import com.flipkart.varadhi.spi.db.MetaStoreEventListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Default implementation of {@link MetaStoreEventListener} that bridges MetaStore events to EntityEvents.
 * <p>
 * This class acts as an adapter between the MetaStore event system and the ResourceEvent system,
 * translating MetaStoreChangeEvents into appropriate EntityEvents and forwarding them to an
 * ResourceEventListener. It handles different resource types and manages both update and invalidation
 * scenarios.
 *
 * @see MetaStoreEventListener
 * @see ResourceEventListener
 * @see MetaStoreChangeEvent
 * @see ResourceEvent
 */
@Slf4j
@RequiredArgsConstructor
public class DefaultMetaStoreChangeListener implements MetaStoreEventListener {

    private final MetaStore metaStore;
    private final ResourceEventListener<Resource> listener;

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
        MetaStoreEntityType type = event.getEntityType();
        String name = event.getResourceName();

        log.debug("Processing MetaStore change event for {} {}", type, name);

        try {
            switch (type) {
                case TOPIC -> {
                    VaradhiTopic topic = metaStore.topics().get(name);
                    log.debug("Retrieved topic {}, creating UPSERT event", name);
                    processUpsertEvent(ResourceType.TOPIC, name, topic, type, event::markAsProcessed);
                }
                case SUBSCRIPTION -> {
                    VaradhiSubscription subscription = metaStore.subscriptions().get(name);
                    log.debug("Retrieved subscription {}, creating UPSERT event", name);
                    processUpsertEvent(ResourceType.SUBSCRIPTION, name, subscription, type, event::markAsProcessed);
                }
                case PROJECT -> {
                    Project project = metaStore.projects().get(name);
                    log.debug("Retrieved project {}, creating UPSERT event", name);
                    processUpsertEvent(ResourceType.PROJECT, name, project, type, event::markAsProcessed);
                }
                case ORG, ORG_FILTER -> {
                    Org org = metaStore.orgs().get(name);
                    OrgFilters orgFilters = metaStore.orgs().getFilter(name);
                    log.debug("Retrieved org details {}, creating UPSERT event", name);
                    OrgDetails orgDetails = new OrgDetails(org, orgFilters);
                    listener.onChange(
                        new ResourceEvent<>(
                            ResourceType.ORG,
                            name,
                            EventType.UPSERT,
                            orgDetails,
                            orgDetails.getVersion(),
                            event::markAsProcessed
                        )
                    );
                }
                default -> throw new IllegalArgumentException("Unsupported resource type: " + type);
            }
        } catch (ResourceNotFoundException e) {
            log.debug("Resource not found, creating INVALIDATE event for {} {}", type, name);
            if (type == MetaStoreEntityType.TOPIC) {
                processInvalidateEvent(ResourceType.TOPIC, name, event::markAsProcessed);
            } else if (type == MetaStoreEntityType.SUBSCRIPTION) {
                processInvalidateEvent(ResourceType.SUBSCRIPTION, name, event::markAsProcessed);
            } else if (type == MetaStoreEntityType.PROJECT) {
                processInvalidateEvent(ResourceType.PROJECT, name, event::markAsProcessed);
            } else if (type == MetaStoreEntityType.ORG) {
                processInvalidateEvent(ResourceType.ORG, name, event::markAsProcessed);
            }
        }
    }

    private <T extends MetaStoreEntity> void processUpsertEvent(
        ResourceType resourceType,
        String name,
        T entity,
        MetaStoreEntityType metaStoreEntityType,
        Runnable committer
    ) {
        listener.onChange(
            new ResourceEvent<>(
                resourceType,
                name,
                EventType.UPSERT,
                Resource.of(entity, resourceType),
                entity.getVersion(),
                committer
            )
        );
    }

    private void processInvalidateEvent(ResourceType resourceType, String name, Runnable committer) {
        listener.onChange(new ResourceEvent<>(resourceType, name, EventType.INVALIDATE, null, 0, committer));
    }

}
