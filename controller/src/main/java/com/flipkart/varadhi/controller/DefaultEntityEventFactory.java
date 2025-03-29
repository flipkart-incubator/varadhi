package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.common.events.EntityEvent;
import com.flipkart.varadhi.common.events.MetaStoreEntityResult;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultEntityEventFactory implements EntityEventFactory {

    @Override
    public EntityEvent<?> createEvent(ResourceType resourceType, String resourceName, MetaStore metaStore) {
        MetaStoreEntityResult<?> result = fetchTypedEntity(resourceType, resourceName, metaStore);
        return EntityEvent.of(resourceType, resourceName, result.resourceOperation(), result.state());
    }

    @SuppressWarnings ("unchecked")
    private <T> MetaStoreEntityResult<T> fetchTypedEntity(ResourceType type, String name, MetaStore metaStore) {
        try {
            switch (type) {
                case TOPIC -> {
                    VaradhiTopic topic = metaStore.getTopic(name);
                    return (MetaStoreEntityResult<T>)MetaStoreEntityResult.of(topic);
                }
                case SUBSCRIPTION -> {
                    VaradhiSubscription subscription = metaStore.getSubscription(name);
                    return (MetaStoreEntityResult<T>)MetaStoreEntityResult.of(subscription);
                }
                case PROJECT -> {
                    Project project = metaStore.getProject(name);
                    return (MetaStoreEntityResult<T>)MetaStoreEntityResult.of(project);
                }
                default -> throw new IllegalArgumentException("Unsupported resource type: " + type);
            }
        } catch (ResourceNotFoundException e) {
            return MetaStoreEntityResult.notFound();
        } catch (MetaStoreException e) {
            log.error("MetaStore error while fetching entity", e);
            throw e;
        }
    }
}
