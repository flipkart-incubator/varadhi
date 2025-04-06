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

@Slf4j
@RequiredArgsConstructor
public class DefaultMetaStoreChangeListener implements MetaStoreEventListener {

    private final MetaStore metaStore;
    private final EntityEventListener listener;

    @Override
    public void onEvent(MetaStoreChangeEvent event) {
        ResourceType type = event.getResourceType();
        String name = event.getResourceName();
        try {
            switch (type) {
                case TOPIC -> {
                    VaradhiTopic topic = metaStore.getTopic(name);
                    listener.onChange(new EntityEvent<>(type, name, EventType.UPSERT, topic, event::markAsProcessed));
                }
                case SUBSCRIPTION -> {
                    VaradhiSubscription subscription = metaStore.getSubscription(name);
                    listener.onChange(
                        new EntityEvent<>(type, name, EventType.UPSERT, subscription, event::markAsProcessed)
                    );
                }
                case PROJECT -> {
                    Project project = metaStore.getProject(name);
                    listener.onChange(new EntityEvent<>(type, name, EventType.UPSERT, project, event::markAsProcessed));
                }
                default -> throw new IllegalArgumentException("Unsupported resource type: " + type);
            }
        } catch (ResourceNotFoundException e) {
            listener.onChange(new EntityEvent<>(type, name, EventType.INVALIDATE, null, event::markAsProcessed));
        }
    }
}
