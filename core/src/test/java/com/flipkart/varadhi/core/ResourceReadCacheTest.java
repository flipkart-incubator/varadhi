package com.flipkart.varadhi.core;

import com.flipkart.varadhi.core.cluster.events.EventType;
import com.flipkart.varadhi.core.cluster.events.ResourceEvent;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ResourceReadCacheTest {

    @Test
    void addOnInvalidate_NotifiesAfterCacheRemovesEntry() {
        ResourceReadCache<Resource> cache = new ResourceReadCache<>(ResourceType.TOPIC, List::of);
        AtomicReference<String> invalidated = new AtomicReference<>();
        cache.addOnInvalidate(invalidated::set);

        cache.onChange(invalidateEvent("topic-a"));

        assertEquals("topic-a", invalidated.get());
        assertTrue(cache.get("topic-a").isEmpty());
    }

    @Test
    void addOnInvalidate_NotCalledOnUpsert() {
        ResourceReadCache<Resource> cache = new ResourceReadCache<>(ResourceType.TOPIC, List::of);
        AtomicReference<String> invalidated = new AtomicReference<>("unset");
        cache.addOnInvalidate(invalidated::set);

        cache.onChange(
            new ResourceEvent<>(
                ResourceType.TOPIC,
                "topic-a",
                EventType.UPSERT,
                new Resource("topic-a", 1, ResourceType.TOPIC),
                1,
                null
            )
        );

        assertEquals("unset", invalidated.get());
        assertTrue(cache.get("topic-a").isPresent());
    }

    private static ResourceEvent<Resource> invalidateEvent(String resourceName) {
        return new ResourceEvent<>(ResourceType.TOPIC, resourceName, EventType.INVALIDATE, null, 1, null);
    }
}
