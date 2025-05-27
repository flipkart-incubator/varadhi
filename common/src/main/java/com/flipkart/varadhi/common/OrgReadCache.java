package com.flipkart.varadhi.common;

import com.flipkart.varadhi.common.events.ResourceEvent;
import com.flipkart.varadhi.common.events.EventType;
import com.flipkart.varadhi.entities.OrgDetails;
import com.flipkart.varadhi.entities.ResourceType;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

@Slf4j
public class OrgReadCache extends ResourceReadCache<OrgDetails> {

    public OrgReadCache(ResourceType resourceType, Supplier<List<OrgDetails>> resourceLoader) {
        super(resourceType, resourceLoader);
    }

    @Override
    public void onChange(ResourceEvent<? extends OrgDetails> event) {
        String entityName = event.resourceName();
        EventType operation = event.operation();

        if (operation == EventType.UPSERT) {
            OrgDetails entity = event.resource();
            if (entity != null) {
                resource.compute(entityName, (key, existingEntity) -> {
                    if (existingEntity == null || (event.version() > existingEntity.getOrg().getVersion() || event
                                                                                                                  .resource()
                                                                                                                  .getOrgFilters()
                                                                                                                  .getVersion()
                                                                                                             > existingEntity.getOrgFilters()
                                                                                                                             .getVersion())) {
                        log.info("Updating OrgDetails: {}", entityName);
                        return entity;
                    }
                    return existingEntity;
                });
            }
        } else if (operation == EventType.INVALIDATE) {
            // Custom invalidation logic for OrgDetails
            log.info("Invalidating OrgDetails: {}", entityName);
            resource.remove(entityName);
        }
    }

    public static Future<OrgReadCache> createOrgReadCache(
        ResourceType resourceType,
        Supplier<List<OrgDetails>> entityLoader,
        Vertx vertx
    ) {
        Objects.requireNonNull(resourceType, "Resource type cannot be null");
        Objects.requireNonNull(entityLoader, "Entity loader cannot be null");
        Objects.requireNonNull(vertx, "Vertx instance cannot be null");

        OrgReadCache cache = new OrgReadCache(resourceType, entityLoader);

        return cache.preload(vertx).map(v -> cache);
    }
}
