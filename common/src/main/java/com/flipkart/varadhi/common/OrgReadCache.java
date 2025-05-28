package com.flipkart.varadhi.common;

import com.flipkart.varadhi.common.events.ResourceEvent;
import com.flipkart.varadhi.common.events.EventType;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.OrgDetails;
import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.entities.filters.OrgFilters;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.function.Supplier;

@Slf4j
public class OrgReadCache extends ResourceReadCache<OrgDetails> {

    public OrgReadCache(ResourceType resourceType, Supplier<List<OrgDetails>> resourceLoader) {
        super(resourceType, resourceLoader);
    }

    @Override
    public void onChange(ResourceEvent<? extends OrgDetails> event) {
        String orgName = event.resourceName();
        EventType operation = event.operation();

        if (operation == EventType.UPSERT) {
            resource.merge(orgName, event.resource(), OrgReadCache::merge);
        } else if (operation == EventType.INVALIDATE) {
            log.info("Invalidating OrgDetails: {}", orgName);
            resource.remove(orgName);
        }
    }

    static OrgDetails merge(OrgDetails previous, OrgDetails value) {
        if (previous == null) {
            log.info("New Org {}, version: {}", value.getOrg().getName(), value.getOrg().getVersion());
            return value;
        }

        Org org = previous.getOrg();
        OrgFilters filters = previous.getOrgFilters();

        if (value.getOrg().getVersion() > previous.getOrg().getVersion()) {
            org = value.getOrg();
            log.info("Updating Org: {}, version: {}", value.getOrg().getName(), value.getOrg().getName());
        }
        if (value.getOrgFilters().getVersion() > previous.getOrgFilters().getVersion()) {
            filters = value.getOrgFilters();
            log.info(
                "Updating OrgFilters: {}, version: {}",
                value.getOrg().getName(),
                value.getOrgFilters().getVersion()
            );
        }
        return new OrgDetails(org, filters);
    }
}
