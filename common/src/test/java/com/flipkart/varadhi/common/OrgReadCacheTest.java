package com.flipkart.varadhi.common;

import com.flipkart.varadhi.common.events.EventType;
import com.flipkart.varadhi.common.events.ResourceEvent;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.filters.Condition;
import com.flipkart.varadhi.entities.filters.OrgFilters;
import com.flipkart.varadhi.entities.filters.StringConditions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

class OrgReadCacheTest {

    private OrgReadCache orgReadCache;

    @BeforeEach
    void setUp() {
        orgReadCache = new OrgReadCache(ResourceType.ORG, null);
    }

    @Test
    void testInitializeWithSupplier() {
        Org org = new Org("test-org", 1);
        OrgFilters filters = new OrgFilters(1, new HashMap<>());
        OrgDetails details = new OrgDetails(org, filters);
        Supplier<List<OrgDetails>> supplier = () -> List.of(details);
        OrgReadCache cache = new OrgReadCache(ResourceType.ORG, supplier);
        ResourceEvent<OrgDetails> orgEvent = new ResourceEvent<>(
            ResourceType.ORG,
            "test-org",
            EventType.UPSERT,
            details,
            1,
            () -> {
            }
        );
        cache.onChange(orgEvent);
        Optional<OrgDetails> result = cache.get("test-org");
        assertTrue(result.isPresent());
        assertEquals("test-org", result.get().getOrg().getName());
    }

    @Test
    void testOnChangeUpsertNew() {
        Org org = new Org("test-org", 1);
        OrgFilters filters = new OrgFilters(1, new HashMap<>());
        OrgDetails details = new OrgDetails(org, filters);

        ResourceEvent<OrgDetails> event = new ResourceEvent<>(
            ResourceType.ORG,
            "test-org",
            EventType.UPSERT,
            details,
            1,
            () -> {
            }
        );

        orgReadCache.onChange(event);

        Optional<OrgDetails> result = orgReadCache.get("test-org");
        assertTrue(result.isPresent());
        assertEquals(1, result.get().getOrg().getVersion());
    }

    @Test
    void testOnChangeUpsertUpdateOrgVersion() {
        Org initialOrg = new Org("test-org", 1);
        OrgFilters initialFilters = new OrgFilters(2, new HashMap<>());
        OrgDetails initialDetails = new OrgDetails(initialOrg, initialFilters);

        ResourceEvent<OrgDetails> initialEvent = new ResourceEvent<>(
            ResourceType.ORG,
            "test-org",
            EventType.UPSERT,
            initialDetails,
            1,
            () -> {
            }
        );
        orgReadCache.onChange(initialEvent);
        Org updatedOrg = new Org("test-org", 2);
        OrgFilters sameFilters = new OrgFilters(3, new HashMap<>());
        OrgDetails updatedDetails = new OrgDetails(updatedOrg, sameFilters);

        ResourceEvent<OrgDetails> updateEvent = new ResourceEvent<>(
            ResourceType.ORG,
            "test-org",
            EventType.UPSERT,
            updatedDetails,
            2,
            () -> {
            }
        );

        orgReadCache.onChange(updateEvent);

        Optional<OrgDetails> result = orgReadCache.get("test-org");
        assertTrue(result.isPresent());
        assertEquals(2, result.get().getOrg().getVersion());
        assertEquals(3, result.get().getOrgFilters().getVersion());
    }

    @Test
    void testOnChangeUpsertUpdateFiltersVersion() {
        Org initialOrg = new Org("test-org", 2);
        OrgFilters initialFilters = new OrgFilters(1, new HashMap<>());
        OrgDetails initialDetails = new OrgDetails(initialOrg, initialFilters);

        ResourceEvent<OrgDetails> initialEvent = new ResourceEvent<>(
            ResourceType.ORG,
            "test-org",
            EventType.UPSERT,
            initialDetails,
            1,
            () -> {
            }
        );
        orgReadCache.onChange(initialEvent);

        Org sameOrg = new Org("test-org", 2);
        Map<String, Condition> conditions = new HashMap<>();
        conditions.put("key", new StringConditions.InCondition("headerKey", List.of("value")));
        OrgFilters updatedFilters = new OrgFilters(2, conditions);
        OrgDetails updatedDetails = new OrgDetails(sameOrg, updatedFilters);

        ResourceEvent<OrgDetails> updateEvent = new ResourceEvent<>(
            ResourceType.ORG,
            "test-org",
            EventType.UPSERT,
            updatedDetails,
            2,
            () -> {
            }
        );

        orgReadCache.onChange(updateEvent);

        Optional<OrgDetails> result = orgReadCache.get("test-org");
        assertTrue(result.isPresent());
        assertEquals(2, result.get().getOrg().getVersion());
        assertEquals(2, result.get().getOrgFilters().getVersion());
        assertEquals(1, result.get().getOrgFilters().getFilters().size());
    }

    @Test
    void testOnChangeInvalidate() {
        Org org = new Org("test-org", 1);
        OrgFilters filters = new OrgFilters(1, new HashMap<>());
        OrgDetails details = new OrgDetails(org, filters);

        ResourceEvent<OrgDetails> upsertEvent = new ResourceEvent<>(
            ResourceType.ORG,
            "test-org",
            EventType.UPSERT,
            details,
            1,
            () -> {
            }
        );
        orgReadCache.onChange(upsertEvent);

        assertTrue(orgReadCache.get("test-org").isPresent());

        ResourceEvent<OrgDetails> invalidateEvent = new ResourceEvent<>(
            ResourceType.ORG,
            "test-org",
            EventType.INVALIDATE,
            null,
            2,
            () -> {
            }
        );

        orgReadCache.onChange(invalidateEvent);

        assertFalse(orgReadCache.get("test-org").isPresent());
    }

    @Test
    void testMergeWithNullPrevious() {
        Org org = new Org("test-org", 1);
        OrgFilters filters = new OrgFilters(1, new HashMap<>());
        OrgDetails details = new OrgDetails(org, filters);

        OrgDetails result = OrgReadCache.merge(null, details);

        assertNotNull(result);
        assertEquals(1, result.getOrg().getVersion());
    }

    @Test
    void testMergeSelectiveUpdates() {
        Org prevOrg = new Org("test-org", 1);
        OrgFilters prevFilters = new OrgFilters(3, new HashMap<>());
        OrgDetails prevDetails = new OrgDetails(prevOrg, prevFilters);

        Org newOrg = new Org("test-org", 2);
        OrgFilters newFilters = new OrgFilters(2, new HashMap<>());
        OrgDetails newDetails = new OrgDetails(newOrg, newFilters);

        OrgDetails result = OrgReadCache.merge(prevDetails, newDetails);

        assertEquals(2, result.getOrg().getVersion()); // Takes higher org version
        assertEquals(3, result.getOrgFilters().getVersion()); // Keeps higher filter version
    }
}
