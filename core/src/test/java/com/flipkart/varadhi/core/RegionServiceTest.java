package com.flipkart.varadhi.core;

import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.db.VaradhiMetaStore;
import com.flipkart.varadhi.db.ZKMetaStore;
import com.flipkart.varadhi.entities.Region;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.RegionStatus;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RegionServiceTest {

    private TestingServer zkServer;
    private CuratorFramework zkCurator;
    private RegionService regionService;

    @BeforeEach
    void setUp() throws Exception {
        zkServer = new TestingServer();
        zkCurator = CuratorFrameworkFactory.newClient(
            zkServer.getConnectString(),
            new ExponentialBackoffRetry(1000, 1)
        );
        zkCurator.start();
        VaradhiMetaStore metaStore = new VaradhiMetaStore(new ZKMetaStore(zkCurator));
        regionService = new RegionService(metaStore.regions());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (zkCurator != null) {
            zkCurator.close();
        }
        if (zkServer != null) {
            zkServer.close();
        }
    }

    @Test
    void createGetGetAllExistsDelete() {
        RegionName name = new RegionName("dc1");
        Region region = Region.of(name, RegionStatus.AVAILABLE);

        Region created = regionService.createRegion(region);
        assertEquals(region, created);

        assertTrue(regionService.regionExists(name.value()));
        Region loaded = regionService.getRegion(name.value());
        assertEquals(region.getName(), loaded.getName());
        assertEquals(region.getStatus(), loaded.getStatus());

        List<Region> all = regionService.getRegions();
        assertEquals(1, all.size());
        assertEquals(region.getName(), all.get(0).getName());

        regionService.deleteRegion(name.value());
        assertFalse(regionService.regionExists(name.value()));
    }

    @Test
    void getRegions_whenStoreHasMultipleRegions_returnsAllOrderedByName() {
        regionService.createRegion(Region.of(new RegionName("dc-c"), RegionStatus.AVAILABLE));
        regionService.createRegion(Region.of(new RegionName("dc-a"), RegionStatus.UNAVAILABLE));
        regionService.createRegion(Region.of(new RegionName("dc-b"), RegionStatus.PRODUCE_UNAVAILABLE));

        List<Region> all = regionService.getRegions();
        assertEquals(3, all.size());
        List<String> names = all.stream().map(Region::getName).sorted().toList();
        assertEquals(List.of("dc-a", "dc-b", "dc-c"), names);
    }

    @Test
    void getRegion_and_regionExists_whenStoreHasMultipleRegions() {
        regionService.createRegion(Region.of(new RegionName("r-east"), RegionStatus.AVAILABLE));
        regionService.createRegion(Region.of(new RegionName("r-west"), RegionStatus.CONSUME_UNAVAILABLE));

        assertTrue(regionService.regionExists("r-east"));
        assertTrue(regionService.regionExists("r-west"));
        assertFalse(regionService.regionExists("r-unknown"));

        assertEquals(RegionStatus.AVAILABLE, regionService.getRegion("r-east").getStatus());
        assertEquals(RegionStatus.CONSUME_UNAVAILABLE, regionService.getRegion("r-west").getStatus());
        assertEquals("r-east", regionService.getRegion("r-east").getName());
    }

    @Test
    void getRegion_whenMissingButStoreNotEmpty_throwsResourceNotFoundException() {
        regionService.createRegion(Region.of(new RegionName("present"), RegionStatus.AVAILABLE));

        assertThrows(ResourceNotFoundException.class, () -> regionService.getRegion("absent"));
    }

    @Test
    void deleteRegion_whenMultipleRegions_removesOnlyTarget() {
        regionService.createRegion(Region.of(new RegionName("keep-1"), RegionStatus.AVAILABLE));
        regionService.createRegion(Region.of(new RegionName("remove-me"), RegionStatus.AVAILABLE));
        regionService.createRegion(Region.of(new RegionName("keep-2"), RegionStatus.UNAVAILABLE));

        regionService.deleteRegion("remove-me");

        assertFalse(regionService.regionExists("remove-me"));
        assertTrue(regionService.regionExists("keep-1"));
        assertTrue(regionService.regionExists("keep-2"));
        List<String> remaining = regionService.getRegions().stream().map(Region::getName).sorted().toList();
        assertEquals(List.of("keep-1", "keep-2"), remaining);
    }

    @Test
    void deleteRegion_whenMissingButStoreNotEmpty_throwsResourceNotFoundException() {
        regionService.createRegion(Region.of(new RegionName("only"), RegionStatus.AVAILABLE));

        assertThrows(ResourceNotFoundException.class, () -> regionService.deleteRegion("ghost"));
    }
}
