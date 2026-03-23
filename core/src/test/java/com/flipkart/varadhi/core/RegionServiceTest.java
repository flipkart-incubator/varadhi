package com.flipkart.varadhi.core;

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
}
