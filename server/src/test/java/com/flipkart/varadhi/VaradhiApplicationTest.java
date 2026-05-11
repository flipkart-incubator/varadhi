package com.flipkart.varadhi;

import com.flipkart.varadhi.common.exceptions.InvalidConfigException;
import com.flipkart.varadhi.core.cluster.ComponentKind;
import com.flipkart.varadhi.core.cluster.MemberInfo;
import com.flipkart.varadhi.core.cluster.NodeCapacity;
import com.flipkart.varadhi.entities.Region;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.RegionStatus;
import com.flipkart.varadhi.entities.web.RegionCreateRequest;
import com.flipkart.varadhi.spi.mock.InMemoryMetaStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class VaradhiApplicationTest {

    private static final RegionName KNOWN_REGION = new RegionName("dc1");
    private static final RegionName UNKNOWN_REGION_NAME = new RegionName("dc2");

    private InMemoryMetaStore metaStore;

    @BeforeEach
    void setUp() {
        metaStore = new InMemoryMetaStore();
    }

    // --- bootstrapping (empty store) ---

    @Test
    void validateMemberRegion_emptyStore_bootstrapRegion_passes() {
        MemberInfo member = memberWith(RegionName.BOOTSTRAP_REGION);
        assertDoesNotThrow(() -> VaradhiApplication.validateMemberRegion(member, getAllRegions()));
    }

    @Test
    void validateMemberRegion_emptyStore_nonBootstrapRegion_throwsInvalidConfig() {
        MemberInfo member = memberWith(UNKNOWN_REGION_NAME);

        InvalidConfigException ex = assertThrows(
            InvalidConfigException.class,
            () -> VaradhiApplication.validateMemberRegion(member, getAllRegions())
        );
        assertTrue(
            ex.getMessage().contains(RegionName.BOOTSTRAP_REGION.value()),
            "Error message should tell operator to use the bootstrap sentinel"
        );
        assertTrue(ex.getMessage().contains("bootstrapping"), "Error message should mention bootstrapping context");
    }

    // --- non-empty store ---

    @Test
    void validateMemberRegion_storeHasRegions_configuredRegionExists_passes() {
        registerRegion(KNOWN_REGION);
        MemberInfo member = memberWith(KNOWN_REGION);

        assertDoesNotThrow(() -> VaradhiApplication.validateMemberRegion(member, getAllRegions()));
    }

    @Test
    void validateMemberRegion_storeHasRegions_configuredRegionMissing_throwsInvalidConfig() {
        registerRegion(KNOWN_REGION);
        MemberInfo member = memberWith(UNKNOWN_REGION_NAME);

        InvalidConfigException ex = assertThrows(
            InvalidConfigException.class,
            () -> VaradhiApplication.validateMemberRegion(member, getAllRegions())
        );
        assertTrue(
            ex.getMessage().contains(UNKNOWN_REGION_NAME.value()),
            "Error message should name the misconfigured region"
        );
        assertTrue(
            ex.getMessage().contains(KNOWN_REGION.value()),
            "Error message should list the known regions so operator can fix config"
        );
    }

    @Test
    void validateMemberRegion_storeHasRegions_bootstrapSentinelNotSpecial_throwsInvalidConfig() {
        // Once regions are provisioned the bootstrap sentinel is no longer a free pass
        registerRegion(KNOWN_REGION);
        MemberInfo member = memberWith(RegionName.BOOTSTRAP_REGION);

        InvalidConfigException ex = assertThrows(
            InvalidConfigException.class,
            () -> VaradhiApplication.validateMemberRegion(member, getAllRegions())
        );
        assertTrue(ex.getMessage().contains(RegionName.BOOTSTRAP_REGION.value()));
    }

    @Test
    void validateMemberRegion_storeHasMultipleRegions_configuredRegionExists_passes() {
        registerRegion(KNOWN_REGION);
        registerRegion(new RegionName("dc3"));
        MemberInfo member = memberWith(KNOWN_REGION);

        assertDoesNotThrow(() -> VaradhiApplication.validateMemberRegion(member, getAllRegions()));
    }

    // --- helpers ---

    private void registerRegion(RegionName name) {
        metaStore.regions().create(new RegionCreateRequest(name.value(), RegionStatus.AVAILABLE).toRegion());
    }

    private List<Region> getAllRegions() {
        return metaStore.regions().getAll();
    }

    private static MemberInfo memberWith(RegionName region) {
        return new MemberInfo(
            "test-host",
            "127.0.0.1",
            0,
            new ComponentKind[] {ComponentKind.Server},
            new NodeCapacity(),
            region
        );
    }
}
