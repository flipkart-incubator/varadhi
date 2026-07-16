package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrepareTargetTest {

    @Test
    void parse_failover_returnsRegionTarget() {
        PrepareTarget target = PrepareTarget.parse(TransitionType.TOPIC_FAILOVER, "region-b");
        assertTrue(target instanceof PrepareTarget.RegionTarget);
        assertEquals(new RegionName("region-b"), ((PrepareTarget.RegionTarget)target).region());
    }

    @Test
    void parse_storageMigration_returnsStorageTopicTarget() {
        PrepareTarget target = PrepareTarget.parse(TransitionType.STORAGE_MIGRATION, "7");
        assertTrue(target instanceof PrepareTarget.StorageTopicTarget);
        assertEquals(7, ((PrepareTarget.StorageTopicTarget)target).storageTopicId());
    }

    @Test
    void parse_rejectsBlankTarget() {
        assertThrows(IllegalArgumentException.class, () -> PrepareTarget.parse(TransitionType.TOPIC_FAILOVER, "  "));
        assertThrows(IllegalArgumentException.class, () -> PrepareTarget.parse(TransitionType.TOPIC_FAILOVER, null));
    }

    @Test
    void parse_rejectsNonNumericStorageTarget() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> PrepareTarget.parse(TransitionType.STORAGE_MIGRATION, "region-b")
        );
        assertTrue(ex.getMessage().contains("storage-topic id"));
    }

    @Test
    void parse_rejectsBlankRegionName() {
        assertThrows(IllegalArgumentException.class, () -> PrepareTarget.parse(TransitionType.TOPIC_FAILOVER, ""));
    }
}
