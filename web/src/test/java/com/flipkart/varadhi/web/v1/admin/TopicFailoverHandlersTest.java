package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.core.RegionService;
import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.VaradhiTopicService;
import com.flipkart.varadhi.core.cluster.controller.ControllerApi;
import com.flipkart.varadhi.entities.ProduceConfig;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.SegmentedStorageTopic;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.VaradhiTopicTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TopicFailoverHandlersTest {

    private final RegionService regionService = mock(RegionService.class);
    private final TopicFailoverHandlers handlers = new TopicFailoverHandlers(
        mock(ControllerApi.class),
        mockCache(),
        mock(VaradhiTopicService.class),
        regionService,
        RegionName.of("r1")
    );

    @BeforeEach
    void regionsExist() {
        when(regionService.regionExists("r1")).thenReturn(true);
        when(regionService.regionExists("r2")).thenReturn(true);
    }

    @Test
    void acceptsProducingSourceAndConfiguredTarget() {
        assertDoesNotThrow(
            () -> handlers.validateFailoverRegions(
                topic(ProduceConfig.producing(), ProduceConfig.blocked()),
                RegionName.of("r1"),
                RegionName.of("r2")
            )
        );
    }

    @Test
    void rejectsMissingOrIdenticalRegions() {
        VaradhiTopic topic = topic(ProduceConfig.producing(), ProduceConfig.blocked());
        IllegalArgumentException missing = assertThrows(
            IllegalArgumentException.class,
            () -> handlers.validateFailoverRegions(topic, RegionName.of("r1"), null)
        );
        assertTrue(missing.getMessage().contains("required"));
        IllegalArgumentException same = assertThrows(
            IllegalArgumentException.class,
            () -> handlers.validateFailoverRegions(topic, RegionName.of("r1"), RegionName.of("r1"))
        );
        assertTrue(same.getMessage().contains("must differ"));
    }

    @Test
    void rejectsUnregisteredRegion() {
        when(regionService.regionExists("r2")).thenReturn(false);
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> handlers.validateFailoverRegions(
                topic(ProduceConfig.producing(), ProduceConfig.blocked()),
                RegionName.of("r1"),
                RegionName.of("r2")
            )
        );
        assertTrue(ex.getMessage().contains("not registered"));
    }

    @Test
    void rejectsTopicMissingTargetConfig() {
        VaradhiTopic topic = VaradhiTopicTestUtils.getNewTopic(Map.of(RegionName.of("r1"), ProduceConfig.producing()));
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> handlers.validateFailoverRegions(topic, RegionName.of("r1"), RegionName.of("r2"))
        );
        assertTrue(ex.getMessage().contains("not configured for targetRegion"));
    }

    @Test
    void rejectsTopicWithoutStorage() {
        VaradhiTopic topic = topic(ProduceConfig.producing(), ProduceConfig.blocked()).with(
            new SegmentedStorageTopic(new StorageTopic[0])
        );
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> handlers.validateFailoverRegions(topic, RegionName.of("r1"), RegionName.of("r2"))
        );
        assertTrue(ex.getMessage().contains("no storage topic"));
    }

    @Test
    void rejectsSourceThatIsNotProducing() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> handlers.validateFailoverRegions(
                topic(ProduceConfig.blocked(), ProduceConfig.producing()),
                RegionName.of("r1"),
                RegionName.of("r2")
            )
        );
        assertTrue(ex.getMessage().contains("not producing"));
    }

    private static VaradhiTopic topic(ProduceConfig source, ProduceConfig target) {
        return VaradhiTopicTestUtils.getNewTopic(Map.of(RegionName.of("r1"), source, RegionName.of("r2"), target));
    }

    @SuppressWarnings ("unchecked")
    private static ResourceReadCache<Resource.EntityResource<Project>> mockCache() {
        return mock(ResourceReadCache.class);
    }
}
