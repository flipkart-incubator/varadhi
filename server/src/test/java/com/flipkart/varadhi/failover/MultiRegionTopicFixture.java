package com.flipkart.varadhi.failover;

import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.SegmentedStorageTopic;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.VaradhiTopic;
import lombok.EqualsAndHashCode;

/**
 * Test fixture that builds a <b>multi-region</b> {@link VaradhiTopic} — one
 * {@link SegmentedStorageTopic} per region, all initially {@code Producing}.
 *
 * <p>The normal create path ({@code VaradhiTopicFactory.planDeployment}) only provisions the single
 * deployment region, so a topic produced that way cannot be failed over (failover validation requires
 * both a source and a target region to exist). This fixture seeds the multi-region shape directly so
 * integration / E2E tests can exercise the failover stages until the factory grows native multi-region
 * provisioning.
 */
public final class MultiRegionTopicFixture {

    /** Minimal concrete {@link StorageTopic} for tests (no real messaging stack). */
    @EqualsAndHashCode (callSuper = true)
    public static final class FixtureStorageTopic extends StorageTopic {
        public FixtureStorageTopic(String name) {
            super(0, name);
        }
    }

    private MultiRegionTopicFixture() {
    }

    /**
     * Builds a topic {@code <project>.<topicName>} with a storage segment in each of {@code regions}.
     *
     * @param regions at least two regions (a failover needs a distinct source and target)
     */
    public static VaradhiTopic create(String project, String topicName, String... regions) {
        if (regions.length < 2) {
            throw new IllegalArgumentException("a multi-region topic needs at least 2 regions");
        }
        VaradhiTopic topic = VaradhiTopic.of(
            project,
            topicName,
            false,
            new TopicCapacityPolicy(100, 400, 2, 2),
            LifecycleStatus.ActionCode.SYSTEM_ACTION
        );
        for (String region : regions) {
            topic.addInternalTopic(
                region,
                SegmentedStorageTopic.of(new FixtureStorageTopic(topic.getName() + "." + region))
            );
        }
        return topic;
    }
}
