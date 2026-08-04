package com.flipkart.varadhi.entities;

import java.util.Optional;

/**
 * Resolves {@link ProduceKey} / storage segment from a {@link VaradhiTopic} and request region.
 *
 * <p>{@link #resolve(VaradhiTopic, RegionName)} is ungated (cache warm / PREPARE while
 * {@link TopicState#Fenced}). Pass {@code onlyProduceAllowed = true} for HTTP produce.
 */
public final class TopicResolver {

    private TopicResolver() {
    }

    /**
     * Resolves produce routing for {@code region}: topic + produce region + storage segment id.
     *
     * <p>Uses the active storage segment ({@link ProduceConfig#getProduceIdx()} on the resolved produce region).
     * Correct for steady-state produce and topic failover after SWITCH. Storage-migration PREPARE
     * must warm an explicit segment id from
     * {@link com.flipkart.varadhi.entities.cluster.failover.TransitionEvent.Target.StorageTopic#storageTopicId()},
     * not this resolver alone.
     *
     * <p>Does <em>not</em> check {@link TopicState#isProduceAllowed()}.
     */
    public static Optional<ProduceKey> resolve(VaradhiTopic topic, RegionName region) {
        return resolve(topic, region, false);
    }

    /**
     * Resolves produce routing for {@code region}.
     *
     * @param onlyProduceAllowed when {@code true}, returns empty unless {@link TopicState#isProduceAllowed()}
     *                           for the deployed region's {@link ProduceConfig}
     */
    public static Optional<ProduceKey> resolve(VaradhiTopic topic, RegionName region, boolean onlyProduceAllowed) {
        return resolveProduceTarget(topic, region, onlyProduceAllowed).map(
            target -> new ProduceKey(
                VaradhiTopicName.parse(topic.getName()),
                target.produceRegion(),
                target.segment().getId()
            )
        );
    }

    /**
     * Resolves the storage segment for {@code region}'s produce path.
     *
     * @param onlyProduceAllowed when {@code true}, returns empty unless {@link TopicState#isProduceAllowed()}
     *                           for the deployed region's {@link ProduceConfig}
     */
    public static Optional<StorageTopic> resolveStorageTopic(
        VaradhiTopic topic,
        RegionName region,
        boolean onlyProduceAllowed
    ) {
        return resolveProduceTarget(topic, region, onlyProduceAllowed).map(ProduceTarget::segment);
    }

    private static Optional<ProduceTarget> resolveProduceTarget(
        VaradhiTopic topic,
        RegionName region,
        boolean onlyProduceAllowed
    ) {
        Optional<ProduceConfig> config = topic.getProduceConfig(region);
        if (config.isEmpty()) {
            return Optional.empty();
        }
        ProduceConfig produceConfig = config.get();
        if (onlyProduceAllowed && !produceConfig.getState().isProduceAllowed()) {
            return Optional.empty();
        }
        RegionName produceRegion = produceConfig.getFailOverRegion().orElse(region);
        return topic.getProduceConfig(produceRegion)
                    .map(
                        produceRegionConfig -> new ProduceTarget(
                            produceRegion,
                            topic.getSegmentedStorageTopic().getTopicAtIndex(produceRegionConfig.getProduceIdx())
                        )
                    );
    }

    private record ProduceTarget(RegionName produceRegion, StorageTopic segment) {
    }
}
