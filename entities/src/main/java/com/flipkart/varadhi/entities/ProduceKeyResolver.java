package com.flipkart.varadhi.entities;

import java.util.Optional;

/**
 * Resolves {@link ProduceKey} from a {@link VaradhiTopic} and request region.
 *
 * <p>{@link #resolve(VaradhiTopic, RegionName)} is ungated (cache warm / PREPARE while
 * {@link TopicState#Fenced}). Pass {@code onlyProduceAllowed = true} for HTTP produce.
 */
public final class ProduceKeyResolver {

    private ProduceKeyResolver() {
    }

    /**
     * Resolves produce routing for {@code region}: topic + produce region + storage segment id.
     *
     * <p>Uses the <em>active</em> storage segment ({@link SegmentedStorageTopic#getTopicToProduce()}).
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
        Optional<ProduceConfig> config = topic.getProduceConfig(region);
        SegmentedStorageTopic storageTopic = topic.getSegmentedStorageTopic();
        if (config.isEmpty() || storageTopic == null) {
            return Optional.empty();
        }
        ProduceConfig produceConfig = config.get();
        if (onlyProduceAllowed && !produceConfig.state().isProduceAllowed()) {
            return Optional.empty();
        }
        RegionName produceRegion = produceConfig.failOverRegion().orElse(region);
        if (topic.getProduceConfig(produceRegion).isEmpty()) {
            return Optional.empty();
        }
        StorageTopic segment = storageTopic.getTopicToProduce();
        return Optional.of(new ProduceKey(VaradhiTopicName.parse(topic.getName()), produceRegion, segment.getId()));
    }
}
