package com.flipkart.varadhi.entities;

import java.util.Optional;

/**
 * Resolves {@link ProduceKey} from a {@link VaradhiTopic} and request region.
 *
 * <p>Two paths: un-gated {@link #resolve} (cache warm / PREPARE while {@link TopicState#Fenced})
 * vs gated {@link #resolveForProduce} (HTTP produce, requires {@link TopicState#isProduceAllowed()}).
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
        Optional<ProduceConfig> config = topic.getProduceConfig(region);
        SegmentedStorageTopic storageTopic = topic.getSegmentedStorageTopic();
        if (config.isEmpty() || storageTopic == null) {
            return Optional.empty();
        }
        RegionName produceRegion = config.get().getFailoverRegion().orElse(region);
        if (topic.getProduceConfig(produceRegion).isEmpty()) {
            return Optional.empty();
        }
        StorageTopic segment = storageTopic.getTopicToProduce();
        return Optional.of(new ProduceKey(VaradhiTopicName.parse(topic.getName()), produceRegion, segment.getId()));
    }

    /**
     * Resolves produce routing when produce is allowed for {@code region}.
     */
    public static Optional<ProduceKey> resolveForProduce(VaradhiTopic topic, RegionName region) {
        Optional<ProduceConfig> config = topic.getProduceConfig(region);
        if (config.isEmpty() || !config.get().state().isProduceAllowed()) {
            return Optional.empty();
        }
        return resolve(topic, region);
    }
}
