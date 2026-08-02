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
     * <p>Does <em>not</em> check {@link TopicState#isProduceAllowed()}.
     */
    public static Optional<ProduceKey> resolve(VaradhiTopic topic, RegionName region) {
        ProduceConfig config = topic.getProduceConfigs().get(region);
        SegmentedStorageTopic storageTopic = topic.getSegmentedStorageTopic();
        if (config == null || storageTopic == null) {
            return Optional.empty();
        }
        RegionName produceRegion = config.failOverRegion() != null ? config.failOverRegion() : region;
        if (!topic.getProduceConfigs().containsKey(produceRegion)) {
            return Optional.empty();
        }
        StorageTopic segment = storageTopic.getTopicToProduce();
        return Optional.of(new ProduceKey(VaradhiTopicName.parse(topic.getName()), produceRegion, segment.getId()));
    }

    /**
     * Resolves produce routing when produce is allowed for {@code region}.
     */
    public static Optional<ProduceKey> resolveForProduce(VaradhiTopic topic, RegionName region) {
        ProduceConfig config = topic.getProduceConfigs().get(region);
        if (config == null || !config.state().isProduceAllowed()) {
            return Optional.empty();
        }
        return resolve(topic, region);
    }
}
