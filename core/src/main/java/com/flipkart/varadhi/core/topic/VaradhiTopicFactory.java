package com.flipkart.varadhi.core.topic;

import com.flipkart.varadhi.entities.InternalQueueCategory;
import com.flipkart.varadhi.entities.MessageSizeProfile;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.RateLimiterMode;
import com.flipkart.varadhi.entities.SegmentedStorageTopic;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.web.TopicResource;
import com.flipkart.varadhi.spi.services.StorageTopicFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Factory class for creating Varadhi topics.
 */
public class VaradhiTopicFactory {

    private static final Logger log = LoggerFactory.getLogger(VaradhiTopicFactory.class);
    private static final double PER_REGION_WEIGHT_SUM_EPSILON = 1e-9;

    private final StorageTopicFactory<? extends StorageTopic> topicFactory;
    private final TopicCapacityPolicy defaultTopicCapacityPolicy;
    private final RateLimiterMode defaultRateLimiterMode;

    /**
     * TODO: This field is currently used to provide a default value for the primary region of the topic being created.
     * Ideally, this should be derived from the TopicResource as part of the Regional/HA/BCP-DR policy.
     * Since those policies are not yet available, the deploymentRegion is used as a global single primary
     * topic region as a temporary solution.
     */
    private final String deploymentRegion;

    /**
     * Constructs a new VaradhiTopicFactory instance.
     *
     * @param topicFactory               the factory for creating storage topics
     * @param deploymentRegion           the default primary region for the topic
     * @param defaultTopicCapacityPolicy the default capacity policy for the topic
     */
    public VaradhiTopicFactory(
        StorageTopicFactory<? extends StorageTopic> topicFactory,
        String deploymentRegion,
        TopicCapacityPolicy defaultTopicCapacityPolicy
    ) {
        this(topicFactory, deploymentRegion, defaultTopicCapacityPolicy, RateLimiterMode.disabled);
    }

    public VaradhiTopicFactory(
        StorageTopicFactory<? extends StorageTopic> topicFactory,
        String deploymentRegion,
        TopicCapacityPolicy defaultTopicCapacityPolicy,
        RateLimiterMode defaultRateLimiterMode
    ) {
        this.topicFactory = topicFactory;
        this.defaultTopicCapacityPolicy = defaultTopicCapacityPolicy;
        this.deploymentRegion = deploymentRegion;
        this.defaultRateLimiterMode = defaultRateLimiterMode;
    }

    /**
     * Creates a VaradhiTopic instance based on the provided project, topic resource, and topic category.
     *
     * @param project       the project associated with the topic
     * @param topicResource the topic model containing topic details
     * @param category      the topic category (e.g. {@link VaradhiTopic.TopicCategory#TOPIC} or {@link VaradhiTopic.TopicCategory#QUEUE})
     *
     * @return the created VaradhiTopic instance
     */
    public VaradhiTopic get(Project project, TopicResource topicResource, VaradhiTopic.TopicCategory category) {
        topicResource.setCapacity(Optional.ofNullable(topicResource.getCapacity()).orElse(defaultTopicCapacityPolicy));

        topicResource.setRateLimiterMode(
            Optional.ofNullable(topicResource.getRateLimiterMode()).orElse(defaultRateLimiterMode)
        );

        MessageSizeProfile messageSizeProfile = topicResource.getMessageSizeProfile();
        if (messageSizeProfile != null) {
            TopicCapacityPolicy capacity = topicResource.getCapacity();
            if (!capacity.isConsistentWith(messageSizeProfile)) {
                throw new IllegalArgumentException(
                    String.format(
                        "throughputKBps (%d) is below qps (%d) x maxMsgSizeBytes (%d)",
                        capacity.getThroughputKBps(),
                        capacity.getQps(),
                        messageSizeProfile.getMaxMsgSizeBytes()
                    )
                );
            }
            warnIfCapacityTightForAverageMessageSizes(capacity, messageSizeProfile);
        }

        topicResource.setPerRegionQuotaWeights(
            resolvePerRegionQuotaWeights(topicResource.getPerRegionQuotaWeights(), Set.of(deploymentRegion))
        );

        VaradhiTopic varadhiTopic = topicResource.toVaradhiTopic(category);
        planDeployment(project, varadhiTopic);
        return varadhiTopic;
    }

    /**
     * Plans the deployment of the VaradhiTopic by creating and associating an internal storage topic.
     *
     * @param project      the project associated with the topic
     * @param varadhiTopic the VaradhiTopic instance to be deployed
     */
    private void planDeployment(Project project, VaradhiTopic varadhiTopic) {
        StorageTopic storageTopic = topicFactory.getTopic(
            0,
            varadhiTopic.getName(),
            project,
            varadhiTopic.getCapacity(),
            InternalQueueCategory.MAIN
        );

        varadhiTopic.addInternalTopic(deploymentRegion, SegmentedStorageTopic.of(storageTopic));
    }

    private static void warnIfCapacityTightForAverageMessageSizes(
        TopicCapacityPolicy capacity,
        MessageSizeProfile messageSizeProfile
    ) {
        long avgRequiredBytesPerSec = (long)capacity.getQps() * messageSizeProfile.getAvgMsgSizeBytes();
        long actualBytesPerSec = (long)capacity.getThroughputKBps() * 1024L;
        if (actualBytesPerSec < avgRequiredBytesPerSec) {
            log.warn(
                "throughputKBps ({}) is below qps ({}) x avgMsgSizeBytes ({}); capacity may be tight for average message sizes",
                capacity.getThroughputKBps(),
                capacity.getQps(),
                messageSizeProfile.getAvgMsgSizeBytes()
            );
        }
    }

    /**
     * Resolves {@code perRegionQuotaWeights} with partial-map semantics: explicit weights must sum to at most 1,
     * and any unset produce regions receive an even share of the remainder.
     */
    static Map<String, Double> resolvePerRegionQuotaWeights(
        Map<String, Double> explicitWeights,
        Set<String> produceRegions
    ) {
        if (produceRegions.isEmpty()) {
            return Map.of();
        }
        if (explicitWeights == null || explicitWeights.isEmpty()) {
            return evenSplitPerRegionQuotaWeights(produceRegions);
        }

        for (Map.Entry<String, Double> entry : explicitWeights.entrySet()) {
            if (!produceRegions.contains(entry.getKey())) {
                throw new IllegalArgumentException(
                    "Unknown produce region in perRegionQuotaWeights: " + entry.getKey()
                );
            }
            double weight = entry.getValue();
            // Guard before summing: a NaN slips past both the sum and total checks (every NaN
            // comparison is false), and a negative/>1 weight yields an invalid quota downstream.
            if (!Double.isFinite(weight) || weight < 0.0 || weight > 1.0) {
                throw new IllegalArgumentException(
                    "perRegionQuotaWeights value for " + entry.getKey() + " must be in [0, 1]: " + weight
                );
            }
        }

        double explicitSum = explicitWeights.values().stream().mapToDouble(Double::doubleValue).sum();
        if (explicitSum > 1.0 + PER_REGION_WEIGHT_SUM_EPSILON) {
            throw new IllegalArgumentException("perRegionQuotaWeights sum exceeds 1");
        }

        Map<String, Double> resolved = new HashMap<>(explicitWeights);
        Set<String> unsetRegions = produceRegions.stream()
                                                 .filter(region -> !explicitWeights.containsKey(region))
                                                 .collect(Collectors.toSet());

        if (!unsetRegions.isEmpty()) {
            double remaining = 1.0 - explicitSum;
            if (remaining <= PER_REGION_WEIGHT_SUM_EPSILON) {
                throw new IllegalArgumentException("No weight remaining for unset produce regions");
            }
            double shareEach = remaining / unsetRegions.size();
            unsetRegions.forEach(region -> resolved.put(region, shareEach));
        }

        double total = resolved.values().stream().mapToDouble(Double::doubleValue).sum();
        if (Math.abs(total - 1.0) > PER_REGION_WEIGHT_SUM_EPSILON) {
            throw new IllegalArgumentException("perRegionQuotaWeights must sum to 1");
        }
        return resolved;
    }

    private static Map<String, Double> evenSplitPerRegionQuotaWeights(Set<String> produceRegions) {
        double weightEach = 1.0 / produceRegions.size();
        Map<String, Double> weights = new HashMap<>();
        produceRegions.forEach(region -> weights.put(region, weightEach));
        return weights;
    }
}
