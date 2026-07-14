package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;

/**
 * Typed PREPARE target parsed from {@link com.flipkart.varadhi.entities.cluster.failover.TransitionEvent#target()}
 * according to {@link TransitionType}. Keeps string parsing at the handler boundary.
 */
public sealed interface PrepareTarget {

    record RegionTarget(RegionName region) implements PrepareTarget {
        public RegionTarget {
            if (region == null) {
                throw new IllegalArgumentException("region must not be null");
            }
        }
    }

    record StorageTopicTarget(int storageTopicId) implements PrepareTarget {
        public StorageTopicTarget {
            if (storageTopicId < 0) {
                throw new IllegalArgumentException("storageTopicId must be non-negative");
            }
        }
    }

    /**
     * Parses the opaque wire {@code target} for the given transition type.
     *
     * @throws IllegalArgumentException if {@code target} is blank or not valid for {@code type}
     */
    static PrepareTarget parse(TransitionType type, String target) {
        if (type == null) {
            throw new IllegalArgumentException("transition type must not be null");
        }
        if (target == null || target.isBlank()) {
            throw new IllegalArgumentException("PREPARE requires a non-blank target for " + type);
        }
        return switch (type) {
            case TOPIC_FAILOVER -> new RegionTarget(new RegionName(target.trim()));
            case STORAGE_MIGRATION -> {
                try {
                    yield new StorageTopicTarget(Integer.parseInt(target.trim()));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                        "STORAGE_MIGRATION target must be a storage-topic id, got: " + target,
                        e
                    );
                }
            }
        };
    }
}
