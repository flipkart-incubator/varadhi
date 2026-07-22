package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;

/**
 * Typed PREPARE target parsed from {@link com.flipkart.varadhi.entities.cluster.failover.TransitionEvent#target()}
 * according to {@link TransitionType}. Keeps string parsing at the handler boundary.
 */
public sealed interface PrepareTarget {

    record RegionTarget(RegionName region) implements PrepareTarget {
    }


    record StorageTopicTarget(int storageTopicId) implements PrepareTarget {
    }

    static PrepareTarget parse(TransitionType type, String target) {
        return switch (type) {
            case TOPIC_FAILOVER -> new RegionTarget(new RegionName(target.trim()));
            case STORAGE_MIGRATION -> new StorageTopicTarget(Integer.parseInt(target.trim()));
        };
    }
}
