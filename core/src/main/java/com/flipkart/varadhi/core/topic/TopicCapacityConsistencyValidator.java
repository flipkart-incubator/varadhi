package com.flipkart.varadhi.core.topic;

import com.flipkart.varadhi.entities.MessageSizeProfile;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates that topic throughput can sustain the configured qps given the message size profile.
 */
final class TopicCapacityConsistencyValidator {

    private static final Logger log = LoggerFactory.getLogger(TopicCapacityConsistencyValidator.class);

    private TopicCapacityConsistencyValidator() {
    }

    static void validate(TopicCapacityPolicy capacity, MessageSizeProfile messageSizeProfile) {
        int maxMsgSizeBytes = messageSizeProfile.getMaxMsgSizeBytes();
        int avgMsgSizeBytes = messageSizeProfile.getAvgMsgSizeBytes();
        long maxRequiredBytesPerSec = (long)capacity.getQps() * maxMsgSizeBytes;
        long actualBytesPerSec = (long)capacity.getThroughputKBps() * 1024L;

        if (actualBytesPerSec < maxRequiredBytesPerSec) {
            throw new IllegalArgumentException(
                String.format(
                    "throughputKBps (%d) is below qps (%d) x maxMsgSizeBytes (%d)",
                    capacity.getThroughputKBps(),
                    capacity.getQps(),
                    maxMsgSizeBytes
                )
            );
        }

        long avgRequiredBytesPerSec = (long)capacity.getQps() * avgMsgSizeBytes;
        if (actualBytesPerSec < avgRequiredBytesPerSec) {
            log.warn(
                "throughputKBps ({}) is below qps ({}) x avgMsgSizeBytes ({}); capacity may be tight for average message sizes",
                capacity.getThroughputKBps(),
                capacity.getQps(),
                avgMsgSizeBytes
            );
        }
    }
}
