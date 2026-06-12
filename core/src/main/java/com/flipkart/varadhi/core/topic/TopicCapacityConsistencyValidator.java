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
        int minMsgSizeBytes = messageSizeProfile.getMaxMsgSizeBytes();
        int avgMsgSizeBytes = messageSizeProfile.getAvgMsgSizeBytes();
        long minRequiredBytesPerSec = (long)capacity.getQps() * minMsgSizeBytes;
        long actualBytesPerSec = (long)capacity.getThroughputKBps() * 1024L;

        if (actualBytesPerSec < minRequiredBytesPerSec) {
            throw new IllegalArgumentException(
                String.format(
                    "throughputKBps (%d) is below qps (%d) x maxMsgSizeBytes (%d)",
                    capacity.getThroughputKBps(),
                    capacity.getQps(),
                    minMsgSizeBytes
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
