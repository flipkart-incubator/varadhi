package com.flipkart.varadhi.entities;

import lombok.Getter;

/**
 * Observed average and maximum message sizes for a topic.
 */
@Getter
public class MessageSizeProfile {
    private final int avgMsgSizeBytes;
    private final int maxMsgSizeBytes;

    public MessageSizeProfile(int avgMsgSizeBytes, int maxMsgSizeBytes) {
        // The average can never exceed the observed maximum. Clamp rather than reject so a noisy
        // size sample never fails topic creation (fail-open, consistent with the guard-rail stance).
        this.avgMsgSizeBytes = Math.min(avgMsgSizeBytes, maxMsgSizeBytes);
        this.maxMsgSizeBytes = maxMsgSizeBytes;
    }
}
