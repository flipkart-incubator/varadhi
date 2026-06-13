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
        this.avgMsgSizeBytes = avgMsgSizeBytes;
        this.maxMsgSizeBytes = maxMsgSizeBytes;
    }
}
