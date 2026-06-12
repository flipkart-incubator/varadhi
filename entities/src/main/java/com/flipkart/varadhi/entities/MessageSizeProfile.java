package com.flipkart.varadhi.entities;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Observed average and maximum message sizes for a topic (VIP-0001 §8).
 */
@Data
@NoArgsConstructor
public class MessageSizeProfile {
    private static final int DEFAULT_MSG_SIZE_BYTES = 1024;

    private int avgMsgSizeBytes;
    private int maxMsgSizeBytes;

    public MessageSizeProfile(int avgMsgSizeBytes, int maxMsgSizeBytes) {
        this.avgMsgSizeBytes = avgMsgSizeBytes;
        this.maxMsgSizeBytes = maxMsgSizeBytes;
    }

    public static MessageSizeProfile getDefault() {
        return new MessageSizeProfile(DEFAULT_MSG_SIZE_BYTES, DEFAULT_MSG_SIZE_BYTES);
    }
}
