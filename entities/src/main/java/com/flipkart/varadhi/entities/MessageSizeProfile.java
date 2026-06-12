package com.flipkart.varadhi.entities;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Observed average and maximum message sizes for a topic.
 */
@Data
@NoArgsConstructor
public class MessageSizeProfile {
    private int avgMsgSizeBytes;
    private int maxMsgSizeBytes;

    public MessageSizeProfile(int avgMsgSizeBytes, int maxMsgSizeBytes) {
        this.avgMsgSizeBytes = avgMsgSizeBytes;
        this.maxMsgSizeBytes = maxMsgSizeBytes;
    }
}
