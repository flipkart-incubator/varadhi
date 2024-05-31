package com.flipkart.varadhi.consumer.ordering;

import com.flipkart.varadhi.entities.Offset;
import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class MessagePointer implements Comparable<MessagePointer> {
    int mainTopicIdx;
    Offset mainTopicOffset;
    final int internalTopicIdx;
    final Offset internalTopicOffset;

    public MessagePointer(int internalTopicIdx, Offset internalTopicOffset) {
        this.internalTopicIdx = internalTopicIdx;
        this.internalTopicOffset = internalTopicOffset;
    }

    @Override
    public int compareTo(MessagePointer o) {
        if (o == null) {
            return 1;
        }

        if (internalTopicIdx != o.internalTopicIdx) {
            return internalTopicIdx - o.internalTopicIdx;
        }
        return internalTopicOffset.compareTo(o.internalTopicOffset);
    }
}
