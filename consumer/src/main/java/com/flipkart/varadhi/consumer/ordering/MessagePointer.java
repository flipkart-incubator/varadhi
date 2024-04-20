package com.flipkart.varadhi.consumer.ordering;

import com.flipkart.varadhi.entities.Offset;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class MessagePointer {
    int mainTopicIdx;
    Offset mainTopicOffset;
    int internalTopicIdx;
    Offset internalTopicOffset;
}
