package com.flipkart.varadhi.web.config;

import com.flipkart.varadhi.common.Constants;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class RestOptions {

    private TopicCapacityPolicy defaultTopicCapacity = Constants.DEFAULT_TOPIC_CAPACITY;
    private int payloadSizeMax = Constants.RestDefaults.PAYLOAD_SIZE_MAX;

    private int unsidelineApiMsgCountMax = 1000;
    private int unsidelineApiGroupCountMax = 100;
    private int unsidelineApiMsgCountDefault = 100;
    private int unsidelineApiGroupCountDefault = 20;

    private int getMessagesApiMessagesLimitMax = 1000;
    private int getMessagesApiMessagesLimitDefault = 100;
}
