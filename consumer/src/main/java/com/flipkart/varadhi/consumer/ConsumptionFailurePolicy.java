package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.RetryPolicy;
import com.flipkart.varadhi.entities.StorageTopic;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class ConsumptionFailurePolicy {

    private final RetryPolicy retryPolicy;

    private final StorageRetryTopic retryTopic;

    private final StorageTopic deadLetterTopic;
}
