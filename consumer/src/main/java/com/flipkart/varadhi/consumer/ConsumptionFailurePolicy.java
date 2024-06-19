package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.*;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class ConsumptionFailurePolicy {

    private final RetryPolicy retryPolicy;

    private final RetrySubscription retrySubscription;

    private final InternalCompositeSubscription deadLetterSubscription;
}
