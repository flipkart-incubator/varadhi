package com.flipkart.varadhi.entities.cluster;

public interface OrderedOperation extends Operation {
    String getOrderingKey();

    int getRetryAttempt();

    OrderedOperation nextRetry();
}
