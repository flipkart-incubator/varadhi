package com.flipkart.varadhi.entities;

import lombok.Data;

@Data
public class ConsumptionPolicy {

    private final int maxParallelism;

    /**
     * Proportion of the parallelism that is allocated for delivery of failed messages.
     */
    private final float maxRecoveryAllocation;

    /**
     * true if recovering dlt is preferred over retryable errors.
     */
    private final boolean dltRecoveryPreferred;

    /**
     * In terms of proportion of consumption rate over some time window. Between 0 and 1.
     */
    private final float maxErrorThreshold;

    private final ThrottlePolicy throttlePolicy;
}
