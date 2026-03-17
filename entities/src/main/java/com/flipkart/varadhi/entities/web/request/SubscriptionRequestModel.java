package com.flipkart.varadhi.entities.web.request;

import com.flipkart.varadhi.entities.CallbackConfig;
import com.flipkart.varadhi.entities.RetryPolicy;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

/**
 * Subscription request model - base + subscription-specific (consumer) fields.
 */
@Getter
@Setter
public class SubscriptionRequestModel extends com.flipkart.varadhi.entities.web.BaseResource {
    @NotNull
    private String topicName;

    private Integer noOfConsumers;
    private String subscriptionType;
    private String subscriptionVersion;
    private Integer requestTimeoutMilliSeconds;
    private RetryPolicy retryPolicy;
    private String deadLetterQueueName;
    private Map<Integer, Object> retryQueueMetadata;
    private Integer retentionDays;
    private Boolean archivalEnabled;

    private String endPointUrl;
    private String httpMethod;
    private String targetClientId;
    private List<FilterKeyValuePair> filterKeyValueList;

    private String subscriptionMode;
    private CallbackConfig callbackConfig;
    private Map<String, List<String>> targetClientIds;
    private Boolean disableDefaultTargetClientId;
}
