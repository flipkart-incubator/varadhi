package com.flipkart.varadhi.core.cluster.entities;

import com.flipkart.varadhi.entities.UnsidelineRequest;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class UnsidelineOpRequest {
    String SubscriptionId;
    String requestedBy;
    //TODO::fix request in request.
    UnsidelineRequest request;
}
