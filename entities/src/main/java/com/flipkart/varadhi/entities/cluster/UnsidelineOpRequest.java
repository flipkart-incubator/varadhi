package com.flipkart.varadhi.entities.cluster;

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
