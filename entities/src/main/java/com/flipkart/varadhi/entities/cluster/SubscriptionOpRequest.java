package com.flipkart.varadhi.entities.cluster;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SubscriptionOpRequest {
    String SubscriptionId;
    String requestedBy;
}
