package com.flipkart.varadhi.core.subscription;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SubscriptionOpRequest {
    String SubscriptionId;
    String requestedBy;
}
