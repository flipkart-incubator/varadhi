package com.flipkart.varadhi.core.subscription;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;


@Getter
@Setter
@AllArgsConstructor
public class ShardStatusRequest {
    private String subscriptionId;
    private int shardId;
}
