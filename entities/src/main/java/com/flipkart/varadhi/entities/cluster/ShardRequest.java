package com.flipkart.varadhi.entities.cluster;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;


@Getter
@Setter
@AllArgsConstructor
public class ShardRequest {
    private String subscriptionId;
    private int shardId;
}
