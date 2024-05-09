package com.flipkart.varadhi.controller.entities;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Assignment {
    private String subscriptionId;
    private  int shardId;
    private  String consumerId;
}
