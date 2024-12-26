package com.flipkart.varadhi.core.cluster.entities;


import com.flipkart.varadhi.entities.cluster.ShardOperation;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class ShardOpResponse {
    private String subOpId;
    private String shardOpId;
    private ShardOperation.State state;
    private String errorMsg;
}
