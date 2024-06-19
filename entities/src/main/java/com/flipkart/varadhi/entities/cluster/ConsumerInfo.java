package com.flipkart.varadhi.entities.cluster;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ConsumerInfo {
    // Consumer Info as maintained by the consumer node itself.
    private String consumerId;
    private NodeCapacity available;

    public static ConsumerInfo from(MemberInfo memberInfo) {
        return new ConsumerInfo(memberInfo.hostname(), memberInfo.provisionedCapacity().clone());
    }
}
