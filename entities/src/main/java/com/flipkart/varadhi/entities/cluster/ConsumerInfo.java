package com.flipkart.varadhi.entities.cluster;


import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ConsumerInfo {
    private String consumerId;
    private NodeCapacity available;

    public static ConsumerInfo from(MemberInfo memberInfo) {
        return new ConsumerInfo(
                memberInfo.hostname(), new NodeCapacity(1000, memberInfo.capacity().getNetworkMBps()));
    }
}
