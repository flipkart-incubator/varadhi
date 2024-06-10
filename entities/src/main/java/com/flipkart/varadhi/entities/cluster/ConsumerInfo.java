package com.flipkart.varadhi.entities.cluster;


import com.flipkart.varadhi.entities.CapacityPolicy;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ConsumerInfo {
    private String consumerId;
    private CapacityPolicy available;

    public static ConsumerInfo from(MemberInfo memberInfo) {
        return new ConsumerInfo(
                memberInfo.hostname(), new CapacityPolicy(1000, memberInfo.capacity().getNetworkMBps()));
    }
}
