package com.flipkart.varadhi.entities;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class TopicResource extends VaradhiResource {

    private String name;
    private String project;
    private boolean isGrouped;
    private boolean isExclusiveSubscription;
    private CapacityPolicy capacityPolicy;

}
