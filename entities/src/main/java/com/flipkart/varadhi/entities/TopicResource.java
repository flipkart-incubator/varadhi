package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.ValidateVaradhiResource;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(callSuper = true)
@ValidateVaradhiResource(message = "Invalid Topic name. Check naming constraints.", max = 64)
public class TopicResource extends VaradhiResource {
    private static final String RESOURCE_TYPE_NAME = "TopicResource";

    String project;
    boolean grouped;
    CapacityPolicy capacityPolicy;

    public TopicResource(
            String name,
            int version,
            String project,
            boolean grouped,
            CapacityPolicy capacityPolicy
    ) {
        super(name, version);
        this.project = project;
        this.grouped = grouped;
        this.capacityPolicy = capacityPolicy;
    }
}
