package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.ValidateVaradhiResource;
import lombok.EqualsAndHashCode;
import lombok.Value;

import static com.flipkart.varadhi.Constants.INITIAL_VERSION;

@Value
@EqualsAndHashCode(callSuper = true)
@ValidateVaradhiResource(message = "Invalid Topic name. Check naming constraints.")
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

    public TopicResource cloneForCreate(String project) {
        return new TopicResource(getName(), INITIAL_VERSION, project, grouped, capacityPolicy);
    }
}
