package com.flipkart.varadhi.entities;

import lombok.EqualsAndHashCode;
import lombok.Value;

import javax.validation.ConstraintViolation;
import javax.validation.Valid;
import javax.validation.ValidationException;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Set;
import java.util.stream.Collectors;

@Value
@EqualsAndHashCode(callSuper = true)
@Valid
public class TopicResource extends VaradhiResource {

    private static final String RESOURCE_TYPE_NAME = "TopicResource";

    @Size(min = 5, max = 50, message = "Project Length must be between 5 and 50")
    String project;

    @NotNull
    boolean grouped;

    @NotNull
    boolean exclusiveSubscription;

    CapacityPolicy capacityPolicy;

    //TODO::check if private constructor suffices.
    public TopicResource(
            String name,
            int version,
            String project,
            boolean grouped,
            boolean exclusiveSubscription,
            CapacityPolicy capacityPolicy
    ) {
        super(name, version);
        this.project = project;
        this.grouped = grouped;
        this.exclusiveSubscription = exclusiveSubscription;
        this.capacityPolicy = capacityPolicy;
    }

    public void validate() {
        Set<ConstraintViolation<TopicResource>> violations = super.getValidator().validate(this);
        if (violations.isEmpty()) {
            return;
        }
        throw new ValidationException(violations.stream()
                .map(violation -> violation.getPropertyPath() + ": " + violation.getMessage())
                .collect(Collectors.joining(", ")));
    }
}
