package com.flipkart.varadhi.entities.web;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.flipkart.varadhi.entities.Versioned;
import jakarta.validation.constraints.NotBlank;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * Base resource with common fields; extends Versioned (name, version).
 * TopicResource, SubscriptionResource, and QueueResource extend this.
 * <p>
 * Constraints on {@code project} live on this field so bean validation applies to every subclass without overriding
 * getters (subclass getter overrides do not reliably participate in cascading validation).
 */
@Getter
@Setter
@EqualsAndHashCode (callSuper = true)
@JsonInclude (JsonInclude.Include.NON_NULL)
public abstract class BaseResource extends Versioned {

    @NotBlank (message = "project must not be null or blank")
    protected String project;
    protected Boolean secured;
    protected Boolean grouped;
    protected String appId;
    protected String nfrStrategy;

    /** For subclasses that supply name and version at construction (TopicResource, SubscriptionResource). */
    protected BaseResource(String name, int version) {
        super(name, version);
    }
}
