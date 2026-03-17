package com.flipkart.varadhi.entities.web;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;

/**
 * Base resource with common fields. Topic, subscription, and queue resources add their own extra fields.
 */
@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class BaseResource {
    @NotNull
    protected String name;
    protected String project;
    protected Boolean secured;
    protected Boolean grouped;
    protected String appId;
    protected String nfrStrategy;
    protected String team;
}
