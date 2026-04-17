package com.flipkart.varadhi.entities.web;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.varadhi.entities.RegionStatus;
import com.flipkart.varadhi.entities.Validatable;

/**
 * Request body for {@code PATCH /v1/regions/:region} — only the status to apply.
 */
@JsonIgnoreProperties (ignoreUnknown = true)
public record RegionStatusUpdateRequest(RegionStatus status) implements Validatable {

    @Override
    public void validate() {
        if (status == null) {
            throw new IllegalArgumentException("status must not be null");
        }
    }
}
