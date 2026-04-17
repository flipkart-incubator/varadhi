package com.flipkart.varadhi.entities.web;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.varadhi.entities.Region;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.RegionStatus;
import com.flipkart.varadhi.entities.Validatable;

/**
 * Request body for {@code POST /v1/regions}. Clients send only {@code name} and {@code status};
 * version and entity type are assigned by the server when persisting {@link Region}.
 */
@JsonIgnoreProperties (ignoreUnknown = true)
public record RegionCreateRequest(String name, RegionStatus status) implements Validatable {

    @Override
    public void validate() {
        new Region(name, Region.INITIAL_VERSION, status).validate();
    }

    /**
     * Builds an entity suitable for persistence (initial version, correct {@link com.flipkart.varadhi.entities.MetaStoreEntityType}).
     */
    public Region toRegion() {
        validate();
        return Region.of(new RegionName(name), status);
    }
}
