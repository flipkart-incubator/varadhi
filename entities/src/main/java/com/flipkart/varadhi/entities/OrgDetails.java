package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.entities.filters.OrgFilters;
import lombok.Getter;

@Getter

public class OrgDetails extends MetaStoreEntity {
    Org org;
    OrgFilters orgFilters;

    /**
     * Constructs an OrgDetails instance with the specified organization and filters.
     *
     * @param org the organization entity
     * @param orgFilters the filters associated with the organization
     */
    public OrgDetails(Org org, OrgFilters orgFilters) {
        super(org.getName(), org.getVersion());
        this.org = org;
        this.orgFilters = orgFilters;
    }

    /**
     * Constructs an OrgDetails instance with the specified name and version.
     *
     * @param name the name of the organization
     * @param version the version of the organization details
     */
    protected OrgDetails(String name, int version) {
        super(name, version);
    }
}
