package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.entities.filters.OrgFilters;
import lombok.Getter;

@Getter
public class OrgDetails extends Resource {
    private final Org org;
    private final OrgFilters orgFilters;

    public OrgDetails(Org org, OrgFilters orgFilters) {
        super(org.getName(), org.getVersion());
        this.org = org;
        this.orgFilters = orgFilters;
    }
}
