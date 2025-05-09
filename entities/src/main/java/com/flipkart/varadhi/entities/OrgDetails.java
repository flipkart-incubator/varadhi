package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.entities.filters.OrgFilters;
import lombok.Getter;
@Getter

public class OrgDetails extends MetaStoreEntity {
    Org org;
    OrgFilters orgFilters;

    public OrgDetails(Org org, OrgFilters orgFilters) {
        super(org.getName(), org.getVersion());
        this.org = org;
        this.orgFilters = orgFilters;
    }

    protected OrgDetails(String name, int version) {
        super(name, version);
    }
}
