package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.varadhi.entities.filters.OrgFilters;
import lombok.Getter;

@Getter
public class OrgDetails extends Resource {
    private final Org org;
    private final OrgFilters orgFilters;

    @JsonCreator
    public OrgDetails(@JsonProperty ("org") Org org, @JsonProperty ("orgFilters") OrgFilters orgFilters) {
        super(org.getName(), org.getVersion(), ResourceType.ORG);
        this.org = org;
        this.orgFilters = orgFilters;
    }
}
