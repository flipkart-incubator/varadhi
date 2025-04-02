package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.filters.Condition;
import com.flipkart.varadhi.entities.filters.OrgFilters;
import com.flipkart.varadhi.spi.db.org.OrgMetaStore;

import java.util.List;

public class OrgFilterService {
    private final OrgMetaStore orgFilterOperations;

    public OrgFilterService(OrgMetaStore orgFilterOperations) {
        this.orgFilterOperations = orgFilterOperations;
    }

    public Condition getOrgFilterByName(String orgName, String filterName) {
        return orgFilterOperations.getOrgFilter(orgName, filterName).getFilters().get(filterName);
    }

    public List<OrgFilters> getAllOrgFilters(String orgName) {
        return orgFilterOperations.getOrgFilters(orgName);
    }

    public boolean checkIfOrgFilterExists(String orgName, String filterName) {
        return orgFilterOperations.getOrgFilter(orgName, filterName).getFilters().get(filterName) != null;
    }

    public void updateOrgFilter(String orgName, String filterName, OrgFilters orgFilters) {
        orgFilterOperations.updateOrgFilter(orgName, filterName, orgFilters);
    }

    public OrgFilters createOrgFilter(String orgName, OrgFilters OrgFilter) {
        return orgFilterOperations.createOrgFilter(orgName, OrgFilter);
    }
}
