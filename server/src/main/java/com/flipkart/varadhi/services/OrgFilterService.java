package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.filters.OrgFilters;
import com.flipkart.varadhi.spi.db.org.OrgOperations;

import java.util.List;

public class OrgFilterService {
    private final OrgOperations orgFilterOperations;

    public OrgFilterService(OrgOperations orgFilterOperations) {
        this.orgFilterOperations = orgFilterOperations;
    }

    public OrgFilters getOrgFilterByName(String orgName, String filterName) {
        return orgFilterOperations.getOrgFilter(orgName, filterName);
    }

    public List<OrgFilters> getAllOrgFilters(String orgName) {
        return orgFilterOperations.getOrgFilters(orgName);
    }

    public boolean checkIfOrgFilterExists(String orgName, String filterName) {
        return orgFilterOperations.checkOrgFilterExists(orgName, filterName);
    }

    public void updateOrgFilter(String orgName, String filterName, OrgFilters orgFilters) {
        orgFilterOperations.updateOrgFilter(orgName, filterName, orgFilters);
    }

    public OrgFilters createOrgFilter(String orgName, OrgFilters OrgFilter) {
        return orgFilterOperations.createOrgFilter(orgName, OrgFilter);
    }
}