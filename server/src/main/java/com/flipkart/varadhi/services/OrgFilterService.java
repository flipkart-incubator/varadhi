package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.filters.OrgFilters;
import com.flipkart.varadhi.spi.db.org.OrgOperations;

import java.util.List;

public class OrgFilterService {
    private final OrgOperations orgFilterOperations;

    public OrgFilterService(OrgOperations orgFilterOperations) {
        this.orgFilterOperations = orgFilterOperations;
    }

    public List<OrgFilters> getAllGlobalFilters(String orgName) {
        return orgFilterOperations.getAllNamedFilters(orgName);
    }

    public OrgFilters getNamedFilterByName(String orgName, String filterName) {
        return orgFilterOperations.getNamedFilterByName(orgName, filterName);
    }

    public List<OrgFilters> getAllNamedFilters(String orgName) {
        return orgFilterOperations.getAllNamedFilters(orgName);
    }

    public boolean checkIfNamedFilterExists(String orgName, String filterName) {
        return orgFilterOperations.checkIfNamedFilterExists(orgName, filterName);
    }

    public void updateNamedFilter(String orgName, String filterName, OrgFilters orgFilters) {
        orgFilterOperations.updateNamedFilter(orgName, filterName, orgFilters);
    }

    public OrgFilters createNamedFilter(String orgName, OrgFilters namedFilter) {
        return orgFilterOperations.createNamedFilter(orgName, namedFilter);
    }

    public void replaceGlobalFilters(String orgName, List<OrgFilters> filters) {
        // Implementation to replace all global filters for the given organization
    }

    public void addOrUpdateGlobalFilter(String orgName, OrgFilters filter) {
        // Implementation to add or update a global filter for the given organization
    }

    public void updateGlobalFilter(String orgName, OrgFilters filter) {
        // Implementation to update a global filter for the given organization
    }

    public void deleteGlobalFilters(String orgName) {
        // Implementation to delete all global filters for the given organization
    }
}