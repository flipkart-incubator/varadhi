package com.flipkart.varadhi.spi.db.org;

import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.filters.OrgFilters;

import java.util.List;

public interface OrgMetaStore {
    void createOrg(Org org);

    Org getOrg(String orgName);

    List<Org> getOrgs();

    boolean checkOrgExists(String orgName);

    void deleteOrg(String orgName);

    OrgFilters getOrgFilter(String orgName);

    void updateOrgFilter(String orgName, String filterName, OrgFilters orgFilters);

    OrgFilters createOrgFilter(String orgName, OrgFilters namedFilter);

    void deleteOrgFilter(String orgName);
}
