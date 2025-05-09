package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.OrgDetails;
import com.flipkart.varadhi.entities.filters.OrgFilters;

import java.util.List;

public interface OrgStore {
    void create(Org org);

    Org get(String orgName);

    List<Org> getAll();

    boolean exists(String orgName);

    void delete(String orgName);

    OrgFilters getFilter(String orgName);

    void updateFilter(String orgName, OrgFilters orgFilters);

    OrgFilters createFilter(String orgName, OrgFilters namedFilter);

    List<OrgDetails> getAllOrgDetails();
}
