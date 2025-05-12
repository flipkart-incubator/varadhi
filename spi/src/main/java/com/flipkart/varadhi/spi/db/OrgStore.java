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

    /**
 * Updates the filter settings for the specified organization.
 *
 * @param orgName the name of the organization whose filters are to be updated
 * @param orgFilters the new filter settings to apply
 */
void updateFilter(String orgName, OrgFilters orgFilters);

    /**
 * Creates and associates a new filter with the specified organization.
 *
 * @param orgName the name of the organization
 * @param namedFilter the filter settings to associate with the organization
 * @return the created filter associated with the organization
 */
OrgFilters createFilter(String orgName, OrgFilters namedFilter);

    /**
 * Retrieves detailed information for all organizations.
 *
 * @return a list of OrgDetails objects representing each organization
 */
List<OrgDetails> getAllOrgDetails();
}
