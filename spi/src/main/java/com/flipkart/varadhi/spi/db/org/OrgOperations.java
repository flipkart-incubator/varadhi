package com.flipkart.varadhi.spi.db.org;

import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.filters.OrgFilters;

import java.util.List;

public interface OrgOperations {
    void createOrg(Org org);

    Org getOrg(String orgName);

    List<Org> getOrgs();

    boolean checkOrgExists(String orgName);

    void deleteOrg(String orgName);

    /**
     * Retrieves a named filter by its name within a specified organization.
     *
     * @param orgName    the name of the organization
     * @param filterName the name of the filter
     * @return the named filter with the specified name
     */
    OrgFilters getNamedFilterByName(String orgName, String filterName);

    /**
     * Retrieves all named filters within a specified organization.
     *
     * @param orgName the name of the organization
     * @return a list of all named filters within the organization
     */
    List<OrgFilters> getAllNamedFilters(String orgName);

    /**
     * Checks if a named filter exists within a specified organization.
     *
     * @param orgName    the name of the organization
     * @param filterName the name of the filter
     * @return true if the filter exists, false otherwise
     */
    boolean checkIfNamedFilterExists(String orgName, String filterName);

    /**
     * Updates a named filter within a specified organization.
     *
     * @param orgName    the name of the organization
     * @param filterName the name of the filter
     * @return the updated named filter
     */

    void updateNamedFilter(String orgName, String filterName, OrgFilters orgFilters);

    /**
     * Creates a new named filter within a specified organization.
     *
     * @param orgName     the name of the organization
     * @param namedFilter the named filter to create
     * @return the created named filter
     */
    OrgFilters createNamedFilter(String orgName, OrgFilters namedFilter);
}
