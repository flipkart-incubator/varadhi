package com.flipkart.varadhi.db;

import com.flipkart.varadhi.common.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.entities.filters.OrgFilters;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import com.flipkart.varadhi.spi.db.org.OrgLevelFilters;

import java.util.List;

public class OrgLevelFiltersImpl implements OrgLevelFilters {
    private final ZKMetaStore zkMetaStore;

    private OrgLevelFiltersImpl(ZKMetaStore zkMetaStore) {
        this.zkMetaStore = zkMetaStore;
    }

    /**
     * Retrieves a named filter by its name within a specified organization.
     *
     * @param orgName    the name of the organization
     * @param filterName the name of the filter
     * @return the named filter with the specified name
     */
    @Override
    public OrgFilters getNamedFilterByName(String orgName, String filterName) {
        ZNode znode = ZNode.ofOrgNamedFilter(orgName, filterName);
        return zkMetaStore.getZNodeDataAsPojo(znode, OrgFilters.class);
    }

    /**
     * Retrieves all named filters within a specified organization.
     *
     * @param orgName the name of the organization
     * @return a list of all named filters within the organization
     */
    @Override
    public List<OrgFilters> getAllNamedFilters(String orgName) {
        ZNode znode = ZNode.ofOrg(orgName);
        return zkMetaStore.listChildren(znode).stream()
                .map(
                        id -> zkMetaStore.getZNodeDataAsPojo(
                                ZNode.ofOrgNamedFilter(orgName, id),
                                OrgFilters.class
                        )
                ).toList();
    }

    /**
     * Checks if a named filter exists within a specified organization.
     *
     * @param orgName    the name of the organization
     * @param filterName the name of the filter
     * @return true if the filter exists, false otherwise
     */
    @Override
    public boolean checkIfNamedFilterExists(String orgName, String filterName) {
        ZNode znode = ZNode.ofTeam(orgName, filterName);
        return zkMetaStore.zkPathExist(znode);
    }

    /**
     * Updates a named filter within a specified organization.
     *
     * @param orgName    the name of the organization
     * @param filterName the name of the filter
     * @return the updated named filter
     */
    @Override
    public void updateNamedFilter(String orgName, String filterName, OrgFilters orgFilters) {
        ZNode znode = ZNode.ofOrgNamedFilter(orgName, filterName);
        zkMetaStore.updateZNodeWithData(znode, orgFilters);
    }

    /**
     * Creates a new named filter in the system.
     *
     * @param namedFilter the named filter to create
     * @throws IllegalArgumentException   if namedFilter is null or invalid
     * @throws DuplicateResourceException if named filter already exists
     * @throws MetaStoreException         if there's an error during creation
     */
    @Override
    public OrgFilters createNamedFilter(String orgName, OrgFilters namedFilter) {
        ZNode znode = ZNode.ofOrgNamedFilter(orgName, namedFilter.getName());
        zkMetaStore.createZNodeWithData(znode, namedFilter);
        return namedFilter;
    }
}
