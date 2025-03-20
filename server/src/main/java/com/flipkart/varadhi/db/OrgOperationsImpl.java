package com.flipkart.varadhi.db;

import com.flipkart.varadhi.common.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import com.flipkart.varadhi.spi.db.org.OrgOperations;

import java.util.List;

import static com.flipkart.varadhi.db.ZNode.ORG;

public class OrgOperationsImpl implements OrgOperations {
    private final ZKMetaStore zkMetaStore;

    public OrgOperationsImpl(ZKMetaStore zkMetaStore) {
        this.zkMetaStore = zkMetaStore;
    }

    /**
     * Creates a new organization in the system.
     *
     * @param org the organization to create
     * @throws IllegalArgumentException   if org is null or invalid
     * @throws DuplicateResourceException if organization already exists
     * @throws MetaStoreException         if there's an error during creation
     */
    @Override
    public void createOrg(Org org) {
        ZNode znode = ZNode.ofOrg(org.getName());
        zkMetaStore.createZNodeWithData(znode, org);
    }

    /**
     * Retrieves an organization by its name.
     *
     * @param orgName the name of the organization
     * @return the organization entity
     * @throws ResourceNotFoundException if organization doesn't exist
     * @throws MetaStoreException        if there's an error during retrieval
     */
    @Override
    public Org getOrg(String orgName) {
        ZNode znode = ZNode.ofOrg(orgName);
        return zkMetaStore.getZNodeDataAsPojo(znode, Org.class);
    }

    /**
     * Retrieves all organizations.
     *
     * @return list of all organizations
     * @throws MetaStoreException if there's an error during retrieval
     */
    @Override
    public List<Org> getOrgs() {
        ZNode znode = ZNode.ofEntityType(ORG);
        return zkMetaStore.listChildren(znode).stream().map(this::getOrg).toList();
    }

    /**
     * Checks if an organization exists.
     *
     * @param orgName the name of the organization
     * @return true if organization exists, false otherwise
     * @throws MetaStoreException if there's an error checking existence
     */
    @Override
    public boolean checkOrgExists(String orgName) {
        ZNode znode = ZNode.ofOrg(orgName);
        return zkMetaStore.zkPathExist(znode);
    }

    /**
     * Deletes an organization by its name.
     *
     * @param orgName the name of the organization to delete
     * @throws ResourceNotFoundException            if organization doesn't exist
     * @throws InvalidOperationForResourceException if organization has associated teams
     * @throws MetaStoreException                   if there's an error during deletion
     */
    @Override
    public void deleteOrg(String orgName) {
        ZNode znode = ZNode.ofOrg(orgName);
        zkMetaStore.deleteZNode(znode);
    }
}