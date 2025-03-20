package com.flipkart.varadhi.db;

import com.flipkart.varadhi.common.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import com.flipkart.varadhi.spi.db.team.TeamOperations;

import java.util.List;

import static com.flipkart.varadhi.db.ZNode.RESOURCE_NAME_SEPARATOR;
import static com.flipkart.varadhi.db.ZNode.TEAM;

public class TeamOperationsImpl implements TeamOperations {
    private final ZKMetaStore zkMetaStore;

    public TeamOperationsImpl(ZKMetaStore zkMetaStore) {
        this.zkMetaStore = zkMetaStore;
    }

    /**
     * Creates a new team.
     *
     * @param team the team to create
     * @throws IllegalArgumentException   if team is null or invalid
     * @throws DuplicateResourceException if team already exists
     * @throws ResourceNotFoundException  if associated organization doesn't exist
     * @throws MetaStoreException         if there's an error during creation
     */
    @Override
    public void createTeam(Team team) {
        ZNode znode = ZNode.ofTeam(team.getOrg(), team.getName());
        zkMetaStore.createZNodeWithData(znode, team);
    }

    /**
     * Retrieves a team by its name and organization.
     *
     * @param teamName the name of the team
     * @param orgName  the name of the organization
     * @return the team entity
     * @throws ResourceNotFoundException if team or organization doesn't exist
     * @throws MetaStoreException        if there's an error during retrieval
     */
    @Override
    public Team getTeam(String teamName, String orgName) {
        ZNode znode = ZNode.ofTeam(orgName, teamName);
        return zkMetaStore.getZNodeDataAsPojo(znode, Team.class);
    }

    /**
     * Retrieves all teams in an organization.
     *
     * @param orgName the organization name
     * @return list of teams
     * @throws ResourceNotFoundException if organization doesn't exist
     * @throws MetaStoreException        if there's an error during retrieval
     */
    @Override
    public List<Team> getTeams(String orgName) {
        return getTeamNames(orgName).stream().map(teamName -> getTeam(teamName, orgName)).toList();
    }

    /**
     * Retrieves team names for an organization.
     *
     * @param orgName the organization name
     * @return list of team names
     * @throws ResourceNotFoundException if organization doesn't exist
     * @throws MetaStoreException        if there's an error during retrieval
     */
    @Override
    public List<String> getTeamNames(String orgName) {
        String orgPrefix = orgName + RESOURCE_NAME_SEPARATOR;
        ZNode znode = ZNode.ofEntityType(TEAM);

        return zkMetaStore.listChildren(znode)
                .stream()
                .filter(teamName -> teamName.startsWith(orgPrefix))
                .map(teamName -> teamName.split(RESOURCE_NAME_SEPARATOR)[1])
                .toList();
    }

    /**
     * Checks if a team exists in an organization.
     *
     * @param teamName the name of the team
     * @param orgName  the name of the organization
     * @return true if team exists, false otherwise
     * @throws MetaStoreException if there's an error checking existence
     */
    @Override
    public boolean checkTeamExists(String teamName, String orgName) {
        ZNode znode = ZNode.ofTeam(orgName, teamName);
        return zkMetaStore.zkPathExist(znode);
    }

    /**
     * Deletes a team from an organization.
     *
     * @param teamName the name of the team
     * @param orgName  the name of the organization
     * @throws ResourceNotFoundException            if team or organization doesn't exist
     * @throws InvalidOperationForResourceException if team has associated projects
     * @throws MetaStoreException                   if there's an error during deletion
     */
    @Override
    public void deleteTeam(String teamName, String orgName) {
        ZNode znode = ZNode.ofTeam(orgName, teamName);
        zkMetaStore.deleteZNode(znode);
    }
}
