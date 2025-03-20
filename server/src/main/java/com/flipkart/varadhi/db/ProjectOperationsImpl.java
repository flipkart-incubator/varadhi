package com.flipkart.varadhi.db;

import com.flipkart.varadhi.common.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import com.flipkart.varadhi.spi.db.project.ProjectOperations;

import java.util.List;

import static com.flipkart.varadhi.db.ZNode.PROJECT;

public class ProjectOperationsImpl implements ProjectOperations {
    private final ZKMetaStore zkMetaStore;

    public ProjectOperationsImpl(ZKMetaStore zkMetaStore) {
        this.zkMetaStore = zkMetaStore;
    }

    /**
     * Creates a new project.
     *
     * @param project the project to create
     * @throws IllegalArgumentException   if project is null or invalid
     * @throws DuplicateResourceException if project already exists
     * @throws ResourceNotFoundException  if associated team or organization doesn't exist
     * @throws MetaStoreException         if there's an error during creation
     */
    @Override
    public void createProject(Project project) {
        ZNode znode = ZNode.ofProject(project.getName());
        zkMetaStore.createTrackedZNodeWithData(znode, project, ResourceType.PROJECT);
    }

    /**
     * Retrieves a project by its name.
     *
     * @param projectName the name of the project
     * @return the project entity
     * @throws ResourceNotFoundException if project doesn't exist
     * @throws MetaStoreException        if there's an error during retrieval
     */
    @Override
    public Project getProject(String projectName) {
        ZNode znode = ZNode.ofProject(projectName);
        return zkMetaStore.getZNodeDataAsPojo(znode, Project.class);
    }

    /**
     * Retrieves all projects for a team.
     *
     * @param teamName the name of the team
     * @param orgName  the name of the organization
     * @return list of projects
     * @throws ResourceNotFoundException if team or organization doesn't exist
     * @throws MetaStoreException        if there's an error during retrieval
     */
    @Override
    public List<Project> getProjects(String teamName, String orgName) {
        ZNode znode = ZNode.ofEntityType(PROJECT);
        return zkMetaStore.listChildren(znode)
                .stream()
                .map(this::getProject)
                .filter(project -> matchesTeamAndOrg(project, teamName, orgName))
                .toList();
    }

    /**
     * Checks if a project exists.
     *
     * @param projectName the name of the project
     * @return true if project exists, false otherwise
     * @throws MetaStoreException if there's an error checking existence
     */
    @Override
    public boolean checkProjectExists(String projectName) {
        ZNode znode = ZNode.ofProject(projectName);
        return zkMetaStore.zkPathExist(znode);
    }

    /**
     * Updates an existing project.
     *
     * @param project the project to update
     * @throws ResourceNotFoundException            if project doesn't exist
     * @throws IllegalArgumentException             if project update is invalid or no changes detected
     * @throws InvalidOperationForResourceException if there's a version conflict
     * @throws MetaStoreException                   if there's an error during update
     */
    @Override
    public void updateProject(Project project) {
        ZNode znode = ZNode.ofProject(project.getName());
        zkMetaStore.updateTrackedZNodeWithData(znode, project, ResourceType.PROJECT);
    }

    /**
     * Deletes a project.
     *
     * @param projectName the name of the project to delete
     * @throws ResourceNotFoundException            if project doesn't exist
     * @throws InvalidOperationForResourceException if project has associated topics or subscriptions
     * @throws MetaStoreException                   if there's an error during deletion
     */
    @Override
    public void deleteProject(String projectName) {
        ZNode znode = ZNode.ofProject(projectName);
        zkMetaStore.deleteTrackedZNode(znode, ResourceType.PROJECT);
    }

    /**
     * Checks if a project belongs to the specified team and organization.
     *
     * @param project  the project to check
     * @param teamName the team name to match
     * @param orgName  the organization name to match
     * @return true if the project belongs to the specified team and organization
     */
    private boolean matchesTeamAndOrg(Project project, String teamName, String orgName) {
        return project.getTeam().equals(teamName) && project.getOrg().equals(orgName);
    }
}