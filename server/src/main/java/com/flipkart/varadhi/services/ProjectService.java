package com.flipkart.varadhi.services;

import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.spi.db.MetaStore;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;

/**
 * Service for managing projects in Varadhi.
 * <p>
 * This service provides methods for creating, retrieving, updating, and deleting projects.
 */
@Slf4j
public class ProjectService {

    /**
     * The metadata store for persistent storage of projects.
     */
    private final MetaStore metaStore;

    /**
     * Creates a new ProjectService with the specified MetaStore.
     *
     * @param metaStore     the MetaStore for persistent storage
     * @throws NullPointerException if metaStore is null
     */
    public ProjectService(MetaStore metaStore) {
        this.metaStore = Objects.requireNonNull(metaStore, "MetaStore cannot be null");
    }

    /**
     * Creates a new project.
     *
     * @param project the project to create
     * @return the created project
     * @throws ResourceNotFoundException if the org or team does not exist
     */
    public Project createProject(Project project) {
        log.info("Creating project: {}", project.getName());
        validateProjectDependencies(project);

        metaStore.projects().create(project);
        return project;
    }

    /**
     * Validates that the organization and team specified in the project exist.
     *
     * @param project the project to validate
     * @throws ResourceNotFoundException if the org or team does not exist
     */
    private void validateProjectDependencies(Project project) {
        // Check if org exists
        if (!metaStore.orgs().exists(project.getOrg())) {
            throw new ResourceNotFoundException(
                String.format(
                    "Org(%s) not found. For Project creation, associated Org and Team should exist.",
                    project.getOrg()
                )
            );
        }

        // Check if team exists
        if (!metaStore.teams().exists(project.getTeam(), project.getOrg())) {
            throw new ResourceNotFoundException(
                String.format(
                    "Team(%s) not found. For Project creation, associated Org and Team should exist.",
                    project.getTeam()
                )
            );
        }
    }

    /**
     * Retrieves a project by name from the MetaStore.
     *
     * @param projectName the name of the project to retrieve
     * @return the project
     * @throws ResourceNotFoundException if the project does not exist
     */
    public Project getProject(String projectName) {
        return metaStore.projects().get(projectName);
    }

    /**
     * Updates an existing project.
     * <p>
     * This method validates the update operation to ensure it is valid and
     * there are no conflicts.
     *
     * @param project the project with updated information
     * @return the updated project
     * @throws IllegalArgumentException             if the project cannot be updated
     * @throws InvalidOperationForResourceException if there is a version conflict
     * @throws ResourceNotFoundException            if the project does not exist
     */
    public Project updateProject(Project project) {
        log.info("Updating project: {}", project.getName());

        Project existingProject = getProject(project.getName());
        validateProjectUpdate(project, existingProject);

        metaStore.projects().update(project);
        return project;
    }

    /**
     * Validates that a project update operation is valid.
     *
     * @param project         the project with updated information
     * @param existingProject the existing project
     * @throws IllegalArgumentException             if the project cannot be updated
     * @throws InvalidOperationForResourceException if there is a version conflict
     */
    private void validateProjectUpdate(Project project, Project existingProject) {
        // Check if org is being changed
        if (!project.getOrg().equals(existingProject.getOrg())) {
            throw new IllegalArgumentException(
                String.format("Project(%s) cannot be moved across organization.", project.getName())
            );
        }

        // Check if there are any changes
        if (project.getTeam().equals(existingProject.getTeam()) && project.getDescription()
                                                                          .equals(existingProject.getDescription())) {
            throw new IllegalArgumentException(
                String.format("Project(%s) has same team name and description. Nothing to update.", project.getName())
            );
        }

        // Check for version conflict
        if (project.getVersion() != existingProject.getVersion()) {
            throw new InvalidOperationForResourceException(
                String.format(
                    "Conflicting update, Project(%s) has been modified. Fetch latest and try again.",
                    project.getName()
                )
            );
        }
    }

    /**
     * Deletes a project by name after validating that it has no dependencies.
     *
     * @param projectName the name of the project to delete
     * @throws InvalidOperationForResourceException if the project has associated resource
     * @throws ResourceNotFoundException            if the project does not exist
     */
    public void deleteProject(String projectName) {
        log.info("Deleting project: {}", projectName);
        validateProjectDeletion(projectName);

        metaStore.projects().delete(projectName);
    }

    /**
     * Validates that a project can be deleted by checking for dependencies.
     *
     * @param projectName the name of the project to validate
     * @throws InvalidOperationForResourceException if the project has associated resource
     */
    private void validateProjectDeletion(String projectName) {
        // Check for topics
        List<String> topicNames = metaStore.topics().getAllNames(projectName);
        if (!topicNames.isEmpty()) {
            throw new InvalidOperationForResourceException(
                String.format(
                    "Cannot delete Project(%s), it has %d associated topic(s).",
                    projectName,
                    topicNames.size()
                )
            );
        }

        // Check for subscriptions
        List<String> subscriptionNames = metaStore.subscriptions().getAllNames(projectName);
        if (!subscriptionNames.isEmpty()) {
            throw new InvalidOperationForResourceException(
                String.format(
                    "Cannot delete Project(%s), it has %d associated subscription(s).",
                    projectName,
                    subscriptionNames.size()
                )
            );
        }
    }
}
