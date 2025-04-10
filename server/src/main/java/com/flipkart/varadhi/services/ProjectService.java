package com.flipkart.varadhi.services;

import com.flipkart.varadhi.common.VaradhiCache;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.spi.db.MetaStore;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Service for managing projects in Varadhi.
 * <p>
 * This service provides methods for creating, retrieving, updating, and deleting projects,
 * as well as managing a cache of projects for efficient access.
 */
@Slf4j
public class ProjectService {
    private static final String CACHE_NAME = "project";

    private final MetaStore metaStore;

    @Getter
    private final VaradhiCache<String, Project> projectCache;

    /**
     * Creates a new ProjectService with the specified MetaStore and MeterRegistry.
     *
     * @param metaStore     the MetaStore for persistent storage
     * @param meterRegistry the MeterRegistry for metrics
     * @throws NullPointerException if metaStore or meterRegistry is null
     */
    public ProjectService(MetaStore metaStore, MeterRegistry meterRegistry) {
        this.metaStore = Objects.requireNonNull(metaStore, "MetaStore cannot be null");
        this.projectCache = new VaradhiCache<>(CACHE_NAME, meterRegistry);
    }

    /**
     * Preloads all projects into the cache.
     * <p>
     * This method should be called during system startup to ensure the cache is populated
     * before handling requests.
     *
     * @return a Future that completes when the cache is preloaded
     */
    public Future<Void> preloadCache() {
        Promise<Void> promise = Promise.promise();
        try {
            log.info("Starting to preload project cache");
            List<Project> projects = metaStore.getAllProjects();

            if (projects.isEmpty()) {
                log.info("No projects found to preload");
                promise.complete();
                return promise.future();
            }

            Map<String, Project> projectMap = projects.stream()
                                                      .collect(Collectors.toMap(Project::getName, Function.identity()));

            projectCache.putAll(projectMap);
            log.info("Successfully preloaded {} projects into cache", projects.size());
            promise.complete();
        } catch (Exception e) {
            log.error("Failed to preload project cache", e);
            promise.fail(e);
        }
        return promise.future();
    }

    /**
     * Creates a new project.
     *
     * @param project the project to create
     * @return the created project
     * @throws ResourceNotFoundException if the org or team does not exist
     */
    public Project createProject(Project project) {
        boolean orgExists = metaStore.checkOrgExists(project.getOrg());
        if (!orgExists) {
            throw new ResourceNotFoundException(
                String.format(
                    "Org(%s) not found. For Project creation, associated Org and Team should exist.",
                    project.getOrg()
                )
            );
        }
        boolean teamExists = metaStore.checkTeamExists(project.getTeam(), project.getOrg());
        if (!teamExists) {
            throw new ResourceNotFoundException(
                String.format(
                    "Team(%s) not found. For Project creation, associated Org and Team should exist.",
                    project.getTeam()
                )
            );
        }
        metaStore.createProject(project);
        return project;
    }

    /**
     * Retrieves a project by name from the MetaStore.
     *
     * @param projectName the name of the project to retrieve
     * @return the project
     * @throws ResourceNotFoundException if the project does not exist
     */
    public Project getProject(String projectName) {
        return metaStore.getProject(projectName);
    }

    /**
     * Retrieves a project by name from the cache, or null if not found.
     * <p>
     * This method does not access the MetaStore and will return null if the project
     * is not in the cache.
     *
     * @param projectName the name of the project to retrieve
     * @return the project, or null if not in the cache
     */
    public Project getCachedProject(String projectName) {
        return projectCache.get(projectName);
    }

    /**
     * Retrieves a project by name, first checking the cache and then falling back to the MetaStore.
     * <p>
     * This method provides efficient access to projects by using the cache when possible.
     *
     * @param projectName the name of the project to retrieve
     * @return the project
     * @throws ResourceNotFoundException if the project does not exist
     */
    public Project getProjectWithCache(String projectName) {
        return projectCache.getOrCompute(projectName, this::getProject);
    }

    /**
     * Updates an existing project.
     *
     * @param project the project with updated information
     * @return the updated project
     * @throws IllegalArgumentException             if the project cannot be updated
     * @throws InvalidOperationForResourceException if there is a version conflict
     * @throws ResourceNotFoundException            if the project does not exist
     */
    public Project updateProject(Project project) {
        Project existingProject = metaStore.getProject(project.getName());
        if (!project.getOrg().equals(existingProject.getOrg())) {
            throw new IllegalArgumentException(
                String.format("Project(%s) can not be moved across organisation.", project.getName())
            );
        }
        if (project.getTeam().equals(existingProject.getTeam()) && project.getDescription()
                                                                          .equals(existingProject.getDescription())) {
            throw new IllegalArgumentException(
                String.format("Project(%s) has same team name and description. Nothing to update.", project.getName())
            );
        }
        if (project.getVersion() != existingProject.getVersion()) {
            throw new InvalidOperationForResourceException(
                String.format(
                    "Conflicting update, Project(%s) has been modified. Fetch latest and try again.",
                    project.getName()
                )
            );
        }
        metaStore.updateProject(project);
        return project;
    }

    /**
     * Deletes a project by name.
     *
     * @param projectName the name of the project to delete
     * @throws InvalidOperationForResourceException if the project has associated entities
     * @throws ResourceNotFoundException            if the project does not exist
     */
    public void deleteProject(String projectName) {
        validateDelete(projectName);
        metaStore.deleteProject(projectName);
    }

    private void validateDelete(String projectName) {
        ensureNoTopicExist(projectName);
        ensureNoSubscriptionExist(projectName);
    }

    private void ensureNoTopicExist(String projectName) {
        List<String> varadhiTopicNames = metaStore.getTopicNames(projectName);
        if (!varadhiTopicNames.isEmpty()) {
            throw new InvalidOperationForResourceException(
                String.format("Can not delete Project(%s), it has associated entities.", projectName)
            );
        }
    }

    private void ensureNoSubscriptionExist(String projectName) {
        List<String> varadhiSubscriptionNames = metaStore.getSubscriptionNames(projectName);
        if (!varadhiSubscriptionNames.isEmpty()) {
            throw new InvalidOperationForResourceException(
                String.format("Can not delete Project(%s), it has associated subscription entities.", projectName)
            );
        }
    }
}
