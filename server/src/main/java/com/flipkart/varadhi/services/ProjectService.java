package com.flipkart.varadhi.services;

import com.flipkart.varadhi.VaradhiCache;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.spi.db.MetaStore;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;

@Slf4j
public class ProjectService {
    private static final String CACHE_NAME = "project";

    private final MetaStore metaStore;

    @Getter
    private final VaradhiCache<String, Project> projectCache;

    public ProjectService(MetaStore metaStore, MeterRegistry meterRegistry) {
        this.metaStore = Objects.requireNonNull(metaStore, "MetaStore cannot be null");
        this.projectCache = new VaradhiCache<>(CACHE_NAME, meterRegistry);
    }

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

    public Project getProject(String projectName) {
        return metaStore.getProject(projectName);
    }

    public Project getCachedProject(String projectName) {
        return projectCache.get(projectName);
    }

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

    public Future<Void> initializeCache() {
        Promise<Void> promise = Promise.promise();
        try {
            List<Project> projects = metaStore.getAllProjects();
            log.info("Starting project cache initialization with {} projects", projects.size());

            projects.forEach(project -> projectCache.put(project.getName(), project));

            log.info("Successfully initialized project cache with {} entries", projects.size());
            promise.complete();
        } catch (Exception e) {
            log.error("Failed to initialize project cache", e);
            promise.fail(e);
        }
        return promise.future();
    }
}
