package com.flipkart.varadhi.services;

import com.flipkart.varadhi.common.VaradhiCache;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.spi.db.MetaStore;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.List;
import java.util.function.Function;

public class ProjectService {
    private final MetaStore metaStore;
    private final VaradhiCache<String, Project> projectCache;

    public ProjectService(MetaStore metaStore, String cacheSpec, MeterRegistry meterRegistry) {
        this.metaStore = metaStore;
        this.projectCache = buildProjectCache(cacheSpec, this::getProject, meterRegistry);
    }

    public Project createProject(Project project) {
        boolean orgExists = metaStore.orgOperations().checkOrgExists(project.getOrg());
        if (!orgExists) {
            throw new ResourceNotFoundException(
                String.format(
                    "Org(%s) not found. For Project creation, associated Org and Team should exist.",
                    project.getOrg()
                )
            );
        }
        boolean teamExists = metaStore.teamOperations().checkTeamExists(project.getTeam(), project.getOrg());
        if (!teamExists) {
            throw new ResourceNotFoundException(
                String.format(
                    "Team(%s) not found. For Project creation, associated Org and Team should exist.",
                    project.getTeam()
                )
            );
        }
        metaStore.projectOperations().createProject(project);
        return project;
    }

    public Project getProject(String projectName) {
        return metaStore.projectOperations().getProject(projectName);
    }

    public Project getCachedProject(String projectName) {
        return projectCache.get(projectName);
    }

    public Project updateProject(Project project) {
        Project existingProject = metaStore.projectOperations().getProject(project.getName());
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
        metaStore.projectOperations().updateProject(project);
        return project;
    }

    public void deleteProject(String projectName) {
        validateDelete(projectName);
        metaStore.projectOperations().deleteProject(projectName);
    }

    private void validateDelete(String projectName) {
        ensureNoTopicExist(projectName);
        ensureNoSubscriptionExist(projectName);
    }

    private void ensureNoTopicExist(String projectName) {
        List<String> varadhiTopicNames = metaStore.topicOperations().getTopicNames(projectName);
        if (!varadhiTopicNames.isEmpty()) {
            throw new InvalidOperationForResourceException(
                String.format("Can not delete Project(%s), it has associated entities.", projectName)
            );
        }
    }

    private void ensureNoSubscriptionExist(String projectName) {
        List<String> varadhiSubscriptionNames = metaStore.subscriptionOperations().getSubscriptionNames(projectName);
        if (!varadhiSubscriptionNames.isEmpty()) {
            throw new InvalidOperationForResourceException(
                String.format("Can not delete Project(%s), it has associated subscription entities.", projectName)
            );
        }
    }

    private VaradhiCache<String, Project> buildProjectCache(
        String cacheSpec,
        Function<String, Project> projectProvider,
        MeterRegistry meterRegistry
    ) {
        return new VaradhiCache<>(
            cacheSpec,
            projectProvider,
            (projectName, exception) -> new ResourceNotFoundException(
                String.format("Failed to get project(%s). %s", projectName, exception.getMessage()),
                exception
            ),
            "project",
            meterRegistry
        );
    }
}
