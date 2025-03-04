package com.flipkart.varadhi.events;

import com.flipkart.varadhi.VaradhiCache;
import com.flipkart.varadhi.core.cluster.EntityEventHandler;
import com.flipkart.varadhi.entities.EntityEvent;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.services.ProjectService;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

@Slf4j
public class ProjectEntityEventHandler implements EntityEventHandler {
    private static final Set<ResourceType> SUPPORTED_TYPES = Collections.singleton(ResourceType.PROJECT);
    private final VaradhiCache<String, Project> projectCache;

    public ProjectEntityEventHandler(ProjectService projectService) {
        Objects.requireNonNull(projectService, "Project service cannot be null");
        this.projectCache = projectService.getProjectCache();
        Objects.requireNonNull(this.projectCache, "Project cache cannot be null");
    }

    @Override
    public void handleEvent(EntityEvent event) {
        Objects.requireNonNull(event, "Event cannot be null");

        if (event.resourceType() != ResourceType.PROJECT) {
            if (log.isDebugEnabled()) {
                log.debug("Skipping non-project event type: {}", event.resourceType());
            }
            return;
        }

        String projectName = event.resourceName();
        try {
            switch (event.operation()) {
                case UPSERT -> handleUpsert(projectName, event);
                case INVALIDATE -> handleInvalidate(projectName);
                default -> log.warn("Unsupported operation {} for project: {}", event.operation(), projectName);
            }
        } catch (Exception e) {
            log.error("Failed to process {} operation for project: {}", event.operation(), projectName, e);
            throw e;
        }
    }

    @Override
    public Set<ResourceType> getSupportedResourceTypes() {
        return SUPPORTED_TYPES;
    }

    private void handleUpsert(String projectName, EntityEvent event) {
        Project project = (Project) event.resourceState();
        Objects.requireNonNull(project, "Project state cannot be null for UPSERT operation");

        projectCache.put(projectName, project);
        log.info("Updated project cache for: {}", projectName);
    }

    private void handleInvalidate(String projectName) {
        projectCache.invalidate(projectName);
        log.info("Removed project from cache: {}", projectName);
    }
}
