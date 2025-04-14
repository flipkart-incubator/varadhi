package com.flipkart.varadhi.providers;

import com.flipkart.varadhi.common.EntityProvider;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.spi.db.MetaStore;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;

/**
 * Provider for Project entities with caching capabilities.
 * <p>
 * This class extends the generic {@link EntityProvider} to provide specialized
 * project management functionality. It maintains an in-memory cache of projects
 * that can be preloaded and dynamically updated through entity events.
 * <p>
 * The provider is designed for high-performance, thread-safe access to project
 * information in a distributed environment. It supports:
 * <ul>
 *   <li>Efficient project lookup by name</li>
 *   <li>Preloading projects from the metadata store</li>
 *   <li>Automatic cache updates via entity events</li>
 * </ul>
 *
 * @see EntityProvider
 * @see Project
 */
@Slf4j
public class ProjectProvider extends EntityProvider<Project> {

    /**
     * The metadata store used to load projects.
     */
    private final MetaStore metaStore;

    /**
     * Creates a new ProjectProvider with the specified metadata store.
     *
     * @param metaStore the metadata store to load projects from
     * @throws NullPointerException if metaStore is null
     */
    public ProjectProvider(MetaStore metaStore) {
        super(ResourceType.PROJECT, "project");
        this.metaStore = Objects.requireNonNull(metaStore, "MetaStore cannot be null");
    }

    /**
     * {@inheritDoc}
     * <p>
     * Loads all projects from the metadata store.
     *
     * @return a list of all projects
     */
    @Override
    protected List<Project> loadAllEntities() {
        return metaStore.getAllProjects();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns the name of the given project.
     *
     * @param project the project to get the name for
     * @return the name of the project
     */
    @Override
    protected String getEntityName(Project project) {
        return project.getName();
    }
}
