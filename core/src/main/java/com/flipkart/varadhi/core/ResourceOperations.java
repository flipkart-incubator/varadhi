package com.flipkart.varadhi.core;

import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.ResourceDeletionType;

import java.util.List;

/**
 * Common interface for resource CRUD operations shared by Topic and Queue.
 * <p>
 * Topic and Queue both support create, get, delete, exists, and list; Topic also supports restore.
 * A future Queue implementation can implement this interface (e.g. with a queue view type) and
 * delegate internally to topic + subscription + queue metadata as per queue-modelling-resolution.md.
 * </p>
 *
 * @param <T> the resource entity type (e.g. VaradhiTopic for topics, or a queue view for queues)
 */
public interface ResourceOperations<T> {

    /**
     * Creates the resource.
     *
     * @param resource the resource to create
     * @param project  the project associated with the resource
     */
    void create(T resource, Project project);

    /**
     * Gets the resource by name.
     *
     * @param name the resource name
     * @return the resource
     */
    T get(String name);

    /**
     * Deletes the resource.
     *
     * @param name         the resource name
     * @param deletionType soft or hard delete
     * @param actionRequest action code and message for the deletion
     */
    void delete(String name, ResourceDeletionType deletionType, RequestActionType actionRequest);

    /**
     * Checks if the resource exists.
     *
     * @param name the resource name
     * @return true if it exists
     */
    boolean exists(String name);

    /**
     * Lists resource names for the given project.
     *
     * @param projectName     the project name
     * @param includeInactive whether to include inactive/soft-deleted resources
     * @return list of resource names
     */
    List<String> list(String projectName, boolean includeInactive);

    /**
     * Restores a soft-deleted resource. Optional; not all resource types support restore.
     *
     * @param name          the resource name
     * @param actionRequest action code and message for the restoration
     */
    default void restore(String name, RequestActionType actionRequest) {
        throw new UnsupportedOperationException("restore not supported for this resource type");
    }
}
